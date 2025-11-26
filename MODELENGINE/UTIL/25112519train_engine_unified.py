
# ============================================================
# train_engine_unified_V34_complete_fixed.py
#  - REAL/RESEARCH 동시 학습 지원 (mode=all 기본)
#  - REAL: 전체 기간 학습 (검증 분리 없음)
#  - RESEARCH: 최근 valid_days 검증, 나머지 학습
#  - 저장 규칙: HOJ_ENGINE_{MODE}_{DBVER}_dYYMMDD_h{h}_w{Full|N}_n{N}_tYYMMDD.pkl
#  - input_window=0 -> wFull, >0 -> w{input_window}
#  - 파일/주석/로직 최대한 보존하면서 필수 수정만 반영
# ============================================================

import os
import numpy as np
np.random.seed(42)
import sys
import re
INT_PAT = re.compile(r"\d+")
import pickle
import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

# ------------------------------------------------------------
# 0) 경로 유틸 (프로젝트 표준 경로 우선 시도 → 폴백)
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
modelengine_dir = os.path.dirname(current_dir)   # .../MODELENGINE/UTIL → .../MODELENGINE
root_dir = os.path.dirname(modelengine_dir)      # 프로젝트 루트
sys.path.extend([root_dir, modelengine_dir])

def _try_import_paths():
    HOJ_DB, HOJ_ENGINE_REAL, HOJ_ENGINE_RESEARCH, OUTPUT = None, None, None, None
    try:
        from MODELENGINE.UTIL.config_paths import get_path
        from MODELENGINE.UTIL.version_utils import find_latest_file
        def _get(purpose, *args): return get_path(purpose, *args)
        _find = find_latest_file
    except Exception:
        def _get(purpose, *args):
            base = os.path.join(root_dir, "MODELENGINE")
            if purpose == "HOJ_DB":
                return os.path.join(base, "HOJ_DB")
            if purpose == "HOJ_ENGINE":
                return os.path.join(base, "HOJ_ENGINE")
            if purpose == "OUTPUT":
                return os.path.join(base, "OUTPUT")
            return os.path.join(base, "HOJ_ENGINE", args[0]) if args else os.path.join(base, "HOJ_ENGINE")
        def _find(folder, prefix):
            if not os.path.isdir(folder):
                return None
            cand = [os.path.join(folder, f) for f in os.listdir(folder) if f.startswith(prefix)]
            return sorted(cand)[-1] if cand else None
    return _get, _find

get_path, find_latest_file = _try_import_paths()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
    return path

# ------------------------------------------------------------
# 한 줄 갱신형 LightGBM 로그 콜백
# ------------------------------------------------------------
def single_line_logger(period=50):
    def _callback(env):
        if period > 0 and env.iteration % period == 0 and env.evaluation_result_list:
            name, metric, value, _ = env.evaluation_result_list[0]
            print("\r" + f"[{env.iteration}] {name}-{metric}: {value:.6f}", end="", flush=True)
        if hasattr(env, "end_iteration") and env.iteration == env.end_iteration - 1:
            print()
    _callback.order = 10
    return _callback

# ------------------------------------------------------------
# 1) 데이터 로드/피처 선택
# ------------------------------------------------------------
NUM_DTYPES = ("int", "uint", "float", "double")

NON_FEATURE_CANDIDATES = {
    "Date","date",
    "Code","code",
    "Name","name",
    "Open","High","Low","Close","ClosePrice","Adj Close","AdjClose","Volume","Amount",
    "open","high","low","close","volume","amount",
}

def pick_close_column(df: pd.DataFrame) -> str:
    for c in ["Close","ClosePrice","Adj Close","AdjClose","close"]:
        if c in df.columns:
            return c
    raise KeyError("Close/ClosePrice/Adj Close 컬럼을 찾지 못했습니다.")

def load_latest_db(version: str = "V31") -> pd.DataFrame:
    base_dir = get_path("HOJ_DB")
    if "REAL" in base_dir or "RESEARCH" in base_dir:
        base_dir = os.path.dirname(base_dir)
    latest = find_latest_file(base_dir, f"HOJ_DB_{version}")
    if latest is None:
        cand = os.path.join(base_dir, f"HOJ_DB_{version}.parquet")
        if not os.path.exists(cand):
            raise FileNotFoundError(f"HOJ_DB 파일을 찾을 수 없음: {base_dir} (prefix=HOJ_DB_{version})")
        return pd.read_parquet(cand)
    return pd.read_parquet(latest)

def select_feature_columns(df):
    drop_cols = [
        'Date','Code','Open','High','Low','Close','Volume',
        'Return_1d','Return_5d','Label_1d','Label_5d'
    ]
    feats = []
    for col in df.columns:
        if col in drop_cols:
            continue
        if str(df[col].dtype).startswith(("float","int")):
            feats.append(col)
    return feats

def feature_period(col: str) -> int:
    m = [int(x) for x in INT_PAT.findall(col)]
    return max(m) if m else 0

def apply_A_mask(df: pd.DataFrame, features: list, input_window: int, close_col: str, horizon: int) -> pd.DataFrame:
    if input_window and input_window > 0:
        feats = []
        for c in features:
            p = feature_period(c)
            if (p == 0) or (p <= input_window):
                feats.append(c)
        features = feats

    max_period = max([feature_period(c) for c in features] + [0])

    parts = []
    for code, g in df.groupby("Code", sort=False):
        g = g.sort_values("Date")
        if max_period > 0:
            g = g.iloc[max_period:].copy()
        if horizon > 0:
            g = g.iloc[:-horizon] if len(g) > horizon else g.iloc[0:0]
        parts.append(g)
    df_m = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()

    df_m["TargetRet"] = df_m.groupby("Code")[close_col].shift(-horizon) / df_m[close_col] - 1.0
    df_m["TargetUp"] = (df_m["TargetRet"] > 0).astype("int8")

    use_cols = ["Date","Code", close_col] + features + ["TargetRet","TargetUp"]
    df_m = df_m[use_cols].dropna(subset=features + ["TargetRet"])
    return df_m, features, max_period

# ------------------------------------------------------------
# 3) 학습/검증 스플릿 & 모델 학습
# ------------------------------------------------------------
def split_train_valid(df: pd.DataFrame, valid_days: int) -> tuple:
    max_day = pd.to_datetime(df["Date"].max()).normalize()
    valid_start = max_day - timedelta(days=int(valid_days))
    is_valid = df["Date"] >= valid_start
    train = df.loc[~is_valid].copy()
    valid = df.loc[ is_valid].copy()
    return train, valid, valid_start.date(), max_day.date()

def train_models(df_m: pd.DataFrame, features: list, n_estimators: int = 1000):
    # is_train 컬럼 유무/크기에 따라 eval_set 구성 결정
    if "is_train" in df_m.columns:
        X_tr = df_m.loc[df_m["is_train"], features]
        y_reg_tr = df_m.loc[df_m["is_train"], "TargetRet"]
        y_cls_tr = df_m.loc[df_m["is_train"], "TargetUp"]
        X_va = df_m.loc[~df_m["is_train"], features]
        y_reg_va = df_m.loc[~df_m["is_train"], "TargetRet"]
        y_cls_va = df_m.loc[~df_m["is_train"], "TargetUp"]
        has_valid = len(X_va) > 0
    else:
        X_tr = df_m[features]
        y_reg_tr = df_m["TargetRet"]
        y_cls_tr = df_m["TargetUp"]
        has_valid = False

    # LightGBM Regressor
    model_reg = lgb.LGBMRegressor(
        n_estimators=n_estimators,
        max_depth=-1,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="regression",
        n_jobs=-1
    )
    if has_valid:
        model_reg.fit(X_tr, y_reg_tr,
                      eval_set=[(X_va, y_reg_va)],
                      eval_metric="rmse",
                      callbacks=[single_line_logger(period=50)])
    else:
        model_reg.fit(X_tr, y_reg_tr)

    # LightGBM Classifier
    model_cls = lgb.LGBMClassifier(
        n_estimators=n_estimators,
        max_depth=-1,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary",
        n_jobs=-1
    )
    if has_valid:
        model_cls.fit(X_tr, y_cls_tr,
                      eval_set=[(X_va, y_cls_va)],
                      eval_metric="auc",
                      callbacks=[single_line_logger(period=50)])
    else:
        model_cls.fit(X_tr, y_cls_tr)

    return model_reg, model_cls

# ------------------------------------------------------------
# 4) 저장 (파일명 규칙 반영)
# ------------------------------------------------------------
def _hash_list(lst):
    import hashlib
    s = "|".join(map(str, lst)).encode('utf-8')
    return hashlib.md5(s).hexdigest()

def _format_tags(db_version: str, data_date: str, horizon: int, input_window: int, n_estimators: int) -> str:
    d_tag = pd.to_datetime(data_date).strftime("%y%m%d")  # dYYMMDD
    w_tag = "wFull" if int(input_window) == 0 else f"w{int(input_window)}"
    n_tag = f"n{int(n_estimators)}"
    h_tag = f"h{int(horizon)}"
    t_tag = "t" + datetime.now().strftime("%y%m%d")
    return f"{db_version}_d{d_tag}_{h_tag}_{w_tag}_{n_tag}_{t_tag}"

def save_engine(payload: dict, mode: str):
    base = get_path("HOJ_ENGINE")
    if os.path.isfile(base):
        base = os.path.dirname(base)
    out_dir = os.path.join(base, mode.upper())
    ensure_dir(out_dir)

    tags = _format_tags(
        db_version   = payload["meta"]["db_version"],
        data_date    = payload["meta"]["data_date"],
        horizon      = payload["meta"]["horizon"],
        input_window = payload["meta"]["input_window"],
        n_estimators = payload["meta"]["n_estimators"],
    )
    fname = f"HOJ_ENGINE_{mode.upper()}_{tags}.pkl"
    path = os.path.join(out_dir, fname)
    with open(path, "wb") as f:
        pickle.dump(payload, f)
    print(f"\n💾 엔진 저장 완료: {path}")

# ------------------------------------------------------------
# 5) 메인 러너
# ------------------------------------------------------------
def run_unified_training(
    mode: str = "all",
    horizon: int = 5,
    input_window: int = 0,
    valid_days: int = 365,
    n_estimators: int = 1000,
    version: str = "V31",
):
    assert mode in ("real","research","all")

    print("=== 🚀 Unified HOJ Trainer V34 ===")
    print(f"[CFG] mode={mode}  horizon={horizon}  input_window={input_window}  valid_days={valid_days}  n_estimators={n_estimators}")

    # 1) DB 로드
    df = load_latest_db(version)
    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    close_col = pick_close_column(df)
    max_date = df["Date"].max().date()
    print(f"[DATA] DB max(Date) = {max_date}  | rows={len(df):,}")

    # 2) 피처 선택
    features = select_feature_columns(df)
    print(f"[FEAT] 후보 피처 수 = {len(features)}")

    # 3) A안 마스크 + Horizon Tail + 타겟
    df_m, features, max_period = apply_A_mask(df, features, input_window, close_col, horizon)
    print(f"[MASK] MaxPeriod={max_period}d  | After Mask rows={len(df_m):,}")

    # 공통 메타
    meta_common = {
        "version": "V34",
        "db_version": version,        # ← 저장 규칙에 사용
        "data_date": str(max_date),
        "horizon": int(horizon),
        "input_window": int(input_window),
        "valid_days": int(valid_days),
        "feature_hash": _hash_list(features),
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "close_col": close_col,
        "n_estimators": int(n_estimators),
    }

    def _train_and_save(mode_one: str):
        if mode_one == "research":
            tr, va, valid_start, valid_end = split_train_valid(df_m, valid_days)
            tr["is_train"] = True
            va["is_train"] = False
            data = pd.concat([tr, va], ignore_index=True)
            print(f"[SPLIT] Train rows={len(tr):,}  Valid rows={len(va):,}  (valid={valid_start}~{valid_end})")
        else:  # real
            data = df_m.copy()
            data["is_train"] = True
            print(f"[SPLIT] REAL 모드: 전체 {len(data):,}행 학습, 검증 없음")

        model_reg, model_cls = train_models(data, features, n_estimators=n_estimators)
        print(f"[TRAIN] {mode_one.upper()} LightGBM reg/cls 학습 완료")

        payload = {
            "model_reg": model_reg,
            "model_cls": model_cls,
            "features": features,
            "meta": meta_common,
        }
        save_engine(payload, mode_one)

    if mode == "all":
        for m in ("research","real"):
            print(f"\n🚀 [{m.upper()}] 엔진 학습 시작")
            _train_and_save(m)
    else:
        _train_and_save(mode)

    print("=== 🏁 Done. ===")

# ------------------------------------------------------------
# 6) CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=["real","research","all"])
    ap.add_argument("--horizon", type=int, default=5)
    ap.add_argument("--input_window", type=int, default=0)
    ap.add_argument("--valid_days", type=int, default=365)
    ap.add_argument("--n_estimators", type=int, default=1000)
    ap.add_argument("--version", default="V31")
    args = ap.parse_args()

    try:
        run_unified_training(
            mode=args.mode,
            horizon=args.horizon,
            input_window=args.input_window,
            valid_days=args.valid_days,
            n_estimators=args.n_estimators,
            version=args.version,
        )
    except Exception as e:
        print(f"\n❌ [Error] {e}")
