# ============================================================
# train_engine_unified_V31_complete.py
#  - 통합 DB 하나(HOJ_DB_V31.*)로 REAL/RESEARCH 모두 학습
#  - A안: 선택 피처의 '최대 기간'만큼 각 종목 앞구간 제거(초기 오염 0%)
#  - Horizon Tail: 각 종목 뒤에서 h일 제거(미래정보 누수 0%)
#  - input_window: 0이면 전체 피처 사용, >0이면 기간 초과 지표 제외
#  - Close/ClosePrice 자동 인식으로 타겟 생성
#  - meta 저장: feature_hash, data_date, horizon, input_window, valid_days 등
#  - 저장 규칙: MODELENGINE/HOJ_ENGINE/{REAL|RESEARCH}/HOJ_ENGINE_{MODE}_YYYYMMDD_w{input_window}.pkl
#  - [추가] 실행 시 Research -> Real 순차 자동 실행 지원
# ============================================================

import os
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
    """
    프로젝트 표준 유틸 우선 사용.
    (없으면 폴백: 기본 폴더 레이아웃 추정)
    """
    HOJ_DB, HOJ_ENGINE_REAL, HOJ_ENGINE_RESEARCH, OUTPUT = None, None, None, None
    try:
        from MODELENGINE.UTIL.config_paths import get_path
        from MODELENGINE.UTIL.version_utils import find_latest_file
        def _get(purpose, *args): return get_path(purpose, *args)
        _find = find_latest_file
    except Exception:
        # 폴백: 기본 폴더 레이아웃 추정
        def _get(purpose, *args):
            base = os.path.join(root_dir, "MODELENGINE")
            if purpose == "HOJ_DB":
                return os.path.join(base, "HOJ_DB")
            if purpose == "HOJ_ENGINE":
                return os.path.join(base, "HOJ_ENGINE")
            if purpose == "OUTPUT":
                return os.path.join(base, "OUTPUT")
            # 하위 REAL/RESEARCH
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
# 추가: 한 줄 갱신형 LightGBM 로그 콜백
# ------------------------------------------------------------
def single_line_logger(period=50):
    def _callback(env):
        if period > 0 and env.iteration % period == 0:
            # env.evaluation_result_list 예: [('valid', 'rmse', 0.0234, False)]
            if env.evaluation_result_list:
                name, metric, value, _ = env.evaluation_result_list[0]
                msg = f"[{env.iteration}] {name}-{metric}: {value:.6f}"
                print("\r" + msg, end="", flush=True)
            # 마지막이면 줄바꿈
            if env.iteration == env.end_iteration - 1:
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
    "KOSPI_Close", "KOSPI_Change" # [추가] KOSPI 단순 컬럼은 피처후보에서 제외
}

def pick_close_column(df: pd.DataFrame) -> str:
    """Close/ClosePrice/Adj Close 자동 선택."""
    for c in ["Close","ClosePrice","Adj Close","AdjClose","close"]:
        if c in df.columns:
            return c
    raise KeyError("Close/ClosePrice/Adj Close 컬럼을 찾지 못했습니다.")


def load_latest_db(version: str = "V31") -> pd.DataFrame:
    """HOJ_DB 경로에서 최신 V31 파일을 찾아 로드."""
    base_dir = get_path("HOJ_DB")
    if "REAL" in base_dir or "RESEARCH" in base_dir:
        base_dir = os.path.dirname(base_dir)
    latest = find_latest_file(base_dir, f"HOJ_DB_{version}")
    if latest is None:
        # 기본 이름 폴백
        cand = os.path.join(base_dir, f"HOJ_DB_{version}.parquet")
        if not os.path.exists(cand):
            raise FileNotFoundError(
                f"HOJ_DB 파일을 찾을 수 없음: {base_dir} (prefix=HOJ_DB_{version})"
            )
        return pd.read_parquet(cand)

    return pd.read_parquet(latest)

def select_feature_columns(df):
    # [수정] 피처 제외 리스트 보강
    drop_cols = [
        'Date','Code','Open','High','Low','Close','Volume',
        'Return_1d','Return_5d','Label_1d','Label_5d',
        'KOSPI_Close', 'KOSPI_Change' 
    ]
    feats = []
    for col in df.columns:
        if col in drop_cols:
            continue
        if str(df[col].dtype).startswith(("float","int")):
            feats.append(col)
    return feats

def feature_period(col: str) -> int:
    """
    피처명에서 최대 숫자를 기간으로 추출 (예: sma120 → 120, macd_12_26 → 26).
    숫자가 없으면 0으로 간주.
    """
    m = [int(x) for x in INT_PAT.findall(col)]
    return max(m) if m else 0

def apply_A_mask(df: pd.DataFrame, features: list, input_window: int, close_col: str, horizon: int) -> pd.DataFrame:
    """
    A안: 선택 피처들의 '최대 기간'만큼 각 종목 앞구간 제거 + 각 종목 뒤 h일 제거.
    """
    # input_window>0이면 해당 창을 초과하는 기간의 지표 제외
    if input_window and input_window > 0:
        feats = []
        for c in features:
            p = feature_period(c)
            if (p == 0) or (p <= input_window):
                feats.append(c)
        features = feats

    # 최대 기간 계산
    max_period = max([feature_period(c) for c in features] + [0])

    # 종목별 앞/뒤 자르기
    parts = []
    for code, g in df.groupby("Code", sort=False):
        g = g.sort_values("Date")
        # 앞쪽 제거
        if max_period > 0:
            g = g.iloc[max_period:].copy()
        # 뒤쪽 Tail 제거
        if horizon > 0:
            g = g.iloc[:-horizon] if len(g) > horizon else g.iloc[0:0]
        parts.append(g)
    df_m = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()

    # 타겟 생성 (미래 h일 수익률)
    df_m["TargetRet"] = (
        df_m.groupby("Code")[close_col].shift(-horizon) / df_m[close_col] - 1.0
    )
    df_m["TargetUp"] = (df_m["TargetRet"] > 0).astype("int8")

    # 학습에 사용할 컬럼만 남기고 결측 제거
    use_cols = ["Date","Code", close_col] + features + ["TargetRet","TargetUp"]
    df_m = df_m[use_cols].dropna(subset=features + ["TargetRet"])
    return df_m, features, max_period

# ------------------------------------------------------------
# 3) 학습/검증 스플릿 & 모델 학습
# ------------------------------------------------------------
def split_train_valid(df: pd.DataFrame, valid_days: int) -> tuple:
    max_day = df["Date"].max().normalize()
    valid_start = max_day - timedelta(days=int(valid_days))
    is_valid = df["Date"] >= valid_start
    train = df.loc[~is_valid].copy()
    valid = df.loc[ is_valid].copy()
    return train, valid, valid_start.date(), max_day.date()

def train_models(df_m: pd.DataFrame, features: list, n_estimators: int = 1000):
    X_tr = df_m.loc[df_m["is_train"], features]
    y_reg_tr = df_m.loc[df_m["is_train"], "TargetRet"]
    y_cls_tr = df_m.loc[df_m["is_train"], "TargetUp"]

    X_va = df_m.loc[~df_m["is_train"], features]
    y_reg_va = df_m.loc[~df_m["is_train"], "TargetRet"]
    y_cls_va = df_m.loc[~df_m["is_train"], "TargetUp"]

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
    if len(X_va) > 0:
        model_reg.fit(X_tr, y_reg_tr,
                      eval_set=[(X_va, y_reg_va)],
                      eval_metric="rmse",
                      callbacks=[single_line_logger(period=50)])
    else:
        model_reg.fit(X_tr, y_reg_tr)

    # LightGBM Classifier (상승확률)
    model_cls = lgb.LGBMClassifier(
        n_estimators=n_estimators,
        max_depth=-1,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary",
        n_jobs=-1
    )
    if len(X_va) > 0:
        model_cls.fit(X_tr, y_cls_tr,
                      eval_set=[(X_va, y_cls_va)],
                      eval_metric="auc",
                      callbacks=[single_line_logger(period=50)])
    else:
        model_cls.fit(X_tr, y_cls_tr)

    return model_reg, model_cls

# ------------------------------------------------------------
# 4) 저장
# ------------------------------------------------------------
def _hash_list(lst: list) -> str:
    return str(abs(hash("|".join(map(str, lst)))))

def save_engine(payload: dict, mode: str):
    base = get_path("HOJ_ENGINE")
    # REAL/RESEARCH 하위 폴더 보장
    if os.path.isfile(base):
        base = os.path.dirname(base)
    out_dir = os.path.join(base, mode.upper())
    ensure_dir(out_dir)

    tag = datetime.strptime(payload["meta"]["data_date"], "%Y-%m-%d").strftime("%y%m%d")
    # === A안 파일명 규칙 적용 ===
    # HOJ_ENGINE_{MODE}_V31_h{horizon}_w{window}_n{n}_{YYMMDD}.pkl
    # [수정] 파일명 생성 로직 (f-string 중괄호 오류 수정 및 날짜 슬라이싱)
    fname = (
        f"HOJ_ENGINE_{mode.upper()}_V31"
        f"_h{payload['meta']['horizon']}"
        f"_w{payload['meta']['input_window']}"
        f"_n{payload['meta']['n_estimators']}"
        f"_{tag}.pkl"
    )

    path = os.path.join(out_dir, fname)
    with open(path, "wb") as f:
        pickle.dump(payload, f)

    print(f"\n💾 엔진 저장 완료: {path}")

# ------------------------------------------------------------
# 5) 메인 러너
# ------------------------------------------------------------
def run_unified_training(
    mode: str = "research",
    horizon: int = 5,
    input_window: int = 60,
    valid_days: int = 365,
    n_estimators: int = 1000,
    version: str = "V31",
):
    """
    mode: "real" | "research"
    """
    assert mode in ("real","research")

    print(f"=== 🚀 Unified HOJ Trainer V31 ({mode.upper()}) ===")
    print(f"[CFG] mode={mode}  horizon={horizon}  input_window={input_window}  valid_days={valid_days}  n_estimators={n_estimators}")

    # 1) DB 로드
    df = load_latest_db(version)
    close_col = pick_close_column(df)
    
    # [보완] Date 컬럼 타입 안전 변환
    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"])
        
    max_date = df["Date"].max().date()
    print(f"[DATA] DB max(Date) = {max_date}  | rows={len(df):,}")

    # ============================================================
    # [추가] 입구컷 SKIP: 이미 동일한 설정과 데이터 날짜의 엔진이 있으면 SKIP
    # ============================================================
    base = get_path("HOJ_ENGINE")
    if os.path.isfile(base):
        base = os.path.dirname(base)
    out_dir = os.path.join(base, mode.upper())
    ensure_dir(out_dir)

    tag_chk = max_date.strftime("%y%m%d") # 251126
    # 파일명 생성 규칙 (save_engine과 동일)
    fname_chk = (
        f"HOJ_ENGINE_{mode.upper()}_V31"
        f"_h{horizon}"
        f"_w{input_window}"
        f"_n{n_estimators}"
        f"_{tag_chk}.pkl"
    )
    path_chk = os.path.join(out_dir, fname_chk)

    if os.path.exists(path_chk):
        print(f"\n[SKIP] 이미 동일한 설정과 데이터 날짜의 엔진이 존재합니다.")
        print(f"       파일명: {fname_chk}")
        print("       (학습을 건너뜁니다.)")
        return # <--- 해당 모드는 종료 (루프가 있으면 다음 모드로 넘어감)
    # ============================================================

    # 2) 피처 선택
    features = select_feature_columns(df)
    if close_col in features:
        features = [c for c in features if c != close_col]
    print(f"[FEAT] 후보 피처 수 = {len(features)}")

    # 3) A안 마스크 + Horizon Tail + 타겟
    df_m, features, max_period = apply_A_mask(df, features, input_window, close_col, horizon)
    mask_min = df_m["Date"].min().date() if len(df_m) else None
    mask_max = df_m["Date"].max().date() if len(df_m) else None
    print(f"[MASK] MaxPeriod={max_period}d  | After Mask rows={len(df_m):,}  | Date range: {mask_min} ~ {mask_max}")

    # 4) 검증 분리
    if mode == "research":
        tr, va, valid_start, valid_end = split_train_valid(df_m, valid_days)
        tr["is_train"] = True
        va["is_train"] = False
        data = pd.concat([tr, va], ignore_index=True)
        print(f"[SPLIT] Train rows={len(tr):,}  Valid rows={len(va):,}  (valid={valid_start}~{valid_end})")
    else:
        data = df_m.copy()
        data["is_train"] = True
        print(f"[SPLIT] REAL 모드: 전체 {len(data):,}행 학습, 검증 분할 없음")

    # 5) 학습
    model_reg, model_cls = train_models(data, features, n_estimators=n_estimators)
    print("[TRAIN] LightGBM reg/cls 학습 완료")

    # 6) 메타/페이로드
    meta = {
        "version": "V31",
        "data_date": str(max_date),
        "horizon": int(horizon),
        "input_window": int(input_window),
        "valid_days": int(valid_days),
        "feature_hash": _hash_list(features),
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "close_col": close_col,
        "n_estimators": int(n_estimators),
    }
    payload = {
        "model_reg": model_reg,
        "model_cls": model_cls,
        "features": features,
        "meta": meta,
    }

    # 7) 저장
    save_engine(payload, mode)

    print("=== 🏁 Done. ===")

# ------------------------------------------------------------
# 6) CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # [수정] default='all' 로 변경 (Research -> Real 순차 실행)
    ap.add_argument("--mode", default="all", choices=["real","research","all"])
    ap.add_argument("--horizon", type=int, default=5)
    ap.add_argument("--input_window", type=int, default=60)
    ap.add_argument("--valid_days", type=int, default=365)
    ap.add_argument("--n_estimators", type=int, default=1000)
    ap.add_argument("--version", default="V31")
    args = ap.parse_args()

    # 실행할 모드 리스트 결정
    if args.mode == "all":
        modes_to_run = ["research", "real"]
    else:
        modes_to_run = [args.mode]

    try:
        # [수정] 순차 실행 루프
        for m in modes_to_run:
            run_unified_training(
                mode=m,
                horizon=args.horizon,
                input_window=args.input_window,
                valid_days=args.valid_days,
                n_estimators=args.n_estimators,
                version=args.version,
            )
            print("-" * 60) # 구분선

    except Exception as e:
        print(f"\n❌ [Error] {e}")
