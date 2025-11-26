
# ============================================================
# Unified HOJ Trainer (V34 - Period Mask + Horizon Tail, wFull)
#  - A안: 각 피처의 기간만큼 앞 구간 자동 제외(오염 0%)
#  - Horizon 꼬리 제거: 마지막 h일 학습 제외
#  - Input Window 기본 0 (전체 피처 사용). >0이면 제한 가능.
#  - 파일 규칙/경로 유틸은 기존과 동일 사용
# ============================================================

import os
import sys
import pickle
import argparse
import re
import glob
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

# ------------------------------------------------------------
# 1) 경로 유틸 로드
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)   # MODELENGINE
root_dir = os.path.dirname(parent_dir)      # Root
sys.path.append(root_dir)

try:
    from MODELENGINE.UTIL.config_paths import get_path
    from MODELENGINE.UTIL.version_utils import find_latest_file
except ImportError:
    sys.path.append(parent_dir)
    from UTIL.config_paths import get_path
    from UTIL.version_utils import find_latest_file

# ------------------------------------------------------------
# 2) 헬퍼들
# ------------------------------------------------------------
def get_db_path(version: str = "V31") -> str:
    """최신 날짜 태그가 붙은 HOJ_DB_{version}_YYMMDD[_n].parquet 우선 사용"""
    base_dir = get_path("HOJ_DB")
    if "REAL" in base_dir or "RESEARCH" in base_dir:
        base_dir = os.path.dirname(base_dir)
    latest_db = find_latest_file(base_dir, f"HOJ_DB_{version}")
    if latest_db:
        return latest_db
    return os.path.join(base_dir, f"HOJ_DB_{version}.parquet")

def ensure_datetime(df: pd.DataFrame, col: str = "Date") -> pd.DataFrame:
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def build_dynamic_target(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """요청한 Horizon(일) 정답(Return_{h}d, Label_{h}d) 없으면 생성"""
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"
    if ret_col in df.columns and lab_col in df.columns:
        return df
    if "Close" not in df.columns:
        raise KeyError("DB에 'Close'가 없어 타겟을 생성할 수 없습니다.")
    df = df.sort_values(["Code", "Date"]).copy()
    df[ret_col] = df.groupby("Code")["Close"].shift(-horizon) / df["Close"] - 1.0
    df[lab_col] = (df[ret_col] > 0).astype(int)
    return df

def get_save_filename(mode: str, version: str, data_date: str, horizon: int,
                      input_window: int, n_estimators: int, train_date: str) -> str:
    iw_tag = f"w{input_window}" if input_window > 0 else "wFull"
    return (
        f"HOJ_ENGINE_{mode.upper()}_{version}_"
        f"d{data_date}_"
        f"h{horizon}_"
        f"{iw_tag}_"
        f"n{n_estimators}_"
        f"t{train_date}.pkl"
    )

def extract_max_period_from_features(columns) -> int:
    """컬럼명 내 숫자들에서 가장 큰 기간을 추출 (없으면 0)"""
    max_p = 0
    for col in columns:
        nums = re.findall(r"\d+", str(col))
        if nums:
            try:
                period = int(nums[-1])
                if period > max_p:
                    max_p = period
            except:
                pass
    return max_p

# ------------------------------------------------------------
# 3) 메인 로직
# ------------------------------------------------------------
def run_unified_training(
    mode: str = "research",
    horizon: int = 5,
    input_window: int = 0,        # 기본: 전체 피처 사용
    valid_days: int = 365,
    n_estimators: int = 1000,
    version: str = "V31",
) -> None:
    mode = mode.lower()
    print("\\n=== [HOJ Engine Factory V34] 시작 =========================")
    print(f"[Config] Mode={mode.upper()} | Horizon={horizon}d | InputWindow={input_window or 'Full'} | Valid={valid_days}d")

    # [A] DB 로드
    db_path = get_db_path(version)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")
    print(f"[Load] DB: {os.path.basename(db_path)}")
    df = pd.read_parquet(db_path)
    df = ensure_datetime(df)
    df = df.sort_values(['Date','Code']).reset_index(drop=True)

    min_date = df['Date'].min().date()
    max_date_obj = df['Date'].max()
    max_date = max_date_obj.date()
    data_date_tag = max_date.strftime("%y%m%d")
    print(f"[Info] 데이터 기간: {min_date} ~ {max_date} | Rows={len(df):,}")

    # [B] 타겟 준비
    df = build_dynamic_target(df, horizon)
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    # [C] 피처 선정
    exclude_cols = [
        "Code","Date","Name","Market",
        "Open","High","Low","Close","Volume","Amount","Marcap",
        "KOSPI_종가","KOSPI_수익률",
        ret_col, lab_col, f"Expected_{ret_col}",
    ]
    exclude_cols += [c for c in df.columns if c.startswith("Return_") or c.startswith("Label_")]
    feature_cols = df.columns.difference(exclude_cols).tolist()
    feature_cols = df[feature_cols].select_dtypes(include=['number','bool']).columns.tolist()

    # [C-1] Input Window가 >0 이면 긴 지표 제외(옵션)
    if input_window and input_window > 0:
        print(f"[Filter] InputWindow={input_window} 적용(기간 초과 지표 제외)")
        keep, drop = [], []
        for col in feature_cols:
            nums = re.findall(r"\\d+", col)
            if nums and int(nums[-1]) > input_window:
                drop.append(col)
            else:
                keep.append(col)
        feature_cols = keep
        if drop:
            print(f"        제외 {len(drop)}: {drop}")

    if not feature_cols:
        raise ValueError("학습할 피처가 없습니다. Input Window 설정 또는 DB 컬럼 확인.")

    # [D] A안: 기간만큼 앞 구간 제외 + Horizon 꼬리 제거
    max_period = extract_max_period_from_features(feature_cols)
    print(f"[Mask] MaxPeriod={max_period}d | HorizonTail={horizon}d 제거")

    def _apply_masks(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("Date")
        # 앞: 최대 기간만큼 제외
        if max_period > 0:
            g = g.iloc[max_period:]
        # 뒤: horizon 꼬리 제외
        if horizon > 0 and len(g) > horizon:
            g = g.iloc[:-horizon]
        return g

    df_masked = (
        df.groupby("Code", group_keys=False)
          .apply(_apply_masks)
          .reset_index(drop=True)
    )
    # 선택된 피처 + 타겟 결측 제거
    mask = df_masked[feature_cols].notnull().all(axis=1) & df_masked[ret_col].notnull()
    df_train = df_masked[mask].copy()
    print(f"[Data] 마스크/NaN 제거 후 학습 데이터: {len(df_train):,} rows (From {df_train['Date'].min().date()})")

    # [E] Train/Valid Split & 학습
    params_common = {
        "n_estimators": n_estimators,
        "learning_rate": 0.03,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 3,
        "n_jobs": -1,
        "verbose": -1,
        "random_state": 42,
    }

    metrics = {}
    if mode == "research":
        split_date = max_date_obj - timedelta(days=valid_days)
        print(f"[Split] Research 분할: split={split_date.date()}")
        mask_tr = df_train["Date"] < split_date
        mask_va = df_train["Date"] >= split_date

        X_tr = df_train.loc[mask_tr, feature_cols]
        y_tr_reg = df_train.loc[mask_tr, ret_col]
        y_tr_cls = df_train.loc[mask_tr, lab_col]

        X_va = df_train.loc[mask_va, feature_cols]
        y_va_reg = df_train.loc[mask_va, ret_col]
        y_va_cls = df_train.loc[mask_va, lab_col]

        print(f"[Size] Train={len(X_tr):,} | Valid={len(X_va):,}")
        print("[Train] 회귀/분류 동시 학습(ES=100)...")
        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg, eval_set=[(X_va, y_va_reg)], callbacks=[lgb.early_stopping(100, verbose=False)])

        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls, eval_set=[(X_va, y_va_cls)], callbacks=[lgb.early_stopping(100, verbose=False)])

        rmse = float(np.sqrt(np.mean((model_reg.predict(X_va) - y_va_reg) ** 2)))
        acc  = float(np.mean(model_cls.predict(X_va) == y_va_cls))
        print(f"[Eval] RMSE={rmse:.5f} | ACC={acc:.2%}")
        metrics = {"rmse": rmse, "acc": acc}
    else:
        print("[Train] Real 모드: 전체 데이터로 학습")
        X_tr = df_train[feature_cols]
        y_tr_reg = df_train[ret_col]
        y_tr_cls = df_train[lab_col]

        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg)

        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls)

        metrics = {"note": "Real mode full train"}

    # [F] 저장
    save_dir = get_path("HOJ_ENGINE", mode.upper())
    os.makedirs(save_dir, exist_ok=True)
    train_date_tag = datetime.now().strftime("%y%m%d")
    save_name = get_save_filename(mode, version, data_date_tag, horizon, input_window, n_estimators, train_date_tag)
    save_path = os.path.join(save_dir, save_name)

    payload = {
        "model_reg": model_reg,
        "model_cls": model_cls,
        "features": feature_cols,
        "meta": {
            "mode": mode,
            "version": version,
            "horizon": horizon,
            "input_window": input_window,
            "n_estimators": n_estimators,
            "data_date": str(max_date),
            "train_date": str(datetime.now().date()),
            "metrics": metrics,
        },
    }
    with open(save_path, "wb") as f:
        pickle.dump(payload, f)

    print(f"[Save] 엔진 저장 완료: {os.path.basename(save_path)}")
    print("=== [HOJ Engine Factory V34] 완료 =========================\\n")


# ------------------------------------------------------------
# 4) CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="all", help="all | research | real (기본 all: 둘 다 순차 실행)")
    p.add_argument("--horizon", type=int, default=5, help="예측할 미래 일수 (예: 5)")
    p.add_argument("--input_window", type=int, default=0, help="0이면 전체 피처, >0이면 기간 제한")
    p.add_argument("--valid_days", type=int, default=365)
    p.add_argument("--n_estimators", type=int, default=1000)
    p.add_argument("--version", type=str, default="V31")
    args = p.parse_args()

    modes = []
    if args.mode.lower() in ("all","research"):
        modes.append("research")
    if args.mode.lower() in ("all","real"):
        modes.append("real")

    for m in modes:
        print(f"\\n🚀 [Pipeline] {m.upper()} 엔진 학습 시작")
        run_unified_training(
            mode=m,
            horizon=args.horizon,
            input_window=args.input_window,
            valid_days=args.valid_days,
            n_estimators=args.n_estimators,
            version=args.version,
        )
