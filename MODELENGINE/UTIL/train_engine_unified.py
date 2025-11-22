# ============================================================
# Unified HOJ Trainer (Research/Real) - V31
#   - Single entrypoint to train Research/Real with parameters
#   - Auto-build target (Return_{h}, Label_{h}) if missing
#   - Auto-pick numeric features (15피처 호환)
# ============================================================

import os
import sys
import pickle
import argparse
from datetime import timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

# 프로젝트 유틸 (환경에 맞게 경로 보정)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_paths import get_path
from version_utils import backup_existing_file

def ensure_datetime(df, col="Date"):
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def build_targets_if_missing(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """
    DB에 Return_{h}, Label_{h}가 없으면 Close로 즉시 생성.
      Return_{h} = Close.shift(-h) / Close - 1
      Label_{h}  = (Return_{h} > 0).astype(int)
    """
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    if ret_col not in df.columns or lab_col not in df.columns:
        if "Close" not in df.columns:
            raise KeyError("Close 컬럼이 없어 라벨을 생성할 수 없습니다.")
        df = df.sort_values(["Code", "Date"]).copy()
        # 종목별로 미래 h일 수익률 계산
        df[ret_col] = (
            df.groupby("Code", group_keys=False)["Close"]
              .apply(lambda s: s.shift(-horizon) / s - 1.0)
        )
        df[lab_col] = (df[ret_col] > 0).astype("float")  # float로 둔 뒤 나중에 astype(int)
    return df

def pick_numeric_features(df: pd.DataFrame, meta_cols: list) -> list:
    raw = [c for c in df.columns if c not in meta_cols]
    feats = (
        df[raw]
        .select_dtypes(include=["number", "bool"])
        .columns
        .tolist()
    )
    return feats

def load_db(mode: str) -> str:
    if mode == "research":
        return get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
    elif mode == "real":
        return get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")
    else:
        raise ValueError("mode must be 'research' or 'real'")

def make_engine_path(mode: str) -> str:
    if mode == "research":
        eng_dir = get_path("HOJ_ENGINE", "RESEARCH")
        name = "HOJ_ENGINE_RESEARCH_V31.pkl"
    else:
        eng_dir = get_path("HOJ_ENGINE", "REAL")
        name = "HOJ_ENGINE_REAL_V31.pkl"
    os.makedirs(eng_dir, exist_ok=True)
    return os.path.join(eng_dir, name)

def train(args):
    mode = args.mode.lower()
    horizon = int(args.horizon)
    valid_days = int(args.valid_days)

    db_path = args.db_path if args.db_path else load_db(mode)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")

    print(f"=== [{mode.upper()}] Unified HOJ Trainer 시작 ===")
    print(f"  📥 입력 DB: {db_path}")
    print(f"  🔧 horizon: {horizon}d, valid_days(Research): {valid_days}")

    df = pd.read_parquet(db_path)
    df = ensure_datetime(df, "Date")
    df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

    min_date, max_date = df["Date"].min().date(), df["Date"].max().date()
    n_rows, n_codes = len(df), df["Code"].nunique()
    print(f"  📅 DB 기간: {min_date} ~ {max_date}")
    print(f"  📊 전체 행 수: {n_rows:,} / 종목 수: {n_codes:,}")

    # 라벨 보장
    df = build_targets_if_missing(df, horizon=horizon)
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    # 메타/원본 제외 목록(환경에 맞게 확장 가능)
    meta_cols = [
        "Code", "Date",
        "Open", "High", "Low", "Close", "Volume",
        "KOSPI_종가", "KOSPI_수익률",
        # 아래는 다른 horizon이 존재해도 자동 예외로 둠
        ret_col, "Expected_Return_5d", lab_col,
        "Return_1d","Return_2d","Return_5d","Return_10d","Return_20d","Return_40d","Return_60d",
        "Label_1d","Label_2d","Label_5d","Label_10d","Label_20d","Label_40d","Label_60d",
    ]

    feature_cols = pick_numeric_features(df, meta_cols=meta_cols)
    if not feature_cols:
        raise ValueError("학습 가능한 수치형 피처가 없습니다.")

    # 결측 제거 마스크
    mask = df[feature_cols].notnull().all(axis=1) & df[ret_col].notnull()
    df_trainable = df[mask].copy()

    # -------------------------
    # 데이터 분할
    # -------------------------
    if mode == "research":
        # 최근 valid_days(기본 365일) = 검증, 그 이전 = 학습
        vd_start = df_trainable["Date"].max() - timedelta(days=valid_days)
        m_train = df_trainable["Date"] < vd_start
        m_valid = df_trainable["Date"] >= vd_start

        dtr, dvl = df_trainable[m_train], df_trainable[m_valid]
        if len(dtr) == 0 or len(dvl) == 0:
            raise ValueError("학습/검증 구간이 비었습니다. valid_days를 조정하세요.")

        X_train = dtr[feature_cols]
        y_train_reg = dtr[ret_col]
        y_train_cls = dtr[lab_col].astype(int)

        X_valid = dvl[feature_cols]
        y_valid_reg = dvl[ret_col]
        y_valid_cls = dvl[lab_col].astype(int)

        print(f"  📚 학습 샘플: {len(X_train):,}  /  🧪 검증 샘플: {len(X_valid):,}")

    else:  # real
        X_train = df_trainable[feature_cols]
        y_train_reg = df_trainable[ret_col]
        y_train_cls = df_trainable[lab_col].astype(int)
        print(f"  📚 FULL 학습 샘플: {len(X_train):,}")

    # -------------------------
    # LightGBM 설정
    # -------------------------
    params_reg = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.03,
        "num_leaves": 63,
        "max_depth": -1,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 3,
        "verbose": -1,
        "n_estimators": args.n_estimators,
        "n_jobs": -1,
    }
    params_cls = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.03,
        "num_leaves": 63,
        "max_depth": -1,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 3,
        "verbose": -1,
        "n_estimators": args.n_estimators,
        "n_jobs": -1,
    }

    # -------------------------
    # 학습
    # -------------------------
    print("\n[1] 회귀 모델 학습")
    model_reg = lgb.LGBMRegressor(**params_reg)
    if mode == "research":
        model_reg.fit(
            X_train, y_train_reg,
            eval_set=[(X_valid, y_valid_reg)],
            eval_metric="rmse",
            callbacks=[lgb.early_stopping(stopping_rounds=args.early_stop, verbose=True)],
        )
        pred_reg = model_reg.predict(X_valid)
        rmse = float(np.sqrt(((pred_reg - y_valid_reg) ** 2).mean()))
        print(f"   ✅ RMSE(valid): {rmse:.6f}")
    else:
        model_reg.fit(X_train, y_train_reg)
        pred_reg = model_reg.predict(X_train)
        rmse = float(np.sqrt(((pred_reg - y_train_reg) ** 2).mean()))
        print(f"   ℹ RMSE(train, 참고): {rmse:.6f}")

    print("\n[2] 분류 모델 학습")
    model_cls = lgb.LGBMClassifier(**params_cls)
    if mode == "research":
        model_cls.fit(
            X_train, y_train_cls,
            eval_set=[(X_valid, y_valid_cls)],
            eval_metric="binary_logloss",
            callbacks=[lgb.early_stopping(stopping_rounds=args.early_stop, verbose=True)],
        )
        prob = model_cls.predict_proba(X_valid)[:, 1]
        acc = float(((prob > 0.5).astype(int) == y_valid_cls).mean())
        print(f"   ✅ ACC(valid): {acc:.4f}")
    else:
        model_cls.fit(X_train, y_train_cls)
        prob = model_cls.predict_proba(X_train)[:, 1]
        acc = float(((prob > 0.5).astype(int) == y_train_cls).mean())
        print(f"   ℹ ACC(train, 참고): {acc:.4f}")

    # -------------------------
    # 저장
    # -------------------------
    eng_path = make_engine_path(mode)
    # 백업 태그: DB 최신일자
    date_tag = None
    try:
        dtmp = pd.read_parquet(db_path, columns=["Date"])
        _dt = pd.to_datetime(dtmp["Date"], errors="coerce").max()
        if pd.notnull(_dt):
            date_tag = _dt.strftime("%y%m%d")
    except Exception:
        pass
    backup_existing_file(eng_path, date_tag=date_tag)

    meta = {
        "mode": mode,
        "horizon": horizon,
        "features": feature_cols,
        "db_path": db_path,
        "train_range": (
            str(X_train.index.min()) if len(X_train) else None,
            str(X_train.index.max()) if len(X_train) else None,
        ),
        "valid_days": (valid_days if mode == "research" else None),
        "metrics": {"rmse": rmse, "acc": acc},
    }
    with open(eng_path, "wb") as f:
        pickle.dump({"model_reg": model_reg, "model_cls": model_cls, "meta": meta}, f)

    print(f"\n💾 엔진 저장 완료 → {eng_path}")
    print("=== Trainer 종료 ===")

def parse_args():
    p = argparse.ArgumentParser(description="Unified HOJ Trainer (Research/Real)")
    p.add_argument("--mode", choices=["research", "real"], required=True)
    p.add_argument("--horizon", type=int, default=5, help="라벨 윈도우 일수 (예: 1/2/5/10/20)")
    p.add_argument("--valid_days", type=int, default=365, help="research 검증 기간(일)")
    p.add_argument("--db_path", type=str, default=None, help="DB 경로(미지정시 자동)")
    p.add_argument("--n_estimators", type=int, default=1000)
    p.add_argument("--early_stop", type=int, default=100)
    return p.parse_args()

if __name__ == "__main__":
    train(parse_args())
