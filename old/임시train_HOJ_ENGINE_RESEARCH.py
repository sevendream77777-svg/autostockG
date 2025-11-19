# ===========================================================
# train_HOJ_ENGINE_RESEARCH.py
# 연구용 HOJ 엔진 학습 스크립트 (HOJ_DB_RESEARCH_V31 기준)
# ===========================================================

import os
import pickle
from datetime import timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

from config_paths import get_path
from version_utils import backup_existing_file


# ─────────────────────────────────────────────
# 경로 설정 (MODELENGINE 구조 기준)
# ─────────────────────────────────────────────
DB_PATH = get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
ENGINE_FILE = get_path("HOJ_ENGINE", "RESEARCH", "HOJ_ENGINE_RESEARCH_V31.pkl")

# HOJ 엔진 피처 후보 (없는 컬럼은 자동 제외)
FEATURE_CANDIDATES = [
    "SMA_20", "SMA_40", "SMA_60", "SMA_90",
    "RSI_14",
    "VOL_SMA_20",
    "MACD", "MACD_Sig",
    "BBP_20",
    "ATR_14",
    "STOCH_K", "STOCH_D",
    "CCI_20",
    "KOSPI_수익률",
    "ALPHA_SMA_20",
]


# ─────────────────────────────────────────────
# 유틸 함수
# ─────────────────────────────────────────────
def select_features(df: pd.DataFrame):
    """데이터프레임에 실제 존재하는 피처만 선택"""
    available = [f for f in FEATURE_CANDIDATES if f in df.columns]
    missing = [f for f in FEATURE_CANDIDATES if f not in df.columns]

    print(f"  🔎 사용 피처({len(available)}개): {', '.join(available) if available else '(없음)'}")
    if missing:
        print(f"  ⚠ 누락된 피처({len(missing)}개): {', '.join(missing)}")

    return available


def train_valid_split_by_date(df: pd.DataFrame, valid_days: int = 365):
    """마지막 valid_days 일자를 검증 세트로 사용하는 분리 방식"""
    max_date = pd.to_datetime(df["Date"]).max()
    split_date = max_date - timedelta(days=valid_days)

    train_df = df[df["Date"] < split_date]
    valid_df = df[df["Date"] >= split_date]

    return train_df, valid_df, split_date, max_date


def train_regressor(X_train, y_train, X_valid, y_valid):
    """회귀 모델 학습 (Return_5d 회귀)"""
    train_data = lgb.Dataset(X_train, label=y_train)
    valid_data = lgb.Dataset(X_valid, label=y_valid, reference=train_data)

    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 50,
        "verbosity": -1,
    }

    model = lgb.train(
        params,
        train_data,
        num_boost_round=2000,
        valid_sets=[train_data, valid_data],
        valid_names=["train", "valid"],
        early_stopping_rounds=100,
        verbose_eval=100,
    )

    pred_valid = model.predict(X_valid, num_iteration=model.best_iteration)
    rmse = float(np.sqrt(np.mean((y_valid - pred_valid) ** 2)))
    return model, rmse


def train_classifier(X_train, y_train, X_valid, y_valid):
    """분류 모델 학습 (Label_5d 상승/하락 구분)"""
    train_data = lgb.Dataset(X_train, label=y_train)
    valid_data = lgb.Dataset(X_valid, label=y_valid, reference=train_data)

    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "min_data_in_leaf": 50,
        "verbosity": -1,
    }

    model = lgb.train(
        params,
        train_data,
        num_boost_round=2000,
        valid_sets=[train_data, valid_data],
        valid_names=["train", "valid"],
        early_stopping_rounds=100,
        verbose_eval=100,
    )

    prob_valid = model.predict(X_valid, num_iteration=model.best_iteration)
    pred_valid = (prob_valid > 0.5).astype(int)
    acc = float((pred_valid == y_valid).mean())
    return model, acc


# ─────────────────────────────────────────────
# 메인 로직
# ─────────────────────────────────────────────
def train_research_engine():
    print("=== [RESEARCH] HOJ 엔진 학습 시작 ===")
    print(f"  📥 입력 DB: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        print("❌ 연구용 DB 파일이 없습니다. (build_HOJ_DB_RESEARCH.py 먼저 실행 필요)")
        return

    df = pd.read_parquet(DB_PATH)
    print(f"  - DB 로드 완료: {df.shape}")

    # 날짜 타입 보정 (컬럼명: Date)
    df["Date"] = pd.to_datetime(df["Date"])

    # 피처 선택
    feature_cols = select_features(df)

    # 학습에 필요한 컬럼 체크
    required_cols = ["Return_5d", "Label_5d"]
    for c in required_cols:
        if c not in df.columns:
            raise KeyError(f"❌ '{c}' 컬럼이 HOJ_DB_RESEARCH_V31에 없습니다.")

    # 결측 제거
    df_model = df.dropna(subset=feature_cols + required_cols).reset_index(drop=True)
    print(f"  - 결측 제거 후: {df_model.shape}")

    # Train / Valid 분리
    train_df, valid_df, split_date, max_date = train_valid_split_by_date(df_model, valid_days=365)

    print(f"  📅 학습 기간: {train_df['Date'].min().date()} ~ {train_df['Date'].max().date()}")
    print(f"  📅 검증 기간: {valid_df['Date'].min().date()} ~ {valid_df['Date'].max().date()}")

    X_train = train_df[feature_cols].values
    y_train_reg = train_df["Return_5d"].values
    y_train_cls = train_df["Label_5d"].values

    X_valid = valid_df[feature_cols].values
    y_valid_reg = valid_df["Return_5d"].values
    y_valid_cls = valid_df["Label_5d"].values

    # 회귀 모델 학습
    print("\n[1] 회귀 모델 학습 (Return_5d)")
    reg_model, rmse = train_regressor(X_train, y_train_reg, X_valid, y_valid_reg)
    print(f"   📉 검증 RMSE: {rmse:.6f}")

    # 분류 모델 학습
    print("\n[2] 분류 모델 학습 (Label_5d)")
    cls_model, acc = train_classifier(X_train, y_train_cls, X_valid, y_valid_cls)
    print(f"   🎯 검증 정확도: {acc:.4f}")

    # 메타 정보
    meta = {
        "type": "RESEARCH",
        "features": feature_cols,
        "train_start": str(train_df["Date"].min().date()),
        "train_end": str(train_df["Date"].max().date()),
        "valid_start": str(valid_df["Date"].min().date()),
        "valid_end": str(valid_df["Date"].max().date()),
        "rmse_valid": rmse,
        "acc_valid": acc,
    }

    # 엔진 번들
    engine_bundle = {
        "reg": reg_model,
        "cls": cls_model,
        "features": feature_cols,
        "meta": meta,
    }

    # 기존 파일 백업 + 저장
    os.makedirs(os.path.dirname(ENGINE_FILE), exist_ok=True)
    backup_existing_file(ENGINE_FILE)

    with open(ENGINE_FILE, "wb") as f:
        pickle.dump(engine_bundle, f)

    print(f"\n💾 연구용 엔진 저장 완료 → {ENGINE_FILE}")
    print("=== [RESEARCH] HOJ 엔진 학습 종료 ===")


if __name__ == "__main__":
    train_research_engine()

