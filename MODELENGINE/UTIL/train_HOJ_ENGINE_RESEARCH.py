# ============================================================
# HOJ ENGINE - RESEARCH TRAINER (V31) - 날짜 자동 인식 버전
#   - 최근 1년 검증, 나머지 전체 학습 (옵션 A)
#   - DB 실제 최신 날짜 기준으로 split
# ============================================================

import os
import pickle
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

from config_paths import get_path
from version_utils import backup_existing_file

print("=== [RESEARCH] HOJ 엔진 학습 시작 ===")

# ------------------------------------------------------------
# 1. 경로 설정
# ------------------------------------------------------------
DB_PATH = get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
ENGINE_DIR = get_path("HOJ_ENGINE", "RESEARCH")
ENGINE_NAME = "HOJ_ENGINE_RESEARCH_V31.pkl"
ENGINE_PATH = os.path.join(ENGINE_DIR, ENGINE_NAME)

print(f"  📥 입력 DB: {DB_PATH}")
print(f"  💾 출력 엔진: {ENGINE_PATH}")

os.makedirs(ENGINE_DIR, exist_ok=True)

# ------------------------------------------------------------
# 2. 데이터 로드
# ------------------------------------------------------------
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"리서치 DB 파일을 찾을 수 없습니다: {DB_PATH}")

df = pd.read_parquet(DB_PATH)

# Date 컬럼을 datetime으로 보장
if not np.issubdtype(df["Date"].dtype, np.datetime64):
    df["Date"] = pd.to_datetime(df["Date"])

# 정렬
df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

# 기본 통계 출력
min_date = df["Date"].min()
max_date = df["Date"].max()
n_rows = len(df)
n_codes = df["Code"].nunique()

print(f"  📅 DB 기간: {min_date.date()} ~ {max_date.date()}")
print(f"  📊 전체 행 수: {n_rows:,}  / 종목 수: {n_codes:,}")

# ------------------------------------------------------------
# 3. 학습/검증 기간 자동 설정 (옵션 A)
#    - 기준: DB의 실제 최신일자(max_date)
#    - 검증: 최근 1년 (calendar 365일)
#    - 학습: 그 이전 전체
# ------------------------------------------------------------
valid_start_date = max_date - timedelta(days=365)

print(f"  🔧 검증 시작일(자동): {valid_start_date.date()} (max_date 기준 -365일)")
print(f"  🔧 학습 구간: {min_date.date()} ~ {valid_start_date.date() - timedelta(days=1)}")
print(f"  🔧 검증 구간: {valid_start_date.date()} ~ {max_date.date()}")

mask_valid = df["Date"] >= valid_start_date
mask_train = df["Date"] < valid_start_date

df_train = df[mask_train].copy()
df_valid = df[mask_valid].copy()

if len(df_train) == 0 or len(df_valid) == 0:
    raise ValueError("학습/검증 구간 중 하나가 비어 있습니다. 날짜/DB 범위를 확인하세요.")

print(f"  📚 학습 샘플 수: {len(df_train):,}")
print(f"  🧪 검증 샘플 수: {len(df_valid):,}")

# ------------------------------------------------------------
# 4. 피처/타겟 분리
#    - 타겟: Return_5d (회귀), Label_5d (분류)
#    - 피처: 메타/원본컬럼 제외한 나머지 (15피처 자동 인식)
# ------------------------------------------------------------
meta_cols = [
    "Code", "Date",
    "Open", "High", "Low", "Close", "Volume",
    "KOSPI_종가", "KOSPI_수익률",
    "Return_5d", "Expected_Return_5d", "Label_5d",
]

for col in ["Return_5d", "Label_5d"]:
    if col not in df.columns:
        raise KeyError(f"필수 컬럼이 DB에 존재하지 않습니다: {col}")

feature_cols = [c for c in df.columns if c not in meta_cols]

print(f"  🧬 피처 개수: {len(feature_cols)}")
print("  🧬 피처 예시:", feature_cols[:10])

X_train = df_train[feature_cols]
y_train_reg = df_train["Return_5d"]
y_train_cls = df_train["Label_5d"].astype(int)

X_valid = df_valid[feature_cols]
y_valid_reg = df_valid["Return_5d"]
y_valid_cls = df_valid["Label_5d"].astype(int)

# 결측 제거 (혹시 모를 NaN 방지)
train_mask = X_train.notnull().all(axis=1) & y_train_reg.notnull()
valid_mask = X_valid.notnull().all(axis=1) & y_valid_reg.notnull()

X_train = X_train[train_mask]
y_train_reg = y_train_reg[train_mask]
y_train_cls = y_train_cls[train_mask]

X_valid = X_valid[valid_mask]
y_valid_reg = y_valid_reg[valid_mask]
y_valid_cls = y_valid_cls[valid_mask]

print(f"  ✅ NaN 제거 후 학습 샘플: {len(X_train):,}")
print(f"  ✅ NaN 제거 후 검증 샘플: {len(X_valid):,}")

# ------------------------------------------------------------
# 5. LightGBM 모델 설정
# ------------------------------------------------------------
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
    "n_estimators": 1000,
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
    "n_estimators": 1000,
    "n_jobs": -1,
}

# ------------------------------------------------------------
# 6. 회귀 모델 학습
# ------------------------------------------------------------
print("\n[1] 회귀 모델 학습 (Return_5d)")
model_reg = lgb.LGBMRegressor(**params_reg)

model_reg.fit(
    X_train, y_train_reg,
    eval_set=[(X_valid, y_valid_reg)],
    eval_metric="rmse",
    callbacks=[
        lgb.early_stopping(stopping_rounds=100, verbose=True),
    ],
)

# 검증 RMSE 계산
pred_reg = model_reg.predict(X_valid)
rmse = float(np.sqrt(((pred_reg - y_valid_reg) ** 2).mean()))
print(f"   ✅ 회귀 RMSE (valid): {rmse:.6f}")

# ------------------------------------------------------------
# 7. 분류 모델 학습
# ------------------------------------------------------------
print("\n[2] 분류 모델 학습 (Label_5d)")
model_cls = lgb.LGBMClassifier(**params_cls)

model_cls.fit(
    X_train, y_train_cls,
    eval_set=[(X_valid, y_valid_cls)],
    eval_metric="binary_logloss",
    callbacks=[
        lgb.early_stopping(stopping_rounds=100, verbose=True),
    ],
)

pred_prob = model_cls.predict_proba(X_valid)[:, 1]
pred_label = (pred_prob > 0.5).astype(int)
acc = float((pred_label == y_valid_cls).mean())
print(f"   ✅ 분류 정확도 (valid): {acc:.4f}")

# ------------------------------------------------------------
# 8. 엔진 저장 (기존 파일 백업 후 저장)
# ------------------------------------------------------------
print("\n[3] 엔진 저장")

backup_existing_file(ENGINE_PATH)

with open(ENGINE_PATH, "wb") as f:
    pickle.dump(
        {
            "model_reg": model_reg,
            "model_cls": model_cls,
            "features": feature_cols,
            "train_range": (str(min_date.date()), str(valid_start_date.date() - timedelta(days=1))),
            "valid_range": (str(valid_start_date.date()), str(max_date.date())),
        },
        f,
    )

print(f"💾 연구용 엔진 저장 완료 → {ENGINE_PATH}")
print("=== [RESEARCH] HOJ 엔진 학습 종료 ===")
