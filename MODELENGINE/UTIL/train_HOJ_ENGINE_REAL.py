# ============================================================
# HOJ ENGINE - REAL TRAINER (V31) - 날짜 자동 인식 버전
#   - DB 실제 최신 날짜까지 전체 구간 학습
# ============================================================

import os
import pickle
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb

from config_paths import get_path
from version_utils import backup_existing_file

print("=== [REAL] HOJ 엔진 학습 시작 ===")

# ------------------------------------------------------------
# 1. 경로 설정
# ------------------------------------------------------------
DB_PATH = get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")
ENGINE_DIR = get_path("HOJ_ENGINE", "REAL")
ENGINE_NAME = "HOJ_ENGINE_REAL_V31.pkl"
ENGINE_PATH = os.path.join(ENGINE_DIR, ENGINE_NAME)

print(f"  📥 입력 DB: {DB_PATH}")
print(f"  💾 출력 엔진: {ENGINE_PATH}")

os.makedirs(ENGINE_DIR, exist_ok=True)

# ------------------------------------------------------------
# 2. 데이터 로드
# ------------------------------------------------------------
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"리얼 DB 파일을 찾을 수 없습니다: {DB_PATH}")

df = pd.read_parquet(DB_PATH)

# Date 컬럼 datetime 보장
if not np.issubdtype(df["Date"].dtype, np.datetime64):
    df["Date"] = pd.to_datetime(df["Date"])

df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

min_date = df["Date"].min()
max_date = df["Date"].max()
n_rows = len(df)
n_codes = df["Code"].nunique()

print(f"  📅 DB 기간(REAL): {min_date.date()} ~ {max_date.date()}")
print(f"  📊 전체 행 수: {n_rows:,}  / 종목 수: {n_codes:,}")

# ------------------------------------------------------------
# 3. 피처/타겟 분리
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

raw_feature_cols = [c for c in df.columns if c not in meta_cols]

# LightGBM은 수치/불리언형만 허용하므로 숫자/불리언 컬럼만 사용
feature_cols = (
    df[raw_feature_cols]
    .select_dtypes(include=["number", "bool"])
    .columns
    .tolist()
)

if not feature_cols:
    raise ValueError("학습 가능한 수치형 피처가 없습니다. HOJ_DB 구성을 확인하세요.")

removed_cols = sorted(set(raw_feature_cols) - set(feature_cols))
if removed_cols:
    print(f"  ⚠ 제외된 비수치 컬럼: {removed_cols[:5]}{'...' if len(removed_cols) > 5 else ''}")


print(f"  🧬 피처 개수: {len(feature_cols)}")
print("  🧬 피처 예시:", feature_cols[:10])

X = df[feature_cols]
y_reg = df["Return_5d"]
y_cls = df["Label_5d"].astype(int)

mask = X.notnull().all(axis=1) & y_reg.notnull()
X = X[mask]
y_reg = y_reg[mask]
y_cls = y_cls[mask]

print(f"  ✅ NaN 제거 후 학습 샘플 수: {len(X):,}")

# ------------------------------------------------------------
# 4. LightGBM 모델 설정
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
# 5. 회귀 모델 학습 (FULL DATA)
# ------------------------------------------------------------
print("\n[1] 회귀 모델 학습 (FULL DATA)")
model_reg = lgb.LGBMRegressor(**params_reg)
model_reg.fit(X, y_reg)

pred_reg = model_reg.predict(X)
rmse_train = float(np.sqrt(((pred_reg - y_reg) ** 2).mean()))
print(f"   ℹ 학습 RMSE (train, 참고): {rmse_train:.6f}")

# ------------------------------------------------------------
# 6. 분류 모델 학습 (FULL DATA)
# ------------------------------------------------------------
print("\n[2] 분류 모델 학습 (FULL DATA)")
model_cls = lgb.LGBMClassifier(**params_cls)
model_cls.fit(X, y_cls)

pred_prob = model_cls.predict_proba(X)[:, 1]
pred_label = (pred_prob > 0.5).astype(int)
acc_train = float((pred_label == y_cls).mean())
print(f"   ℹ 학습 정확도 (train, 참고): {acc_train:.4f}")

# ------------------------------------------------------------
# 7. 엔진 저장 (기존 파일 백업 후 저장)
# ------------------------------------------------------------
print("\n[3] 엔진 저장")

backup_existing_file(ENGINE_PATH)

with open(ENGINE_PATH, "wb") as f:
    pickle.dump(
        {
            "model_reg": model_reg,
            "model_cls": model_cls,
            "features": feature_cols,
            "train_range": (str(min_date.date()), str(max_date.date())),
        },
        f,
    )

print(f"💾 실전용 엔진 저장 완료 → {ENGINE_PATH}")
print("=== [REAL] HOJ 엔진 학습 종료 ===")
