# ============================================================
# HOJ ENGINE - REAL TRAINER (V31) - LightGBM 4.x 대응
# ============================================================

import pandas as pd
import numpy as np
import lightgbm as lgb
import os
from datetime import datetime
from config_paths import get_path

print("=== [REAL] HOJ 엔진 학습 시작 ===")

# ------------------------------------------------------------
# 1. 데이터 경로 / 출력 경로
# ------------------------------------------------------------
DB_PATH = os.path.join(
    get_path("HOJ_DB"), "REAL", "HOJ_DB_REAL_V31.parquet"
)
SAVE_DIR = os.path.join(get_path("HOJ_ENGINE"), "REAL")

print(f"  📥 입력 DB: {DB_PATH}")

df = pd.read_parquet(DB_PATH)
print(f"  - DB 로드 완료: {df.shape}")

# ------------------------------------------------------------
# 2. 15개 피처 고정
# ------------------------------------------------------------
feature_cols = [
    "SMA_20","SMA_40","SMA_60","SMA_90",
    "RSI_14",
    "VOL_SMA_20",
    "MACD","MACD_Sig",
    "BBP_20",
    "ATR_14",
    "STOCH_K","STOCH_D",
    "CCI_20",
    "ALPHA_SMA_20",
    "KOSPI_수익률"
]

target_reg = "Return_5d"
target_cls = "Label_5d"

# ------------------------------------------------------------
# 3. 학습/검증 분리 (실전은 전체 학습 + 1년 검증)
# ------------------------------------------------------------
df["Date"] = pd.to_datetime(df["Date"])
val_start = df["Date"].max() - pd.Timedelta(days=365)

train_df = df[df["Date"] < val_start]
valid_df = df[df["Date"] >= val_start]

print(f"  📅 학습 구간: {train_df['Date'].min().date()} ~ {train_df['Date'].max().date()}")
print(f"  📅 검증 구간: {valid_df['Date'].min().date()} ~ {valid_df['Date'].max().date()}")

X_train = train_df[feature_cols]
y_train_reg = train_df[target_reg]
y_train_cls = train_df[target_cls]

X_valid = valid_df[feature_cols]
y_valid_reg = valid_df[target_reg]
y_valid_cls = valid_df[target_cls]

# ------------------------------------------------------------
# 4. LightGBM 파라미터
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
    "verbose": -1
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
    "verbose": -1
}

# ------------------------------------------------------------
# 5. 회귀 학습
# ------------------------------------------------------------
print("\n[1] 회귀 모델 학습")

dtrain = lgb.Dataset(X_train, label=y_train_reg)
dvalid = lgb.Dataset(X_valid, label=y_valid_reg)

model_reg = lgb.train(
    params_reg,
    dtrain,
    valid_sets=[dvalid],
    num_boost_round=2000,
    callbacks=[
        lgb.early_stopping(100),
        lgb.log_evaluation(50)
    ]
)

print(f"   ✅ 회귀 RMSE(valid): {model_reg.best_score['valid_0']['rmse']:.6f}")

# ------------------------------------------------------------
# 6. 분류 학습
# ------------------------------------------------------------
print("\n[2] 분류 모델 학습")

dtrain = lgb.Dataset(X_train, label=y_train_cls)
dvalid = lgb.Dataset(X_valid, label=y_valid_cls)

model_cls = lgb.train(
    params_cls,
    dtrain,
    valid_sets=[dvalid],
    num_boost_round=2000,
    callbacks=[
        lgb.early_stopping(100),
        lgb.log_evaluation(50)
    ]
)

print(f"   ✅ 분류 Logloss(valid): {model_cls.best_score['valid_0']['binary_logloss']:.6f}")

# ------------------------------------------------------------
# 7. 정확도
# ------------------------------------------------------------
pred_prob = model_cls.predict(X_valid)
pred_label = (pred_prob > 0.5).astype(int)
acc = (pred_label == y_valid_cls).mean()

print(f"   📊 분류 정확도(valid): {acc:.4f}")

# ------------------------------------------------------------
# 8. 저장 (백업 포함)
# ------------------------------------------------------------
ts = datetime.now().strftime("%y%m%d_%H%M%S")
final_path = os.path.join(SAVE_DIR, "HOJ_ENGINE_REAL_V31.pkl")
backup_path = final_path.replace(".pkl", f"_{ts}.pkl")

import pickle

if os.path.exists(final_path):
    os.rename(final_path, backup_path)

pickle.dump(
    {"model_reg": model_reg, "model_cls": model_cls, "features": feature_cols},
    open(final_path, "wb")
)

print(f"💾 실전용 엔진 저장 완료 → {final_path}")
print("=== [REAL] HOJ 엔진 학습 종료 ===")
