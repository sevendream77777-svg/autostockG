# ================================================================
#  train_real_engine_V25.py  (FINAL SAFE VERSION)
# ================================================================
# - 15개 피처 기반 Hoj 엔진 V25 학습 스크립트
# - 회귀(5일 예상수익률) + 분류(5일 상승 여부) 2개 모델 동시 학습
# - 최근 1년은 검증(Valid), 나머지는 학습(Train)
# - 모델은 pickle로 안전 저장 (new_Hoj_MODELENGINE_V25.pkl)
# - 이전 모델은 Hoj_MODELENGINE/ 폴더로 자동 백업
# ================================================================

import os
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb
import pickle

# ------------------------------------------------
# 0. 기본 설정
# ------------------------------------------------
INPUT_DB = "new_Hoj_DB_V25.parquet"
OUTPUT_MODEL = "new_Hoj_MODELENGINE_V25.pkl"
BACKUP_DIR = "Hoj_MODELENGINE"

os.makedirs(BACKUP_DIR, exist_ok=True)

FEATURE_COLS = [
    "SMA_20", "SMA_40", "SMA_60", "SMA_90",
    "RSI_14", "VOL_SMA_20", "MACD", "MACD_Sig",
    "BBP_20", "ATR_14", "STOCH_K", "STOCH_D",
    "CCI_20", "KOSPI_수익률", "ALPHA_SMA_20",
]

TARGET_REG = "Expected_Return_5d"   # 5일 동안의 예상 수익률
TARGET_CLS = "Label_5d"             # 5일 동안 수익 > 0 ? 1 : 0

# ------------------------------------------------
# 1. 데이터 로드
# ------------------------------------------------
print("--- 새 Hoj '뇌' (V25) 학습 스크립트 시작 ---")
print(f"입력 DB: {INPUT_DB}")

df = pd.read_parquet(INPUT_DB)
print(f"학습 DB 로드 성공. (총 {len(df):,} 행)")

# 날짜 정리
df["Date"] = pd.to_datetime(df["Date"])
min_date = df["Date"].min()
max_date = df["Date"].max()
last_date_str = max_date.strftime("%y%m%d")

print(f"📅 전체 데이터 기간: {min_date.date()} ~ {max_date.date()}")
print(f"학습 DB 데이터 기준일 확인: {last_date_str}")

# ------------------------------------------------
# 2. 컬럼 존재 여부 체크
# ------------------------------------------------
for col in FEATURE_COLS:
    if col not in df.columns:
        raise KeyError(f"❌ 피처 컬럼 누락: {col}")

if TARGET_REG not in df.columns:
    raise KeyError("❌ 'Expected_Return_5d' 컬럼이 없습니다. build_database_V25.py 라벨 생성 로직을 확인하세요.")

if TARGET_CLS not in df.columns:
    raise KeyError("❌ 'Label_5d' 컬럼이 없습니다. build_database_V25.py 라벨 생성 로직을 확인하세요.")

print("[알림] 학습에 사용될 '진짜 15개 피처' 확인 완료.")
print("🧩 피처 목록:", ", ".join(FEATURE_COLS))

# ------------------------------------------------
# 3. Train / Valid 분리 (최근 1년 = 검증)
# ------------------------------------------------
cut_date = max_date - pd.DateOffset(years=1)

print(f"📅 학습(Train): {min_date.date()} ~ {(cut_date - pd.Timedelta(days=1)).date()}")
print(f"📅 검증(Valid): {cut_date.date()} ~ {max_date.date()}")

train_df = df[df["Date"] < cut_date].copy()
valid_df = df[df["Date"] >= cut_date].copy()

# 결측 제거 (피처 + 타깃)
train_df = train_df.dropna(subset=FEATURE_COLS + [TARGET_REG, TARGET_CLS])
valid_df = valid_df.dropna(subset=FEATURE_COLS + [TARGET_REG, TARGET_CLS])

print(f"⚠️ 학습 데이터 행수: {len(train_df):,}")
print(f"⚠️ 검증 데이터 행수: {len(valid_df):,}")

X_train = train_df[FEATURE_COLS]
y_train_reg = train_df[TARGET_REG]
y_train_cls = train_df[TARGET_CLS]

X_valid = valid_df[FEATURE_COLS]
y_valid_reg = valid_df[TARGET_REG]
y_valid_cls = valid_df[TARGET_CLS]

# ------------------------------------------------
# 4. 회귀 엔진 (Expected_Return_5d 예측)
# ------------------------------------------------
print("=================================================")
print("[1] 회귀 엔진 학습 (Expected_Return_5d) 시작...")

reg_model = lgb.LGBMRegressor(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=41,
    subsample=0.85,
    colsample_bytree=0.85,
    objective="regression",
)

reg_model.fit(X_train, y_train_reg)

# 검증 RMSE 수동 계산
pred_valid_reg = reg_model.predict(X_valid)
mse = np.mean((pred_valid_reg - y_valid_reg) ** 2)
rmse = float(mse ** 0.5)

print(f"📉 회귀 엔진 RMSE: {rmse:.6f}")

# ------------------------------------------------
# 5. 분류 엔진 (상승 여부 예측)
# ------------------------------------------------
print("=================================================")
print("[2] 분류 엔진 학습 (Label_5d) 시작...")

cls_model = lgb.LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=41,
    subsample=0.85,
    colsample_bytree=0.85,
    objective="binary",
)

cls_model.fit(X_train, y_train_cls)

# 검증 정확도 계산
pred_prob = cls_model.predict_proba(X_valid)[:, 1]
pred_cls = (pred_prob >= 0.5).astype(int)
acc = float((pred_cls == y_valid_cls).mean())

print(f"🎯 분류 엔진 정확도: {acc:.4f}")

# ------------------------------------------------
# 6. 모델 저장 + 백업 (pickle, 안전 버전)
# ------------------------------------------------
print("=================================================")
print("[3] 엔진 저장 및 백업 처리...")

# 이전 엔진 백업
if os.path.exists(OUTPUT_MODEL):
    backup_path = os.path.join(BACKUP_DIR, f"Hoj_MODELENGINE_V25_{last_date_str}.pkl")
    try:
        os.replace(OUTPUT_MODEL, backup_path)
        print(f"📦 이전 엔진 백업: {backup_path}")
    except Exception as e:
        print(f"⚠️ 이전 엔진 백업 실패 (무시하고 진행): {e}")

# 새 엔진 번들 생성
model_bundle = {
    "reg": reg_model,
    "cls": cls_model,
    "features": FEATURE_COLS,
    "meta": {
        "rmse_valid": rmse,
        "acc_valid": acc,
        "train_start": str(min_date.date()),
        "train_end": str((cut_date - pd.Timedelta(days=1)).date()),
        "valid_start": str(cut_date.date()),
        "valid_end": str(max_date.date()),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
}

# 안전한 pickle 저장
with open(OUTPUT_MODEL, "wb") as f:
    pickle.dump(model_bundle, f, protocol=pickle.HIGHEST_PROTOCOL)

print(f"💾 새 엔진 저장 완료 → {OUTPUT_MODEL}")
print("=================================================")
print("[V25] 엔진 학습 최종 완료!")
print("=================================================")
