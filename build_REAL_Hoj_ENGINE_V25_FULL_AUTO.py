# =============================================================
# FULL DB 정리 + REAL 엔진 재학습 통합 자동 스크립트 (날짜 컬럼 자동 탐지)
# =============================================================

import os
import pandas as pd
import joblib
from datetime import datetime
import lightgbm as lgb

# -------------------------------------------------------------
# [1] 경로 설정
# -------------------------------------------------------------
BASE = r"F:\autostockG"
FULL_DB = os.path.join(BASE, "new_Hoj_DB_V25_FULL.parquet")
CLEAN_DB = os.path.join(BASE, "new_Hoj_DB_V25_FULL_CLEAN.parquet")
MODEL_DIR = os.path.join(BASE, "Hoj_MODELENGINE")
FINAL_MODEL = os.path.join(MODEL_DIR, "REAL_Hoj_MODELENGINE_V25.pkl")

os.makedirs(MODEL_DIR, exist_ok=True)

# -------------------------------------------------------------
# [2] 지침: 피처 15개 강제
# -------------------------------------------------------------
FEATURE_COLS = [
    "SMA_20", "SMA_40", "SMA_60", "SMA_90",
    "RSI_14", "VOL_SMA_20",
    "MACD", "MACD_Sig",
    "BBP_20", "ATR_14",
    "STOCH_K", "STOCH_D",
    "CCI_20", "KOSPI_수익률", "ALPHA_SMA_20"
]

REMOVE_COLS = ["KOSPI_Close", "KOSPI_Return_20"]

TARGET_REG = "Expected_Return_5d"
TARGET_CLS = "Label_5d"

# -------------------------------------------------------------
# 날짜 컬럼 자동 탐지 함수
# -------------------------------------------------------------
def find_date_column(df):
    candidates = ["date", "Date", "날짜", "DATE"]
    for col in candidates:
        if col in df.columns:
            return col
    
    # fallback: dtype 검사
    datetime_cols = df.select_dtypes(include=["datetime64", "datetime64[ns]"]).columns
    if len(datetime_cols) > 0:
        return datetime_cols[0]

    raise KeyError("날짜 컬럼을 찾을 수 없습니다. (date, Date, 날짜 중 하나여야 함)")

# -------------------------------------------------------------
# [3] FULL DB 로드 + CLEAN 생성
# -------------------------------------------------------------
print("\n=== [STEP 1] FULL DB 로드 ===")
df = pd.read_parquet(FULL_DB)
print(f"FULL DB 전체 행수: {len(df):,}")

print("\n=== [STEP 2] 불필요 컬럼 제거 ===")
for col in REMOVE_COLS:
    if col in df.columns:
        print(f" - 제거됨: {col}")
        df = df.drop(columns=[col])
    else:
        print(f" - 없음(무시): {col}")

# Clean 파일 저장
df.to_parquet(CLEAN_DB, index=False)
print(f"\n[CLEAN 저장 완료] → {CLEAN_DB}")

# -------------------------------------------------------------
# [4] 날짜 컬럼 자동 탐지
# -------------------------------------------------------------
print("\n=== [STEP 3] 날짜 컬럼 자동 탐지 ===")
date_col = find_date_column(df)
print(f" - 날짜 컬럼 발견: {date_col}")

df[date_col] = pd.to_datetime(df[date_col])

# -------------------------------------------------------------
# [5] 학습/검증 데이터 구성
# -------------------------------------------------------------
print("\n=== [STEP 4] 학습/검증 데이터 구축 ===")

train_df = df[df[date_col] < "2024-11-05"]
valid_df = df[df[date_col] >= "2024-11-05"]

print(f"학습 데이터: {len(train_df):,}행")
print(f"검증 데이터: {len(valid_df):,}행")

X_train = train_df[FEATURE_COLS]
X_valid = valid_df[FEATURE_COLS]

y_train_reg = train_df[TARGET_REG]
y_valid_reg = valid_df[TARGET_REG]

y_train_cls = train_df[TARGET_CLS]
y_valid_cls = valid_df[TARGET_CLS]

# -------------------------------------------------------------
# [6] 회귀 모델 학습
# -------------------------------------------------------------
print("\n=== [STEP 5] 회귀 모델 학습 ===")

reg_model = lgb.LGBMRegressor(
    n_estimators=2000,
    learning_rate=0.03,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)

reg_model.fit(
    X_train, y_train_reg,
    eval_set=[(X_valid, y_valid_reg)],
    eval_metric="rmse",
    callbacks=[
        lgb.early_stopping(stopping_rounds=100),
        lgb.log_evaluation(period=50)
    ]
)


# -------------------------------------------------------------
# [7] 분류 모델 학습
# -------------------------------------------------------------
print("\n=== [STEP 6] 분류 모델 학습 ===")

clf_model = lgb.LGBMClassifier(
    n_estimators=2000,
    learning_rate=0.03,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="binary",
    random_state=42
)

print("\n=== [STEP 6] 분류 모델 학습 ===")

clf_model = lgb.LGBMClassifier(
    n_estimators=2000,
    learning_rate=0.03,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="binary",
    random_state=42
)

clf_model.fit(
    X_train, y_train_cls,
    eval_set=[(X_valid, y_valid_cls)],
    eval_metric="binary_logloss",
    callbacks=[
        lgb.early_stopping(stopping_rounds=100),
        lgb.log_evaluation(period=50)
    ]
)


# -------------------------------------------------------------
# [8] 백업 처리
# -------------------------------------------------------------
if os.path.exists(FINAL_MODEL):
    backup = os.path.join(
        MODEL_DIR,
        f"REAL_Hoj_MODELENGINE_V25_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
    )
    os.rename(FINAL_MODEL, backup)
    print(f"\n[백업 완료] → {backup}")

# -------------------------------------------------------------
# [9] 모델 저장
# -------------------------------------------------------------
engine = {
    "reg_model": reg_model,
    "clf_model": clf_model,
    "feature_cols": FEATURE_COLS,
}

joblib.dump(engine, FINAL_MODEL)
print(f"\n[저장 완료] REAL 엔진 → {FINAL_MODEL}")

print("\n=== 🎉 모든 작업 완료! ===")
print("CLEAN DB 생성 + REAL 엔진 완전 재학습 + 날짜 컬럼 자동 탐지 완료.")
