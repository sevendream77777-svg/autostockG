import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import os
from datetime import datetime

# ==========================================================
#  설정
# ==========================================================
DB_FILE = "new_Hoj_DB_V25_FULL.parquet"
MODEL_FOLDER = "Hoj_MODELENGINE"
OUTPUT_MODEL = "REAL_Hoj_MODELENGINE_V25.pkl"

os.makedirs(MODEL_FOLDER, exist_ok=True)

# ==========================================================
#  유틸 함수
# ==========================================================
def backup_old_model():
    old_path = os.path.join(MODEL_FOLDER, OUTPUT_MODEL)
    if os.path.exists(old_path):
        ts = datetime.now().strftime("%y%m%d_%H%M%S")
        new_name = f"REAL_Hoj_MODELENGINE_V25_{ts}.pkl"
        new_path = os.path.join(MODEL_FOLDER, new_name)
        os.rename(old_path, new_path)
        print(f"[백업] 기존 모델 → {new_name}")


# ==========================================================
#  메인
# ==========================================================
def main():
    print("=== [1] FULL DB 로드 ===")
    df = pd.read_parquet(DB_FILE)
    print(f"전체 행수: {len(df):,}행")
    print(f"날짜 범위: {df['Date'].min()} ~ {df['Date'].max()}")

    # Target null 제거
    df = df.dropna(subset=["Expected_Return_5d", "Label_5d"])
    print(f"타겟 결측 제거 후: {len(df):,}행\n")

    # ==========================================================
    # 2. 학습/검증 구간 분리
    # ==========================================================
    print("=== [2] 학습/검증 구간 ===")
    df["Date"] = pd.to_datetime(df["Date"])

    train_end = pd.to_datetime("2024-11-04")
    valid_start = pd.to_datetime("2024-11-05")

    df_train = df[df["Date"] <= train_end]
    df_valid = df[df["Date"] >= valid_start]

    print(f"학습 구간: {df_train['Date'].min().date()} ~ {df_train['Date'].max().date()}  ({len(df_train):,}행)")
    print(f"검증 구간: {df_valid['Date'].min().date()} ~ {df_valid['Date'].max().date()}  ({len(df_valid):,}행)\n")

    # Feature / Target
    FEATURES = [
        "SMA_20","SMA_40","SMA_60","SMA_90","RSI_14","VOL_SMA_20",
        "MACD","MACD_Sig","BBP_20","ATR_14","STOCH_K","STOCH_D",
        "CCI_20","KOSPI_수익률","ALPHA_SMA_20","Return_1d"
    ]

    TARGET_REG = "Expected_Return_5d"
    TARGET_CLS = "Label_5d"

    X_train = df_train[FEATURES]
    y_train_reg = df_train[TARGET_REG]
    y_train_cls = df_train[TARGET_CLS]

    X_valid = df_valid[FEATURES]
    y_valid_reg = df_valid[TARGET_REG]
    y_valid_cls = df_valid[TARGET_CLS]

    # ==========================================================
    # 3. 회귀 모델 학습
    # ==========================================================
    print("=== [3] 회귀 모델 학습 (Expected_Return_5d) ===")

    reg_model = lgb.LGBMRegressor(
        n_estimators=2000,
        learning_rate=0.03,
        max_depth=-1,
        num_leaves=64,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="regression"
    )

    reg_model.fit(
        X_train, y_train_reg,
        eval_set=[(X_valid, y_valid_reg)],
        eval_metric="rmse",
        callbacks=[lgb.early_stopping(100)]
    )

    print(f"회귀 모델 best_iteration_: {reg_model.best_iteration_}\n")

    # ==========================================================
    # 4. 분류 모델 학습
    # ==========================================================
    print("=== [4] 분류 모델 학습 (Label_5d) ===")

    cls_model = lgb.LGBMClassifier(
        n_estimators=2000,
        learning_rate=0.03,
        max_depth=-1,
        num_leaves=64,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary"
    )

    cls_model.fit(
        X_train, y_train_cls,
        eval_set=[(X_valid, y_valid_cls)],
        eval_metric="binary_logloss",
        callbacks=[lgb.early_stopping(100)]
    )

    print(f"분류 모델 best_iteration_: {cls_model.best_iteration_}\n")

    # ==========================================================
    # 5. 저장 (기존 모델은 자동 백업)
    # ==========================================================
    print("=== [5] 모델 저장 ===")
    backup_old_model()

    out_path = os.path.join(MODEL_FOLDER, OUTPUT_MODEL)
    with open(out_path, "wb") as f:
        pickle.dump({"reg": reg_model, "cls": cls_model}, f)

    print(f"저장 완료 → {out_path}")
    print("\n=== 실전 엔진 학습 완료 ===")


# 실행
if __name__ == "__main__":
    main()
