from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# build_REAL_HOJ_V25.py
# -------------------------------------------------------------
# 1) 연구용 FULL DB -> 실전용 DB 생성 (불필요 컬럼 제거)
# 2) 실전용 DB로 REAL 엔진 학습
# 3) DB/엔진 백업 (날짜 + 중복 시 _1, _2 ...)
# -------------------------------------------------------------

import os
import shutil
import datetime
import pandas as pd
import lightgbm as lgb
import joblib

# ==============================
# [0] 경로/상수 정의
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_RESEARCH = os.path.join(BASE_DIR, HOJ_DB_RESEARCH)  # 연구용 FULL
DB_REAL = os.path.join(BASE_DIR, HOJ_DB_REAL)          # 실전용 CLEAN

ENGINE_RESEARCH = os.path.join(BASE_DIR, HOJ_ENGINE_RESEARCH)  # 연구 엔진 (참고용)
ENGINE_REAL = os.path.join(BASE_DIR, HOJ_ENGINE_REAL)          # 실전 엔진

# 백업 폴더
BACKUP_DB_RESEARCH = os.path.join(BASE_DIR, "backup", "DB_RESEARCH")
BACKUP_DB_REAL = os.path.join(BASE_DIR, "backup", "DB_REAL")
BACKUP_ENGINE_RESEARCH = os.path.join(BASE_DIR, "backup", "ENGINE_RESEARCH")
BACKUP_ENGINE_REAL = os.path.join(BASE_DIR, "backup", "ENGINE_REAL")

for p in [BACKUP_DB_RESEARCH, BACKUP_DB_REAL, BACKUP_ENGINE_RESEARCH, BACKUP_ENGINE_REAL]:
    os.makedirs(p, exist_ok=True)

# V25 표준 피처 15개
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


# ==============================
# [1] 공용 백업 함수
# ==============================
def backup_file(src_path: str, dst_dir: str, version: str = "V25"):
    """src_path 파일을 dst_dir에 V25_YYMMDD[_n].ext 형식으로 백업"""
    if not os.path.exists(src_path):
        return None

    os.makedirs(dst_dir, exist_ok=True)

    today = datetime.datetime.now().strftime("%y%m%d")  # 251113
    base = f"{version}_{today}"
    ext = os.path.splitext(src_path)[1]

    count = 0
    while True:
        suffix = f"_{count}" if count > 0 else ""
        dst_name = base + suffix + ext
        dst_path = os.path.join(dst_dir, dst_name)
        if not os.path.exists(dst_path):
            break
        count += 1

    shutil.copy2(src_path, dst_path)
    print(f"[백업 완료] {src_path} -> {dst_path}")
    return dst_path


# ==============================
# [2] 날짜 컬럼 자동 탐지
# ==============================
def find_date_column(df: pd.DataFrame) -> str:
    candidates = ["date", "Date", "날짜", "DATE"]
    for c in candidates:
        if c in df.columns:
            return c

    # dtype으로 마지막 시도
    dt_cols = df.select_dtypes(include=["datetime64[ns]", "datetime64"]).columns
    if len(dt_cols) > 0:
        return dt_cols[0]

    raise KeyError("날짜 컬럼을 찾을 수 없습니다. (date/Date/날짜 중 하나 필요)")


# ==============================
# [3] 메인 파이프라인
# ==============================
def main():
    # --- 3-1. 연구용 DB 로드 ---
    if not os.path.exists(DB_RESEARCH):
        raise FileNotFoundError(f"연구용 DB 파일을 찾을 수 없습니다: {DB_RESEARCH}")

    print("\n=== [STEP 1] 연구용 FULL DB 로드 ===")
    df = pd.read_parquet(DB_RESEARCH)
    print(f"[INFO] 연구용 FULL DB 행수: {len(df):,}")

    # --- 3-2. 불필요 컬럼 제거 & 실전 DB 생성 ---
    print("\n=== [STEP 2] 실전용 DB 생성 (불필요 컬럼 제거) ===")
    for col in REMOVE_COLS:
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f" - 제거됨: {col}")
        else:
            print(f" - 없음(무시): {col}")

    # 실전 DB 백업 (기존 파일이 있을 경우)
    if os.path.exists(DB_REAL):
        backup_file(DB_REAL, BACKUP_DB_REAL, version="V25")

    df.to_parquet(DB_REAL, index=False)
    print(f"[SAVE] 실전용 DB 저장 완료 → {DB_REAL}")
    print(f"[INFO] 컬럼 수: {len(df.columns)}개")

    # --- 3-3. 날짜 컬럼 인식 ---
    print("\n=== [STEP 3] 날짜 컬럼 인식 및 train/valid 분리 ===")
    date_col = find_date_column(df)
    print(f"[INFO] 날짜 컬럼: {date_col}")

    df[date_col] = pd.to_datetime(df[date_col])

    train_df = df[df[date_col] < "2024-11-05"]
    valid_df = df[df[date_col] >= "2024-11-05"]

    print(f"[INFO] 학습 데이터: {len(train_df):,}행")
    print(f"[INFO] 검증 데이터: {len(valid_df):,}행")

    # --- 3-4. 피처/타겟 분리 ---
    print("\n=== [STEP 4] 피처/타겟 분리 ===")

    missing_features = [c for c in FEATURE_COLS if c not in df.columns]
    if missing_features:
        print("[ERROR] DB에 존재하지 않는 피처 컬럼:")
        for c in missing_features:
            print("  -", c)
        raise KeyError("위 피처 컬럼이 DB에 없습니다.")

    X_train = train_df[FEATURE_COLS]
    X_valid = valid_df[FEATURE_COLS]

    y_train_reg = train_df[TARGET_REG]
    y_valid_reg = valid_df[TARGET_REG]

    y_train_cls = train_df[TARGET_CLS]
    y_valid_cls = valid_df[TARGET_CLS]

    print(f"[INFO] 피처 개수: {len(FEATURE_COLS)}개")

    # --- 3-5. 회귀 모델 학습 ---
    print("\n=== [STEP 5] 회귀 모델 학습 (Expected_Return_5d) ===")

    reg_model = lgb.LGBMRegressor(
        n_estimators=2000,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )

    reg_model.fit(
        X_train, y_train_reg,
        eval_set=[(X_valid, y_valid_reg)],
        eval_metric="rmse",
        callbacks=[
            lgb.early_stopping(stopping_rounds=100),
            lgb.log_evaluation(period=50),
        ]
    )

    # --- 3-6. 분류 모델 학습 ---
    print("\n=== [STEP 6] 분류 모델 학습 (Label_5d) ===")

    clf_model = lgb.LGBMClassifier(
        n_estimators=2000,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary",
        random_state=42,
    )

    clf_model.fit(
        X_train, y_train_cls,
        eval_set=[(X_valid, y_valid_cls)],
        eval_metric="binary_logloss",
        callbacks=[
            lgb.early_stopping(stopping_rounds=100),
            lgb.log_evaluation(period=50),
        ]
    )

    # --- 3-7. REAL 엔진 백업 & 저장 ---
    print("\n=== [STEP 7] REAL 엔진 저장 ===")

    if os.path.exists(ENGINE_REAL):
        backup_file(ENGINE_REAL, BACKUP_ENGINE_REAL, version="V25")

    engine = {
        "reg_model": reg_model,
        "clf_model": clf_model,
        "feature_cols": FEATURE_COLS,
    }

    joblib.dump(engine, ENGINE_REAL)
    print(f"[SAVE] REAL 엔진 저장 완료 → {ENGINE_REAL}")

    print("\n=== 🎉 모든 작업 완료! ===")
    print("1) 실전용 DB 생성 (HOJ_DB_REAL_V25.parquet)")
    print("2) REAL_HOJ 엔진 재학습 (HOJ_ENGINE_REAL_V25.pkl)")
    print("3) DB/엔진 백업 자동 처리 완료")


if __name__ == "__main__":
    main()
