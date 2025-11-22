# ============================================================
# build_HOJ_DB_REAL.py  (V31 - 15피처 기반 HOJ 실전용 DB)
# ============================================================

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config_paths import get_path, versioned_filename

import pandas as pd
import numpy as np
from datetime import timedelta

# ------------------------------------
# 설정
# ------------------------------------
FEATURE_FILE = r"F:\autostockG\MODELENGINE\FEATURE\features_V31.parquet"
SAVE_FILE = get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")


# ------------------------------------
# 5일 수익률 계산
# ------------------------------------
def compute_return_5d(df):
    df["Return_5d"] = df.groupby("Code")["Close"].shift(-5) / df["Close"] - 1
    return df


# ------------------------------------
# 분류 라벨 생성
# ------------------------------------
def create_label(df):
    df["Label_5d"] = (df["Return_5d"] > 0).astype(int)
    return df


# ------------------------------------
# 메인 DB 작성
# ------------------------------------
def build_hoj_real_db():
    print("=== [REAL] HOJ_DB V31 생성 시작 ===")
    print(f"📥 FEATURE 로드: {FEATURE_FILE}")

    df = pd.read_parquet(FEATURE_FILE)
    print(f"  - 로드 완료: {df.shape}")

    # 5일 수익률
    df = compute_return_5d(df)
    df = create_label(df)

    # NaN 제거
    before = len(df)
    df = df.dropna(subset=["Return_5d", "Label_5d"])
    after = len(df)
    print(f"  - 라벨 생성 후 NaN 제거: {before} → {after}")

    # 저장
    print(f"💾 저장: {SAVE_FILE}")

    # FM: backup first (rename existing), then write new
    if os.path.exists(SAVE_FILE):
        backup = versioned_filename(SAVE_FILE)  # uses EXISTING file's Date.max()
        os.rename(SAVE_FILE, backup)
        print(f"📑 백업 저장(원본 rename): {backup}")

    # write new
    df.to_parquet(SAVE_FILE, index=False)
    print(f"💾 저장 완료: {SAVE_FILE}")

    print("=== [REAL] HOJ_DB V31 생성 완료 ===")


# ------------------------------------
# 실행
# ------------------------------------
if __name__ == "__main__":
    build_hoj_real_db()
