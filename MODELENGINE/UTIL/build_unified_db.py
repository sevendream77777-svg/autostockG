# ============================================================
# Unified DB Builder (V32)
#   - Feature 파일을 로드하여 통합 DB(HOJ_DB_V31.parquet) 생성
#   - 기존 REAL/RESEARCH 분리 방식을 폐기하고 단일 파일로 관리
# ============================================================

import os
import sys
import pandas as pd
import numpy as np

# 프로젝트 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_paths import get_path, versioned_filename

def build_unified_db():
    # 1. 경로 설정
    feat_path = get_path("FEATURE", "features_V31.parquet")
    db_dir = get_path("HOJ_DB")
    db_path = os.path.join(db_dir, "HOJ_DB_V31.parquet")

    print("=" * 60)
    print("[DB BUILDER] 통합 DB 생성 시작 (HOJ_DB_V31)")
    print(f"  📥 입력: {feat_path}")
    print(f"  💾 출력: {db_path}")

    # 2. Feature 파일 로드
    if not os.path.exists(feat_path):
        print("❌ [Error] 피처 파일이 없습니다. build_features.py를 먼저 실행하세요.")
        return

    try:
        df = pd.read_parquet(feat_path)
        print(f"  ✅ 피처 로드 성공: {len(df):,} rows")
    except Exception as e:
        print(f"❌ 피처 로드 실패: {e}")
        return

    # 3. 데이터 검증 및 정렬
    required_cols = ["Date", "Code", "Close"] # 최소 필수 컬럼
    if not set(required_cols).issubset(df.columns):
        print(f"❌ 필수 컬럼 누락: {set(required_cols) - set(df.columns)}")
        return

    # 날짜 형식 보장
    if not np.issubdtype(df["Date"].dtype, np.datetime64):
        df["Date"] = pd.to_datetime(df["Date"])

    # 정렬 (날짜 오름차순)
    df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

    # 데이터 기간 확인
    min_date = df["Date"].min().date()
    max_date = df["Date"].max().date()
    print(f"  📅 데이터 기간: {min_date} ~ {max_date}")

    # 4. 저장 (기존 파일 백업 후 저장)
    os.makedirs(db_dir, exist_ok=True)

    if os.path.exists(db_path):
        try:
            backup = versioned_filename(db_path)
            os.rename(db_path, backup)
            print(f"  📦 기존 DB 백업 완료: {os.path.basename(backup)}")
        except Exception as e:
            print(f"  ⚠ 백업 중 오류 (무시됨): {e}")

    try:
        df.to_parquet(db_path, index=False)
        print(f"  🎉 [완료] 통합 DB 저장 성공: {os.path.basename(db_path)}")
    except Exception as e:
        print(f"❌ DB 저장 실패: {e}")

if __name__ == "__main__":
    build_unified_db()