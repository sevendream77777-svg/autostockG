# ============================================================
# Data Pipeline Runner (Stage 1 Executor)
#   - 순서: RAW 업데이트 -> 피처 생성 -> 통합 DB 생성
#   - 이 스크립트 하나로 데이터 준비 끝!
# ============================================================

import os
import sys
import time

# 모듈 임포트
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import update_raw_data
import build_features
import build_unified_db

def run_pipeline():
    start_time = time.time()
    print("\n🚀 [Stage 1] 데이터 팩토리 가동 시작...\n")

    # [Step 1] RAW 데이터 점검 및 백업
    try:
        print("\n>>> [1/3] RAW Data Check & Backup")
        update_raw_data.main()
    except Exception as e:
        print(f"❌ RAW 단계 실패: {e}")
        return

    # [Step 2] 피처 엔지니어링
    try:
        print("\n>>> [2/3] Feature Engineering (V31)")
        build_features.main()
    except Exception as e:
        print(f"❌ Feature 단계 실패: {e}")
        return

    # [Step 3] 통합 DB 빌드
    try:
        print("\n>>> [3/3] Building Unified DB")
        build_unified_db.build_unified_db()
    except Exception as e:
        print(f"❌ DB Build 단계 실패: {e}")
        return

    elapsed = time.time() - start_time
    print(f"\n✨ [Stage 1] 모든 데이터 준비 완료! ({elapsed:.1f}초 소요)")
    print("   이제 'Engine Manager'에서 학습(Train)을 시작할 수 있습니다.")

if __name__ == "__main__":
    run_pipeline()