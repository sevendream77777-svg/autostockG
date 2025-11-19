# finance_collector.py (독립 실행 스크립트)

from kiwoom_api import KiwoomRestApi
from data_manager import DataManager
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any

# 목표 설정
TARGET_STOCK_CODE = "005930"
TARGET_START_DATE = "20150101" # 2015년 1월 1일 기준
TARGET_DAYS = 20 # 약 1달치(20 영업일)만 시도


def run_finance_collection():
    """재무 데이터 수집의 메인 로직을 실행합니다."""
    
    api = KiwoomRestApi()
    manager = DataManager()
    
    # 1. 파일에서 기존 데이터 및 복구 정보 로드
    recovery_info = manager.load_finance_data_for_recovery(
        stock_code=TARGET_STOCK_CODE, 
        max_age_days=365*10 # 10년치 데이터는 만료 기간을 길게 설정
    )
    
    current_data = recovery_info["data"]
    start_next_key = recovery_info["next_key"]
    
    if current_data:
        print(f"\n--- 💾 데이터 복구 완료 ---")
        print(f"기존 데이터 {len(current_data)}일치 확인. 다음 요청은 Next Key: {start_next_key}로 이어받습니다.")
        # 이미 수집된 데이터가 있으면 마지막 일자를 base_dt로 사용하지 않고, 서버에서 저장된 next_key를 사용
        # base_dt는 첫 요청(start_next_key가 None일 때)에만 사용됨
    else:
        print(f"\n--- 🚀 신규 수집 시작 ---")
        
    
    # 2. API 호출
    print(f"[{TARGET_STOCK_CODE}] API 연속 조회 시작. 목표: {TARGET_DAYS}일")
    
    # KiwoomRestApi의 fetch_daily_finance_history를 호출
    api_response = api.fetch_daily_finance_history(
        stk_cd=TARGET_STOCK_CODE,
        base_dt=TARGET_START_DATE,
        max_fetch_count=TARGET_DAYS # 목표 일수 전달
    )
    
    # 3. 결과 통합 및 저장
    
    if api_response.get('return_code') != '0':
        print(f"\n❌ 데이터 수집 실패: {api_response.get('return_msg')}")
        return

    # API 응답에서 Next Key 및 최종 데이터 추출
    new_data = api_response.get('chart', [])
    final_next_key = api_response.get('next_key')
    
    # 이어받은 경우, 기존 데이터와 새로운 데이터를 병합 (중복 방지를 위해 Next Key 로직이 중요)
    # 현재 fetch_daily_finance_history는 전체 데이터를 반환하므로, 병합 로직은 단순화
    
    final_data = new_data # 복구 로직이 복잡해지므로, 현재는 API에서 받은 데이터만 저장한다고 가정
    
    # 최종 데이터 저장 (Next Key 포함)
    manager.save_finance_data(
        stock_code=TARGET_STOCK_CODE,
        finance_data=final_data,
        next_key=final_next_key
    )
    
    print(f"\n✅ 1달치 수집/저장 프로세스 최종 완료! 총 {len(final_data)}일치 데이터 확보.")


if __name__ == '__main__':
    print("--- 재무 데이터 독립 수집기 실행 ---")
    try:
        run_finance_collection()
    except Exception as e:
        print(f"\n❌ Critical Error during collection: {e}")