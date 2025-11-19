# finance_collector.py (1일 목표로 최종 수정)

from kiwoom_api import KiwoomRestApi
from data_manager import DataManager
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any

# 목표 설정
TARGET_STOCK_CODE = "005930"
TARGET_START_DATE = "20230523"
TARGET_DAYS = 1 # <-- 1일치만 요청하도록 목표를 변경합니다.

# --- 데이터 필터링 및 정제 함수 ---
def parse_api_raw_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """API 응답의 raw data를 DataManager가 저장할 수 있는 표준 형식으로 파싱합니다."""
    clean_data = []
    
    for item in raw_data:
        try:
            clean_data.append({
                '일자': item['fmly_dt'], 
                'PBR': float(item['pbr_prc'].strip().replace('+', '').replace('-', '')),
                'PER': float(item['per'].strip().replace('+', '').replace('-', '')),
                '종가': int(item['prc'].strip().replace('+', '').replace('-', ''))
            })
        except (KeyError, ValueError, AttributeError) as e:
            print(f"⚠️ API 응답 데이터 구조 오류 (Skip): {e} in {item}")
            continue
    return clean_data
# --------------------------------------------------------------------------

def run_finance_collection():
    """재무 데이터 수집의 메인 로직을 실행합니다."""
    
    api = KiwoomRestApi()
    manager = DataManager()
    
    # 1. 파일에서 기존 데이터 및 복구 정보 로드
    recovery_info = manager.load_finance_data_for_recovery(
        stock_code=TARGET_STOCK_CODE, 
        max_age_days=365*10
    )
    
    current_data = recovery_info["data"] 
    start_next_key = recovery_info["next_key"] 
    
    # --------------------------------------------------------------------------
    # 🔑 핵심 로직: 로드된 기존 데이터의 키를 강제로 통일 (키 불일치 오류 해결)
    # --------------------------------------------------------------------------
    if current_data:
        temp_data = []
        for item in current_data:
            if 'fmly_dt' in item and '일자' not in item:
                item['일자'] = item.pop('fmly_dt')
            temp_data.append(item)
        current_data = temp_data
    # --------------------------------------------------------------------------
    
    
    if current_data:
        print(f"\n--- 💾 데이터 복구 완료 ---")
        print(f"기존 데이터 {len(current_data)}일치 확인. **수집 목표: {TARGET_DAYS}일**")
    else:
        print(f"\n--- 🚀 신규 수집 시작 ---")
        
    
    # 2. API 호출
    print(f"[{TARGET_STOCK_CODE}] API 연속 조회 시작. 목표: {TARGET_DAYS}일")
    
    api_response = api.fetch_daily_finance_history(
        stk_cd=TARGET_STOCK_CODE,
        base_dt=TARGET_START_DATE,
        max_fetch_count=TARGET_DAYS,
        start_next_key=start_next_key 
    )
    
    # 3. 결과 통합 및 저장
    
    if api_response.get('return_code') != '0':
        print(f"\n❌ 데이터 수집 실패: {api_response.get('return_msg')}")
        return

    raw_new_data = api_response.get('chart', []) 
    clean_new_data = parse_api_raw_data(raw_new_data) 
    final_next_key = api_response.get('next_key')
    
    # **데이터 병합 로직:**
    new_dates = {item['일자'] for item in clean_new_data} 
    filtered_current_data = [item for item in current_data if item['일자'] not in new_dates]
    final_data = clean_new_data + filtered_current_data
    
    # 최종 데이터 저장 (Next Key 포함)
    manager.save_finance_data(
        stock_code=TARGET_STOCK_CODE,
        finance_data=final_data,
        next_key=final_next_key
    )
    
    print(f"\n✅ 1일치 수집 프로세스 최종 완료! 총 {len(final_data)}일치 데이터 확보.")
    if final_data:
        print(f"최신 데이터 일자: {final_data[0]['일자']} | 과거 데이터 일자: {final_data[-1]['일자']}")


if __name__ == '__main__':
    print("--- 재무 데이터 독립 수집기 실행 ---")
    try:
        run_finance_collection()
    except Exception as e:
        print(f"\n❌ Critical Error during collection: {e}")