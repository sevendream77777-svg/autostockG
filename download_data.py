# download_data.py - V34 프로젝트 장기 데이터 다운로드 실행 파일

import sys
from datetime import datetime
from kiwoom_api import KiwoomRestApi 
from data_manager import DataManager 
from typing import List

# ==========================================================
# 다운로드 설정
# ==========================================================
TARGET_STOCKS: List[str] = [
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "051910",  # LG화학
    # TODO: 여기에 다운로드할 모든 종목 코드를 추가하세요.
]
TARGET_CHART_DAYS: int = 2500 # 10년치 데이터 목표 (약 2500 영업일)


def run_data_download():
    """
    모든 목표 종목에 대해 10년치 일봉 데이터를 다운로드하고 DataManager에 저장합니다.
    """
    print("--- 📥 V34 호엔진 장기 데이터 다운로드 시작 ---")
    
    try:
        # 1. API 클라이언트 초기화 (통신 엔진)
        api_client = KiwoomRestApi()
        
        # 2. DataManager 초기화 (저장 엔진)
        manager = DataManager()
        print(f"✅ DataManager 준비 완료. 저장 경로: {manager.data_path}")

        
        # 3. 데이터 다운로드 루프
        for stock_code in TARGET_STOCKS:
            print(f"\n[종목: {stock_code}] 데이터 처리 시작...")
            
            # 3-1. 파일에 저장된 데이터 로드 시도 (만료 여부 확인)
            loaded_data = manager.load_chart_data(stock_code, max_age_days=30) # 30일 이내 파일은 재사용
            
            if loaded_data and len(loaded_data) >= TARGET_CHART_DAYS:
                print(f"🟡 데이터 재사용: {stock_code}의 {len(loaded_data)}일치 데이터가 유효합니다.")
                continue # 다음 종목으로 넘어감

            # 3-2. API를 통한 데이터 수집 실행 (가상 데이터 주입 없음. 실제 서버 연결)
            base_date = datetime.now().strftime('%Y%m%d')
            chart_response = api_client.get_stock_daily_chart_continuous(
                stk_cd=stock_code, 
                base_dt=base_date, 
                upd_stkpc_tp="1",
                target_days=TARGET_CHART_DAYS
            )
            
            if str(chart_response.get('return_code')) != '0':
                print(f"❌ 다운로드 실패: {chart_response.get('return_msg')}")
                continue
                
            final_data = chart_response.get('chart', [])
            
            print(f"✅ 다운로드 완료! 총 확보 일봉 수: {len(final_data)}개")
            
            # 3-3. DataManager를 통해 파일에 저장
            if final_data:
                manager.save_chart_data(stock_code, final_data)
        
        print("\n--- 🏁 모든 종목 다운로드 요청 완료 ---")

    except Exception as e:
        print(f"\n[다운로드 프로그램 메인 오류]: {e}")
        
if __name__ == '__main__':
    run_data_download()