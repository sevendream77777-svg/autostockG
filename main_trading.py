# main_trading.py - V34 호엔진 프로젝트 최종 버전 (카카오 알림 통합 및 오류 수정)

import configparser
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
from kakao_notifier import KakaoNotifier 

# ... (read_config_for_api, calculate_moving_average 함수는 이전과 동일) ...
# (코드가 길어 여기에 모든 유틸리티 함수를 반복하지 않습니다. 이전 단계의 로직을 유지합니다.)
def read_config_for_api():
    config_parser = configparser.ConfigParser()
    config_parser.read('config.ini', encoding='utf-8')
    try:
        return config_parser.getboolean('KIWOOM', 'IS_MOCK_MODE', fallback=True)
    except Exception:
        return True 

def calculate_moving_average(data: List[Dict[str, str]], days: int) -> float:
    if not data or len(data) < days:
        return 0.0
    
    recent_data = data[:days]
    total_price = 0
    valid_count = 0
    for item in recent_data:
        try:
            price = int(item.get('prc', 0)) # prc 키가 없으면 0으로 처리
            if price > 0:
                total_price += price
                valid_count += 1
        except ValueError:
            continue
            
    if valid_count == 0:
        return 0.0
        
    return total_price / valid_count


# ==========================================================
# 메인 전략 실행 함수 (호엔진)
# ==========================================================

def run_trading_strategy(api_client: KiwoomRestApi, notifier: KakaoNotifier): 
    
    # -----------------------------------------------
    # 🌟 [설정] 매매 대상 종목 및 KRX 구분 코드 🌟
    # -----------------------------------------------
    TARGET_STOCKS = [
        "005930",  # 삼성전자
        "000660",  # SK하이닉스
    ]
    DMST_STEX_TP = "KRX"
    HOLDING_DAYS = 5
    TARGET_CHART_DAYS = 3
    
    
    # 1. 장 운영 시간 확인 및 잔고/보유 종목 확인 
    current_time = datetime.now()
    current_hour = current_time.hour
    is_market_open = (9 <= current_hour < 16)
    
    if not is_market_open:
         print(f"🔔 현재 장 마감 시간({current_time.strftime('%H:%M')})입니다. 매매 로직(매수/매도)은 실행하지 않습니다.")
         
    print("\n--- 1. 현재 계좌 잔고 및 보유 종목 확인 ---")
    
    balance_response = api_client.get_account_balance_details(qry_tp="2", dmst_stex_tp=DMST_STEX_TP)
    
    if str(balance_response.get('return_code')) != '0':
        print(f"❌ 잔고 조회 실패: {balance_response.get('return_msg')}")
        return
        
    asset_info = balance_response 
    asset_value = int(asset_info.get('prsm_dpst_aset_amt', 0))
    print(f"✅ 초기 잔고 조회 성공. 추정예탁자산: {asset_value:,} 원")
    

    # 2. 호엔진 매도 전략 실행 
    print("\n--- 2. 호엔진 매도 전략 실행 ---")
    if not is_market_open:
        print("🟡 장 마감 시간: 매도 로직 실행 건너뜀.")
    else:
        pass


    # 3. 호엔진 매수 전략 실행 (데이터 조회 및 매수)
    print(f"\n--- 3. 호엔진 매수 전략 실행 ---")
    
    for stock_code in TARGET_STOCKS:
        print(f"\n[종목: {stock_code}] 데이터 조회 시작...")

        data_response = api_client.get_stock_daily_chart_continuous(
            stk_cd=stock_code, 
            base_dt=datetime.now().strftime('%Y%m%d'), 
            upd_stkpc_tp="1", 
            target_days=TARGET_CHART_DAYS
        )
        
        daily_data = data_response.get('chart', [])
        
        if not daily_data:
            print(f"⚠️ {stock_code} 일별 차트 데이터 조회 실패 또는 데이터 부족.")
            continue 

        # 이평선 계산 (3일 데이터 사용)
        ma5 = calculate_moving_average(daily_data, 3) 
        ma20 = calculate_moving_average(daily_data[1:], 3) 
        
        # 골든 크로스 조건 체크 (MA3(현재) > MA3(이전)로 최종 테스트)
        is_golden_cross = ma5 > ma20
        
        print(f"  > [이평선] MA3(현재)={ma5:.2f}, MA3(이전)={ma20:.2f}") 


        if not is_market_open:
             print("🟡 장 마감 시간: 매수 조건 체크 건너뜔.")
             
        elif is_golden_cross:
            print(f"🚀 [매수 신호]: {stock_code} 골든 크로스 발생! 주문 실행 준비.")
            
            order_quantity = "1" 
            
            order_response = api_client.buy_order(
                dmst_stex_tp="KRX", 
                stk_cd=stock_code, 
                ord_qty=order_quantity, 
                ord_uv="", 
                trde_tp="3"
            )
            
            if str(order_response.get('return_code')) == '0':
                order_no = order_response.get('ord_no')
                print(f"🎉 매수 주문 성공! 주문번호: {order_no}")
                
                # 🌟 [핵심 로직] 카카오톡 알림 전송
                message = (f"🎉 호엔진 매수 알림\n\n"
                           f"종목: {stock_code}\n"
                           f"수량: {order_quantity}주 (시장가)\n"
                           f"주문번호: {order_no}\n"
                           f"-----------------\n"
                           f"MA3: {ma5:.2f} (골든 크로스)")
                           
                notifier.send_message(message)
                
            else:
                print(f"⚠️ 매수 주문 실패: {order_response.get('return_msg')}")
                
        else:
            print("🟡 [대기]: 매수 조건 미충족.")


if __name__ == '__main__':
    # 1. 설정값 읽기 및 API 클라이언트 초기화
    IS_MOCK_MODE = read_config_for_api() 
    print(f"모드: {'모의투자' if IS_MOCK_MODE else '실전투자'}")

    try:
        api_client = KiwoomRestApi()
        notifier = KakaoNotifier() 
        run_trading_strategy(api_client, notifier)

    except Exception as e:
        print(f"\n[프로그램 메인 오류]: {e}")
