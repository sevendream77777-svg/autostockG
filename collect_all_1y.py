import pandas as pd
from pykrx import stock
import time
import os
from datetime import datetime, timedelta

# --- 1. 설정 ---
# 오늘 날짜를 기준으로 10년 전 날짜를 계산합니다.
end_date_dt = datetime.now()
start_date_dt = end_date_dt - timedelta(days=3650)

# pykrx가 요구하는 YYYYMMDD 형식의 문자열로 변환
end_date_str = end_date_dt.strftime("%Y%m%d")
start_date_str = start_date_dt.strftime("%Y%m%d")

# 데이터를 저장할 폴더 이름
output_dir = "stock_data_10y_ALL" # (전체 데이터임을 표시)

# --- 2. 폴더 생성 ---
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"'{output_dir}' 폴더를 생성했습니다.")

# --- 3. 전체 종목 티커(코드) 가져오기 ---
# KOSPI, KOSDAQ, KONEX 전체 종목 코드를 가져옵니다.
print("전체 종목 코드를 가져오는 중...")
tickers = stock.get_market_ticker_list(market="ALL")
total_count = len(tickers)
print(f"대상 종목 수: {total_count}개")
print(f"데이터 수집 기간: {start_date_str} ~ {end_date_str}")
print("-" * 30)


# --- 4. 데이터 수집 시작 ---
print("데이터 수집을 시작합니다. (예상 소요 시간: 약 40~60분)")

for i, ticker in enumerate(tickers):
    # 진행 상황 표시
    progress_str = f"[{i+1}/{total_count}] {ticker}"

    try:
        # 1년치 OHLCV(시가,고가,저가,종가,거래량) 데이터 조회
        df = stock.get_market_ohlcv(start_date_str, end_date_str, ticker)

        # 1년 내에 상장했거나 데이터가 없는 경우 건너뜀
        if df.empty:
            print(f"{progress_str} (데이터 없음, 건너뜀)")
            continue

        # 데이터를 CSV 파일로 저장
        # (파일 경로: stock_data_1y_ALL/005930.csv)
        file_path = os.path.join(output_dir, f"{ticker}.csv")
        df.to_csv(file_path, encoding='utf-8-sig') # 한글 깨짐 방지

        print(f"{progress_str} 저장 완료.")

    except Exception as e:
        # 예상치 못한 에러 발생 시 (예: 네트워크 오류)
        print(f"{progress_str} 처리 중 에러 발생: {e}")

    # (필수!) IP 차단을 방지하기 위한 랜덤 딜레이
    # 0.5초 ~ 1.5초 사이의 랜덤한 시간 동안 대기합니다.
    time.sleep(0.5 + (time.time() % 1000) / 1000)

print("-" * 30)
print(f"모든 데이터 수집이 완료되었습니다. '{output_dir}' 폴더를 확인하세요.")