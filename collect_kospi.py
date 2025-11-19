import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta

# --- 1. 설정 ---
# 10년 치 기간 설정
end_date_dt = datetime.now()
start_date_dt = end_date_dt - timedelta(days=3650) # 10년

end_date_str = end_date_dt.strftime("%Y%m%d")
start_date_str = start_date_dt.strftime("%Y%m%d")

print(f"KOSPI 지수 10년치 데이터를 수집합니다...")
print(f"기간: {start_date_str} ~ {end_date_str}")

# --- 2. KOSPI 지수 데이터 가져오기 ---
# KOSPI의 티커는 '1001'입니다.
try:
    df_kospi = stock.get_index_ohlcv(start_date_str, end_date_str, "1001")
    
    # '날짜' 컬럼을 인덱스에서 컬럼으로 재설정 (다루기 편하게)
    df_kospi.reset_index(inplace=True)
    df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])
    
    # '시가', '고가', '저가', '종가', '거래량' 컬럼만 남김
    df_kospi = df_kospi[['날짜', '시가', '고가', '저가', '종가', '거래량']]
    
    # --- 3. KOSPI 파일로 저장 ---
    output_file = "kospi_index_10y.parquet"
    df_kospi.to_parquet(output_file, index=False)
    
    print("-" * 30)
    print(f"KOSPI 지수 저장 완료! (총 {len(df_kospi)}일치)")
    print(f"'{output_file}' 파일로 저장되었습니다.")
    print("\nKOSPI 데이터 샘플:")
    print(df_kospi.tail())

except Exception as e:
    print(f"KOSPI 데이터 수집 중 오류 발생: {e}")