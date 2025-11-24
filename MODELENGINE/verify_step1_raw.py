import pandas as pd
import os
import sys

# 경로 설정 (사용자 환경에 맞게 수정 가능)
base_path = 'MODELENGINE/RAW'
kospi_file = os.path.join(base_path, 'kospi_index_10y.csv')
# 샘플로 삼성전자(005930) 파일 확인. pykrx 덤프 방식에 따라 경로가 다를 수 있으니 확인 필요
# 보통 stocks 폴더 내에 있거나 pykrx_dump 폴더 내에 있을 수 있습니다.
# 업로드된 파일 구조를 보면 'stocks' 폴더가 보이므로 그쪽을 우선 타겟합니다.
stock_code = '005930' # 삼성전자
stock_file_path = os.path.join(base_path, 'stocks', f'{stock_code}.csv')

print('='*50)
print('[Step 1] 원시 데이터(Raw Data) 정합성 점검')
print('='*50)

# 1. KOSPI 데이터 확인
if not os.path.exists(kospi_file):
    print(f'[Error] KOSPI 파일을 찾을 수 없습니다: {kospi_file}')
    print('-> make_kospi_index_10y.py 를 먼저 실행해야 할 수 있습니다.')
else:
    try:
        df_kospi = pd.read_csv(kospi_file)
        # 날짜 컬럼 찾기 (보통 'Date' 또는 '날짜')
        date_col = [c for c in df_kospi.columns if 'date' in c.lower() or '날짜' in c]
        if date_col:
            df_kospi[date_col[0]] = pd.to_datetime(df_kospi[date_col[0]])
            df_kospi = df_kospi.sort_values(by=date_col[0])
            print(f'[OK] KOSPI 파일 로드 성공 ({len(df_kospi)} rows)')
            print(f'   - 기간: {df_kospi[date_col[0]].iloc[0].date()} ~ {df_kospi[date_col[0]].iloc[-1].date()}')
            
            # Null 체크
            if df_kospi.isnull().sum().sum() > 0:
                print('[Warning] KOSPI 데이터에 결측치(NaN)가 존재합니다.')
                print(df_kospi.isnull().sum())
            else:
                print('   - 결측치(NaN) 없음: Pass')
        else:
            print('[Error] KOSPI 파일에서 날짜 컬럼을 찾을 수 없습니다.')
    except Exception as e:
        print(f'[Error] KOSPI 파일 읽기 실패: {e}')

print('-'*30)

# 2. 개별 종목 데이터(삼성전자) 확인
if not os.path.exists(stock_file_path):
    print(f'[Error] 종목 파일을 찾을 수 없습니다: {stock_file_path}')
    print('-> 경로가 다르다면 verify_step1_raw.py 파일의 stock_file_path를 수정해주세요.')
    # 파일이 없다면 MODELENGINE/RAW 폴더 리스트를 출력해 힌트 제공
    if os.path.exists(base_path):
        print(f'   참고: {base_path} 내부 폴더 목록: {os.listdir(base_path)}')
else:
    try:
        df_stock = pd.read_csv(stock_file_path)
        date_col_s = [c for c in df_stock.columns if 'date' in c.lower() or '날짜' in c]
        
        if date_col_s:
            df_stock[date_col_s[0]] = pd.to_datetime(df_stock[date_col_s[0]])
            df_stock = df_stock.sort_values(by=date_col_s[0])
            print(f'[OK] 삼성전자(005930) 파일 로드 성공 ({len(df_stock)} rows)')
            print(f'   - 기간: {df_stock[date_col_s[0]].iloc[0].date()} ~ {df_stock[date_col_s[0]].iloc[-1].date()}')
            
            # 데이터 순서 확인 (내림차순인지 오름차순인지)
            is_monotonic = df_stock[date_col_s[0]].is_monotonic_increasing
            print(f'   - 날짜 오름차순 정렬 여부: {is_monotonic}')
            
            if not is_monotonic:
                print('   [Warning] 데이터가 날짜순으로 정렬되어 있지 않습니다. 학습 시 순서 꼬임의 원인이 됩니다.')

            # 3. 날짜 매칭 (Inner Join 테스트)
            # KOSPI 데이터와 종목 데이터를 합칠 때 날짜가 얼마나 겹치는지 확인
            if 'df_kospi' in locals():
                common_dates = pd.merge(df_kospi, df_stock, left_on=date_col[0], right_on=date_col_s[0], how='inner')
                print(f'[Check] KOSPI와 종목 데이터 날짜 교집합 개수: {len(common_dates)}')
                
                if len(common_dates) < len(df_stock) * 0.9:
                    print('[Warning] KOSPI 데이터와 종목 데이터의 날짜 매칭률이 낮습니다. 휴장일 처리가 다르거나 데이터 누락 가능성 있음.')
                else:
                    print('   - 날짜 매칭 상태 양호.')

        else:
            print('[Error] 종목 파일에서 날짜 컬럼을 찾을 수 없습니다.')
            
    except Exception as e:
        print(f'[Error] 종목 파일 읽기 실패: {e}')

print('='*50)
