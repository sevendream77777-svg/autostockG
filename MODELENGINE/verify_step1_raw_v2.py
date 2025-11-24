import pandas as pd
import os
import glob

# 경로 설정
base_path = 'MODELENGINE/RAW'
kospi_dir = os.path.join(base_path, 'kospi_data')
stock_file_path = os.path.join(base_path, 'stocks', 'all_stocks_cumulative.parquet')

print('='*60)
print('[Step 1] 원시 데이터(Raw Data) 정합성 점검 V2 (Parquet 기반)')
print('='*60)

# -------------------------------------------------------------------------
# 1. KOSPI 데이터 확인
# -------------------------------------------------------------------------
print('\n[1. KOSPI 데이터 확인]')
if not os.path.exists(kospi_dir):
    print(f'[Error] KOSPI 폴더를 찾을 수 없습니다: {kospi_dir}')
    print('-> make_kospi_index_10y.py 실행 필요.')
else:
    # 폴더 내 가장 최신 parquet 파일 찾기
    parquet_files = glob.glob(os.path.join(kospi_dir, '*.parquet'))
    if not parquet_files:
        print(f'[Error] {kospi_dir} 안에 .parquet 파일이 없습니다.')
        print('-> make_kospi_index_10y.py 실행 필요.')
    else:
        # 가장 최근에 수정된 파일 선택
        latest_kospi_file = max(parquet_files, key=os.path.getmtime)
        print(f'   Target File: {os.path.basename(latest_kospi_file)}')
        
        try:
            df_kospi = pd.read_parquet(latest_kospi_file)
            
            # 컬럼 확인 ('Date' 컬럼 필수)
            if 'Date' in df_kospi.columns:
                df_kospi['Date'] = pd.to_datetime(df_kospi['Date'])
                df_kospi = df_kospi.sort_values('Date')
                
                print(f'[OK] KOSPI 로드 성공 ({len(df_kospi)} rows)')
                print(f'   - 기간: {df_kospi["Date"].iloc[0].date()} ~ {df_kospi["Date"].iloc[-1].date()}')
                
                # Null 체크
                null_cnt = df_kospi.isnull().sum().sum()
                if null_cnt > 0:
                    print(f'[Warning] KOSPI 데이터에 결측치(NaN) 존재: {null_cnt}개')
                else:
                    print('   - 결측치(NaN) 없음: Pass')
            else:
                print('[Error] KOSPI 파일에 "Date" 컬럼이 없습니다.')
                print(f'   - 발견된 컬럼: {df_kospi.columns.tolist()}')
                
        except Exception as e:
            print(f'[Error] KOSPI 파일 읽기 실패: {e}')

# -------------------------------------------------------------------------
# 2. 주식 통합 데이터 확인 (삼성전자 005930 추출)
# -------------------------------------------------------------------------
print('\n[2. 주식 통합 데이터(all_stocks_cumulative) 확인]')
if not os.path.exists(stock_file_path):
    print(f'[Error] 주식 통합 파일을 찾을 수 없습니다: {stock_file_path}')
    print('-> safe_raw_builder_v2.py 실행 필요 (시간이 좀 걸릴 수 있음)')
else:
    try:
        print(f'   Target File: {os.path.basename(stock_file_path)}')
        # 전체 파일을 다 읽으면 메모리 부족할 수 있으므로, 
        # 일단 읽어서 삼성전자만 필터링 (Parquet는 컬럼별 로딩은 쉽지만 로우 필터링은 라이브러리에 따라 다름)
        # 여기선 pandas로 읽고 필터링 (파일이 아주 크면 pyarrow 등을 써야 함)
        
        # 팁: 일부 컬럼만 읽어서 확인
        cols = ['Date', 'Code', 'Close', 'Volume']
        try:
            df_all = pd.read_parquet(stock_file_path, columns=cols)
        except:
            df_all = pd.read_parquet(stock_file_path) # 컬럼 지정 실패시 전체 로드

        print(f'[Check] 전체 데이터 로드 완료 ({len(df_all)} rows)')
        
        # 삼성전자(005930) 필터링
        target_code = '005930' 
        # Code 컬럼이 숫자일 수도 있고 문자일 수도 있음. 처리.
        df_all['Code'] = df_all['Code'].astype(str).str.zfill(6)
        
        df_stock = df_all[df_all['Code'] == target_code].copy()
        
        if df_stock.empty:
            print(f'[Error] 통합 파일 내에 삼성전자({target_code}) 데이터가 없습니다.')
        else:
            df_stock['Date'] = pd.to_datetime(df_stock['Date'])
            df_stock = df_stock.sort_values('Date')
            
            print(f'[OK] 삼성전자({target_code}) 데이터 추출 성공 ({len(df_stock)} rows)')
            print(f'   - 기간: {df_stock["Date"].iloc[0].date()} ~ {df_stock["Date"].iloc[-1].date()}')
            
            # 데이터 순서 확인
            is_monotonic = df_stock['Date'].is_monotonic_increasing
            print(f'   - 날짜 오름차순 정렬 여부: {is_monotonic}')
            if not is_monotonic:
                print('   [Critical Warning] 날짜가 뒤섞여 있습니다! 학습 데이터 생성 시 치명적 오류 발생 가능.')

            # KOSPI와 날짜 매칭 확인
            if 'df_kospi' in locals():
                common = pd.merge(df_kospi, df_stock, on='Date', how='inner')
                match_rate = len(common) / len(df_stock) * 100
                print(f'[Check] KOSPI - 삼성전자 날짜 매칭률: {match_rate:.2f}% ({len(common)}/{len(df_stock)})')
                
                if match_rate < 90:
                    print('[Warning] 날짜 매칭률이 90% 미만입니다. 휴장일 처리나 데이터 누락 확인 필요.')
                else:
                    print('   - 날짜 매칭 양호: Pass')

    except Exception as e:
        print(f'[Error] 주식 파일 읽기 실패: {e}')
        print('   -> pyarrow 또는 fastparquet 라이브러리가 설치되어 있어야 합니다.')

print('='*60)
