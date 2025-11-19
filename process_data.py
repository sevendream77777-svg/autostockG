import pandas as pd
import pandas_ta as ta
import numpy as np
from tqdm import tqdm

# (필수 설치) 
# pip install pandas-ta

# 1. 2.1단계에서 만든 통합 파일 불러오기
input_file = "stock_data_10y_combined.parquet"
print(f"'{input_file}' 파일을 불러오는 중... (시간이 다소 걸릴 수 있음)")
try:
    df = pd.read_parquet(input_file)
except Exception as e:
    print(f"파일 읽기 오류: {e}")
    print("이전 단계(combine_data.py)가 성공적으로 완료되었는지 확인하세요.")
    exit()

print("파일 불러오기 완료.")
print(f"원본 데이터 행 수: {len(df)}")


# 2. (필수!) 기술적 지표 계산
#    지표는 '종목별'로 따로 계산해야 함. (A종목 20일선, B종목 20일선...)
#    pandas의 groupby()와 apply()를 사용하면 종목별 계산을 효율적으로 할 수 있음.

print("기술적 지표 계산을 시작합니다 (종목별로 처리 중)...")

# 우리가 추가할 지표 리스트 (시나리오 2 가설 기반)
# (나중에 이 리스트만 수정하면 다른 지표도 추가 가능)

def calculate_indicators(group_df):
    """한 종목(group_df)을 받아서 기술적 지표를 계산하고 반환"""
    
    # 1. 이동평균선 (SMA - Simple Moving Average)
    group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
    group_df['SMA_60'] = ta.sma(group_df['종가'], length=60)
    
    # 2. RSI (Relative Strength Index)
    group_df['RSI_14'] = ta.rsi(group_df['종가'], length=14)
    
    # 3. 거래량 이동평균
    group_df['VOL_SMA_20'] = ta.sma(group_df['거래량'], length=20)
    
    # 4. MACD (Moving Average Convergence Divergence)
    #    ta.macd()는 3개의 값(MACD, MACD_Signal, MACD_Hist)을 반환함
    macd = ta.macd(group_df['종가'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        group_df['MACD'] = macd.iloc[:, 0]      # MACD 값
        group_df['MACD_Sig'] = macd.iloc[:, 1]  # Signal 값

    return group_df

# groupby('종목코드')로 2800개 종목을 각각 분리하여 calculate_indicators 함수 적용
# tqdm.pandas()는 groupby.apply()의 진행 상황을 보여줌
tqdm.pandas(desc="Calculating Indicators")
df_processed = df.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators)


# 3. (매우 중요!) 결측치(NaN) 제거
#    SMA_60을 계산했다면, 각 종목의 '첫 59일'은 값이 NaN(비어있음)이 됩니다.
#    모델은 NaN 값을 학습할 수 없으므로, 이 행들은 '전부' 삭제해야 합니다.
original_rows = len(df_processed)
df_processed.dropna(inplace=True)
cleaned_rows = len(df_processed)

print("-" * 30)
print("결측치(NaN) 제거 완료.")
print(f"제거된 행 수: {original_rows - cleaned_rows} (주로 각 종목의 초반 60일 데이터)")


# 4. 최종 결과물 저장
output_file = "stock_data_10y_processed.parquet"
df_processed.to_parquet(output_file, index=False)

print(f"모든 처리 완료! '{output_file}' 파일로 저장되었습니다.")
print(f"최종 학습 데이터 행 수: {cleaned_rows}")
print("\n최종 데이터 샘플 5줄 (지표 추가 확인):")
print(df_processed.tail()) # tail()로 마지막 데이터 확인