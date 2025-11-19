import pandas as pd
import pandas_ta as ta
import numpy as np
from tqdm import tqdm

# (필수 설치) 
# pip install pandas-ta

# --- 1. 데이터 불러오기 ---
stock_file = "stock_data_10y_combined.parquet"
kospi_file = "kospi_index_10y.parquet"

print(f"'{stock_file}' (개별 종목) 파일을 불러오는 중...")
try:
    df_stocks = pd.read_parquet(stock_file)
    df_stocks['날짜'] = pd.to_datetime(df_stocks['날짜'])
except Exception as e:
    print(f"개별 종목 파일 읽기 오류: {e}")
    exit()

print(f"'{kospi_file}' (KOSPI) 파일을 불러오는 중...")
try:
    df_kospi = pd.read_parquet(kospi_file)
    df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])
except Exception as e:
    print(f"KOSPI 파일 읽기 오류: {e}")
    exit()

# --- 2. KOSPI 데이터 가공 ---
print("KOSPI 데이터 가공 (일일 수익률 계산)...")
# KOSPI의 '종가' 컬럼 이름을 겹치지 않게 변경
df_kospi.rename(columns={'종가': 'KOSPI_종가', '거래량': 'KOSPI_거래량'}, inplace=True)

# KOSPI 일일 수익률 계산
# (오늘 종가 - 어제 종가) / 어제 종가
df_kospi['KOSPI_수익률'] = df_kospi['KOSPI_종가'].pct_change()
# KOSPI 20일 평균 수익률 (시장의 단기 추세)
df_kospi['KOSPI_SMA_20'] = ta.sma(df_kospi['KOSPI_종가'], length=20)


# --- 3. (핵심) 개별 종목과 KOSPI 데이터 병합 ---
print("개별 종목 데이터와 KOSPI 데이터를 '날짜' 기준으로 병합합니다...")
# '날짜'를 기준으로 두 테이블을 합침 (SQL의 LEFT JOIN과 동일)
df_merged = pd.merge(df_stocks, 
                     df_kospi[['날짜', 'KOSPI_종가', 'KOSPI_수익률', 'KOSPI_SMA_20']], 
                     on='날짜', 
                     how='left')

# (중요) 합병 후 날짜/종목코드로 다시 정렬
df_merged.sort_values(by=['날짜', '종목코드'], inplace=True)


# --- 4. V4 기술적 지표 계산 ---
print("V4 기술적 지표 계산을 시작합니다 (종목별 처리)...")

def calculate_indicators_v4(group_df):
    """한 종목(group_df)을 받아서 V4 지표 계산"""
    
    # --- 1. 기존 V2 지표 (유지) ---
    group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
    group_df['SMA_60'] = ta.sma(group_df['종가'], length=60)
    group_df['RSI_14'] = ta.rsi(group_df['종가'], length=14)
    group_df['VOL_SMA_20'] = ta.sma(group_df['거래량'], length=20)
    
    macd = ta.macd(group_df['종가'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        group_df['MACD'] = macd.iloc[:, 0]
        group_df['MACD_Sig'] = macd.iloc[:, 1]
    
    bbands = ta.bbands(group_df['종가'], length=20, std=2)
    if bbands is not None and not bbands.empty:
        group_df['BBP_20'] = bbands.iloc[:, 4] 
    
    group_df['ATR_14'] = ta.atr(group_df['고가'], group_df['저가'], group_df['종가'], length=14)

    # --- 2. (★★★ V4 신규 지표 ★★★) ---
    
    # (A) 스토캐스틱 (Stochastic)
    # (fastk, fastd) 2개 값 반환
    stoch = ta.stoch(group_df['고가'], group_df['저가'], group_df['종가'], k=14, d=3, smooth_k=3)
    if stoch is not None and not stoch.empty:
        group_df['STOCH_K'] = stoch.iloc[:, 0] # K값
        group_df['STOCH_D'] = stoch.iloc[:, 1] # D값
        
    # (B) CCI (Commodity Channel Index)
    group_df['CCI_20'] = ta.cci(group_df['고가'], group_df['저가'], group_df['종가'], length=20)

    # (C) 시장 대비 초과 수익률 (Alpha)
    #    (개별 종목의 일일 수익률 계산)
    daily_return = group_df['종가'].pct_change()
    #    (초과 수익률 = 내 수익률 - KOSPI 수익률)
    alpha = daily_return - group_df['KOSPI_수익률']
    #    (초과 수익률의 20일 이동평균 -> '시장 대비 강한 추세'를 봄)
    group_df['ALPHA_SMA_20'] = ta.sma(alpha, length=20)

    return group_df

# groupby('종목코드')로 2800개 종목을 각각 분리하여 calculate_indicators_v4 함수 적용
tqdm.pandas(desc="Calculating Indicators (V4)")
df_processed = df_merged.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_v4)


# --- 5. 결측치(NaN) 제거 ---
original_rows = len(df_processed)
df_processed.dropna(inplace=True)
cleaned_rows = len(df_processed)

print("-" * 30)
print("결측치(NaN) 제거 완료.")
print(f"제거된 행 수: {original_rows - cleaned_rows}")


# --- 6. 최종 결과물 저장 (★새 이름으로 저장★)
output_file = "stock_data_10y_processed_v4.parquet"
df_processed.to_parquet(output_file, index=False)

print(f"모든 처리 완료! '{output_file}' 파일로 저장되었습니다.")
print(f"최종 학습 데이터 행 수: {cleaned_rows}")
print("\n최종 데이터 샘플 (새 V4 지표 STOCH, CCI, ALPHA 등 추가 확인):")
# 맨 뒤 컬럼들을 보기 위해 [:, -6:] (뒤에서 6개 컬럼) 출력
print(df_processed.iloc[:, -6:].tail())