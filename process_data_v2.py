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
    exit()

print("파일 불러오기 완료.")


# 2. 기술적 지표 계산 (V2: 볼린저 밴드, ATR 추가)
print("기술적 지표 계산(V2)을 시작합니다...")

def calculate_indicators_v2(group_df):
    """한 종목(group_df)을 받아서 기술적 지표를 계산하고 반환"""
    
    # --- 1. 기존 지표 (유지) ---
    group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
    group_df['SMA_60'] = ta.sma(group_df['종가'], length=60)
    group_df['RSI_14'] = ta.rsi(group_df['종가'], length=14)
    group_df['VOL_SMA_20'] = ta.sma(group_df['거래량'], length=20)
    
    macd = ta.macd(group_df['종가'], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        group_df['MACD'] = macd.iloc[:, 0]
        group_df['MACD_Sig'] = macd.iloc[:, 1]
    
    # --- 2. (★★★ V2 신규 지표 ★★★) ---
    
    # (A) 볼린저 밴드 (BBANDS)
    #    ta.bbands()는 5개의 값(Lower, Middle, Upper, Bandwidth, Percent)을 반환
    bbands = ta.bbands(group_df['종가'], length=20, std=2)
    if bbands is not None and not bbands.empty:
        # 주가가 밴드 대비 어디있는지 (0~1)
        group_df['BBP_20'] = bbands.iloc[:, 4] 
    
    # (B) ATR (Average True Range) - 변동성 지표
    #    고가, 저가, 종가 데이터가 모두 필요함
    group_df['ATR_14'] = ta.atr(group_df['고가'], group_df['저가'], group_df['종가'], length=14)

    return group_df

# groupby('종목코드')로 2800개 종목을 각각 분리하여 calculate_indicators_v2 함수 적용
tqdm.pandas(desc="Calculating Indicators (V2)")
df_processed = df.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_v2)


# 3. (매우 중요!) 결측치(NaN) 제거
#    새 지표를 계산하면서 생긴 NaN 값들 제거
original_rows = len(df_processed)
df_processed.dropna(inplace=True)
cleaned_rows = len(df_processed)

print("-" * 30)
print("결측치(NaN) 제거 완료.")
print(f"제거된 행 수: {original_rows - cleaned_rows}")


# 4. 최종 결과물 저장 (★새 이름으로 저장★)
output_file = "stock_data_10y_processed_v2.parquet"
df_processed.to_parquet(output_file, index=False)

print(f"모든 처리 완료! '{output_file}' 파일로 저장되었습니다.")
print(f"최종 학습 데이터 행 수: {cleaned_rows}")
print("\n최종 데이터 샘플 (새 지표 BBP_20, ATR_14 추가 확인):")
print(df_processed.tail())