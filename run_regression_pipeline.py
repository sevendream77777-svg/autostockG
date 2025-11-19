import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from datetime import datetime, timedelta
from tqdm import tqdm
import re
import os 
import time

# (필수 라이브러리)
# pip install pandas pandas-ta lightgbm scikit-learn tqdm tables fastparquet

# --- 1. 2단계(회귀) 파라미터 설정 ---
# ★★★ 1단계 '최강 가설' 고정 ★★★
LOOKBACK_LONGEST = 60  # 60일 기준
TARGET_DAYS = 10     # 10일 예측
# ★★★★★★★★★★★★★★★★★★★★★★

# 백테스트(검증) 기간
TEST_DURATION_DAYS = 365 

# V5/V6 피처 리스트 (60일 기준)
feature_columns_v5 = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14',
    'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

print("--- 2단계(회귀) 통합 파이프라인 시작 ---")
print(f"  > 모델: 60일 입력 / 10일 '예상 수익률' 예측")
print("-" * 50)


# --- 2. 데이터 처리 (Process) ---
# (이전 V5와 동일한 데이터 처리 로직)
def process_data_v5():
    print("[2단계] V5 데이터 처리 시작 (캐시된 V4 데이터 사용)...")
    
    stock_file = "stock_data_10y_combined.parquet"
    kospi_file = "kospi_index_10y.parquet"
    
    if not (os.path.exists(stock_file) and os.path.exists(kospi_file)):
        print(f"오류: '{stock_file}' 또는 '{kospi_file}' 파일이 없습니다.")
        return None

    df_stocks = pd.read_parquet(stock_file)
    df_stocks['날짜'] = pd.to_datetime(df_stocks['날짜'])
    df_kospi = pd.read_parquet(kospi_file)
    df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])

    # KOSPI 가공
    df_kospi.rename(columns={'종가': 'KOSPI_종가', '거래량': 'KOSPI_거래량'}, inplace=True)
    df_kospi['KOSPI_수익률'] = df_kospi['KOSPI_종가'].pct_change()
    df_kospi['KOSPI_SMA_20'] = ta.sma(df_kospi['KOSPI_종가'], length=20)

    # 병합
    print("  > KOSPI 데이터 병합 중...")
    df_merged = pd.merge(df_stocks, 
                         df_kospi[['날짜', 'KOSPI_종가', 'KOSPI_수익률', 'KOSPI_SMA_20']], 
                         on='날짜', 
                         how='left')
    df_merged.sort_values(by=['날짜', '종목코드'], inplace=True)

    # V5 지표 계산 함수
    def calculate_indicators_v5(group_df):
        group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
        group_df['SMA_60'] = ta.sma(group_df['종가'], length=LOOKBACK_LONGEST) # 60일
        group_df['RSI_14'] = ta.rsi(group_df['종가'], length=14)
        group_df['VOL_SMA_20'] = ta.sma(group_df['거래량'], length=20)
        macd = ta.macd(group_df['종가'], fast=12, slow=26, signal=9)
        if macd is not None and not macd.empty:
            group_df['MACD'] = macd.iloc[:, 0]; group_df['MACD_Sig'] = macd.iloc[:, 1]
        bbands = ta.bbands(group_df['종가'], length=20, std=2)
        if bbands is not None and not bbands.empty:
            group_df['BBP_20'] = bbands.iloc[:, 4] 
        group_df['ATR_14'] = ta.atr(group_df['고가'], group_df['저가'], group_df['종가'], length=14)
        stoch = ta.stoch(group_df['고가'], group_df['저가'], group_df['종가'], k=14, d=3, smooth_k=3)
        if stoch is not None and not stoch.empty:
            group_df['STOCH_K'] = stoch.iloc[:, 0]; group_df['STOCH_D'] = stoch.iloc[:, 1]
        group_df['CCI_20'] = ta.cci(group_df['고가'], group_df['저가'], group_df['종가'], length=20)
        daily_return = group_df['종가'].pct_change()
        alpha = daily_return - group_df['KOSPI_수익률']
        group_df['ALPHA_SMA_20'] = ta.sma(alpha, length=20)
        return group_df

    # 지표 계산 실행
    print("  > V5 기술적 지표 계산 중...")
    tqdm.pandas(desc="Calculating Indicators (V5)")
    df_processed = df_merged.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_v5)
    df_processed.dropna(inplace=True)
    return df_processed


# --- 3. (★★★) 회귀 '정답' 생성 및 분리 (Split) ---
def create_target_and_split_regression(df):
    print("\n[3단계] 회귀(Regression) '정답' 생성 및 분리 시작...")
    
    # 3.1. '정답(Target)' 컬럼 생성
    print(f"  > '정답({TARGET_DAYS}일 뒤 실제 수익률)' 컬럼 생성 중...")
    def create_target_regression(group_df):
        future_price = group_df['종가'].shift(-TARGET_DAYS)
        # (미래 종가 - 현재 종가) / 현재 종가 = 미래 수익률
        future_return = (future_price - group_df['종가']) / group_df['종가']
        
        # (★★★ 핵심 변경 ★★★)
        # 0과 1이 아닌, '실제 수익률(숫자)' 자체를 'Target'으로 지정
        group_df['Target_Return'] = future_return 
        return group_df

    tqdm.pandas(desc="Creating Regression Target (Y)")
    df_target = df.groupby('종목코드', group_keys=False).progress_apply(create_target_regression)
    
    # (주의) 수익률이 300% (3.0) 넘거나 -90% (-0.9) 미만인 극단값 제거
    df_target = df_target[df_target['Target_Return'] < 3.0]
    df_target = df_target[df_target['Target_Return'] > -0.9]
    
    df_target.dropna(subset=['Target_Return'], inplace=True)
    print(f"  > 정답 계산 불가능/극단값 제거 완료.")

    # 3.2. 9년(학습) / 1년(검증) 데이터 분리
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    print(f"  > 학습용(9년) 데이터: {len(train_df):,} 행")
    print(f"  > 검증용(1년) 데이터: {len(test_df):,} 행")
    
    return train_df, test_df


# --- 4. (★★★) 회귀 모델 학습 (Train) ---
def train_regression_model(train_df):
    print("\n[4단계] 회귀(Regression) 모델 학습 시작...")
    
    y_train = train_df['Target_Return'] # '실제 수익률'이 정답
    X_train = train_df[feature_columns_v5]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    
    # (★★★ 핵심 변경 ★★★)
    # 'Classifier'(분류)가 아닌 'Regressor'(회귀) 사용
    lgbm_model = lgb.LGBMRegressor(
        objective='regression_l1', # MAE (평균절대오차)를 기준으로 최적화
        n_estimators=100,
        learning_rate=0.1,
        num_leaves=31,
        random_state=42,
        n_jobs=-1
    )
    
    print("  > 모델 학습(fit) 중...")
    lgbm_model.fit(X_train, y_train)
    
    print("  > 회귀 모델 학습 완료!")
    return lgbm_model


# --- 5. (★★★) 회귀 백테스팅 (Backtest) ---
def backtest_regression_model(model, test_df):
    print("\n[5단계] 회귀 모델 백테스팅 시작...")
    
    y_test_actual = test_df['Target_Return'] # '실제 수익률'
    X_test = test_df[feature_columns_v5]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

    # 5.1. 예측 수행 (★'숫자' 예측★)
    print("  > 1년 치 검증 데이터 '예상 수익률' 예측 중...")
    y_pred_return = model.predict(X_test) # (AI가 예측한 수익률)
    
    # 5.2. '실제 수익률'과 '예상 수익률'을 DataFrame으로 합치기
    df_result = pd.DataFrame({
        'Predicted_Return': y_pred_return, # AI의 예상
        'Actual_Return': y_test_actual.values # 실제 정답
    })
    
    # 5.3. (핵심) '예상 수익률'이 높은 순서대로 정렬
    df_result.sort_values(by='Predicted_Return', ascending=False, inplace=True)

    print("-" * 50)
    print(f"### 2단계(회귀) 최종 성능 보고서 ###")
    print(f" (모델: {LOOKBACK_LONGEST}일 입력 / {TARGET_DAYS}일 수익률 예측)")
    print("-" * 50)
    
    # 5.4. 전체 데이터의 실제 평균 수익률 (기준선)
    total_avg_return = df_result['Actual_Return'].mean() * 100
    print(f"  > 1. 기준선 (1년간 전체 평균 수익): {total_avg_return:>6.3f}%")
    
    # 5.5. AI 추천 상위 그룹의 실제 수익률 계산
    # (상위 1%, 5%, 10%... 그룹을 나눠서 성과 측정)
    
    quantiles = [0.01, 0.05, 0.10, 0.20] # 상위 1%, 5%, 10%, 20%
    print("\n  > 2. AI 추천 상위 그룹의 '실제' 평균 수익률")
    print("---------------------------------------------")
    print("  AI 추천 그룹 |  추천 수  | 실제 평균 수익률")
    print("---------------------------------------------")
    
    for q in quantiles:
        # AI가 '예상 수익률이 높다'고 예측한 상위 q% 그룹
        top_group = df_result.head(int(len(df_result) * q))
        
        # 이 그룹의 '실제' 평균 수익률
        actual_return_of_top_group = top_group['Actual_Return'].mean() * 100
        
        print(f"   상위 {q*100:>3.0f}%   | {len(top_group):>8,d} |    {actual_return_of_top_group:>+7.3f}%")
    
    print("-" * 50)
    print("회귀 백테스팅(성능 검증) 완료.")


# --- 6. 파이프라인 실행 ---
if __name__ == "__main__":
    start_time = time.time()
    
    # 2단계: 데이터 처리
    processed_df = process_data_v5()
    
    if processed_df is not None:
        # 3단계: 정답 생성 및 분리
        train_df, test_df = create_target_and_split_regression(processed_df)
        
        # 4단계: 모델 학습
        model_regression = train_regression_model(train_df)
        
        # 5단계: 백테스팅
        backtest_regression_model(model_regression, test_df)
        
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")