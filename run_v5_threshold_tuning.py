import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
from sklearn.metrics import classification_report, confusion_matrix, precision_score, recall_score
from datetime import datetime, timedelta
from tqdm import tqdm
import re
import os 

# (필수 라이브러리)
# pip install pandas pandas-ta lightgbm scikit-learn tqdm tables fastparquet

# --- 1. V5 파라미터 설정 ---
# ★★★ V5 가설 (최강 모델) ★★★
LOOKBACK_LONGEST = 60  # 60일 기준 (V5)
TARGET_DAYS = 5      # 5일 예측
TARGET_RETURN = 0.05 # 5% 수익률 목표
# ★★★★★★★★★★★★★★

# ★★★ 우리가 테스트할 기준선 리스트 ★★★
THRESHOLDS_TO_TEST = [0.5, 0.45, 0.4, 0.35, 0.3]
# ★★★★★★★★★★★★★★

# 백테스트(검증) 기간
TEST_DURATION_DAYS = 365 

# V5용 피처 리스트 (SMA_60 포함)
feature_columns_v5 = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14',
    'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

print("--- V5 기준선 튜닝 파이프라인 시작 ---")
print(f"  > 모델: 60일 입력 / 5일 예측")
print(f"  > 테스트할 기준선: {THRESHOLDS_TO_TEST}")
print("-" * 50)


# --- 2. V5 데이터 처리 (Process) ---
def process_data_v5():
    print("[2단계] V5 데이터 처리 시작...")
    
    # 2.1. 데이터 불러오기
    stock_file = "stock_data_10y_combined.parquet"
    kospi_file = "kospi_index_10y.parquet"
    
    if not (os.path.exists(stock_file) and os.path.exists(kospi_file)):
        print(f"오류: '{stock_file}' 또는 '{kospi_file}' 파일이 없습니다.")
        return None

    df_stocks = pd.read_parquet(stock_file)
    df_stocks['날짜'] = pd.to_datetime(df_stocks['날짜'])
    df_kospi = pd.read_parquet(kospi_file)
    df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])

    # 2.2. KOSPI 데이터 가공
    df_kospi.rename(columns={'종가': 'KOSPI_종가', '거래량': 'KOSPI_거래량'}, inplace=True)
    df_kospi['KOSPI_수익률'] = df_kospi['KOSPI_종가'].pct_change()
    df_kospi['KOSPI_SMA_20'] = ta.sma(df_kospi['KOSPI_종가'], length=20)

    # 2.3. 데이터 병합
    print("  > KOSPI 데이터 병합 중...")
    df_merged = pd.merge(df_stocks, 
                         df_kospi[['날짜', 'KOSPI_종가', 'KOSPI_수익률', 'KOSPI_SMA_20']], 
                         on='날짜', 
                         how='left')
    df_merged.sort_values(by=['날짜', '종목코드'], inplace=True)

    # 2.4. V5 기술적 지표 계산 함수
    def calculate_indicators_v5(group_df):
        group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
        group_df['SMA_60'] = ta.sma(group_df['종가'], length=LOOKBACK_LONGEST) # 60일
        
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
        
        stoch = ta.stoch(group_df['고가'], group_df['저가'], group_df['종가'], k=14, d=3, smooth_k=3)
        if stoch is not None and not stoch.empty:
            group_df['STOCH_K'] = stoch.iloc[:, 0]
            group_df['STOCH_D'] = stoch.iloc[:, 1]
            
        group_df['CCI_20'] = ta.cci(group_df['고가'], group_df['저가'], group_df['종가'], length=20)

        daily_return = group_df['종가'].pct_change()
        alpha = daily_return - group_df['KOSPI_수익률']
        group_df['ALPHA_SMA_20'] = ta.sma(alpha, length=20)
        return group_df

    # 2.5. 지표 계산 실행
    print("  > V5 기술적 지표 계산 중...")
    tqdm.pandas(desc="Calculating Indicators (V5)")
    df_processed = df_merged.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_v5)

    # 2.6. 결측치 제거
    df_processed.dropna(inplace=True)
    print(f"  > 결측치(NaN) 제거 완료. (각 종목 초반 {LOOKBACK_LONGEST}일치 등)")
    
    return df_processed


# --- 3. V5 정답 생성 및 분리 (Split) ---
def create_target_and_split(df):
    print("\n[3단계] V5 정답 생성 및 데이터 분리 시작...")
    
    # 3.1. '정답(Target)' 컬럼 생성
    print(f"  > '정답({TARGET_DAYS}일 뒤)' 컬럼 생성 중...")
    def create_target(group_df):
        future_price = group_df['종가'].shift(-TARGET_DAYS)
        future_return = (future_price - group_df['종가']) / group_df['종가']
        group_df['Target'] = np.where(future_return >= TARGET_RETURN, 1, 0)
        return group_df

    tqdm.pandas(desc="Creating Target (Y)")
    df_target = df.groupby('종목코드', group_keys=False).progress_apply(create_target)
    df_target.dropna(subset=['Target'], inplace=True)

    # 3.2. 9년(학습) / 1년(검증) 데이터 분리
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    print(f"  > 학습용(9년) 데이터: {len(train_df):,} 행")
    print(f"  > 검증용(1년) 데이터: {len(test_df):,} 행")
    
    return train_df, test_df


# --- 4. V5 모델 학습 (Train) ---
def train_model_v5(train_df):
    print("\n[4단계] V5 모델 학습 시작...")
    
    y_train = train_df['Target']
    X_train = train_df[feature_columns_v5]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]

    counts = y_train.value_counts()
    print(f"  > V5 학습 데이터 (0:실패 {counts[0]:,}, 1:성공 {counts[1]:,})")
    
    # 모델 생성 (V5와 동일하게 '가중치 없음' = 보수적 모델)
    lgbm_model = lgb.LGBMClassifier(
        n_estimators=100,
        learning_rate=0.1,
        num_leaves=31,
        random_state=42,
        n_jobs=-1
    )
    
    print("  > 모델 학습(fit) 중...")
    lgbm_model.fit(X_train, y_train)
    
    print("  > V5 모델 학습 완료!")
    return lgbm_model


# --- 5. V5 백테스팅 (반복 튜닝) ---
def backtest_model_v5(model, test_df):
    print("\n[5단계] V5 기준선 튜닝 백테스팅 시작...")
    
    y_test = test_df['Target']
    X_test = test_df[feature_columns_v5]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

    # 5.1. 예측 수행 (★딱 한 번만★)
    print("  > 1년 치 검증 데이터 예측 (확률 계산)...")
    y_pred_proba = model.predict_proba(X_test)[:, 1] # (1이 될 확률)
    
    # 5.2. '자연 성공률' 계산
    natural_success_rate = (y_test.sum() / len(y_test)) * 100

    print("-" * 50)
    print(f"### V5 최종 성능 튜닝 결과 ###")
    print(f" (자연 성공률: {natural_success_rate:.2f}%)")
    print("-" * 50)
    print(" 기준선(%) | 정밀도(Prec) | 재현율(Rec) | 추천 횟수(B+D)")
    print("----------|--------------|-------------|---------------")

    results_summary = []

    # 5.3. (★★★ 핵심 ★★★) 기준선(Threshold)을 바꿔가며 반복 테스트
    for threshold in THRESHOLDS_TO_TEST:
        # 확률(proba)을 기준으로 0/1 분류
        y_pred_class = (y_pred_proba > threshold).astype(int)
        
        # 성능 계산
        precision = precision_score(y_test, y_pred_class)
        recall = recall_score(y_test, y_pred_class)
        
        # 추천 횟수 계산
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred_class).ravel()
        recommend_count = fp + tp
        
        # 결과 저장
        results_summary.append({
            "Threshold": threshold * 100,
            "Precision": precision * 100,
            "Recall": recall * 100,
            "Recommend Count": recommend_count
        })

    # 5.4. 최종 결과 출력
    for result in results_summary:
        print(f"  {result['Threshold']:>5.1f}%   |   {result['Precision']:>7.2f}%   |  {result['Recall']:>7.2f}%  |  {result['Recommend Count']:>10,d} 회")
    
    print("-" * 50)
    print("V5 기준선 튜닝 완료.")


# --- 6. 파이프라인 실행 ---
if __name__ == "__main__":
    # 2단계: 데이터 처리
    processed_df = process_data_v5()
    
    if processed_df is not None:
        # 3단계: 정답 생성 및 분리
        train_df, test_df = create_target_and_split(processed_df)
        
        # 4단계: 모델 학습 (★딱 한 번★)
        model_v5 = train_model_v5(train_df)
        
        # 5단계: 백테스팅 (★여러 번★)
        backtest_model_v5(model_v5, test_df)