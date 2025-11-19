import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
from sklearn.metrics import classification_report, confusion_matrix
from datetime import datetime, timedelta
from tqdm import tqdm
import re
import os # os.path check (파일 존재 확인용)

# (필수 라이브러리)
# pip install pandas pandas-ta lightgbm scikit-learn tqdm tables fastparquet

# --- 1. V6 파라미터 설정 ---
# ★★★ V6 가설 ★★★
LOOKBACK_LONGEST = 40  # 60일(X) -> 40일(O) (가장 긴 지표 기준)
TARGET_DAYS = 5      # 5일 예측
TARGET_RETURN = 0.05 # 5% 수익률 목표
# ★★★★★★★★★★★★★★

# 백테스트(검증) 기간
TEST_DURATION_DAYS = 365 
# 모델 예측 기준선
THRESHOLD = 0.5

# V6용 피처 리스트 (SMA_60 -> SMA_40)
feature_columns_v6 = [
    'SMA_20', 'SMA_40', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14',
    'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

print("--- V6 통합 파이프라인 시작 ---")
print(f"  > 입력(X): {LOOKBACK_LONGEST}일 기준")
print(f"  > 예측(Y): {TARGET_DAYS}일 / {TARGET_RETURN*100}%")
print("-" * 50)


# --- 2. V6 데이터 처리 (Process) ---
# V4-2단계 로직 통합
def process_data_v6():
    print("[2단계] V6 데이터 처리 시작...")
    
    # 2.1. 데이터 불러오기
    stock_file = "stock_data_10y_combined.parquet"
    kospi_file = "kospi_index_10y.parquet"
    
    if not (os.path.exists(stock_file) and os.path.exists(kospi_file)):
        print(f"오류: '{stock_file}' 또는 '{kospi_file}' 파일이 없습니다.")
        print("1단계(combine_data.py)와 V4-1(collect_kospi.py)이 완료되었는지 확인하세요.")
        return None

    try:
        df_stocks = pd.read_parquet(stock_file)
        df_stocks['날짜'] = pd.to_datetime(df_stocks['날짜'])
        
        df_kospi = pd.read_parquet(kospi_file)
        df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return None

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

    # 2.4. V6 기술적 지표 계산 함수
    def calculate_indicators_v6(group_df):
        # (★★★ V6 수정 사항 ★★★)
        # SMA_60 -> SMA_40 (LOOKBACK_LONGEST)
        group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
        group_df['SMA_40'] = ta.sma(group_df['종가'], length=LOOKBACK_LONGEST) 
        
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
            group_df['STOCH_K'] = stoch.iloc[:, 0] # K값
            group_df['STOCH_D'] = stoch.iloc[:, 1] # D값
            
        group_df['CCI_20'] = ta.cci(group_df['고가'], group_df['저가'], group_df['종가'], length=20)

        daily_return = group_df['종가'].pct_change()
        alpha = daily_return - group_df['KOSPI_수익률']
        group_df['ALPHA_SMA_20'] = ta.sma(alpha, length=20)

        return group_df

    # 2.5. 지표 계산 실행
    print("  > V6 기술적 지표 계산 중...")
    tqdm.pandas(desc="Calculating Indicators (V6)")
    df_processed = df_merged.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_v6)

    # 2.6. 결측치 제거
    original_rows = len(df_processed)
    df_processed.dropna(inplace=True)
    print(f"  > 결측치(NaN) 제거 완료. (각 종목 초반 {LOOKBACK_LONGEST}일치 등)")
    
    return df_processed


# --- 3. V6 정답 생성 및 분리 (Split) ---
# V4-3단계 로직 통합
def create_target_and_split(df):
    print("\n[3단계] V6 정답 생성 및 데이터 분리 시작...")
    
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
    print(f"  > 정답 계산 불가능 행 (각 종목 마지막 {TARGET_DAYS}일) 제거 완료.")

    # 3.2. 9년(학습) / 1년(검증) 데이터 분리
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    print(f"  > 학습/검증 분리 (기준 날짜: {split_date.strftime('%Y-%m-%d')})...")
    
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    print(f"  > 학습용(9년) 데이터: {len(train_df):,} 행")
    print(f"  > 검증용(1년) 데이터: {len(test_df):,} 행")
    
    return train_df, test_df


# --- 4. V6 모델 학습 (Train) ---
# V4-4단계 로직 통합
def train_model_v6(train_df):
    print("\n[4단계] V6 모델 학습 시작...")
    
    y_train = train_df['Target']
    X_train = train_df[feature_columns_v6]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]

    # V6 가중치(pavg) 계산
    counts = y_train.value_counts()
    print(f"  > V6 학습 데이터 (0:실패 {counts[0]:,}, 1:성공 {counts[1]:,})")
    
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
    
    print("  > V6 모델 학습 완료!")
    return lgbm_model


# --- 5. V6 백테스팅 (Backtest) ---
# V4-5단계 로직 통합
def backtest_model_v6(model, test_df):
    print("\n[5단계] V6 백테스팅 시작...")
    
    y_test = test_df['Target']
    X_test = test_df[feature_columns_v6]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

    # 5.1. 예측 수행
    print("  > 1년 치 검증 데이터 예측 중...")
    y_pred_proba = model.predict_proba(X_test)[:, 1] # (1이 될 확률)
    y_pred_class = (y_pred_proba > THRESHOLD).astype(int)

    print(f"  > 예측 완료! (기준선={THRESHOLD * 100}%)")
    print("-" * 50)

    # 5.2. 혼동 행렬 (Confusion Matrix)
    print("### 1. V6 혼동 행렬 (예측 vs 실제) ###")
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred_class).ravel()
    
    print(f" ┌───────────────┬──────────────────┐")
    print(f" │               │       실제       │")
    print(f" │     예측      ├──── 0 (실패) ──── 1 (성공) ──┤")
    print(f" ├───────────────┼──────────────────┤")
    print(f" │   0 (실패)    │   {tn:8,d} (A)   {fn:8,d} (C) │")
    print(f" │   1 (성공)    │   {fp:8,d} (B)   {tp:8,d} (D) │")
    print(f" └───────────────┴──────────────────┘")
    print(f" (B) AI의 실수: {fp:,} 건 / (D) AI의 성공: {tp:,} 건")
    print(f" (C) AI가 놓친 기회: {fn:,} 건")


    # 5.3. 분류 성능 평가표 (Classification Report)
    report_dict = classification_report(y_test, y_pred_class, target_names=['0 (실패)', '1 (성공)'], output_dict=True)
    r_0 = report_dict['0 (실패)']
    r_1 = report_dict['1 (성공)']

    # 5.4. 핵심 성능 비교
    print("\n### 2. V6 핵심 성능 비교 (40일 입력 / 5일 예측) ###")
    total_support = r_0['support'] + r_1['support']
    natural_success_rate = (r_1['support'] / total_support) * 100
    model_precision = r_1['precision'] * 100

    print(f"  > 1. 기준선 (자연 성공률) : {natural_success_rate:6.2f}%")
    print(f"  > 2. V6 모델 (예측 정밀도) : {model_precision:6.2f}%")
    print("-" * 30)
    if model_precision > natural_success_rate:
        print(f"  > 평가: 모델 예측이 랜덤보다 {model_precision - natural_success_rate:+.2f}%p 더 정확합니다. (성공!)")
    else:
        print(f"  > 평가: 모델 예측이 랜덤보다 부정확합니다. (실패)")
    print("-" * 50)

    # 5.5. 세부 분류 평가표
    print("\n### 3. V6 세부 분류 평가표 (%) ###")
    print("\n[ 1 (성공) 예측 성능 ] (★ 여기가 가장 중요 ★)")
    print(f"  > 정밀도 (Precision): {r_1['precision'] * 100:6.2f}%")
    print(f"  > 재현율 (Recall)   : {r_1['recall'] * 100:6.2f}%")
    print(f"  > 데이터 수 (Support): {r_1['support']:,} 건")

    print("\n[ 0 (실패) 예측 성능 ]")
    print(f"  > 정밀도 (Precision): {r_0['precision'] * 100:6.2f}%")
    print(f"  > 재현율 (Recall)   : {r_0['recall'] * 100:6.2f}%")
    print(f"  > 데이터 수 (Support): {r_0['support']:,} 건")

    print("-" * 50)
    print("V6 통합 파이프라인 (성능 검증) 완료.")


# --- 6. 파이프라인 실행 ---
if __name__ == "__main__":
    # 2단계: 데이터 처리
    processed_df = process_data_v6()
    
    if processed_df is not None:
        # 3단계: 정답 생성 및 분리
        train_df, test_df = create_target_and_split(processed_df)
        
        # 4단계: 모델 학습
        model_v6 = train_model_v6(train_df)
        
        # 5단계: 백테스팅
        backtest_model_v6(model_v6, test_df)