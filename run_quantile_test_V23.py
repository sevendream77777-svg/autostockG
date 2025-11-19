# --- 코드 버전: V23 (Quantile Optimization Test) ---
import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
import joblib 
import pykrx
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import os 

# --- 1. 'V23' 퀀타일 설정 ---
# (★★★ V23 수정: V25에서 생성한 '종목명' 포함 DB 사용 ★★★)
FEATURE_FILE = "V25_Hoj_DB.parquet" 
KOSPI_FILE = "kospi_index_10y.parquet" # (KOSPI 파일은 V22-B에서만 사용됨)

# (챔피언 가설 고정)
LOOKBACK = 60     # 입력(X) 기간
TARGET_DAYS = 5   # 예측(Y) 기간

TEST_DURATION_DAYS = 365 

# (V21 Hoj 엔진 피처 리스트)
feature_columns = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

# (V22 튜닝된 챔피언 파라미터)
PARAMS_TUNED_A = {
    'objective': 'regression_l1', 'n_estimators': 500, 'learning_rate': 0.05, 
    'num_leaves': 41, 'random_state': 42, 'n_jobs': -1
}

# (★★★ V23: 테스트할 퀀타일(커트라인) 리스트 ★★★)
QUANTILES_TO_TEST = [
    0.80,  # Top 20% (넓은 범위)
    0.90,  # Top 10%
    0.95,  # Top 5%
    0.98,  # Top 2%
    0.99,  # Top 1% (V22 기준)
    0.995, # Top 0.5%
    0.997, # Top 0.3% (호정천재님 Top 10 전략)
    0.999, # Top 0.1%
    0.9999, # Top 0.01% (최상위권)
    0.99999 # Top 0.001% (극상위권)
]


# --- 2. 헬퍼 함수 ---
# (모델 학습 함수 - 변경 없음)
def train_regression_model(train_df, feature_columns, target_col_name, params):
    y_train = train_df[target_col_name] 
    X_train = train_df[feature_columns]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    lgbm_model = lgb.LGBMRegressor(**params)
    lgbm_model.fit(X_train, y_train)
    return lgbm_model

# (★★★ V23 백테스트 함수: 퀀타일 리스트를 받도록 수정 ★★★)
def backtest_model_quantiles(model, test_df, feature_columns, target_col_name):
    
    y_test_actual = test_df[target_col_name] 
    X_test = test_df[feature_columns]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]
    
    print("  > 1년치 검증 데이터 '예상 수익률' 예측 중... (1회)")
    y_pred_return = model.predict(X_test)
    
    df_result = pd.DataFrame({
        'Predicted_Return': y_pred_return,
        'Actual_Return': y_test_actual.values
    })
    
    report = []
    
    # (★★★ V23: 퀀타일 루프 ★★★)
    print("  > 퀀타일별 수익률 계산 중...")
    for q_value in QUANTILES_TO_TEST:
        top_q_threshold = df_result['Predicted_Return'].quantile(q_value)
        top_group = df_result[df_result['Predicted_Return'] >= top_q_threshold]
        
        if top_group.empty:
            avg_return = 0.0
        else:
            avg_return = top_group['Actual_Return'].mean()
            
        report.append((avg_return, f"Top {(1-q_value)*100:.3f}%", len(top_group)))
        
    return report


# --- 3. (★★★) V23 메인 실행 (★★★) ---
if __name__ == "__main__":
    start_time = time.time()
    
    # 1. 'V25 피처 파일' 로드
    try:
        print(f"[1] Hoj 엔진 V25 피처 데이터베이스('{FEATURE_FILE}') 로드 중...")
        df_target_base = pd.read_parquet(FEATURE_FILE)
        df_target_base['날짜'] = pd.to_datetime(df_target_base['날짜'])
        print(f"  > 로드 성공. (총 {len(df_target_base):,} 행)")
    except Exception as e:
        print(f"  > 오류: '{FEATURE_FILE}' 파일이 없습니다. ({e})"); exit()

    
    final_report = []
    hypothesis_name = f"({LOOKBACK}d_IN / {TARGET_DAYS}d_OUT)"
    target_col_name = f'Target_Return_{TARGET_DAYS}d'
    
    # 2. 데이터 준비
    print(f"\n[2] {hypothesis_name} 가설 데이터 준비 중...")
    current_features = feature_columns 
    df_target = df_target_base.dropna(subset=[target_col_name]) # (Y 정답이 있는 것만)
    
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    if train_df.empty or test_df.empty:
        print("  > 오류: 학습 또는 검증 데이터가 비어있습니다."); exit()

    # --- 3. 챔피언 모델 (튜닝) 학습 (1회) ---
    print(f"\n--- [Testing] {hypothesis_name} (튜닝) ---")
    print(f"  > [3.1] '튜닝' 모델 재학습 중... (나무 500개)")
    model_tuned = train_regression_model(train_df, current_features, target_col_name, PARAMS_TUNED_A)
    
    # --- 4. 백테스팅 (퀀타일별) ---
    print(f"  > [3.2] 백테스팅 (퀀타일별 수익률 분석)...")
    final_report = backtest_model_quantiles(model_tuned, test_df, current_features, target_col_name)

    # 5. 최종 결과 보고
    print("\n" + "="*60)
    print(f"### 'V23 Hoj 챔피언' 퀀타일 최적화 보고서 ###")
    print(f" (가설: {hypothesis_name} / 1년 검증)")
    print("="*60)
    
    final_report.sort(key=lambda x: x[0], reverse=True)
    
    print("  순위 |  커트라인 (Top N) |  추천 수(1년) |  Top N 실제 평균 수익률")
    print("-------|-------------------|---------------|-----------------------")
    
    for i, (avg_return, name, count) in enumerate(final_report):
        print(f"  {i+1:<5} |  {name:<17} |  {count:<13,d} |    {avg_return*100:>+8.3f}%")
        
    print("="*60)
    
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")

# --- 코드 버전: V23 (Quantile Optimization) ---