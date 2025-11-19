# --- 코드 버전: V22-A (Hoj 챔피언 튜닝) ---
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

# --- 1. 'V22-A' 튜닝 설정 ---
# (★★★ V22: 'V21'에서 생성한 Hoj 엔진 DB를 로드 ★★★)
FEATURE_FILE = "all_features_cumulative_V21_Hoj.parquet" 

# (★★★ V22: 챔피언 가설 고정 ★★★)
LOOKBACK = 60     # 입력(X) 기간
TARGET_DAYS = 5   # 예측(Y) 기간

TEST_DURATION_DAYS = 365 # 1년치 검증

# (V21 Hoj 엔진 피처 리스트)
feature_columns = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

# (★★★ V22: 테스트할 2가지 하이퍼파라미터 설정 ★★★)
PARAMS_DEFAULT = {
    'objective': 'regression_l1', 'n_estimators': 100, 'learning_rate': 0.1, 
    'num_leaves': 31, 'random_state': 42, 'n_jobs': -1
}
PARAMS_TUNED_A = {
    'objective': 'regression_l1', 'n_estimators': 500, 'learning_rate': 0.05, 
    'num_leaves': 41, 'random_state': 42, 'n_jobs': -1
}

# --- 2. 헬퍼 함수 ---
# (모델 학습 함수 - 파라미터를 입력받도록 수정)
def train_regression_model(train_df, feature_columns, target_col_name, params):
    y_train = train_df[target_col_name] 
    X_train = train_df[feature_columns]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    
    # (★★★ V22: 입력받은 params로 모델 생성 ★★★)
    lgbm_model = lgb.LGBMRegressor(**params)
    
    lgbm_model.fit(X_train, y_train)
    return lgbm_model

# (백테스트 함수 - 변경 없음)
def backtest_model(model, test_df, feature_columns, target_col_name):
    y_test_actual = test_df[target_col_name] 
    X_test = test_df[feature_columns]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]
    y_pred_return = model.predict(X_test)
    df_result = pd.DataFrame({'Predicted_Return': y_pred_return, 'Actual_Return': y_test_actual.values})
    top_1_percent_threshold = df_result['Predicted_Return'].quantile(0.99)
    top_group = df_result[df_result['Predicted_Return'] >= top_1_percent_threshold]
    if top_group.empty: return 0.0
    actual_return_of_top_group = top_group['Actual_Return'].mean()
    return actual_return_of_top_group


# --- 3. (★★★) V22-A 메인 튜닝 실행 (★★★) ---
if __name__ == "__main__":
    start_time = time.time()
    
    # 1. 'V21 피처 파일' 로드
    try:
        print(f"[1] Hoj 엔진 V21 피처 데이터베이스('{FEATURE_FILE}') 로드 중...")
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
    current_features = feature_columns # (SMA_60이 포함된 12개)
    df_target = df_target_base.dropna(subset=[target_col_name])
    
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    if train_df.empty or test_df.empty:
        print("  > 오류: 학습 또는 검증 데이터가 비어있습니다."); exit()

    # --- 3. 모델 1 (기본) 학습 및 백테스트 ---
    print(f"\n--- [Testing 1] {hypothesis_name} (기본값) ---")
    print(f"  > [3.1] '기본' 모델 재학습 중... (나무 100개)")
    model_default = train_regression_model(train_df, current_features, target_col_name, PARAMS_DEFAULT)
    
    print(f"  > [3.2] '기본' 모델 백테스팅 중...")
    avg_return_default = backtest_model(model_default, test_df, current_features, target_col_name)
    
    final_report.append((avg_return_default, "기본 (나무 100개)"))
    print(f"  > [Result] 기본 = {avg_return_default*100:+.3f}%")

    # --- 4. 모델 2 (튜닝) 학습 및 백테스트 ---
    print(f"\n--- [Testing 2] {hypothesis_name} (튜닝) ---")
    print(f"  > [4.1] '튜닝' 모델 재학습 중... (나무 500개)")
    model_tuned = train_regression_model(train_df, current_features, target_col_name, PARAMS_TUNED_A)
    
    print(f"  > [4.2] '튜닝' 모델 백테스팅 중...")
    avg_return_tuned = backtest_model(model_tuned, test_df, current_features, target_col_name)
    
    final_report.append((avg_return_tuned, "튜닝 (나무 500개)"))
    print(f"  > [Result] 튜닝 = {avg_return_tuned*100:+.3f}%")

    # 5. 최종 결과 보고
    print("\n" + "="*60)
    print(f"### 'V22-A Hoj 챔피언' 튜닝 성능 보고서 ###")
    print(f" (가설: {hypothesis_name} / Top 1% 추천, {TEST_DURATION_DAYS}일 검증)")
    print("="*60)
    
    final_report.sort(key=lambda x: x[0], reverse=True)
    
    print("  순위 |  모델 설정 (나무 개수) |  Top 1% 실제 평균 수익률")
    print("-------|----------------------|-----------------------")
    
    for i, (avg_return, name) in enumerate(final_report):
        print(f"  {i+1:<5} |  {name:<20} |    {avg_return*100:>+8.3f}%")
        
    print("="*60)
    
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")

# --- 코드 버전: V22-A (Hoj Tuning) ---