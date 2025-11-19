# --- 코드 버전: V22-B (Market Regime Filter) ---
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

# --- 1. 'V22-B' 설정 ---
FEATURE_FILE = "all_features_cumulative_V21_Hoj.parquet" 
KOSPI_FILE = "kospi_index_10y.parquet"

# (챔피언 가설 고정)
LOOKBACK = 60     # 입력(X) 기간
TARGET_DAYS = 5   # 예측(Y) 기간

TEST_DURATION_DAYS = 365 # 1년치 검증

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

# (★★★ V22-B: 시장 필터 설정 ★★★)
MARKET_FILTER_MA = 20 # KOSPI 20일 이동평균선


# --- 2. 헬퍼 함수 ---
# (모델 학습 함수 - 변경 없음)
def train_regression_model(train_df, feature_columns, target_col_name, params):
    y_train = train_df[target_col_name] 
    X_train = train_df[feature_columns]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    lgbm_model = lgb.LGBMRegressor(**params)
    lgbm_model.fit(X_train, y_train)
    return lgbm_model

# (백테스트 함수 - '필터 적용' 기능 추가)
def backtest_model(model, test_df, feature_columns, target_col_name, use_market_filter=False):
    
    # (★★★ V22-B: 시장 필터 적용 ★★★)
    if use_market_filter:
        # 'KOSPI_Above_MA20' 컬럼이 True인 (매수 가능한) 날의 데이터만 선별
        test_df_filtered = test_df[test_df['KOSPI_Above_MA20'] == True].copy()
        if test_df_filtered.empty:
            print("  > 경고: 시장 필터 적용 후 모든 거래일이 제외됨.")
            return 0.0
    else:
        test_df_filtered = test_df.copy()
    # (★★★ 필터링 끝 ★★★)

    y_test_actual = test_df_filtered[target_col_name] 
    X_test = test_df_filtered[feature_columns]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]
    
    y_pred_return = model.predict(X_test)
    
    df_result = pd.DataFrame({
        'Predicted_Return': y_pred_return,
        'Actual_Return': y_test_actual.values
    })
    
    # '상위 1%' 추천 종목의 '실제' 평균 수익률 계산
    top_1_percent_threshold = df_result['Predicted_Return'].quantile(0.99)
    top_group = df_result[df_result['Predicted_Return'] >= top_1_percent_threshold]
    
    if top_group.empty: return 0.0
    actual_return_of_top_group = top_group['Actual_Return'].mean()
    return actual_return_of_top_group


# --- 3. (★★★) V22-B 메인 실행 (★★★) ---
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

    # (★★★ V22-B: KOSPI 데이터 로드 및 필터 생성 ★★★)
    try:
        print(f"[2] KOSPI 데이터({KOSPI_FILE}) 로드 및 필터 생성 중...")
        df_kospi = pd.read_parquet(KOSPI_FILE)
        df_kospi['날짜'] = pd.to_datetime(df_kospi['날짜'])
        
        # KOSPI 20일선 계산
        df_kospi['KOSPI_SMA_20'] = ta.sma(df_kospi['종가'], length=MARKET_FILTER_MA)
        
        # 'KOSPI 종가 > KOSPI 20일선' 이면 True (매수 가능)
        df_kospi['KOSPI_Above_MA20'] = df_kospi['종가'] > df_kospi['KOSPI_SMA_20']
        
        print("  > KOSPI 필터 생성 완료.")
        
        # V21 DB에 KOSPI 필터(KOSPI_Above_MA20) 병합
        df_target_base = pd.merge(df_target_base, df_kospi[['날짜', 'KOSPI_Above_MA20']], on='날짜', how='left')
        
    except Exception as e:
        print(f"  > 오류: KOSPI 파일 로드 또는 필터 생성 실패. ({e})"); exit()
    # (★★★ V22-B 완료 ★★★)

    
    final_report = []
    hypothesis_name = f"({LOOKBACK}d_IN / {TARGET_DAYS}d_OUT)"
    target_col_name = f'Target_Return_{TARGET_DAYS}d'
    
    # 3. 데이터 준비
    print(f"\n[3] {hypothesis_name} 가설 데이터 준비 중...")
    current_features = feature_columns 
    df_target = df_target_base.dropna(subset=current_features + [target_col_name, 'KOSPI_Above_MA20'])
    
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    
    if train_df.empty or test_df.empty:
        print("  > 오류: 학습 또는 검증 데이터가 비어있습니다."); exit()

    # --- 4. 챔피언 모델 (튜닝) 학습 ---
    print(f"\n--- [Testing] {hypothesis_name} (튜닝) ---")
    print(f"  > [4.1] '튜닝' 모델 재학습 중... (나무 500개)")
    model_tuned = train_regression_model(train_df, current_features, target_col_name, PARAMS_TUNED_A)
    
    # --- 5. 백테스팅 (필터 O / 필터 X) ---
    print(f"  > [4.2] 백테스팅 (1) : 필터 '없이' (V22 기준선)")
    avg_return_no_filter = backtest_model(model_tuned, test_df, current_features, target_col_name, use_market_filter=False)
    final_report.append((avg_return_no_filter, "필터 없음 (V22 챔피언)"))
    print(f"  > [Result] 필터 없음 = {avg_return_no_filter*100:+.3f}%")
    
    print(f"  > [4.2] 백테스팅 (2) : KOSPI {MARKET_FILTER_MA}일선 필터 '적용'")
    avg_return_with_filter = backtest_model(model_tuned, test_df, current_features, target_col_name, use_market_filter=True)
    final_report.append((avg_return_with_filter, f"KOSPI {MARKET_FILTER_MA}일선 필터 적용"))
    print(f"  > [Result] 필터 적용 = {avg_return_with_filter*100:+.3f}%")


    # 6. 최종 결과 보고
    print("\n" + "="*60)
    print(f"### 'V22-B 시장 필터' 성능 보고서 ###")
    print(f" (가설: {hypothesis_name} / Top 1% 추천, {TEST_DURATION_DAYS}일 검증)")
    print("="*60)
    
    final_report.sort(key=lambda x: x[0], reverse=True)
    
    print("  순위 |  전략 (필터)         |  Top 1% 실제 평균 수익률")
    print("-------|----------------------|-----------------------")
    
    for i, (avg_return, name) in enumerate(final_report):
        print(f"  {i+1:<5} |  {name:<20} |    {avg_return*100:>+8.3f}%")
        
    print("="*60)
    
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")

# --- 코드 버전: V22-B (Market Filter) ---