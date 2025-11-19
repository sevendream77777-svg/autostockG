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

# --- 1. 'V13' 그리드 서치 설정 (V12와 동일) ---
FEATURE_FILE = "all_features_cumulative_V12.parquet" 
MODEL_FILE = "champion_model_60_5.pkl" # (사용되지 않음)

# (V12 핵심 가설)
GRID_LOOKBACKS = [40, 60, 90]     # 입력(X) 기간 후보
GRID_TARGET_DAYS = [1, 5, 10, 20]   # 예측(Y) 기간 후보 (1일 추가)

TEST_DURATION_DAYS = 365 # 1년치 검증

# (V12 피처 리스트 - 동적으로 변경될 예정)
base_feature_columns = [
    'SMA_20', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20',
    'PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY'
] # (SMA_60/40/90은 루프 안에서 추가됨)


# --- 2. 헬퍼 함수 (이전과 동일) ---
def calculate_indicators_dynamic(group_df, lookback_longest):
    group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
    group_df[f'SMA_{lookback_longest}'] = ta.sma(group_df['종가'], length=lookback_longest)
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

def create_target_regression(group_df, target_days):
    future_price = group_df['종가'].shift(-target_days)
    future_return = (future_price - group_df['종가']) / group_df['종가']
    group_df[f'Target_Return_{target_days}d'] = future_return 
    return group_df

def train_regression_model(train_df, feature_columns, target_col_name):
    y_train = train_df[target_col_name] 
    X_train = train_df[feature_columns]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    lgbm_model = lgb.LGBMRegressor(
        objective='regression_l1', n_estimators=100, learning_rate=0.1, 
        num_leaves=31, random_state=42, n_jobs=-1
    )
    lgbm_model.fit(X_train, y_train)
    return lgbm_model

# (★★★ V13 백테스트 함수: '로그 저장' 버그 수정 ★★★)
def backtest_model_with_logging(model, test_df, feature_columns, target_col_name, hypothesis_name, ticker_names):
    
    y_test_actual = test_df[target_col_name] 
    X_test = test_df[feature_columns]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

    y_pred_return = model.predict(X_test)
    
    df_result = test_df.copy()
    df_result['Predicted_Return'] = y_pred_return
    df_result['Actual_Return'] = y_test_actual
    
    top_1_percent_threshold = df_result['Predicted_Return'].quantile(0.99)
    top_group = df_result[df_result['Predicted_Return'] >= top_1_percent_threshold]
    
    if top_group.empty:
        return 0.0, [] 

    actual_return_of_top_group = top_group['Actual_Return'].mean()
    
    trade_logs = []
    for index, row in top_group.iterrows():
        trade_logs.append({
            "Hypothesis": hypothesis_name,
            "Date": row['날짜'].strftime("%Y-%m-%d"),
            "Ticker": row['종목코드'],
            "Name": ticker_names.get(row['종목코드'], row['종목코드']),
            "Entry_Price": row['종가'],
            "Predicted_Return_AI": row['Predicted_Return'],
            "Actual_Return_Strategy": row['Actual_Return'],
            "PBR": row.get('PBR', 999), 
            "PER": row.get('PER', 999),
            "FOR_NET_BUY": row.get('FOR_NET_BUY', 0),
            "INS_NET_BUY": row.get('INS_NET_BUY', 0)
        }) # (★★★ V13 수정: 여기에 닫는 괄호 '}'가 누락되었었음 ★★★)
    
    return actual_return_of_top_group, trade_logs


# --- 3. (★★★) V13 메인 그리드 서치 루프 (★★★) ---
if __name__ == "__main__":
    start_time = time.time()
    
    # 1. '전체 피처 파일' 로드 (V12 데이터베이스)
    try:
        print(f"[1] V12 피처 데이터베이스('{FEATURE_FILE}') 로드 중...")
        df_processed_v12 = pd.read_parquet(FEATURE_FILE)
        df_processed_v12['날짜'] = pd.to_datetime(df_processed_v12['날짜'])
        print(f"  > 로드 성공. (총 {len(df_processed_v12):,} 행)")
    except Exception as e:
        print(f"  > 오류: '{FEATURE_FILE}' 파일이 없습니다. ({e})"); exit()

    # (로그 저장을 위해 티커 이름 미리 로드)
    print("  > (로그 저장을 위해 전체 티커 이름 미리 로드 중...)")
    unique_tickers = df_processed_v12['종목코드'].unique()
    ticker_names = {}
    for ticker in tqdm(unique_tickers, desc="Loading Ticker Names"):
        try:
            ticker_names[ticker] = pykrx.stock.get_market_ticker_name(ticker)
            time.sleep(0.01) 
        except:
            ticker_names[ticker] = ticker 
    
    final_report = [] 
    all_trade_logs = [] 
    
    print(f"\n[2] V12 핵심 가설 그리드 서치 시작 (총 {len(GRID_LOOKBACKS) * len(GRID_TARGET_DAYS)}개 모델 테스트)...")
    
    for lookback in GRID_LOOKBACKS:
        
        current_features = base_feature_columns + [f'SMA_{lookback}']
        print(f"\n--- [Processing Lookback {lookback}d] ---")
        print(f"  > [2.1] '정답(Y)' 컬럼 {len(GRID_TARGET_DAYS)}개 생성 중...")
        df_target_base = df_processed_v12.copy()
        
        for target_days in GRID_TARGET_DAYS:
            target_col_name = f'Target_Return_{target_days}d'
            tqdm.pandas(desc=f"Creating Target ({target_days}d)")
            df_target_base = df_target_base.groupby('종목코드', group_keys=False).progress_apply(lambda x: create_target_regression(x, target_days))
        
        df_target_base.dropna(subset=current_features, inplace=True)
        
        for target_days in GRID_TARGET_DAYS:
            
            hypothesis_name = f"({lookback}d_IN / {target_days}d_OUT)"
            print(f"\n--- [Testing Hypothesis] {hypothesis_name} ---")
            
            target_col_name = f'Target_Return_{target_days}d'
            df_target = df_target_base.dropna(subset=[target_col_name])
            
            split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
            train_df = df_target[df_target['날짜'] < split_date]
            test_df = df_target[df_target['날짜'] >= split_date]
            
            if train_df.empty or test_df.empty:
                print("  > 오류: 학습 또는 검증 데이터가 비어있습니다. 건너뜁니다."); continue

            print(f"  > [2.2] '{hypothesis_name}' 모델 재학습 중... (피처 {len(current_features)}개)")
            model = train_regression_model(train_df, current_features, target_col_name)
            
            print(f"  > [2.3] '{hypothesis_name}' 모델 백테스팅 (로그 저장)...")
            avg_return, trade_logs = backtest_model_with_logging(
                model, test_df, current_features, target_col_name, hypothesis_name, ticker_names
            )
            
            final_report.append((avg_return, hypothesis_name, target_days))
            all_trade_logs.extend(trade_logs)
            print(f"  > [Result] {hypothesis_name} = {avg_return*100:+.3f}%")

    # 3. '일일 거래 상세 로그' CSV 파일로 저장
    print("\n[3] 일일 거래 상세 로그 저장 중...")
    log_df = pd.DataFrame(all_trade_logs)
    log_filename = "detailed_trade_log_V12.csv"
    log_df.to_csv(log_filename, index=False, encoding='utf-8-sig')
    print(f"  > ✅ 12개 모델의 모든 거래 내역({len(log_df)}건)이 '{log_filename}'에 저장되었습니다.")

    # 4. 최종 결과 보고
    print("\n" + "="*60)
    print(f"### 'V12 핵심 가설' 최종 성능 보고서 ###")
    print(f" (Top 1% 추천, {TEST_DURATION_DAYS}일 검증, 단순 N일 보유 전략)")
    print("="*60)
    
    final_report.sort(key=lambda x: x[0], reverse=True)
    
    print("  순위 |  모델 가설 (입력/예측) |  보유 기간 |  Top 1% 실제 평균 수익률")
    print("-------|----------------------|-----------|-----------------------")
    
    for i, (avg_return, name, days) in enumerate(final_report):
        print(f"  {i+1:<5} |  {name:<20} |  {days:<9} |    {avg_return*100:>+8.3f}%")
        
    print("="*60)
    
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")

# --- 코드 버전: V13 (V12 + Log Fix) ---