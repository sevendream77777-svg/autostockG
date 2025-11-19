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

# --- 1. 'V9' 가설 설정 ---
LOOKBACK_LONGEST = 60  
TARGET_DAYS = 5      
FEATURE_FILE = "all_features_cumulative.parquet" 
MODEL_FILE = "champion_model_60_5.pkl"

# V5 피처 리스트
feature_columns_v5 = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

# (테스트할 '매도 전략' 목록)
STRATEGY_RULES = {
    "V5_Baseline": {'tp': None, 'sl': None, 'days': 5},
    "V6_Strategy": {'tp': 0.07, 'sl': -0.03, 'days': 5},
    "V7_Aggressive": {'tp': 0.10, 'sl': -0.05, 'days': 5},
    "V8_Balanced": {'tp': 0.05, 'sl': -0.05, 'days': 5}
}
TEST_DURATION_DAYS = 365

# --- 3. (★★★) V9 '전략 그리드 서치 + 일일 로그 저장' 함수 ★★★
def run_strategy_grid_search_v9(model, test_df, full_data):
    print("\n[5단계] V9 '매매 전략 그리드 서치 + 로그 저장' 백테스팅 시작...")
    
    # 5.1. AI가 1년 치 데이터로 '예상 수익률' 예측
    print("  > 1년 치 검증 데이터 '예상 수익률' 예측 중...")
    X_test = test_df[feature_columns_v5]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]
    y_pred_return = model.predict(X_test)
    
    # 5.2. '예상 수익률'과 '실제 데이터' 결합
    df_result = test_df.copy()
    df_result['Predicted_Return'] = y_pred_return
    
    # 5.3. '상위 1%' 추천 종목만 추출
    top_1_percent_threshold = df_result['Predicted_Return'].quantile(0.99)
    buy_list = df_result[df_result['Predicted_Return'] >= top_1_percent_threshold]
    
    print(f"  > AI 추천 'Top 1%' 종목 총 {len(buy_list):,}건 시뮬레이션 시작...")
    
    # (★★★ 신규 1: 속도 향상을 위해 티커 이름 미리 로드 ★★★)
    print("  > (로그 저장을 위해 티커 이름 미리 로드 중...)")
    unique_tickers = buy_list['종목코드'].unique()
    ticker_names = {}
    for ticker in tqdm(unique_tickers, desc="Loading Ticker Names"):
        try:
            ticker_names[ticker] = pykrx.stock.get_market_ticker_name(ticker)
            time.sleep(0.01) 
        except:
            ticker_names[ticker] = ticker 

    # (★★★ 신규 2: 모든 전략의 결과를 저장할 딕셔너리 및 로그 리스트 ★★★)
    strategy_results = {name: [] for name in STRATEGY_RULES.keys()}
    all_trade_logs = [] 
    
    # (★★★ 속도 최적화: MultiIndex 생성 ★★★)
    print("  > (Optimization) Full data MultiIndex 생성 중... (속도 향상용)")
    full_data_indexed = full_data.set_index(['종목코드', '날짜']).sort_index()
    print("  > MultiIndex 생성 완료.")
    
    # 5.4. (핵심) 'Top 1%' 종목을 하나씩 '일일 감시'
    for index, row in tqdm(buy_list.iterrows(), total=len(buy_list), desc="Simulating Trades"):
        ticker = row['종목코드']
        entry_date = row['날짜']
        entry_price = row['종가'] 
        
        for strategy_name, rules in STRATEGY_RULES.items():
            
            # 5일 단순 보유 (V5_Baseline) 로직
            if rules['tp'] is None:
                final_return = row['Target_Return'] 
                strategy_results[strategy_name].append(final_return)
                all_trade_logs.append({
                    "Strategy": strategy_name, "Date": entry_date.strftime("%Y-%m-%d"),
                    "Ticker": ticker, "Name": ticker_names.get(ticker, ticker),
                    "Entry_Price": entry_price, "Predicted_Return_AI": row['Predicted_Return'],
                    "Actual_Return_Strategy": final_return, "Days_Held": 5, "Exit_Reason": "Time_Cut"
                })
                continue 

            # 익절/손절 (V6, V7, V8) 로직
            max_days = rules['days']
            
            # (★★★ 최적화: .loc[]을 사용한 빠른 필터링 ★★★)
            try:
                start_slice = entry_date + timedelta(days=1)
                future_prices_all = full_data_indexed.loc[ticker].loc[start_slice:]
                future_prices_all = future_prices_all[~future_prices_all.index.duplicated(keep='first')]
                future_prices = future_prices_all.head(max_days)
            except KeyError:
                strategy_results[strategy_name].append(0.0); continue
            # (★★★ 최적화 완료 ★★★)

            if future_prices.empty:
                strategy_results[strategy_name].append(0.0); continue

            final_return = 0.0; trade_closed = False; days_held = 0; exit_reason = "N/A"

            for i, day in future_prices.iterrows():
                days_held += 1
                day_high = day['고가']; day_low = day['저가']
                
                if (day_high - entry_price) / entry_price >= rules['tp']:
                    final_return = rules['tp']; trade_closed = True; exit_reason = "Take_Profit"; break 
                if (day_low - entry_price) / entry_price <= rules['sl']:
                    final_return = rules['sl']; trade_closed = True; exit_reason = "Stop_Loss"; break
            
            if not trade_closed:
                last_day_close = future_prices.iloc[-1]['종가']
                final_return = (last_day_close - entry_price) / entry_price
                exit_reason = "Time_Cut"
                
            strategy_results[strategy_name].append(final_return)
            all_trade_logs.append({
                "Strategy": strategy_name, "Date": entry_date.strftime("%Y-%m-%d"),
                "Ticker": ticker, "Name": ticker_names.get(ticker, ticker),
                "Entry_Price": entry_price, "Predicted_Return_AI": row['Predicted_Return'],
                "Actual_Return_Strategy": final_return, "Days_Held": days_held, "Exit_Reason": exit_reason
            })

    # 5.5. 최종 결과 보고 (이전과 동일)
    print("\n" + "="*60)
    print(f"### '매매 전략 그리드 서치' 최종 성능 보고서 ###")
    print(f" (모델: {LOOKBACK_LONGEST}일 입력 / {TARGET_DAYS}일 예측, Top 1% 추천 대상)")
    print("="*60)
    print("  전략 이름       |  익절(TP) |  손절(SL) |  최종 평균 수익률")
    print("-----------------|-----------|-----------|-----------------")
    
    final_report = []
    for name, results in strategy_results.items():
        if not results: continue
        avg_return = np.mean(results) * 100
        rule = STRATEGY_RULES[name]
        tp_str = f"{rule['tp']*100:+.1f}%" if rule['tp'] else " N/A "
        sl_str = f"{rule['sl']*100:+.1f}%" if rule['sl'] else " N/A "
        final_report.append((avg_return, name, tp_str, sl_str))
    final_report.sort(key=lambda x: x[0], reverse=True)
    
    for report in final_report:
        avg_return, name, tp_str, sl_str = report
        print(f"  {name:<15} |  {tp_str:<8} |  {sl_str:<8} |    {avg_return:>+8.3f}%")
    print("="*60)
    
    # 5.6. '일일 거래 상세 로그' CSV 파일로 저장
    print("\n[6단계] 일일 거래 상세 로그 저장 중...")
    log_df = pd.DataFrame(all_trade_logs)
    log_filename = "detailed_trade_log_V9.csv"
    log_df.to_csv(log_filename, index=False, encoding='utf-8-sig')
    print(f"  > ✅ 'Top 1%'의 모든 거래 내역({len(log_df)}건)이 '{log_filename}'에 저장되었습니다.")
    
    print("V9 백테스팅(성능 검증) 완료.")


# --- 4. 메인 실행 (이전과 동일) ---
if __name__ == "__main__":
    start_time = time.time()
    
    try:
        print(f"[0] 챔피언 모델('{MODEL_FILE}') 로드 중...")
        model = joblib.load(MODEL_FILE) 
        print("  > 모델 로드 성공.")
    except Exception as e:
        print(f"  > 오류: '{MODEL_FILE}' 모델 파일이 없습니다. ({e})"); exit()

    try:
        print(f"[1] '{FEATURE_FILE}' (최종 피처 데이터) 로드 중...")
        df_processed = pd.read_parquet(FEATURE_FILE)
        df_processed['날짜'] = pd.to_datetime(df_processed['날짜'])
    except Exception as e:
        print(f"  > 오류: '{FEATURE_FILE}' 파일이 없습니다. ({e})"); exit()

    print("[2] '정답(Target_Return)' 생성 중...")
    def create_target_regression(group_df):
        future_price = group_df['종가'].shift(-TARGET_DAYS)
        future_return = (future_price - group_df['종가']) / group_df['종가']
        group_df['Target_Return'] = future_return 
        return group_df
    tqdm.pandas(desc="Creating Regression Target (Y)")
    df_target = df_processed.groupby('종목코드', group_keys=False).progress_apply(create_target_regression)
    df_target.dropna(subset=['Target_Return'], inplace=True)
    
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    test_df = df_target[df_target['날짜'] >= split_date]
    
    print(f"  > 검증용(1년) 데이터: {len(test_df):,} 행")
    
    run_strategy_grid_search_v9(model, test_df, df_processed)
        
    end_time = time.time()
    print(f"\n(총 실행 시간: {(end_time - start_time)/60:.1f} 분)")