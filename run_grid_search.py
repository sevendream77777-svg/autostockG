import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
from sklearn.metrics import precision_score, recall_score, confusion_matrix
from datetime import datetime, timedelta
from tqdm import tqdm
import re
import os 
import time

# (필수 라이브러리)
# pip install pandas pandas-ta lightgbm scikit-learn tqdm tables fastparquet

# --- 1. ★★★ 그리드 서치 (Grid Search) 파라미터 ★★★ ---
#
# (주의!) 조합이 많을수록 실행 시간이 기하급수적으로 늘어납니다.
# (현재: 2 x 2 x 3 = 12개 모델 생성 / 12 * 4 = 48개 결과)
#
# 1. 입력 기간 (Lookback)
GRID_LOOKBACKS = [60, 40] # (V5, V6 가설)
# 2. 예측 기간 (Target Days)
GRID_TARGET_DAYS = [5, 10] # (5일, 10일)
# 3. 목표 수익률 (Target Return)
GRID_TARGET_RETURNS = [0.03, 0.05, 0.07] # (3%, 5%, 7%)
# 4. 검증 기준선 (Thresholds)
GRID_THRESHOLDS = [0.5, 0.45, 0.4, 0.35]
#
# --- ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★ ---

TEST_DURATION_DAYS = 365
# 모든 결과를 저장할 리스트
MASTER_RESULTS_REPORT = []


# --- 2. V-Dynamic 데이터 처리 (Process) ---
def process_data_dynamic(lookback_longest):
    # (이 함수는 lookback_longest가 바뀔 때만 실행되어야 함 - 캐싱)
    
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
    df_merged = pd.merge(df_stocks, 
                         df_kospi[['날짜', 'KOSPI_종가', 'KOSPI_수익률', 'KOSPI_SMA_20']], 
                         on='날짜', 
                         how='left')
    df_merged.sort_values(by=['날짜', '종목코드'], inplace=True)

    # V-Dynamic 지표 계산 함수
    def calculate_indicators_dynamic(group_df):
        # (★★★ Dynamic 수정 ★★★)
        group_df[f'SMA_{lookback_longest}'] = ta.sma(group_df['종가'], length=lookback_longest) 
        group_df['SMA_20'] = ta.sma(group_df['종가'], length=20)
        
        # (V4/V5/V6 피처는 그대로 유지)
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
    tqdm.pandas(desc=f"Calculating Indicators (Lookback={lookback_longest})")
    df_processed = df_merged.groupby('종목코드', group_keys=False).progress_apply(calculate_indicators_dynamic)
    df_processed.dropna(inplace=True)
    return df_processed

# --- 3. V-Dynamic 정답 생성 및 분리 (Split) ---
def create_target_and_split(df, target_days, target_return):
    # '정답(Target)' 컬럼 생성
    def create_target(group_df):
        future_price = group_df['종가'].shift(-target_days)
        future_return = (future_price - group_df['종가']) / group_df['종가']
        group_df['Target'] = np.where(future_return >= target_return, 1, 0)
        return group_df
    
    tqdm.pandas(desc=f"Creating Target ({target_days}d / {target_return*100}%)")
    df_target = df.groupby('종목코드', group_keys=False).progress_apply(create_target)
    df_target.dropna(subset=['Target'], inplace=True)

    # 데이터 분리
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_target[df_target['날짜'] < split_date]
    test_df = df_target[df_target['날짜'] >= split_date]
    return train_df, test_df

# --- 4. V-Dynamic 모델 학습 (Train) ---
def train_model_dynamic(train_df, lookback_longest):
    y_train = train_df['Target']
    
    # (★★★ Dynamic 수정 ★★★)
    # V4/V5/V6 피처 리스트 (SMA_X가 동적으로 바뀜)
    feature_columns_dynamic = [
        'SMA_20', f'SMA_{lookback_longest}', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
        'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
    ]
    
    X_train = train_df[feature_columns_dynamic]
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]

    # '가중치 없음' (보수적 모델)
    lgbm_model = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.1, num_leaves=31, random_state=42, n_jobs=-1)
    lgbm_model.fit(X_train, y_train)
    return lgbm_model, feature_columns_dynamic

# --- 5. V-Dynamic 백테스팅 (Backtest) ---
def backtest_model_dynamic(model, test_df, feature_columns, threshold_list):
    y_test = test_df['Target']
    X_test = test_df[feature_columns]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

    # 예측 (확률 계산)
    y_pred_proba = model.predict_proba(X_test)[:, 1] # (1이 될 확률)
    
    # '자연 성공률'
    natural_success_rate = (y_test.sum() / len(y_test))
    
    results = []
    
    # 기준선(Threshold)을 바꿔가며 반복 테스트
    for threshold in threshold_list:
        y_pred_class = (y_pred_proba > threshold).astype(int)
        
        precision = precision_score(y_test, y_pred_class)
        recall = recall_score(y_test, y_pred_class)
        
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred_class).ravel()
        recommend_count = fp + tp
        
        results.append({
            "Precision": precision,
            "Recall": recall,
            "Threshold": threshold,
            "Recommend Count": recommend_count,
            "Natural Success Rate": natural_success_rate
        })
    return results


# --- 6. (★★★) 그리드 서치 마스터 루프 (★★★) ---
if __name__ == "__main__":
    print("--- 그리드 서치(자동화) 시작 ---")
    print(f"테스트할 모델 조합: {len(GRID_LOOKBACKS)} (입력) x {len(GRID_TARGET_DAYS)} (예측일) x {len(GRID_TARGET_RETURNS)} (수익률)")
    print(f"총 {len(GRID_LOOKBACKS) * len(GRID_TARGET_DAYS) * len(GRID_TARGET_RETURNS)}개의 모델을 생성합니다.")
    print("경고: 이 작업은 수 시간이 소요될 수 있습니다...")
    
    master_timer_start = time.time()
    
    # (캐시) 처리된 데이터 저장 (Lookback이 같으면 2단계 재실행 방지)
    processed_data_cache = {}

    # --- OUTER LOOP 1 (Lookback) ---
    for lookback in GRID_LOOKBACKS:
        
        # 2단계: 데이터 처리 (V-Dynamic)
        if lookback not in processed_data_cache:
            print(f"\n[Processing] Lookback={lookback} 신규 데이터 처리 시작...")
            processed_df = process_data_dynamic(lookback)
            processed_data_cache[lookback] = processed_df
        else:
            print(f"\n[Processing] Lookback={lookback} 캐시된 데이터 사용.")
            processed_df = processed_data_cache[lookback]

        if processed_df is None:
            print(f"Lookback={lookback} 데이터 처리 실패. 건너뜁니다.")
            continue

        # --- OUTER LOOP 2 & 3 (Target) ---
        for target_days in GRID_TARGET_DAYS:
            for target_return in GRID_TARGET_RETURNS:
                
                hypothesis_name = f"({lookback}d_IN / {target_days}d_OUT / {target_return*100:.0f}%_RET)"
                print(f"\n--- [Testing Hypothesis] {hypothesis_name} ---")
                
                # 3단계: 정답 생성 및 분리 (V-Dynamic)
                train_df, test_df = create_target_and_split(processed_df, target_days, target_return)
                
                # 4단계: 모델 학습 (V-Dynamic)
                model, feature_cols = train_model_dynamic(train_df, lookback)
                
                # 5단계: 백테스팅 (V-Dynamic)
                # (GRID_THRESHOLDS 리스트를 통째로 넘김)
                tuning_results = backtest_model_dynamic(model, test_df, feature_cols, GRID_THRESHOLDS)
                
                # --- 최종 리포트에 결과 추가 ---
                for result in tuning_results:
                    MASTER_RESULTS_REPORT.append({
                        "Hypothesis": hypothesis_name,
                        "Threshold": result["Threshold"] * 100,
                        "Precision": result["Precision"] * 100,
                        "Recall": result["Recall"] * 100,
                        "Recommend Count": result["Recommend Count"],
                        "Natural Rate": result["Natural Success Rate"] * 100
                    })
    
    # --- 7. (★★★) 최종 마스터 리포트 출력 (★★★) ---
    master_timer_end = time.time()
    print("\n" + "="*70)
    print("      ★★★ 최종 그리드 서치 마스터 리포트 ★★★")
    print(f" (총 실행 시간: {(master_timer_end - master_timer_start)/60:.1f} 분)")
    print("="*70)
    
    # DataFrame으로 변환하여 보기 좋게 출력
    report_df = pd.DataFrame(MASTER_RESULTS_REPORT)
    
    # '스위트 스폿' 찾기: 재현율(Recall) 5% 이상, 정밀도(Precision) 35% 이상
    report_df['Sweet Spot'] = np.where(
        (report_df['Recall'] >= 5.0) & (report_df['Precision'] >= 35.0), 
        "★★", ""
    )
    
    # 정밀도(Precision) 순으로 정렬
    report_df.sort_values(by="Precision", ascending=False, inplace=True)
    
    # 소수점 정리
    pd.set_option('display.float_format', '{:.2f}'.format)
    
    print(report_df.to_string(index=False, 
                            columns=["Hypothesis", "Threshold", "Precision", "Recall", "Recommend Count", "Natural Rate", "Sweet Spot"]))
    
    print("\n--- 그리드 서치(자동화) 완료 ---")