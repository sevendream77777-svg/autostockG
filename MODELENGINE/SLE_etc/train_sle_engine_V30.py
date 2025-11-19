from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# --- 코드 버전: V30 (Train Sle Engine Only) ---
import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib 
from datetime import datetime, timedelta
import os
from tqdm import tqdm

# --- 1. V30 설정 ---
HOJ_DB_FILE = "all_features_cumulative_V21_Hoj.parquet" # (Y 정답 추출용)
SLE_DB_FILE = SLE_DB_REAL        # (X 입력 피처)
FINAL_SLE_MODEL_FILE = "SLE_CHAMPION_MODEL_V30.pkl" # (★★★ V30: Sle 엔진 뇌 ★★★)

TARGET_DAYS = 5    # (Hoj 챔피언과 동일하게 5일 예측)
TEST_DURATION_DAYS = 365 # 1년치 검증

# (★★★ V30: 'Sle 엔진' 4개 피처 ★★★)
feature_columns_sle = [
    'PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY'
]

# (V22 튜닝 파라미터 - 동일하게 적용)
PARAMS_TUNED_A = {
    'objective': 'regression_l1', 'n_estimators': 500, 'learning_rate': 0.05, 
    'num_leaves': 41, 'random_state': 42, 'n_jobs': -1
}

# (정답(Y) 생성 함수 - 5일)
def create_target_regression(group_df, target_days):
    future_price = group_df['종가'].shift(-target_days)
    future_return = (future_price - group_df['종가']) / group_df['종가']
    group_df[f'Target_Return_{target_days}d'] = future_return 
    return group_df

# ===========================
# 🚀 V30 메인 함수
# ===========================
def train_v30_sle_engine():
    
    # --- 1. Sle DB 로드 (X 피처) ---
    print(f"[1] Sle 엔진 DB('{SLE_DB_FILE}') 로드 중...")
    if not os.path.exists(SLE_DB_FILE):
        print(f"  > 오류: '{SLE_DB_FILE}' 파일이 없습니다."); return
    try:
        df_sle = pd.read_parquet(SLE_DB_FILE)
        df_sle.rename(columns={'date': '날짜', 'ticker': '종목코드'}, inplace=True)
        df_sle['날짜'] = pd.to_datetime(df_sle['날짜'])
        
        # (Sle 피처 이름 확정)
        df_sle.rename(columns={'외국인순매수': 'FOR_NET_BUY', '기관순매수': 'INS_NET_BUY'}, inplace=True)
        
        # (결측치(NaN)를 패널티 값(9999)으로 채우기 - AI 학습용)
        df_sle['PBR'] = df_sle['PBR'].fillna(9999)
        df_sle['PER'] = df_sle['PER'].fillna(9999)
        df_sle['FOR_NET_BUY'] = df_sle['FOR_NET_BUY'].fillna(0)
        df_sle['INS_NET_BUY'] = df_sle['INS_NET_BUY'].fillna(0)
        
        print(f"  > Sle DB 로드 및 정제 성공. (총 {len(df_sle):,} 행)")
    except Exception as e:
        print(f"  > 오류: {SLE_DB_FILE} 파일 로드 실패. ({e})"); return

    # --- 2. Hoj DB 로드 (Y 정답 추출용) ---
    print(f"[2] Hoj 엔진 DB('{HOJ_DB_FILE}') 로드 중... (정답 Y 추출용)")
    if not os.path.exists(HOJ_DB_FILE):
        print(f"  > 오류: '{HOJ_DB_FILE}' 파일이 없습니다."); return
    try:
        df_hoj = pd.read_parquet(HOJ_DB_FILE)
        df_hoj['날짜'] = pd.to_datetime(df_hoj['날짜'])
        
        # (Hoj DB에서 '정답' 컬럼만 생성)
        target_col_name = f'Target_Return_{TARGET_DAYS}d'
        tqdm.pandas(desc=f"Creating Target ({TARGET_DAYS}d)")
        df_hoj = df_hoj.groupby('종목코드', group_keys=False).progress_apply(lambda x: create_target_regression(x, TARGET_DAYS))
        
        # (정답 추출에 필요한 최소한의 컬럼만 남김)
        df_hoj_target = df_hoj[['날짜', '종목코드', target_col_name]].copy()
        df_hoj_target.dropna(subset=[target_col_name], inplace=True)
        
        print(f"  > Hoj DB에서 '정답({target_col_name})' {len(df_hoj_target):,}건 추출 완료.")
    except Exception as e:
        print(f"  > 오류: {HOJ_DB_FILE} 로드/처리 실패. ({e})"); return

    # --- 3. (V30 핵심) Sle (X) + Hoj (Y) 병합 -> Sle 학습 데이터 완성 ---
    print("[3] Sle (X) + Hoj (Y) 병합 중...")
    df_v30_train_data = pd.merge(
        df_sle, 
        df_hoj_target, 
        on=['날짜', '종목코드'], 
        how='inner' # (Sle 샘플이면서, Hoj에 정답이 있는 데이터만)
    )
    
    # (학습에 필요한 4개 피처 + 1개 정답)
    df_v30_train_data = df_v30_train_data[feature_columns_sle + [target_col_name, '날짜']]
    df_v30_train_data.dropna(inplace=True) # (혹시 모를 결측치 제거)
    
    print(f"  > Sle 엔진 학습용 데이터 {len(df_v30_train_data):,}건 준비 완료.")

    # --- 4. 9년(학습) / 1년(검증) 분리 ---
    split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
    train_df = df_v30_train_data[df_v30_train_data['날짜'] < split_date]
    test_df = df_v30_train_data[df_v30_train_data['날짜'] >= split_date]
    
    if train_df.empty or test_df.empty:
        print("  > 오류: Sle 학습 또는 검증 데이터가 비어있습니다. (기간 문제)"); return

    # --- 5. (★★★) 'Sle 엔진' 학습 (4개 피처) ★★★
    print(f"\n[4] 'Sle 엔진' 학습 시작 (총 {len(train_df):,}건, 4개 피처)...")
    y_train = train_df[target_col_name] 
    X_train = train_df[feature_columns_sle] # (오직 Sle 4개 피처)
    X_train.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_train.columns]
    
    sle_model = lgb.LGBMRegressor(**PARAMS_TUNED_A)
    
    print("  > 모델 학습(fit) 중... (나무 500개)")
    sle_model.fit(X_train, y_train)
    print("  > 'Sle 엔진' 학습 완료!")

    # 6. 'Sle 엔진' 파일로 저장
    joblib.dump(sle_model, FINAL_SLE_MODEL_FILE)
    print(f"  > 'Sle 챔피언 모델'을 '{FINAL_SLE_MODEL_FILE}' 파일로 저장했습니다!")

    # --- 7. 'Sle 엔진' 단독 백테스팅 ---
    print(f"\n[5] 'Sle 엔진 (단독)' 백테스팅 시작...")
    y_test_actual = test_df[target_col_name] 
    X_test = test_df[feature_columns_sle]
    X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]
    
    y_pred_return = sle_model.predict(X_test)
    
    df_result = pd.DataFrame({'Predicted_Return': y_pred_return, 'Actual_Return': y_test_actual.values})
    top_1_percent_threshold = df_result['Predicted_Return'].quantile(0.99)
    top_group = df_result[df_result['Predicted_Return'] >= top_1_percent_threshold]
    
    if top_group.empty:
        avg_return = 0.0
    else:
        avg_return = top_group['Actual_Return'].mean()

    print("\n" + "="*60)
    print(f"### 'V30 Hoj/Sle' 하이브리드 테스트 (1/5) ###")
    print(f" (Hoj 챔피언(V22) vs Sle 챔피언(V30))")
    print("="*60)
    print(f"  > (참고) Hoj 엔진 (12 피처): +3.527%")
    print(f"  > (결과) Sle 엔진 (4 피처) : {avg_return*100:+.3f}%")
    print("="*60)

# ===========================
# 실행
# ===========================
if __name__ == "__main__":
    train_v30_sle_engine()

# --- 코드 버전: V30 (Train Sle Engine Only) ---