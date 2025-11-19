import pandas as pd
import lightgbm as lgb
import joblib # 모델 저장을 위해 사용
import re

# (필수 설치) 
# pip install lightgbm scikit-learn joblib 

# --- 1. 학습 데이터 불러오기 ---
input_file = "train_data.parquet" # V4 피처와 Target_Return이 포함된 파일
print(f"'{input_file}' (9년 치 V4 학습 데이터)를 불러오는 중...")
try:
    df_train = pd.read_parquet(input_file)
except Exception as e:
    print(f"파일 읽기 오류: {e}")
    exit()

print("파일 불러오기 완료.")

# --- 2. '입력(X)'과 '정답(Y)' 분리 ---

# (★★★ 핵심 변경 ★★★)
# 'Target' (0 또는 1)이 아닌, 'Target_Return' (실제 수익률)을 정답(Y)으로 사용
y = df_train['Target_Return'] 

# AI가 학습할 피처 리스트 (총 12개)
feature_columns = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14',
    'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]
X = df_train[feature_columns]
X.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X.columns] # 컬럼명 정리

print(f"회귀 모델 학습을 시작합니다. (입력 피처 총 {len(feature_columns)}개)")


# --- 3. LightGBM 모델 학습 (회귀) ---
# (★★★ 핵심 변경 ★★★)
# LGBMClassifier -> LGBMRegressor (숫자를 예측하는 모델)
lgbm_model = lgb.LGBMRegressor(
    objective='regression_l1', # 손실 함수: MAE (평균절대오차) - 이상치에 강함
    n_estimators=100,
    learning_rate=0.1,
    num_leaves=31,
    random_state=42,
    n_jobs=-1
)

print("-" * 30)
print("회귀 챔피언 모델 학습을 시작합니다...")
lgbm_model.fit(X, y)

print("-" * 30)
print("회귀 챔피언 모델 학습 완료!")

# --- 4. 학습된 모델 파일로 저장 ---
# (★★★ 핵심 변경 ★★★)
# joblib을 사용하여 모델 전체를 .pkl 파일로 저장
output_model_file = "champion_model_reg.pkl" 
joblib.dump(lgbm_model, output_model_file)

print(f"학습된 회귀 챔피언 모델을 '{output_model_file}' 파일로 저장했습니다.")