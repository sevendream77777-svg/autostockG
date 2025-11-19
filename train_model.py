import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
import re

# (필수 설치) 
# pip install lightgbm scikit-learn

# --- 1. 학습 데이터 불러오기 ---
# (V4-3에서 덮어쓴 V4 데이터가 로드됩니다)
input_file = "train_data.parquet"
print(f"'{input_file}' (9년 치 V4 학습 데이터)를 불러오는 중...")
try:
    df_train = pd.read_parquet(input_file)
except Exception as e:
    print(f"파일 읽기 오류: {e}")
    print("이전 단계(create_target_and_split.py)가 V4로 완료되었는지 확인하세요.")
    exit()

print("파일 불러오기 완료.")

# --- 2. '입력(X)'과 '정답(Y)' 분리 ---

# '정답(Y)'은 'Target' 컬럼
y = df_train['Target']

# (★★★ V4 수정 사항 ★★★)
# AI가 학습할 피처 리스트 (총 12개)
feature_columns = [
    # 기존 V2 지표 (8개)
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14',
    # 신규 V4 지표 (4개)
    'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]
X = df_train[feature_columns]

print(f"모델 학습을 시작합니다. (입력 피처 총 {len(feature_columns)}개)")
print(feature_columns)


# --- 3. LightGBM 모델 학습 ---
# LightGBM은 컬럼 이름에 특수문자가 있으면 오류가 날 수 있음
X.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X.columns]

# 모델 생성 (LGBMClassifier: 분류 모델)
lgbm_model = lgb.LGBMClassifier(
    n_estimators=100,      # 100개의 '나무'를 만듦
    learning_rate=0.1,     # 학습률
    num_leaves=31,         # '나무'의 최대 잎사귀 수
    random_state=42,       # 재현성을 위한 시드 고정
    n_jobs=-1             # 모든 CPU 코어 사용
    
    # (★★★ V4 수정 사항 ★★★)
    # 'scale_pos_weight' 옵션을 *제거*함. (모델 1처럼 보수적으로 학습)
)

print("-" * 30)
print("V4 피처로 모델 학습을 시작합니다...")
# (핵심) 모델 학습!
lgbm_model.fit(X, y)

print("-" * 30)
print("V4 모델 학습 완료!")

# --- 4. 학습된 모델 파일로 저장 ---
output_model_file = "lgbm_model.txt"
lgbm_model.booster_.save_model(output_model_file) 

print(f"학습된 V4 모델을 '{output_model_file}' 파일로 덮어썼습니다.")
print("\n이제 'backtest_model_readable.py'를 다시 실행해서")
print("V4 모델의 Precision(정밀도)을 확인할 차례입니다!")