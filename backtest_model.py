import pandas as pd
import lightgbm as lgb
from sklearn.metrics import classification_report, confusion_matrix
import re
import numpy as np

# (필수 설치)
# pip install scikit-learn

# --- 1. 학습된 모델과 시험지(데이터) 불러오기 ---
model_file = "lgbm_model.txt"
test_data_file = "test_data.parquet"

print(f"'{model_file}' 모델을 불러옵니다...")
try:
    model = lgb.Booster(model_file=model_file)
except Exception as e:
    print(f"모델 불러오기 오류: {e}")
    exit()

print(f"'{test_data_file}' (1년 치 검증 데이터)를 불러옵니다...")
try:
    df_test = pd.read_parquet(test_data_file)
except Exception as e:
    print(f"검증 데이터 불러오기 오류: {e}")
    exit()

# --- 2. '시험 문제(X)'와 '정답(Y)' 분리 ---
y_test = df_test['Target']
feature_columns = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig'
]
X_test = df_test[feature_columns]
X_test.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_test.columns]

# --- 3. (핵심) 모델로 1년 치 '예측' 수행 ---
print("모델이 1년 치 데이터를 예측 중입니다...")
y_pred_proba = model.predict(X_test)

# ★★★ 기준선(Threshold) 설정 ★★★
# (일단 0.5로 고정, 나중에 이 숫자를 바꿀 수 있음)
THRESHOLD = 0.5
y_pred_class = (y_pred_proba > THRESHOLD).astype(int)

print(f"예측 완료! (기준선={THRESHOLD * 100}%)")
print("-" * 50)


# --- 4. '성능 평가표' 한글/백분율로 출력 ---

# 4.1. 혼동 행렬 (Confusion Matrix)
print("### 1. 혼동 행렬 (예측 vs 실제) ###")
tn, fp, fn, tp = confusion_matrix(y_test, y_pred_class).ravel()
# tn: (A) 예측 0, 실제 0
# fp: (B) 예측 1, 실제 0
# fn: (C) 예측 0, 실제 1
# tp: (D) 예측 1, 실제 1

print(f" ┌───────────────┬──────────────────┐")
print(f" │               │       실제       │")
print(f" │     예측      ├──── 0 (실패) ──── 1 (성공) ──┤")
print(f" ├───────────────┼──────────────────┤")
print(f" │   0 (실패)    │   {tn:8,d} (A)   {fn:8,d} (C) │")
print(f" │   1 (성공)    │   {fp:8,d} (B)   {tp:8,d} (D) │")
print(f" └───────────────┴──────────────────┘")
print("\n")
print(f" (A) [예측: 실패, 실제: 실패] (하락을 맞힘): {tn:,} 건")
print(f" (B) [예측: 성공, 실제: 실패] (AI의 실수): {fp:,} 건  <- 문제1")
print(f" (C) [예측: 실패, 실제: 성공] (AI가 놓친 기회): {fn:,} 건  <- 문제2")
print(f" (D) [예측: 성공, 실제: 성공] (AI의 성공): {tp:,} 건")


# 4.2. 분류 성능 평가표 (Classification Report)
print("\n### 2. 분류 성능 평가표 (%) ###")
report_dict = classification_report(y_test, y_pred_class, target_names=['0 (실패)', '1 (성공)'], output_dict=True)

# 0 (실패)에 대한 리포트
r_0 = report_dict['0 (실패)']
print("\n[ 0 (실패) 예측 성능 ]")
print(f"  > 정밀도 (Precision): {r_0['precision'] * 100:6.2f}%  (AI가 '실패'라고 한 것 중 실제 '실패'일 확률)")
print(f"  > 재현율 (Recall)   : {r_0['recall'] * 100:6.2f}%  (실제 '실패' 중 AI가 '실패'라고 맞힌 확률)")
print(f"  > 데이터 수 (Support): {r_0['support']:,} 건")

# 1 (성공)에 대한 리포트
r_1 = report_dict['1 (성공)']
print("\n[ 1 (성공) 예측 성능 ] (★ 여기가 가장 중요 ★)")
print(f"  > 정밀도 (Precision): {r_1['precision'] * 100:6.2f}%  (AI가 '성공'이라고 한 것 중 실제 '성공'일 확률)")
print(f"  > 재현율 (Recall)   : {r_1['recall'] * 100:6.2f}%  (실제 '성공' 중 AI가 '성공'이라고 맞힌 확률)")
print(f"  > 데이터 수 (Support): {r_1['support']:,} 건")

# 전체 요약
acc = report_dict['accuracy']
macro_avg = report_dict['macro avg']['f1-score']
weighted_avg = report_dict['weighted avg']['f1-score']
print("\n[ 전체 요약 ]")
print(f"  > 전체 정확도 (Accuracy): {acc * 100:6.2f}%  (단순히 '성공/실패'를 맞힌 비율)")
print(f"  > 단순 평균 (Macro Avg): {macro_avg * 100:6.2f}%")
print(f"  > 가중 평균 (W. Avg)  : {weighted_avg * 100:6.2f}%")

print("-" * 50)
print("백테스팅(성능 검증) 완료.")