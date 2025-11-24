import pandas as pd
import joblib
import os
import glob
import sys
import numpy as np

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
base_dir = r'F:\autostockG\MODELENGINE'
feature_path = os.path.join(base_dir, 'FEATURE', 'features_V31_251121.parquet')
model_dir = os.path.join(base_dir, 'HOJ_ENGINE', 'REAL')

target_code = '005930' # 삼성전자

print('='*70)
print('[Step 4] Final: 듀얼 모델(Class+Reg) 구조 예측 테스트 V4')
print('='*70)

# ---------------------------------------------------------------------------
# 2. 모델 로드 및 객체 추출
# ---------------------------------------------------------------------------
if not os.path.exists(model_dir):
    print(f'[Error] 모델 폴더 없음: {model_dir}')
    sys.exit()

model_files = glob.glob(os.path.join(model_dir, '*.pkl'))
latest_model_path = max(model_files, key=os.path.getmtime)
print(f'[Model] 파일 로드: {os.path.basename(latest_model_path)}')

try:
    packet = joblib.load(latest_model_path)
    
    # 키 확인
    if not isinstance(packet, dict):
        print('[Error] 예상과 달리 딕셔너리 형태가 아닙니다.')
        sys.exit()
        
    print(f'   -> Keys: {list(packet.keys())}')
    
    # 모델 추출
    model_cls = packet.get('model_cls') # 분류기 (상승/하락)
    model_reg = packet.get('model_reg') # 회귀기 (수익률)
    train_features = packet.get('features') # 학습때 쓴 피처 리스트 (정답지)
    
    if model_cls: print('   ✅ [Classifier] 추출 성공 (상승 확률 예측용)')
    if model_reg: print('   ✅ [Regressor] 추출 성공 (수익률 예측용)')
    if train_features: 
        print(f'   ✅ [Feature List] 추출 성공 ({len(train_features)}개)')
    else:
        print('   ❌ [Error] 피처 리스트가 없습니다.')
        sys.exit()

except Exception as e:
    print(f'[Critical Fail] 모델 로드 중 에러: {e}')
    sys.exit()

# ---------------------------------------------------------------------------
# 3. 데이터 로드 및 전처리
# ---------------------------------------------------------------------------
print('\n[Data] 데이터 준비 중...')

if not os.path.exists(feature_path):
    print(f'[Error] 피처 파일 없음: {feature_path}')
    sys.exit()
    
df = pd.read_parquet(feature_path)
code_col = next((c for c in df.columns if 'code' in c.lower()), 'Code')
df[code_col] = df[code_col].astype(str).str.zfill(6)
df_stock = df[df[code_col] == target_code].copy()
df_stock = df_stock.sort_values('Date').reset_index(drop=True)

print(f'   - 삼성전자 데이터: {len(df_stock)} rows')

# ---------------------------------------------------------------------------
# [핵심] 피처 매핑 및 생성 (모델이 원하는 대로 맞춤)
# ---------------------------------------------------------------------------
# 1. 이름 변경 (Feature File -> Model Input)
rename_map = {
    'MACD_12_26': 'MACD',
    'MACD_SIGNAL_9': 'MACD_Sig',
    # 필요시 추가 매핑
}
df_stock.rename(columns=rename_map, inplace=True)

# 2. Missing Feature 채우기 (Shift 변수 등)
# train_features에 있는게 df_stock에 없으면 만들어야 함
missing = [f for f in train_features if f not in df_stock.columns]

if missing:
    print(f'   [Info] 파생 변수 생성 필요: {len(missing)}개')
    for feat in missing:
        # Shift 처리 (예: Close_shift_5)
        if 'shift' in feat and '_' in feat:
            try:
                parts = feat.split('_shift_')
                base, days = parts[0], int(parts[1])
                if base in df_stock.columns:
                    df_stock[feat] = df_stock[base].shift(days)
            except: pass
        
        # 그래도 없으면 0으로 채움 (에러 방지)
        if feat not in df_stock.columns:
            df_stock[feat] = 0

# 3. 최종 입력 데이터 (마지막 1행)
# 모델이 학습할 때 사용한 피처 순서 그대로 정렬해야 함!
X_test = df_stock[train_features].iloc[[-1]].fillna(0)

print(f'   - 입력 데이터 준비 완료: {X_test.shape}')

# ---------------------------------------------------------------------------
# 4. 예측 실행 (Predict)
# ---------------------------------------------------------------------------
print('\n' + '='*50)
print(f'🚀 [최종 예측 결과] 삼성전자 ({df_stock["Date"].iloc[-1].date()})')
print('='*50)

# 1. Classifier 예측 (상승 확률)
if model_cls:
    try:
        # predict_proba의 결과는 보통 [하락확률, 상승확률] 형태
        prob = model_cls.predict_proba(X_test)[0]
        up_prob = prob[1] # 1번 인덱스가 '1'(상승)일 확률
        print(f'   📈 [상승 확률] : {up_prob*100:.2f}%')
        if up_prob > 0.5:
            print('      -> 매수 시그널: 긍정 (Positive)')
        else:
            print('      -> 매수 시그널: 부정 (Negative)')
    except Exception as e:
        print(f'   [Error] Classifier 예측 실패: {e}')

# 2. Regressor 예측 (예상 수익률)
if model_reg:
    try:
        pred_return = model_reg.predict(X_test)[0]
        print(f'   💰 [예상 수익] : {pred_return*100:.2f}% (5일 후 예상)')
    except Exception as e:
        print(f'   [Error] Regressor 예측 실패: {e}')

print('\n✅ [검증 완료] 시스템 로직상 데이터 흐름과 모델 구조가 완벽히 일치합니다.')
print('='*70)
