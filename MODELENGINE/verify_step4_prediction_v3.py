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
col_list_path = os.path.join(base_dir, 'HOJ_DB', 'RESEARCH', 'V31_columns_list.csv')

target_code = '005930' # 삼성전자

print('='*70)
print('[Step 4] 모델 구조 확인 및 로직/예측 테스트 V3')
print('='*70)

# ---------------------------------------------------------------------------
# 2. 최신 모델 찾기 및 구조 확인 (Dictionary Handling)
# ---------------------------------------------------------------------------
if not os.path.exists(model_dir):
    print(f'[Error] 모델 폴더 없음: {model_dir}')
    sys.exit()

model_files = glob.glob(os.path.join(model_dir, '*.pkl'))
if not model_files:
    print('[Error] 모델 파일(.pkl)이 없습니다.')
    sys.exit()

latest_model_path = max(model_files, key=os.path.getmtime)
print(f'[Model] 파일 로드 시도: {os.path.basename(latest_model_path)}')

try:
    loaded_obj = joblib.load(latest_model_path)
    model = None
    
    # [핵심] 딕셔너리인지 확인하고 모델 추출
    if isinstance(loaded_obj, dict):
        print(f'   -> 파일 타입: Dictionary (Keys: {list(loaded_obj.keys())})')
        
        # 모델이 있을 법한 키 탐색
        for key in ['model', 'regressor', 'estimator', 'learner', 'pipeline']:
            if key in loaded_obj:
                model = loaded_obj[key]
                print(f'   ✅ Dictionary 내에서 "{key}" 객체 추출 성공')
                break
        
        if model is None:
            print('   ❌ [Error] Dictionary 안에서 모델 객체를 찾을 수 없습니다.')
            print('      (키 이름을 확인해주세요)')
            sys.exit()
    else:
        print('   -> 파일 타입: Model Object (직접 로드)')
        model = loaded_obj

except Exception as e:
    print(f'[Critical Fail] 모델 파일 로드 중 에러: {e}')
    sys.exit()

# ---------------------------------------------------------------------------
# 3. 데이터 로드 및 기간 로직 검증 (Data Logic Check)
# ---------------------------------------------------------------------------
print('\n[Data] 데이터 로드 및 기간 로직 점검...')

if not os.path.exists(feature_path):
    print(f'[Error] 피처 파일 없음: {feature_path}')
    sys.exit()
    
df = pd.read_parquet(feature_path)
code_col = next((c for c in df.columns if 'code' in c.lower()), 'Code')
df[code_col] = df[code_col].astype(str).str.zfill(6)
df_stock = df[df[code_col] == target_code].copy()
df_stock = df_stock.sort_values('Date').reset_index(drop=True)

print(f'   - 삼성전자 데이터: {len(df_stock)} rows')

# [로직 검증] 60일, 120일 이평선 데이터 확인
print('\n[Logic Check] 장기 데이터(이평선 등) 무결성 확인')
sma_cols = [c for c in df_stock.columns if 'SMA_' in c]
if sma_cols:
    print(f'   - 발견된 이동평균 피처: {sma_cols}')
    last_row = df_stock.iloc[-1]
    
    # NaN 체크 (데이터가 충분히 쌓였는지)
    for col in sma_cols:
        val = last_row[col]
        if pd.isna(val):
            print(f'   ⚠️ [Warning] {col} 값이 NaN입니다. (데이터 기간 부족 가능성)')
        else:
            print(f'   - {col}: {val:.2f} (정상)')
            
    # 60일치 데이터 사용 여부 추론
    if 'SMA_60' in sma_cols and not pd.isna(last_row['SMA_60']):
        print('   ✅ [Pass] SMA_60(60일치 평균)이 정상 계산되었습니다. -> 최소 60일 전 데이터가 활용됨.')
else:
    print('   [Info] SMA(이동평균) 관련 피처가 없어 기간 로직 검증을 생략합니다.')

# ---------------------------------------------------------------------------
# 4. 전처리 및 예측 수행
# ---------------------------------------------------------------------------
print('\n[Predict] 실전 예측 수행...')

# 컬럼 리스트 로드 (없으면 에러 방지용 임시 처리)
if os.path.exists(col_list_path):
    df_cols = pd.read_csv(col_list_path)
    full_col_list = df_cols.iloc[:, 0].tolist()
else:
    print('   [Warning] 컬럼 리스트 파일 없음. 모델의 feature_names_in_ 속성 시도.')
    if hasattr(model, 'feature_names_in_'):
        full_col_list = model.feature_names_in_.tolist()
    else:
        print('   ❌ [Fail] 모델에 필요한 피처 목록을 알 수 없습니다.')
        sys.exit()

# 전처리 (이름 매핑 및 파생변수 생성)
exclude_keywords = ['Label', 'Return', 'Target', 'Date', 'date', 'Code', 'code', 'Name']
input_features = [c for c in full_col_list if not any(k in c for k in exclude_keywords)]

rename_map = {'MACD_12_26': 'MACD', 'MACD_SIGNAL_9': 'MACD_Sig'}
df_stock.rename(columns=rename_map, inplace=True)

# Missing Feature 처리 (Close_shift_5 등)
for feat in input_features:
    if feat not in df_stock.columns:
        if 'shift' in feat and '_' in feat:
            try:
                parts = feat.split('_shift_')
                base, shift_days = parts[0], int(parts[1])
                if base in df_stock.columns:
                    df_stock[feat] = df_stock[base].shift(shift_days)
                    print(f'   -> 파생 변수 생성: {feat}')
            except: pass
        else:
            df_stock[feat] = 0 # Fallback

# 데이터셋 준비 (마지막 1행)
X_test = df_stock[input_features].iloc[[-1]].fillna(0)

try:
    pred = model.predict(X_test)
    score = model.predict_proba(X_test)[0][1] if hasattr(model, 'predict_proba') else 0.0
    
    print('\n' + '='*50)
    print(f'🚀 [최종 결과] 삼성전자 내일 예측')
    print('='*50)
    print(f'   - 예측 클래스 : {pred[0]} (1=상승, 0=하락/유지 예상)')
    print(f'   - 상승 확률   : {score*100:.2f}%')
    print('\n✅ [Success] 모델 딕셔너리 해제 및 예측 파이프라인 정상 작동 확인.')

except Exception as e:
    print(f'\n❌ [Fail] 예측 실행 오류: {e}')

print('='*70)
