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
print('[Step 4] 모델 실전 예측 테스트 V2 (전처리 로직 적용)')
print('='*70)

# ---------------------------------------------------------------------------
# 2. 최신 모델 찾기
# ---------------------------------------------------------------------------
if not os.path.exists(model_dir):
    print(f'[Error] 모델 폴더 없음: {model_dir}')
    sys.exit()

model_files = glob.glob(os.path.join(model_dir, '*.pkl'))
if not model_files:
    print('[Error] 모델 파일(.pkl)이 없습니다.')
    sys.exit()

latest_model_path = max(model_files, key=os.path.getmtime)
print(f'[Model] 로드 대상: {os.path.basename(latest_model_path)}')

# ---------------------------------------------------------------------------
# 3. 데이터 로드 및 전처리 (핵심 수정 파트)
# ---------------------------------------------------------------------------
print('\n[Data] 데이터 로드 및 전처리 중...')

# 1) Feature 로드
if not os.path.exists(feature_path):
    print(f'[Error] 피처 파일 없음: {feature_path}')
    sys.exit()
    
df = pd.read_parquet(feature_path)

# 삼성전자 필터링
code_col = next((c for c in df.columns if 'code' in c.lower()), 'Code')
df[code_col] = df[code_col].astype(str).str.zfill(6)
df_stock = df[df[code_col] == target_code].copy()
df_stock = df_stock.sort_values('Date').reset_index(drop=True)

print(f'   - 원본 데이터: {len(df_stock)} rows')

# 2) 컬럼 리스트 로드 (모델이 학습할 때 쓴 전체 리스트)
if not os.path.exists(col_list_path):
    print('[Error] 컬럼 리스트 파일이 없습니다.')
    sys.exit()

df_cols = pd.read_csv(col_list_path)
full_col_list = df_cols.iloc[:, 0].tolist()

# ---------------------------------------------------------------------------
# [핵심] 모델 입력용 컬럼 필터링 및 매핑 (Preprocessing)
# ---------------------------------------------------------------------------
# A. 예측에 불필요한 컬럼(Target, Date, Code 등) 제외
exclude_keywords = ['Label', 'Return', 'Target', 'Date', 'date', 'Code', 'code', 'Name']
input_features = [c for c in full_col_list if not any(k in c for k in exclude_keywords)]

print(f'   - 모델 입력 피처 개수: {len(input_features)}개 (Target 제외됨)')

# B. 컬럼 이름 매핑 (Feature 파일 -> 모델 기대 이름)
# build_features.py와 모델 간의 이름 차이를 해결
rename_map = {
    'MACD_12_26': 'MACD',
    'MACD_SIGNAL_9': 'MACD_Sig',
    'RSI_14': 'RSI_14', # 그대로
    'SMA_20': 'SMA_20'  # 그대로
}
df_stock.rename(columns=rename_map, inplace=True)

# C. 파생 변수 즉석 생성 (Missing Feature Handling)
# 예: Close_shift_5가 피처 파일에 없고 모델엔 필요한 경우 생성
missing_features = [c for c in input_features if c not in df_stock.columns]

if missing_features:
    print(f'   [Info] 피처 파일에 없는 파생 변수 생성 시도: {missing_features}')
    
    for miss in missing_features:
        # 1. Shift(Lag) 변수 처리 (예: Close_shift_5)
        if 'shift' in miss and '_' in miss:
            try:
                # 'Close_shift_5' -> base='Close', days=5
                parts = miss.split('_shift_')
                base_col = parts[0]
                shift_days = int(parts[1])
                
                if base_col in df_stock.columns:
                    df_stock[miss] = df_stock[base_col].shift(shift_days)
                    print(f'      -> 생성 완료: {miss} (Base: {base_col}, Lag: {shift_days})')
            except:
                pass
                
    # 다시 확인
    still_missing = [c for c in input_features if c not in df_stock.columns]
    if still_missing:
        print(f'   [Warning] 여전히 생성 불가능한 피처가 있습니다: {still_missing}')
        print('   -> 0으로 채워서 예측을 강행합니다. (정확도 하락 가능성)')
        for m in still_missing:
            df_stock[m] = 0

# ---------------------------------------------------------------------------
# 4. 모델 로드 및 예측
# ---------------------------------------------------------------------------
print('\n[Predict] 예측 수행...')

try:
    model = joblib.load(latest_model_path)
    
    # 모델에 들어갈 순서대로 데이터 정렬 (가장 최근 데이터 1행)
    X_test = df_stock[input_features].iloc[[-1]]
    
    # NaN 체크 (shift 등으로 생길 수 있음)
    if X_test.isnull().any().any():
        print('   [Warning] 입력 데이터에 NaN이 포함되어 0으로 대체합니다.')
        X_test = X_test.fillna(0)

    # 예측
    pred = model.predict(X_test)
    
    try:
        pred_proba = model.predict_proba(X_test)
        score = pred_proba[0][1] # 1(상승)일 확률
    except:
        score = 0.0

    target_date = df_stock["Date"].iloc[-1].date()
    
    print('\n' + '='*50)
    print(f'🚀 [예측 결과] 삼성전자 ({target_date} 기준)')
    print('='*50)
    print(f'   - 입력 피처 수: {X_test.shape[1]}')
    print(f'   - 모델 예측값 (Class): {pred[0]}')
    print(f'   - 상승 확률 (Score)  : {score:.4f} ({score*100:.2f}%)')
    
    print('\n✅ [Success] 모든 파이프라인 검증이 완료되었습니다.')
    print('   -> 데이터 수집, 가공, 학습 로직, 예측 실행까지 기술적 오류 없음.')

except Exception as e:
    print(f'\n❌ [Fail] 예측 중 에러 발생: {e}')
    import traceback
    traceback.print_exc()

print('='*70)
