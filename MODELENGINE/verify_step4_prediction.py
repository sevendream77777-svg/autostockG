import pandas as pd
import joblib
import os
import glob
import sys

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
# 파일 경로 (사용자 환경)
base_dir = r'F:\autostockG\MODELENGINE'
feature_path = os.path.join(base_dir, 'FEATURE', 'features_V31_251121.parquet')
model_dir = os.path.join(base_dir, 'HOJ_ENGINE', 'REAL')
col_list_path = os.path.join(base_dir, 'HOJ_DB', 'RESEARCH', 'V31_columns_list.csv')

target_code = '005930' # 삼성전자

print('='*70)
print('[Step 4] 모델 로딩 및 실전 예측 테스트')
print('='*70)

# ---------------------------------------------------------------------------
# 2. 최신 모델 파일 찾기
# ---------------------------------------------------------------------------
if not os.path.exists(model_dir):
    print(f'[Error] 모델 폴더를 찾을 수 없습니다: {model_dir}')
    sys.exit()

# .pkl 파일 중 가장 최근 것 찾기
model_files = glob.glob(os.path.join(model_dir, '*.pkl'))
if not model_files:
    print(f'[Error] 폴더에 .pkl 모델 파일이 없습니다: {model_dir}')
    sys.exit()

latest_model_path = max(model_files, key=os.path.getmtime)
print(f'[Model] 최신 모델 파일 감지: {os.path.basename(latest_model_path)}')

# ---------------------------------------------------------------------------
# 3. 데이터 및 컬럼 리스트 로드
# ---------------------------------------------------------------------------
print('\n[Data] 데이터 로딩 중...')

# 1) Feature 로드
if not os.path.exists(feature_path):
    print(f'[Error] 피처 파일을 찾을 수 없습니다: {feature_path}')
    sys.exit()
    
df = pd.read_parquet(feature_path)

# 삼성전자만 추출 (속도 향상)
code_col = next((c for c in df.columns if 'code' in c.lower()), 'Code')
df[code_col] = df[code_col].astype(str).str.zfill(6)
df_stock = df[df[code_col] == target_code].copy()
df_stock = df_stock.sort_values('Date').reset_index(drop=True)

print(f'   - 삼성전자 데이터: {len(df_stock)} rows (최신 날짜: {df_stock["Date"].iloc[-1].date()})')

# 2) 학습에 사용된 컬럼 리스트 로드 (중요: 모델 입력 순서 맞추기 위함)
train_cols = []
if os.path.exists(col_list_path):
    try:
        df_cols = pd.read_csv(col_list_path)
        # 보통 첫 번째 컬럼이 피처 이름
        train_cols = df_cols.iloc[:, 0].tolist()
        print(f'   - 학습 컬럼 리스트 로드 성공 ({len(train_cols)}개)')
    except Exception as e:
        print(f'   [Warning] 컬럼 리스트 로드 실패: {e}')
else:
    print('   [Warning] 컬럼 리스트 파일(V31_columns_list.csv)이 없습니다.')
    print('   -> 자동으로 숫자형 컬럼만 추려서 시도합니다 (실패 가능성 있음).')
    # 날짜, 코드 등 제외하고 숫자형만 선택
    exclude = ['Date', 'date', 'Code', 'code', 'Name', 'name', 'Market', 'market']
    train_cols = [c for c in df_stock.columns if c not in exclude and pd.api.types.is_numeric_dtype(df_stock[c])]

# ---------------------------------------------------------------------------
# 4. 모델 로드 및 예측 수행
# ---------------------------------------------------------------------------
print('\n[Predict] 모델 예측 시도...')

try:
    # 모델 로드
    model = joblib.load(latest_model_path)
    print('   - 모델 파일 로드 성공 (In-memory)')
    
    # 입력 데이터 준비 (모델이 학습할 때 썼던 피처만 순서대로 추출)
    # 만약 데이터에 없는 컬럼이 모델에 필요하면 에러 발생 -> 미리 체크
    missing_cols = [c for c in train_cols if c not in df_stock.columns]
    if missing_cols:
        print(f'   [Critical Error] 모델에 필요한 컬럼이 데이터에 없습니다: {missing_cols[:5]} ...')
        sys.exit()
        
    X_test = df_stock[train_cols].iloc[[-1]] # 가장 최근 1개 행만 예측
    
    # 예측 실행
    # predict() 또는 predict_proba() 사용
    pred = None
    pred_proba = None
    
    try:
        pred = model.predict(X_test)
        # 확률 제공 모델인지 확인 (Classifier인 경우)
        if hasattr(model, 'predict_proba'):
            pred_proba = model.predict_proba(X_test)
    except Exception as e:
        print(f'   [Error] 예측 실행 중 에러 발생: {e}')
        print('   -> 컬럼 개수나 순서, 데이터 타입 문제일 수 있습니다.')
        sys.exit()

    # -----------------------------------------------------------------------
    # 5. 결과 출력
    # -----------------------------------------------------------------------
    target_date = df_stock["Date"].iloc[-1].date()
    print('\n' + '='*50)
    print(f'🚀 [예측 결과] 삼성전자 ({target_date} 기준)')
    print('='*50)
    
    print(f'   - 모델 예측값 (Raw): {pred[0]}')
    
    if pred_proba is not None:
        # 이진 분류(0, 1)라 가정하고 1(상승)일 확률 출력
        # 모델마다 출력 형태가 다르므로 확인 필요
        print(f'   - 상승 확률 (Probability): {pred_proba[0]}')
        print(f'     (보통 1번 인덱스가 상승 확률입니다: {pred_proba[0][-1]:.4f})')
    
    print('\n✅ [Success] 모델이 정상적으로 로드되고 데이터를 처리하여 결과를 내놓았습니다.')
    print('   시스템 전체 파이프라인(수집->가공->학습->예측) 검증 완료.')

except Exception as e:
    print(f'\n❌ [Fail] 모델 로드 또는 예측 실패: {e}')
    print('   -> 라이브러리 버전 차이(xgboost/sklearn 등)나 파일 손상 가능성 확인 필요.')

print('='*70)
