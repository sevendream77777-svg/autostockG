import pandas as pd
import numpy as np
import os

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
db_path = r'F:\autostockG\MODELENGINE\HOJ_DB\HOJ_DB_V31_251121.parquet'
target_code = '005930'  # 삼성전자 (검증용 샘플)

print('='*70)
print('[Step 3] Unified Model 학습 로직 시뮬레이션 검증')
print('='*70)

if not os.path.exists(db_path):
    print(f'[Error] 파일을 찾을 수 없습니다: {db_path}')
    exit()

try:
    # 1. 데이터 로드 (Feature DB)
    df = pd.read_parquet(db_path)
    
    # 날짜 컬럼 통일
    date_col = next((c for c in df.columns if c.lower() == 'date' or '날짜' in c), None)
    df.rename(columns={date_col: 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 삼성전자 필터링
    code_col = next((c for c in df.columns if 'code' in c.lower()), None)
    df[code_col] = df[code_col].astype(str).str.zfill(6)
    df_stock = df[df[code_col] == target_code].copy()
    df_stock = df_stock.sort_values('Date').reset_index(drop=True)
    
    print(f'[Load] 삼성전자 데이터 로드 완료 ({len(df_stock)} rows)')
    print(f'   - 기간: {df_stock["Date"].iloc[0].date()} ~ {df_stock["Date"].iloc[-1].date()}')

    # -----------------------------------------------------------------------
    # 2. 타겟(Target) 생성 로직 검증 (Training 시뮬레이션)
    # -----------------------------------------------------------------------
    # 일반적인 주가 예측 타겟: (내일 종가 - 오늘 종가) / 오늘 종가
    # 즉, '내일 수익률'을 예측하는 것이 목표
    
    print('\n[Check 1] 타겟(정답) 생성 로직 시뮬레이션')
    
    close_col = 'Close' # 종가 컬럼
    if close_col not in df_stock.columns:
        print(f'[Error] "{close_col}" 컬럼이 없습니다. 타겟을 생성할 수 없습니다.')
    else:
        # [핵심] 타겟 생성: shift(-1)은 '내일 데이터'를 '오늘 행'으로 당겨오는 것
        df_stock['Target_Simulated'] = df_stock[close_col].shift(-1) / df_stock[close_col] - 1
        
        print('   - 타겟 생성 수식: (Next_Close / Current_Close) - 1')
        print(f'   - 타겟 데이터 예시 (마지막 3일):')
        print(df_stock[['Date', 'Close', 'Target_Simulated']].tail(3))
        
        # 마지막 날 확인
        last_val = df_stock['Target_Simulated'].iloc[-1]
        if pd.isna(last_val):
            print('   [Pass] 마지막 날짜(최신)의 타겟은 NaN입니다. (미래를 모르므로 정상)')
        else:
            print(f'   [FAIL] 마지막 날짜에 타겟 값이 있습니다 ({last_val}). 미래 참조 오류 가능성!')

    # -----------------------------------------------------------------------
    # 3. Research Split (학습/검증 분리) 검증
    # -----------------------------------------------------------------------
    print('\n[Check 2] Research 모드 데이터 분리 (Train/Valid) 시뮬레이션')
    # 사용자 설명: "최근 1년은 검증용, 나머지는 학습용"
    
    validation_days = 365 # 1년
    cutoff_date = df_stock['Date'].max() - pd.Timedelta(days=validation_days)
    
    train_set = df_stock[df_stock['Date'] < cutoff_date]
    valid_set = df_stock[df_stock['Date'] >= cutoff_date]
    
    print(f'   - 기준일(Cutoff): {cutoff_date.date()}')
    print(f'   - Train 세트: {len(train_set)} rows (~ {train_set["Date"].iloc[-1].date()})')
    print(f'   - Valid 세트: {len(valid_set)} rows ({valid_set["Date"].iloc[0].date()} ~)')
    
    # 데이터 섞임 확인
    if train_set['Date'].max() >= valid_set['Date'].min():
        print('   [CRITICAL FAIL] 학습 데이터와 검증 데이터의 기간이 겹칩니다! (Data Leakage)')
    else:
        print('   [Pass] 학습 데이터는 과거, 검증 데이터는 미래로 완벽하게 분리되었습니다.')

    # -----------------------------------------------------------------------
    # 4. Real Mode (전체 학습) 검증
    # -----------------------------------------------------------------------
    print('\n[Check 3] Real 모드 (Full Training) 준비 상태')
    
    # Real 모드는 NaN(마지막 날)만 제외하고 전체 사용
    real_train_set = df_stock.dropna(subset=['Target_Simulated'])
    
    print(f'   - Real 학습 데이터 개수: {len(real_train_set)} (전체 {len(df_stock)} 중 마지막 1개 제외)')
    
    if len(real_train_set) == len(df_stock) - 1:
        print('   [Pass] Real 모드는 예측 불가능한 마지막 날을 제외한 "모든 과거 데이터"를 사용합니다.')
    else:
        print(f'   [Warning] 데이터 개수가 예상과 다릅니다. 결측치가 중간에 더 있을 수 있습니다. (결측: {len(df_stock) - len(real_train_set)}개)')

    # -----------------------------------------------------------------------
    # 5. 피처 누수(Feature Leakage) 재검증
    # -----------------------------------------------------------------------
    print('\n[Check 4] 피처 누수(Leakage) 정밀 점검')
    # 현재의 피처 값들이 미래(Target)와 비정상적으로 높은 상관관계를 갖는지 확인
    # 상관계수가 1.0에 가까우면 '정답'이 '문제'에 섞여 들어간 것 (Change 등 확인)
    
    # 비교할 숫자형 컬럼들
    numeric_cols = df_stock.select_dtypes(include=[np.number]).columns.tolist()
    suspicious_cols = []
    
    for col in numeric_cols:
        if col in ['Target_Simulated', 'Close', 'Date']: continue
        
        # 상관계수 계산
        corr = df_stock[[col, 'Target_Simulated']].corr().iloc[0, 1]
        
        if abs(corr) > 0.95: # 0.95 이상이면 매우 의심스러움
            suspicious_cols.append((col, corr))
    
    if not suspicious_cols:
        print('   [Pass] 타겟과 상관관계가 0.95 이상인 "의심스러운 피처"가 발견되지 않았습니다.')
        print('          -> 현재 정보만으로 미래를 예측해야 하는 조건 만족.')
    else:
        print(f'   [FAIL] 미래 정보 유출 의심 피처 발견: {suspicious_cols}')
        print('          -> 해당 컬럼이 shift(-1)된 데이터인지 확인이 필요합니다.')

except Exception as e:
    print(f'\n[Error] 검증 중 오류 발생: {e}')

print('='*70)
