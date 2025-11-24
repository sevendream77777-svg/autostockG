import pandas as pd
import os
import numpy as np

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
db_path = r'F:\autostockG\MODELENGINE\HOJ_DB\HOJ_DB_V31_251121.parquet'
target_code = '005930'  # 삼성전자

print('='*70)
print('[Step 3] 학습 로직 및 레이블(Target) 정합성 검증')
print(f'Target File: {os.path.basename(db_path)}')
print('='*70)

if not os.path.exists(db_path):
    print(f'[Error] 파일을 찾을 수 없습니다: {db_path}')
    exit()

try:
    # 데이터 로드
    df = pd.read_parquet(db_path)
    
    # 날짜 처리
    date_col = next((c for c in df.columns if c.lower() == 'date' or '날짜' in c), 'Date')
    df.rename(columns={date_col: 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 삼성전자 필터링
    code_col = next((c for c in df.columns if 'code' in c.lower()), None)
    if code_col:
        df[code_col] = df[code_col].astype(str).str.zfill(6)
        df = df[df[code_col] == target_code].copy()
    
    df = df.sort_values('Date').reset_index(drop=True)
    print(f'[Load] 삼성전자 데이터 로드 완료 ({len(df)} rows)')

    # -----------------------------------------------------------------------
    # 2. 정답(Label/Target) 컬럼 찾기 및 검증
    # -----------------------------------------------------------------------
    # 보통 학습 목표는 'next_change', 'target', 'profit', 'return' 등으로 명명됨
    potential_targets = [c for c in df.columns if 'next' in c.lower() or 'target' in c.lower() or 'label' in c.lower()]
    
    if not potential_targets:
        print('\n[Warning] 명시적인 타겟(Label) 컬럼을 찾지 못했습니다.')
        print('   -> 학습 스크립트 내부에서 실시간으로 생성하는 방식일 수 있습니다.')
        print(f'   -> 컬럼 목록: {list(df.columns)[:10]} ...')
    else:
        target_col = potential_targets[0]
        print(f'\n[Check] 타겟(Label) 컬럼 후보 발견: "{target_col}"')
        
        # 검증 로직: Target이 정말 "다음날의 변화율"을 의미하는가?
        # (내일 종가 - 오늘 종가) / 오늘 종가
        
        # 종가 컬럼 찾기
        close_col = next((c for c in df.columns if c in ['Close', '종가', 'close']), None)
        
        if close_col and target_col:
            # 직접 계산해보기 (검증용)
            # shift(-1)은 한 칸 위로 당기는 것 (즉, 미래 데이터)
            df['calc_next_return'] = (df[close_col].shift(-1) - df[close_col]) / df[close_col]
            
            # 상관관계 분석
            corr = df[[target_col, 'calc_next_return']].corr().iloc[0, 1]
            print(f'   - 타겟 값과 "내일 주가 변화율"의 상관계수: {corr:.4f}')
            
            if corr > 0.99:
                print('   [Pass] 타겟 데이터가 "미래(다음날) 수익률"을 정확히 반영하고 있습니다.')
                print('   -> Look-ahead Bias(미래 참조) 없이, "예측 대상"으로 올바르게 설정됨.')
            else:
                print('   [Fail] 타겟 데이터가 예상된 미래 수익률과 다릅니다.')
                print('   -> 다른 로직(예: 5일 후 수익률, 로그 수익률 등)인지 확인이 필요합니다.')

            # 3. 마지막 날짜 데이터 확인 (중요!)
            last_row = df.iloc[-1]
            last_target = last_row[target_col]
            print(f'\n[Check] 마지막 날짜({last_row["Date"].date()})의 타겟 값: {last_target}')
            
            if pd.isna(last_target):
                print('   [Pass] 마지막 날짜의 타겟이 NaN입니다. (미래를 알 수 없으므로 정상)')
            else:
                print('   [Warning] 마지막 날짜에 타겟 값이 있습니다! 혹시 0인가요?')
                if last_target == 0:
                    print('   -> 0으로 채워져 있다면 학습 시 노이즈가 될 수 있으니 주의해야 합니다.')
                else:
                    print('   [Critical] 미래 데이터가 없는데 값이 있다면 데이터 생성 오류일 수 있습니다.')

    # -----------------------------------------------------------------------
    # 3. 학습용 피처(Feature) 데이터 누수 확인
    # -----------------------------------------------------------------------
    # 현재(t)의 피처에 미래(t+1)의 정보가 섞여 있는지 간단 확인
    # 예: 'Close' 컬럼 자체가 shift(-1) 되어 있는지 등
    
    print('\n[Check] 피처 데이터 누수(Data Leakage) 점검')
    # 피처 중 'change'나 'rate'가 들어가는 것 확인
    feature_cols = [c for c in df.columns if 'change' in c.lower() and c != target_col]
    if feature_cols:
        print(f'   - 변동률 관련 피처 샘플: {feature_cols[:3]}')
        # 이 피처들이 혹시 타겟과 동일한지(미래를 베껴왔는지) 확인
        for f_col in feature_cols[:3]:
            corr_leak = df[[target_col, f_col]].corr().iloc[0, 1]
            if corr_leak > 0.99:
                 print(f'   [CRITICAL FAIL] 피처 "{f_col}"이 정답(Target)과 똑같습니다! 정답을 보고 문제를 푸는 격입니다.')
            else:
                 print(f'   [Pass] 피처 "{f_col}"는 정답과 다릅니다. (안전)')
    else:
        print('   - 변동률 관련 피처가 없어 누수 점검을 생략합니다.')

except Exception as e:
    print(f'\n[Error] 검증 중 오류 발생: {e}')

print('='*70)
