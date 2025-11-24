import pandas as pd
import os
import sys

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
paths = {
    'RAW_STOCK': r'F:\autostockG\MODELENGINE\RAW\stocks\all_stocks_cumulative_251121.parquet',
    'RAW_KOSPI': r'F:\autostockG\MODELENGINE\RAW\kospi_data\kospi_data_251121_1.parquet',
    'FEATURE':   r'F:\autostockG\MODELENGINE\FEATURE\features_V31_251121.parquet',
    'HOJ_DB':    r'F:\autostockG\MODELENGINE\HOJ_DB\HOJ_DB_V31_251121.parquet'
}

target_code = '005930'  # 삼성전자

print('='*70)
print('[Step 2] 데이터 파이프라인 흐름 검증 V2 (KeyError 수정)')
print(f'Target Sample: {target_code} (Samsung Electronics)')
print('='*70)

data_store = {}

# ---------------------------------------------------------------------------
# 2. 파일 로드 (이미 정상 확인됨, 간소화)
# ---------------------------------------------------------------------------
for name, path in paths.items():
    print(f'[{name}] 로드 중... ', end='')
    if not os.path.exists(path):
        print('File Not Found')
        continue
    
    try:
        df = pd.read_parquet(path)
        
        # 날짜 컬럼 통일
        date_col = next((c for c in df.columns if c.lower() == 'date' or '날짜' in c), None)
        if date_col:
            df.rename(columns={date_col: 'Date'}, inplace=True)
            df['Date'] = pd.to_datetime(df['Date'])
        
        # 종목 필터링
        if name != 'RAW_KOSPI':
            code_col = next((c for c in df.columns if 'code' in c.lower() or '종목' in c), None)
            if code_col:
                df[code_col] = df[code_col].astype(str).str.zfill(6)
                df = df[df[code_col] == target_code].copy()

        df = df.sort_values('Date').reset_index(drop=True)
        data_store[name] = df
        print(f'OK ({len(df)} rows)')

    except Exception as e:
        print(f'Fail: {e}')

print('-'*70)

# ---------------------------------------------------------------------------
# 3. 단계별 정합성 비교 (수정됨)
# ---------------------------------------------------------------------------
df_raw = data_store.get('RAW_STOCK')
df_feat = data_store.get('FEATURE')
df_db = data_store.get('HOJ_DB')

# [검증 1] Raw vs Feature
if df_raw is not None and df_feat is not None:
    print('\n[검증 1] Raw Data vs Feature Data 일치 여부')
    
    # Feature 파일의 종가 컬럼 찾기
    feat_close_col = next((c for c in df_feat.columns if c == 'Close' or c == '종가'), None)
    
    if feat_close_col:
        # 충돌 방지를 위해 컬럼명을 명시적으로 변경하여 Merge
        temp_raw = df_raw[['Date', 'Close']].rename(columns={'Close': 'Close_RAW'})
        temp_feat = df_feat[['Date', feat_close_col]].rename(columns={feat_close_col: 'Close_FEAT'})
        
        merged = pd.merge(temp_raw, temp_feat, on='Date', how='inner')
        
        diff = (merged['Close_RAW'] - merged['Close_FEAT']).abs().sum()
        match_count = len(merged)
        
        print(f'   - 날짜 매칭된 행 개수: {match_count}')
        print(f'   - 종가(Close) 데이터 차이 합계: {diff:.4f}')
        
        if diff < 0.001:
            print('   [Pass] Raw 데이터가 Feature 파일로 정확하게 이관되었습니다.')
        else:
            print(f'   [Fail] 값이 다릅니다. (차이: {diff})')
            print('   -> Feature 생성 과정에서 종가 데이터가 수정되었거나(수정주가 반영 등), 다른 컬럼일 수 있습니다.')
    else:
        print('   [Skip] Feature 파일에서 Close 컬럼을 찾을 수 없습니다.')


# [검증 2] Feature vs DB
if df_feat is not None and df_db is not None:
    print('\n[검증 2] Feature Data vs Training DB (HOJ_DB) 일치 여부')
    
    # 공통 숫자 컬럼 찾기 (비교용)
    common_cols = list(set(df_feat.columns) & set(df_db.columns) - {'Date', 'Code', 'code', 'date', 'Date_x', 'Date_y'})
    # 수치형 컬럼만 필터링
    numeric_cols = [c for c in common_cols if pd.api.types.is_numeric_dtype(df_feat[c])]
    
    if numeric_cols:
        sample_col = numeric_cols[0] # 첫 번째 공통 컬럼 선택
        print(f'   - 샘플 비교 컬럼: {sample_col}')
        
        # 명시적 이름 변경 후 Merge
        temp_feat = df_feat[['Date', sample_col]].rename(columns={sample_col: 'Val_FEAT'})
        temp_db = df_db[['Date', sample_col]].rename(columns={sample_col: 'Val_DB'})
        
        merged_db = pd.merge(temp_feat, temp_db, on='Date', how='inner')
        
        diff_db = (merged_db['Val_FEAT'] - merged_db['Val_DB']).abs().sum()
        print(f'   - 매칭된 행 개수: {len(merged_db)}')
        print(f'   - 값 차이 합계: {diff_db:.4f}')
        
        if diff_db < 0.001:
            print('   [Pass] Feature 데이터가 학습 DB로 변형 없이 정확히 전달됨.')
        else:
            print('   [Fail] Feature -> DB 과정에서 값이 변경되었습니다.')
            print('   -> 정규화(Scaling)나 전처리가 수행되었는지 확인하세요.')
            
        # 라벨(Target) 확인 - 미래 데이터 참조 여부 체크
        # 보통 라벨은 'next_change', 'target', 'label' 등의 이름을 가짐
        potential_labels = [c for c in df_db.columns if 'next' in c.lower() or 'target' in c.lower() or 'label' in c.lower()]
        if potential_labels:
            print(f'   - 발견된 라벨 후보: {potential_labels}')
            last_row = df_db.iloc[-1]
            print(f'   - 마지막 날짜({last_row["Date"].date()}) 라벨 값: {last_row[potential_labels[0]]}')
            # 마지막 날짜의 '미래 수익률'은 알 수 없으므로 NaN이어야 정상 (학습시 제외됨)
    else:
        print('   [Warning] 비교할 공통 수치 컬럼이 없습니다.')

print('='*70)
