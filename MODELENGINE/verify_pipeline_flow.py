import pandas as pd
import os
import sys

# ---------------------------------------------------------------------------
# 1. 설정: 사용자님이 제공해주신 절대 경로 사용
# ---------------------------------------------------------------------------
paths = {
    'RAW_STOCK': r'F:\autostockG\MODELENGINE\RAW\stocks\all_stocks_cumulative_251121.parquet',
    'RAW_KOSPI': r'F:\autostockG\MODELENGINE\RAW\kospi_data\kospi_data_251121_1.parquet',
    'FEATURE':   r'F:\autostockG\MODELENGINE\FEATURE\features_V31_251121.parquet',
    'HOJ_DB':    r'F:\autostockG\MODELENGINE\HOJ_DB\HOJ_DB_V31_251121.parquet'
}

target_code = '005930'  # 검증용 샘플 종목 (삼성전자)

print('='*70)
print('[Step 2] 데이터 파이프라인 흐름 검증 (Raw -> Feature -> DB)')
print(f'Target Sample: {target_code} (Samsung Electronics)')
print('='*70)

data_store = {}

# ---------------------------------------------------------------------------
# 2. 파일 로드 및 기본 검사
# ---------------------------------------------------------------------------
for name, path in paths.items():
    print(f'\n[{name}] 로드 중... ', end='')
    if not os.path.exists(path):
        print(f'\n[Error] 파일을 찾을 수 없습니다: {path}')
        sys.exit()
    
    try:
        # 전체를 다 읽으면 느리므로, 특정 컬럼만 읽거나 전체 읽고 필터링
        # 여기서는 정확성을 위해 전체 로드 후 필터링 (메모리 주의)
        df = pd.read_parquet(path)
        
        # 날짜 컬럼 통일 (Date or 날짜)
        date_col = next((c for c in df.columns if c.lower() == 'date' or '날짜' in c), None)
        if not date_col:
            print(f'[Fail] 날짜 컬럼 없음. Columns: {list(df.columns)[:5]}...')
            continue
            
        df.rename(columns={date_col: 'Date'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # 종목 코드로 필터링 (KOSPI 데이터는 종목 코드가 없으므로 제외)
        if name != 'RAW_KOSPI':
            code_col = next((c for c in df.columns if 'code' in c.lower() or '종목' in c), None)
            if code_col:
                # 코드 포맷 통일 (005930)
                df[code_col] = df[code_col].astype(str).str.zfill(6)
                df = df[df[code_col] == target_code].copy()
            else:
                print(f'[Warning] 종목코드 컬럼을 찾을 수 없습니다. 전체 데이터로 진행.')

        # 날짜순 정렬
        df = df.sort_values('Date').reset_index(drop=True)
        
        data_store[name] = df
        print(f'OK ({len(df)} rows)')
        print(f'   - 기간: {df["Date"].iloc[0].date()} ~ {df["Date"].iloc[-1].date()}')
        
        # 날짜 오름차순 검사 (필수)
        if not df['Date'].is_monotonic_increasing:
            print(f'   [CRITICAL] 날짜 정렬 꼬임 발생! (is_monotonic_increasing=False)')
        else:
            print(f'   - 날짜 정렬 상태: 정상')

    except Exception as e:
        print(f'\n[Error] 읽기 실패: {e}')

print('-'*70)

# ---------------------------------------------------------------------------
# 3. 단계별 정합성 비교 (Cross-Check)
# ---------------------------------------------------------------------------
# 비교할 주요 컬럼 (종가)
check_col = 'Close' # 만약 컬럼명이 한글('종가')이면 변경 필요할 수 있음

df_raw = data_store.get('RAW_STOCK')
df_feat = data_store.get('FEATURE')
df_db = data_store.get('HOJ_DB')

if df_raw is not None and df_feat is not None:
    print('\n[검증 1] Raw Data vs Feature Data 일치 여부')
    # Feature 파일에도 보통 종가(Close) 컬럼이 포함됨. 이를 비교.
    # 날짜를 기준으로 Inner Join
    merged = pd.merge(df_raw[['Date', 'Close']], df_feat, on='Date', suffixes=('_raw', '_feat'), how='inner')
    
    # Close 컬럼 비교 (Feature 파일의 Close 컬럼명 찾기)
    feat_close_col = next((c for c in df_feat.columns if c == 'Close' or c == '종가'), None)
    
    if feat_close_col:
        diff = (merged['Close'] - merged[feat_close_col]).abs().sum()
        print(f'   - 날짜 매칭된 행 개수: {len(merged)} / {len(df_feat)} (Feature 기준)')
        print(f'   - 종가(Close) 데이터 차이 합계: {diff:.4f}')
        if diff < 0.001:
            print('   [Pass] Raw 데이터가 Feature 파일로 정확하게 이관되었습니다.')
        else:
            print('   [Fail] Raw 데이터와 Feature 데이터의 값이 다릅니다! 가공 중 왜곡 발생 가능성.')
    else:
        print('   [Info] Feature 파일에 "Close" 컬럼이 없어 값 비교 생략.')

    # 윈도우 함수로 인한 앞부분 결측치(NaN) 확인
    null_rows = df_feat.isnull().any(axis=1).sum()
    print(f'   - 결측치(NaN) 포함 행 개수: {null_rows} (초기 이동평균 계산 등으로 인한 자연스러운 현상인지 확인 필요)')


if df_feat is not None and df_db is not None:
    print('\n[검증 2] Feature Data vs Training DB (HOJ_DB) 일치 여부')
    # DB는 학습을 위해 Feature를 가공하거나 라벨을 붙인 상태.
    # Feature 데이터가 DB에 순서대로 잘 들어갔는지 확인.
    
    # 공통 컬럼 찾기 (Date 제외)
    common_cols = list(set(df_feat.columns) & set(df_db.columns) - {'Date', 'Code', 'code', 'date'})
    if common_cols:
        sample_col = common_cols[0]
        print(f'   - 샘플 비교 컬럼: {sample_col}')
        
        merged_db = pd.merge(df_feat[['Date', sample_col]], df_db[['Date', sample_col]], on='Date', suffixes=('_feat', '_db'), how='inner')
        
        # 값 비교
        # 주의: float 정밀도 문제로 아주 미세한 차이는 있을 수 있음
        try:
            diff_db = (merged_db[f'{sample_col}_feat'] - merged_db[f'{sample_col}_db']).abs().sum()
            print(f'   - 매칭된 행 개수: {len(merged_db)}')
            print(f'   - 값 차이 합계: {diff_db:.4f}')
            
            if diff_db < 0.001:
                print('   [Pass] Feature 데이터가 학습 DB로 변형 없이 정확히 전달됨.')
            else:
                print('   [Fail] Feature -> DB 과정에서 값이 변경되었습니다. (스케일링/정규화 때문일 수 있음)')
        except:
            print('   [Check] 수치형 데이터가 아니거나 비교 불가.')
            
    else:
        print('   [Warning] Feature와 DB 사이에 공통된 컬럼 이름이 없습니다. 컬럼명이 변경되었을 수 있습니다.')

    # 라벨(Target) 확인 - 미래 데이터 참조 여부 확인
    # 보통 'Label'이나 'Return' 같은 이름.
    # 여기서는 논리만 체크: DB의 맨 마지막 날짜에 라벨이 비어있는지 확인 (미래 데이터가 없으므로 비어있어야 정상인 경우가 많음)
    print(f'   - DB 마지막 날짜({df_db["Date"].iloc[-1].date()}) 데이터 확인:')
    print(df_db.iloc[-1].to_dict())

print('='*70)
