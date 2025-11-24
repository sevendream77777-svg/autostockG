import pandas as pd
import os
import sys

# ---------------------------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------------------------
base_dir = r'F:\autostockG\MODELENGINE'
path_db = os.path.join(base_dir, 'HOJ_DB', 'HOJ_DB_V31_251121.parquet')
path_feat = os.path.join(base_dir, 'FEATURE', 'features_V31_251121.parquet')

target_code = '005930' # 삼성전자

print('='*80)
print('[Final Step] High/Low 뒤바뀜(Swap) 방지용 물리 법칙 검사')
print('='*80)

def check_sanity(path, name):
    if not os.path.exists(path):
        print(f'[Skip] {name} 파일이 없습니다.')
        return

    print(f'\n[{name}] 데이터 검사 중...')
    try:
        df = pd.read_parquet(path)
        
        # 컬럼 이름 찾기 (대소문자 무관)
        high_col = next((c for c in df.columns if c.lower() == 'high' or '고가' in c), None)
        low_col = next((c for c in df.columns if c.lower() == 'low' or '저가' in c), None)
        
        if high_col and low_col:
            print(f'   - 컬럼 감지: High="{high_col}", Low="{low_col}"')
            
            # [핵심] 물리 법칙 검사: High가 Low보다 작은 경우가 있는가?
            # (High < Low) 인 행을 찾음
            errors = df[df[high_col] < df[low_col]]
            
            if len(errors) == 0:
                print(f'   ✅ [Pass] 모든 데이터({len(df)}건)에서 High >= Low 법칙이 지켜지고 있습니다.')
                print('      -> 데이터 생성/가공 중 High와 Low가 뒤바뀌지 않았음을 100% 보장합니다.')
            else:
                print(f'   ❌ [CRITICAL FAIL] High < Low 인 데이터가 {len(errors)}건 발견되었습니다!')
                print('      -> 데이터 수집 혹은 피처 생성 과정에서 컬럼이 뒤바뀌었을 수 있습니다.')
                print('      -> 오류 샘플:')
                print(errors[[high_col, low_col]].head())
        else:
            print('   ⚠️ High 또는 Low 컬럼을 찾을 수 없어 검사를 생략합니다.')
            
    except Exception as e:
        print(f'   [Error] 읽기 실패: {e}')

# 1. Feature 파일 검사
check_sanity(path_feat, 'FEATURE (가공 데이터)')

# 2. DB 파일 검사
check_sanity(path_db, 'HOJ_DB (학습용 데이터)')

print('\n' + '='*80)
print(' [결론 요약]')
print(' 1. 예측 엔진은 컬럼 "이름(Name)"으로 데이터를 찾으므로 순서가 섞일 위험이 없습니다.')
print(' 2. 위 검사에서 Pass가 나오면, 데이터 값 자체도 서로 뒤바뀌지 않았음이 증명됩니다.')
print('='*80)
