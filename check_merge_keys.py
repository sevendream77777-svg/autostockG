# check_merge_keys.py
# [진단용 스크립트] build_database_V25.py의 'KeyError: Code' 원인을
# 1분 30초 기다림 없이 즉시 확인합니다.

import pandas as pd
import sys

# 검증할 파일 경로
FEATURES_FILE = "all_features_cumulative_V21_Hoj.parquet"
TICKER_MAP_FILE = "ticker_map.parquet"

print("--- 'KeyError: Code' 진단 스크립트 시작 ---")
print("3단계(merge)가 실패하는 원인을 찾기 위해 2개 파일의 'Code' 컬럼을 검사합니다.\n")

has_error = False

# 1. V21 DB (df_features) 검사
try:
    print(f"[검사 1] {FEATURES_FILE} (V21 DB) 로드 중...")
    df_features = pd.read_parquet(FEATURES_FILE)
    
    print(f"\n[V21 DB] 컬럼 목록:")
    print(list(df_features.columns))
    
    if 'Code' in df_features.columns:
        print("✅ [V21 DB] 'Code' 컬럼이 정상적으로 존재합니다.")
    else:
        print("❌ [V21 DB] 'Code' 컬럼이 없습니다.")
        print(f"   (참고) V21 DB의 인덱스(index) 이름: {df_features.index.name}")
        has_error = True

except Exception as e:
    print(f"❌ [V21 DB] 파일 로드 실패: {e}")
    has_error = True

print("\n" + "="*30 + "\n")

# 2. 종목 사전 (df_ticker_map) 검사
try:
    print(f"[검사 2] {TICKER_MAP_FILE} (종목 사전) 로드 중...")
    df_ticker_map = pd.read_parquet(TICKER_MAP_FILE)

    print(f"\n[종목 사전] 컬럼 목록:")
    print(list(df_ticker_map.columns))

    if 'Code' in df_ticker_map.columns:
        print("✅ [종목 사전] 'Code' 컬럼이 정상적으로 존재합니다.")
    else:
        print("❌ [종목 사전] 'Code' 컬럼이 없습니다.")
        print(f"   (참고) 종목 사전의 인덱스(index) 이름: {df_ticker_map.index.name}")
        has_error = True
        
except Exception as e:
    print(f"❌ [종목 사전] 파일 로드 실패: {e}")
    has_error = True

print("\n" + "="*30 + "\n")

if has_error:
    print("--- [진단 결과] ❌ 'Code' 컬럼 누락이 확인되었습니다. ---")
else:
    print("--- [진단 결과] ✅ 두 파일 모두 'Code' 컬럼이 존재합니다. (오류 원인 불명) ---")