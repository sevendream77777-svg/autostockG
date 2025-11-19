# check_restored_file_KOSPI.py
# [진단용 스크립트 4]
# '복원된' kospi_index_10y.parquet 파일이 '깨끗한'(영어) 상태인지 확인합니다.

import pandas as pd
import sys
import os

KOSPI_FILE = "kospi_index_10y.parquet"

print(f"--- '복원된' {KOSPI_FILE} 파일 컬럼 진단 시작 ---")

try:
    df = pd.read_parquet(KOSPI_FILE)
    
    print("\n[1. '복원된' KOSPI 파일의 실제 컬럼 목록]")
    cols = list(df.columns)
    print(cols)
    
    print("\n[2. '날짜' (한글) / 'Date' (영어) 컬럼 존재 여부]")
    if '날짜' in cols:
        print("❌ '날짜' (한글) 컬럼이 존재합니다. (-> '오염된' 파일)")
    elif 'Date' in cols:
        print("✅ 'Date' (영어) 컬럼만 존재합니다. (-> '깨끗한' 파일)")
    else:
        print("❌ '날짜' 또는 'Date' 컬럼을 찾을 수 없습니다.")

    print("\n[3. '종가' (한글) / 'Close' (영어) 컬럼 존재 여부]")
    if '종가' in cols: print("❌ '종가' (한글) 존재")
    if 'Close' in cols: print("✅ 'Close' (영어) 존재")
    
    print("\n--- 진단 완료 ---")

except Exception as e:
    print(f"❌ [오류] {KOSPI_FILE} 파일 로드 실패: {e}")