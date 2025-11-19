# check_restored_file.py
# [진단용 스크립트 3]
# 휴지통에서 '복원'한 all_stocks_cumulative.parquet 파일에
# '진짜'로 어떤 컬럼이 들어있는지 1초 만에 확인합니다.

import pandas as pd
import sys
import os

STOCK_FILE = "all_stocks_cumulative.parquet"

print(f"--- '복원된' {STOCK_FILE} 파일 컬럼 진단 시작 ---")

try:
    df = pd.read_parquet(STOCK_FILE)
    
    print("\n[1. '복원된' 파일의 실제 컬럼 목록]")
    cols = list(df.columns)
    print(cols)
    
    print("\n[2. '날짜' (한글) 컬럼 존재 여부]")
    if '날짜' in cols:
        print("✅ '날짜' (한글) 컬럼이 존재합니다.")
    else:
        print("❌ '날짜' (한글) 컬럼이 없습니다.")
        
    print("\n[3. 'Date' (영어) 컬럼 존재 여부]")
    if 'Date' in cols:
        print("✅ 'Date' (영어) 컬럼이 존재합니다.")
    else:
        print("❌ 'Date' (영어) 컬럼이 없습니다.")

    print("\n[4. '종가' (한글) / 'Close' (영어) 컬럼 존재 여부]")
    if '종가' in cols: print("✅ '종가' (한글) 존재")
    if 'Close' in cols: print("✅ 'Close' (영어) 존재")
    
    print("\n--- 진단 완료 ---")

except Exception as e:
    print(f"❌ [오류] {STOCK_FILE} 파일 로드 실패: {e}")