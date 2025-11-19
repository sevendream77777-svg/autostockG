# fix_corrupted_KOSPI_v2.py
# [1회용 복구 스크립트 2탄]
# '한/영 혼용' 상태인 'kospi_index_10y.parquet' 파일만 '영어'로 통일합니다.

import pandas as pd
import os
import sys
import numpy as np

KOSPI_FILE = "kospi_index_10y.parquet"
# 2단계가 사용할 '영어' 컬럼
FINAL_COLS = ['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']

print(f"--- 1단계 '숲' 파일 ({KOSPI_FILE}) 복구 시작 ---")
if not os.path.exists(KOSPI_FILE):
    print(f"❌ 오류: {KOSPI_FILE} 파일이 없습니다.")
    sys.exit(1)

try:
    df_kospi = pd.read_parquet(KOSPI_FILE)
    print(f"   > '오염된' KOSPI 파일 로드 성공. (총 {len(df_kospi)} 행)")

    # [언어 통일] '한글' 컬럼을 '영어' 컬럼으로 강제 병합
    if '날짜' in df_kospi.columns:
        df_kospi['Date']   = df_kospi['날짜']
    if '시가' in df_kospi.columns:
        df_kospi['Open']   = df_kospi['Open'].fillna(df_kospi['시가'])
    if '고가' in df_kospi.columns:
        df_kospi['High']   = df_kospi['High'].fillna(df_kospi['고가'])
    if '저가' in df_kospi.columns:
        df_kospi['Low']    = df_kospi['Low'].fillna(df_kospi['저가'])
    if '종가' in df_kospi.columns:
        df_kospi['Close']  = df_kospi['Close'].fillna(df_kospi['종가'])
    if '거래량' in df_kospi.columns:
        df_kospi['Volume'] = df_kospi['Volume'].fillna(df_kospi['거래량'])
    
    # 'Change' 컬럼이 없을 경우 (FDR KS11 호환)
    if 'Change' not in df_kospi.columns:
        df_kospi['Change'] = df_kospi['Close'].pct_change()
        print("   > 'Change'(등락률) 컬럼이 없어 새로 생성함.")

    # '깨끗한' 컬럼만 선택
    df_kospi_clean = df_kospi[[col for col in FINAL_COLS if col in df_kospi.columns]]
    
    # 덮어쓰기
    df_kospi_clean.to_parquet(KOSPI_FILE, index=False)
    print(f"✅ [성공] {KOSPI_FILE} 파일이 '영어'로 통일되어 복구되었습니다.")
    
except Exception as e:
    print(f"❌ [실패] {KOSPI_FILE} 복구 중 오류 발생: {e}")
    sys.exit(1)

print("\n>>> '나무'와 '숲' 파일이 모두 '영어'로 통일되었습니다.")
print(">>> 이제 'run_weekly_update.py'를 실행하세요. (1분 30초 소요)")