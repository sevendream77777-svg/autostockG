# fix_corrupted_data_v1.py
# [1회용 복구 스크립트]
# '한글/영어'로 쪼개진 1단계 원본 재료 2개를 '영어'로 강제 통일합니다.

import pandas as pd
import os
import sys

# --- 복구할 파일 2개 ---
STOCK_FILE = "all_stocks_cumulative.parquet"
KOSPI_FILE = "kospi_index_10y.parquet"

# --- 2단계(update_features...)가 사용할 '영어' 컬럼 8개 ---
# (이 8개만 있으면 '진짜 15개 피처' 계산 가능)
FINAL_COLS = ['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Market', 'Name']

def print_break():
    print("\n" + "="*80 + "\n")

print(f"--- '데이터 오염' 1회용 복구 스크립트 시작 ---")
print(f"    '한글'(종가)과 '영어'(Close)로 쪼개진 파일을 '영어'로 통일합니다.")
print_break()

# --- 1. 'all_stocks_cumulative.parquet' 복구 ---
print(f"--- 1. {STOCK_FILE} 복구 시작 ---")
if not os.path.exists(STOCK_FILE):
    print(f"❌ 오류: {STOCK_FILE} 파일이 없습니다. (1단계 재실행 필요)")
    sys.exit(1)

try:
    df_stock = pd.read_parquet(STOCK_FILE)
    print(f"   > '오염된' 파일 로드 성공. (총 {len(df_stock)} 행)")

    # [언어 통일] '한글' 컬럼을 '영어' 컬럼으로 강제 병합
    # (예: '시가'가 NaN이면 'Open' 값을, 'Open'이 NaN이면 '시가' 값을 사용)
    df_stock['Date']   = df_stock['날짜']
    df_stock['Code']   = df_stock['Code'].fillna(df_stock['종목코드'])
    df_stock['Open']   = df_stock['Open'].fillna(df_stock['시가'])
    df_stock['High']   = df_stock['High'].fillna(df_stock['고가'])
    df_stock['Low']    = df_stock['Low'].fillna(df_stock['저가'])
    df_stock['Close']  = df_stock['Close'].fillna(df_stock['종가'])
    df_stock['Volume'] = df_stock['Volume'].fillna(df_stock['거래량'])
    df_stock['Change'] = df_stock['Change'].fillna(df_stock['등락률'])
    
    # '깨끗한' 8개 컬럼만 선택 (나머지 한글/중복 컬럼은 버림)
    df_stock_clean = df_stock[[col for col in FINAL_COLS if col in df_stock.columns]]
    
    # NaN이 아닌 행 개수 확인
    print(f"   > 'Close' (종가) 복구 후 Non-Null 개수: {df_stock_clean['Close'].count()} 행")
    
    # 덮어쓰기
    df_stock_clean.to_parquet(STOCK_FILE, index=False)
    print(f"✅ [성공] {STOCK_FILE} 파일이 '영어'로 통일되어 복구되었습니다.")

except Exception as e:
    print(f"❌ [실패] {STOCK_FILE} 복구 중 오류 발생: {e}")
    sys.exit(1)

print_break()

# --- 2. 'kospi_index_10y.parquet' 복구 ---
print(f"--- 2. {KOSPI_FILE} 복구 시작 ---")
if not os.path.exists(KOSPI_FILE):
    print(f"❌ 오류: {KOSPI_FILE} 파일이 없습니다. (1단계 재실행 필요)")
    sys.exit(1)

try:
    df_kospi = pd.read_parquet(KOSPI_FILE)
    print(f"   > '오염된' 파일 로드 성공. (총 {len(df_kospi)} 행)")

    # [언어 통일] (KOSPI는 'Change'가 없을 수 있음)
    df_kospi['Date']   = df_kospi['날짜']
    df_kospi['Open']   = df_kospi['Open'].fillna(df_kospi['시가'])
    df_kospi['High']   = df_kospi['High'].fillna(df_kospi['고가'])
    df_kospi['Low']    = df_kospi['Low'].fillna(df_kospi['저가'])
    df_kospi['Close']  = df_kospi['Close'].fillna(df_kospi['종가'])
    df_kospi['Volume'] = df_kospi['Volume'].fillna(df_kospi['거래량'])
    if 'Change' not in df_kospi.columns: df_kospi['Change'] = np.nan
    
    # '깨끗한' 컬럼만 선택
    df_kospi_clean = df_kospi[['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']]
    
    # 덮어쓰기
    df_kospi_clean.to_parquet(KOSPI_FILE, index=False)
    print(f"✅ [성공] {KOSPI_FILE} 파일이 '영어'로 통일되어 복구되었습니다.")
    
except Exception as e:
    print(f"❌ [실패] {KOSPI_FILE} 복구 중 오류 발생: {e}")
    sys.exit(1)

print_break()
print(">>> 모든 '오염된' 데이터가 '깨끗하게' 복구되었습니다.")
print(">>> 2단계(update_features...)가 '깡통 피처'를 재생성해야 하니,")
print(">>> 'all_features_cumulative_V21_Hoj.parquet' 파일을 '수동으로 삭제'한 후,")
print(">>> 'python run_weekly_update.py'를 실행해 주세요. (약 1분 30초 소요)")