# --- 코드 버전: V28.1 (KeyError Fix) ---
import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
import joblib 
import pykrx
from datetime import datetime, timedelta
import time
import os 

# --- 1. V28 설정 ---
MODEL_FILE = "REAL_CHAMPION_MODEL_V25.pkl" # (V25 실전 10년 엔진)
FEATURE_FILE = "V25_Hoj_DB.parquet"       # (V25 Hoj DB + 종목명)
TICKER_MAP_FILE = "ticker_map.parquet"    # (종목명 사전)

# (Hoj 엔진 12개 피처 리스트)
feature_columns_v5 = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

# (종목명 사전 로드)
try:
    TICKER_NAME_MAP = pd.read_parquet(TICKER_MAP_FILE).set_index('종목코드')['종목명'].to_dict()
    print(f"[V28.1] '종목명 사전'({TICKER_MAP_FILE}) 로드 성공.")
except Exception as e:
    print(f"  > 경고: '{TICKER_MAP_FILE}' 파일이 없습니다. V25 DB의 종목명을 사용합니다.")
    TICKER_NAME_MAP = {} 

# --- 2. 헬퍼 함수 ---
def get_single_stock_prediction(model, df_db, ticker_code):
    
    # 1. DB에서 해당 종목 데이터만 추출
    stock_data = df_db[df_db['종목코드'] == ticker_code].copy()
    
    if stock_data.empty:
        print(f"  > 오류: '{ticker_code}'에 대한 데이터를 DB에서 찾을 수 없습니다.")
        return None

    # 2. 해당 종목의 '가장 최근 날짜' 데이터(1줄)만 추출
    latest_date = stock_data['날짜'].max()
    today_data = stock_data[stock_data['날짜'] == latest_date]
    
    if today_data.empty:
        print(f"  > 오류: '{ticker_code}'의 최신 데이터가 없습니다."); return None

    # 3. '오늘' 데이터로 '예측'
    print(f"  > '{latest_date.strftime('%Y-%m-%d')}' 종가 기준 '예상 수익률' 예측 중...")
    X_today = today_data[feature_columns_v5] 
    X_today.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_today.columns]
    
    prediction = model.predict(X_today)[0] # (단 1개의 숫자)
    
    return prediction, today_data.iloc[0] # (예측값과 그날의 정보 반환)


# --- 3. 메인 실행 ---
if __name__ == "__main__":
    
    # 1. DB 로드
    try:
        print(f"[1] Hoj 엔진 V25 데이터베이스('{FEATURE_FILE}') 로드 중...")
        df_processed_v25 = pd.read_parquet(FEATURE_FILE)
        df_processed_v25['날짜'] = pd.to_datetime(df_processed_v25['날짜'])
        print(f"  > 로드 성공. (총 {len(df_processed_v25):,} 행)")
    except Exception as e:
        print(f"  > 오류: '{FEATURE_FILE}' 파일이 없습니다. ({e})"); exit()

    # 2. 모델 로드
    try:
        print(f"[0] Hoj 실전 챔피언 모델('{MODEL_FILE}') 로드 중...")
        model = joblib.load(MODEL_FILE) 
        print("  > 모델 로드 성공.")
    except Exception as e:
        print(f"  > 오류: '{MODEL_FILE}' 모델 파일이 없습니다. ({e})"); exit()
        
    # 3. 사용자 입력 (종목 코드)
    print("\n" + "="*50)
    ticker_input = input("  > 5일 뒤 수익률을 예측할 '종목 코드'를 입력하세요 (예: 005930): ")
    print("="*50)
    
    if not ticker_input.isdigit() or len(ticker_input) != 6:
        print("  > ❌ 오류: '005930' 형식의 6자리 숫자 코드를 입력해야 합니다.")
        exit()
        
    # 4. 개별 종목 예측 실행
    result = get_single_stock_prediction(model, df_processed_v25, ticker_input)
    
    if result:
        prediction, info = result
        name = info['종목명']
        
        # (★★★ V28.1 수정: '현재가' -> '종가'로 변경 ★★★)
        price = info['종가'] 
        
        date_str = info['날짜'].strftime('%Y-%m-%d')
        
        print("\n" + "="*50)
        print(f"★★★ Hoj 엔진 (60d/5d) 개별 예측 결과 ★★★")
        print("="*50)
        print(f"  > 예측 기준일: {date_str}")
        print(f"  > 종목명 (코드): {name} ({ticker_input})")
        print(f"  > 기준일 종가: {price:,} 원")
        print(f"  > 5일 뒤 예상 수익률: {prediction*100:+.2f} %")
        print("="*50)
        
    else:
        print(f"\n❌ '{ticker_input}' 종목의 예측에 실패했습니다.")

# --- 코드 버전: V28.1 (KeyError Fix) ---