# daily_recommender.py
# [V27-Fixed 4차] : '진짜 15개 피처' 커리큘럼 적용

import pandas as pd
import joblib
import os
import sys
from datetime import datetime
import time 

# --- [파일 경로 정의] ---
MODEL_FILE = "new_Hoj_MODELENGINE_V25.pkl"
DB_FILE = "new_Hoj_DB_V25.parquet"
# --------------------

# --- [V21] '진짜 15개 피처' 목록 ---
# (4단계 train... 스크립트와 100% 동일해야 함)
FEATURES = [
    'SMA_20', 'SMA_40', 'SMA_60', 'SMA_90',
    'RSI_14',
    'VOL_SMA_20',
    'MACD', 'MACD_Sig',
    'BBP_20',
    'ATR_14',
    'STOCH_K', 'STOCH_D',
    'CCI_20',
    'KOSPI_수익률', # (V21에서 추가됨)
    'ALPHA_SMA_20'
]
# ---------------------------------

def get_latest_data(df):
    try:
        df['Date'] = pd.to_datetime(df['Date'])
        latest_date = df['Date'].max()
        print(f"  > 'Date' 기준 '{latest_date.strftime('%Y-%m-%d')}' 데이터로 '예상 수익률' 예측 중...")
        latest_df = df[df['Date'] == latest_date].copy()
        return latest_df, latest_date
    except Exception as e:
        print(f"오류: V25 DB에서 최신 날짜 데이터를 필터링하는 중 실패. {e}")
        return None, None

def predict_top10(model, latest_df):
    try:
        valid_features = [f for f in FEATURES if f in latest_df.columns]
        if len(valid_features) != len(FEATURES):
             print(f"❌ 오류: '진짜 15개 피처' 중 일부가 DB에 없습니다. 2/3단계 스크립트를 확인하세요.")
             missing = [f for f in FEATURES if f not in latest_df.columns]
             print(f"   > 누락된 피처: {missing}")
             sys.exit(1)
             
        print(f"  > 예측에 {len(valid_features)}개 피처 사용...")
        X_latest = latest_df[valid_features]
        
        probabilities = model.predict_proba(X_latest)
        latest_df['예상수익률'] = probabilities[:, 1]
        
        final_df = latest_df # (V25 DB는 이미 '종목명'이 병합된 완성본)
        top_10 = final_df.sort_values(by='예상수익률', ascending=False).head(10)
        
        if '현재가' not in top_10.columns and 'Close' in top_10.columns:
            top_10['현재가'] = top_10['Close']
            
        top_10['예상수익률(%)'] = (top_10['예상수익률'] * 100).round(2)
        
        output_column_name = '종목명' if '종목명' in top_10.columns else 'Name'
        
        return top_10[[output_column_name, 'Code', '현재가', '예상수익률(%)', '예상수익률']]

    except Exception as e:
        print(f"오류: Top 10 예측 중 실패. {e}")
        return None

if __name__ == "__main__":
    try:
        print(f"[0] Hoj 실전 챔피언 모델('{MODEL_FILE}') 로드 중...")
        model = joblib.load(MODEL_FILE)
        print("  > 모델 로드 성공.")
    except Exception as e:
        print(f"❌ 치명적 오류: {MODEL_FILE} 로드 실패. {e}")
        sys.exit(1)
        
    try:
        print(f"[1] '{DB_FILE}' (V25 Hoj 피처 데이터) 로드 중...")
        start_time = time.time()
        df = pd.read_parquet(DB_FILE)
        print(f"  > 로드 성공. (총 {len(df)} 행, {time.time() - start_time:.0f}초)")
    except Exception as e:
        print(f"❌ 치명적 오류: {DB_FILE} 로드 실패. {e}")
        sys.exit(1)

    latest_df, latest_date = get_latest_data(df)
    if latest_df is None:
        sys.exit(1)
        
    top_10_df = predict_top10(model, latest_df)
    if top_10_df is None:
        sys.exit(1)
        
    date_str = latest_date.strftime('%Y-%m-%d')
    print("\n" + "="*80)
    print(f"★★★ '{date_str}' Hoj 실전 엔진(V25) Top 10 추천 ★★★")
    print("="*80)
    if 'Name' in top_10_df.columns and '종목명' not in top_10_df.columns:
        top_10_df = top_10_df.rename(columns={'Name': '종목명'})
        
    print(top_10_df.to_string(index=False))
    print("="*80)
    
    now_str = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    output_filename = f"recommendation_HOJ_V27_{date_str}_{now_str}.csv" 
    try:
        top_10_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"  > 추천 결과가 '{output_filename}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"경고: CSV 저장 실패. {e}")