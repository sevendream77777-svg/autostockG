from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# --- 코드 버전: V30 (Merge Hoj + Sle DB) ---
import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# --- 1. V30 설정 ---
HOJ_DB_FILE = "all_features_cumulative_V21_Hoj.parquet" # (12개 Hoj 피처)
SLE_DB_FILE = SLE_DB_REAL        # (4개 Sle 피처)

# (★★★ V30: 최종 16개 피처 데이터베이스 ★★★)
FINAL_DB_FILE = "V30_HojSle_Final.parquet" 

# (V12/V30 최종 16개 피처 리스트)
feature_columns_v30 = [
    # Hoj (12개)
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20',
    # Sle (4개)
    'PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY'
]

# ===========================
# 🚀 V30 메인 함수
# ===========================
def merge_v30_database():
    
    # --- 1. Hoj DB 로드 (12개 피처) ---
    print(f"[1] Hoj 엔진 DB('{HOJ_DB_FILE}') 로드 중...")
    if not os.path.exists(HOJ_DB_FILE):
        print(f"  > 오류: '{HOJ_DB_FILE}' 파일이 없습니다."); return
    try:
        df_hoj = pd.read_parquet(HOJ_DB_FILE)
        df_hoj['날짜'] = pd.to_datetime(df_hoj['날짜'])
        print(f"  > Hoj DB 로드 성공. (총 {len(df_hoj):,} 행)")
    except Exception as e:
        print(f"  > 오류: {HOJ_DB_FILE} 파일 로드 실패. ({e})"); return

    # --- 2. Sle DB 로드 (4개 피처) ---
    print(f"[2] Sle 엔진 DB('{SLE_DB_FILE}') 로드 중...")
    if not os.path.exists(SLE_DB_FILE):
        print(f"  > 오류: '{SLE_DB_FILE}' 파일이 없습니다."); return
    try:
        df_sle = pd.read_parquet(SLE_DB_FILE)
        # (Sle DB의 날짜/종목코드 컬럼명을 Hoj DB와 통일)
        df_sle.rename(columns={'date': '날짜', 'ticker': '종목코드'}, inplace=True)
        df_sle['날짜'] = pd.to_datetime(df_sle['날짜'])
        
        # (★★★ V30: Sle 피처 이름 확정 ★★★)
        sle_features = ['PBR', 'PER', '외국인순매수', '기관순매수']
        df_sle.rename(columns={'외국인순매수': 'FOR_NET_BUY', '기관순매수': 'INS_NET_BUY'}, inplace=True)
        
        # (결측치(NaN)를 패널티 값(9999)으로 채우기 - AI 학습용)
        df_sle['PBR'] = df_sle['PBR'].fillna(9999)
        df_sle['PER'] = df_sle['PER'].fillna(9999)
        df_sle['FOR_NET_BUY'] = df_sle['FOR_NET_BUY'].fillna(0)
        df_sle['INS_NET_BUY'] = df_sle['INS_NET_BUY'].fillna(0)
        
        print(f"  > Sle DB 로드 및 정제 성공. (총 {len(df_sle):,} 행)")
    except Exception as e:
        print(f"  > 오류: {SLE_DB_FILE} 파일 로드 실패. ({e})"); return

    # --- 3. (★★★ V30 핵심) Hoj DB + Sle DB 병합 ★★★
    print("[3] Hoj DB(12개)와 Sle DB(4개) 병합 중...")
    
    # (Hoj DB를 기준으로, Sle DB의 16개 피처를 '왼쪽(left)'으로 붙임)
    df_v30 = pd.merge(
        df_hoj, 
        df_sle[['날짜', '종목코드'] + ['PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY']], 
        on=['날짜', '종목코드'], 
        how='left' # (Hoj DB 기준)
    )
    
    # (Sle DB에 없던 10년치 데이터는 결측치(NaN)가 됨)
    # (이 결측치를 'ffill' (앞 날짜 데이터로 채우기)로 보간)
    print("  > 16개 피처 결측값 보간 중 (ffill)...")
    df_v30[['PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY']] = df_v30.groupby('종목코드')[['PBR', 'PER', 'FOR_NET_BUY', 'INS_NET_BUY']].ffill()

    # (V5와 동일하게, 16개 피처 모두 NaN이 없는 데이터만 최종 사용)
    df_v30.dropna(subset=feature_columns_v30, inplace=True) 

    # --- 4. V30 최종 데이터베이스 저장 ---
    df_v30.to_parquet(FINAL_FEATURE_FILE, index=False)
    
    print(f"\n✅ V30 (Hoj+Sle 16개 피처) 완전체 데이터베이스 구축 완료!")
    print(f" > 파일명: '{FINAL_FEATURE_FILE}'")
    print(f" > 총 행 수: {len(df_v30):,}")
    print(f" > 피처: 16개 (Hoj 12개 + Sle 4개)")

# ===========================
# 실행
# ===========================
if __name__ == "__main__":
    merge_v30_database()

# --- 코드 버전: V30 (Merge Hoj + Sle) ---