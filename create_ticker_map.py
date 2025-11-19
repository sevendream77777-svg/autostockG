# --- 코드 버전: V29 (FinanceDataReader Ticker Map) ---
import pandas as pd
import FinanceDataReader as fdr
import os

OUTPUT_MAP_FILE = "ticker_map.parquet"

def create_ticker_name_map_fdr():
    print("[V29] '종목명 사전' 파일 생성 시작 (FinanceDataReader 사용)...")
    
    try:
        # (★★★ V29 수정: fdr.StockListing('KRX')를 사용하여 전체 상장 목록 확보 ★★★)
        # (KOSPI, KOSDAQ, KONEX가 모두 포함됨)
        df_map = fdr.StockListing('KRX')
        
        if df_map.empty:
             print("  > ❌ 오류: FinanceDataReader가 KRX 상장 목록을 가져오지 못했습니다.")
             return

        # (필요한 컬럼만 추출: 'Code' -> '종목코드', 'Name' -> '종목명')
        df_map = df_map[['Code', 'Name']].copy()
        df_map.rename(columns={'Code': '종목코드', 'Name': '종목명'}, inplace=True)
        
        df_map.to_parquet(OUTPUT_MAP_FILE, index=False)
        print(f"\n✅ '종목명 사전' 파일 생성 완료! ({OUTPUT_MAP_FILE})")
        print(f"  > 총 {len(df_map)}개 종목 저장됨.")
        print(df_map.head()) # (샘플 출력)
        
    except Exception as e:
        print(f"  > ❌ 오류: '종목명 사전' 생성 실패. {e}")

if __name__ == "__main__":
    create_ticker_name_map_fdr()

# --- 코드 버전: V29 ---