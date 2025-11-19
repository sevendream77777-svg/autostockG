# --- 코드 버전: V11-D (tqdm import fix) ---
import pandas as pd
import numpy as np
import os
from datetime import datetime
from tqdm import tqdm # (★★★ 수정: tqdm 라이브러리 추가 ★★★)

# --- 설정 ---
SAMPLE_LIST_FILE = "v11_sample_list.parquet"
OUTPUT_TARGETS_CSV = "targets.csv"
NAVER_BASE_URL = "https://finance.naver.com/item/main.naver?code="

def create_targets_csv():
    print(f"[1] V11 샘플 리스트('{SAMPLE_LIST_FILE}') 로드 중...")
    if not os.path.exists(SAMPLE_LIST_FILE):
        print("❌ 오류: 샘플 파일이 없습니다. V11-A 스크립트를 먼저 실행하세요."); return

    try:
        df_samples = pd.read_parquet(SAMPLE_LIST_FILE)
        df_samples['날짜'] = pd.to_datetime(df_samples['날짜'])
    except Exception as e:
        print(f"❌ 오류: 샘플 파일 로드 실패. ({e})"); return

    print(f"  > 총 {len(df_samples):,}건의 샘플 로드 완료.")
    
    # --- 2. 타겟 URL 생성 ---
    targets = []
    print("[2] 타겟 URL 리스트 생성 중...")
    
    # (총 511,245건을 변환)
    for index, row in tqdm(df_samples.iterrows(), total=len(df_samples), desc="Generating URLs"):
        ticker = row['종목코드']
        date_str = row['날짜'].strftime("%Y%m%d")
        
        # 네이버 금융은 특정 날짜의 재무정보를 URL로 직접 조회하는 기능이 없습니다.
        # 따라서 현재 날짜의 재무정보만 조회할 수 있도록 '코드' 기반 URL만 생성합니다.
        
        url = f"{NAVER_BASE_URL}{ticker}" 
        
        # id는 고유해야 하므로 (날짜 + 종목코드)를 사용
        id_str = f"{date_str}_{ticker}" 
        
        targets.append({'id': id_str, 'url': url})
        
    df_targets = pd.DataFrame(targets)
    
    # --- 3. CSV로 저장 ---
    df_targets.to_csv(OUTPUT_TARGETS_CSV, index=False, encoding='utf-8')
    print(f"\n✅ 타겟 CSV 생성 완료: '{OUTPUT_TARGETS_CSV}'")
    print(f" (총 {len(df_targets)}개의 URL 준비 완료. 크롤러 실행 준비 완료.)")

if __name__ == "__main__":
    create_targets_csv()