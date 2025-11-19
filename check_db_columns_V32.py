from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# --- 코드 버전: V32 (Check Sle DB Columns) ---
import pandas as pd
import numpy as np
import os

# --- 1. V32 설정 ---
SLE_DB_FILE = SLE_DB_REAL # (★★★ 확인할 Sle DB 파일 ★★★)

# ===========================
# 🚀 V32 메인 함수
# ===========================
def check_v32_database_columns():
    
    print(f"[V32] Sle 엔진 DB('{SLE_DB_FILE}') 파일 검사 시작...")
    
    if not os.path.exists(SLE_DB_FILE):
        print(f"  > ❌ 오류: '{SLE_DB_FILE}' 파일이 없습니다."); return

    try:
        df_sle = pd.read_parquet(SLE_DB_FILE)
        
        print("\n" + "="*60)
        print(f"           ★★★ '{SLE_DB_FILE}' 컬럼 분석 ★★★")
        print("="*60)
        
        # (1. 컬럼 리스트 전체 출력)
        print("\n[1] 전체 컬럼 리스트:")
        print(df_sle.columns.tolist())
        
        # (2. 상위 5줄 샘플 데이터 출력)
        print("\n[2] 상위 5줄 샘플 데이터:")
        print(df_sle.head().to_string()) # (가로로 길어도 다 보이게 to_string() 사용)
        
        print("\n" + "="*60)
        print("  > ✅ 파일 로드 및 분석 성공.")
        
    except Exception as e:
        print(f"  > ❌ 오류: {SLE_DB_FILE} 파일 로드 실패. ({e})")

# ===========================
# 실행
# ===========================
if __name__ == "__main__":
    check_v32_database_columns()

# --- 코드 버전: V32 ---