# --- 코드 버전: V19.1 (안정적인 날짜 테스트) ---
import pandas as pd
import pykrx
from datetime import datetime
import time

# --- 테스트 날짜 (★★★ V19.1 수정: '버그 의심일'이 아닌, 안정적인 '과거일'로 변경 ★★★) ---
TEST_DATE_STR = "20251027" 

# --- PBR 데이터 수집 함수 (V19와 동일 - 올바른 코드) ---
def fetch_pbr_test(date_str):
    date_str = str(date_str)
    print(f"\n[테스트] {date_str} 날짜의 PBR 데이터 수집 시도...")
    
    try:
        # ticker='ALL'이 올바른 함수 사용법
        df_f = pykrx.stock.get_market_fundamental(date_str, date_str, "ALL") 
        
        print(f"  > ✅ 수집 성공: 데이터 행 수 {len(df_f)}개.")
        return df_f.head()
        
    except Exception as e:
        print(f"  > 🔴 {date_str} 데이터 수집 실패. 오류: {e}")
        return None

# ===========================
# 🚀 메인 테스트 실행
# ===========================
if __name__ == "__main__":
    
    result_df = fetch_pbr_test(TEST_DATE_STR)
    
    print("\n" + "="*50)
    print(f"★★★ PBR 수집 최종 테스트 결과 ({TEST_DATE_STR}) ★★★")
    print("="*50)
    
    if result_df is not None and not result_df.empty:
        print(f"  ✅ PBR/PER 데이터 수집 성공! (샘플 5개):")
        print(result_df)
    else:
        print("  ❌ PBR/PER 데이터 수집 실패. (데이터가 비어있거나 오류 발생)")
    print("="*50)