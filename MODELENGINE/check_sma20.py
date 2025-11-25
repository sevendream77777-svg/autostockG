import pandas as pd
import os
import sys

def check_sma20(file_path):
    if not os.path.exists(file_path):
        print(f"❌ 파일 없음: {file_path}")
        return

    try:
        print(f"\n🔎 [SMA_20 정밀 검사] 대상: {os.path.basename(file_path)}")
        df = pd.read_parquet(file_path)
        
        # SMA_20과 ALPHA_SMA_20 비교
        cols_to_check = ['SMA_20', 'ALPHA_SMA_20']
        print("-" * 50)
        print(f"{'Column':<20} | {'NaN 개수':<10} | {'비고':<10}")
        print("-" * 50)
        
        for col in cols_to_check:
            if col in df.columns:
                null_cnt = df[col].isnull().sum()
                print(f"{col:<20} | {null_cnt:<10,} |")
            else:
                print(f"{col:<20} | {'없음':<10} |")
        print("-" * 50)
        print("✅ 해석: ALPHA가 SMA_20보다 결측치가 조금 더 많은 것이 정상입니다.")
        print("         (수익률 계산 때문에 하루가 더 필요하기 때문)")

    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        check_sma20(r"F:\autostockG\MODELENGINE\FEATURE\features_V31_251124.parquet")
    else:
        check_sma20(sys.argv[1])
