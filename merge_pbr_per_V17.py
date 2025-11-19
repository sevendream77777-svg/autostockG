# --- 코드 버전: V17 (PER/PBR 크롤링 결과 병합기) ---
import os
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# --- 1. 설정 ---
INPUT_DIR = "output_chunks"      # 크롤링 결과 CSV가 저장된 폴더
OUTPUT_FILE = "pbr_per.parquet"  # 최종 병합 파일명

# --- 2. 유틸 함수 ---
def merge_chunks(input_dir: str, output_file: str):
    files = sorted(Path(input_dir).glob("out_*.csv"))
    if not files:
        print(f"❌ 병합할 파일이 없습니다. ({input_dir})")
        return

    print(f"[1] 병합 대상 파일 수: {len(files)}개")
    merged_list = []

    for f in tqdm(files, desc="병합 중"):
        try:
            df = pd.read_csv(f, encoding="utf-8")
            merged_list.append(df)
        except Exception as e:
            print(f"  > {f.name} 읽기 실패: {e}")

    if not merged_list:
        print("❌ 유효한 데이터가 없습니다.")
        return

    df_all = pd.concat(merged_list, ignore_index=True)
    print(f"[2] 병합 완료: 총 {len(df_all):,}행")

    # --- 3. 컬럼 정리 ---
    rename_map = {
        'date': '날짜',
        'ticker': '종목코드',
        'PBR': 'PBR',
        'PER': 'PER'
    }
    df_all.rename(columns=rename_map, inplace=True)

    # 날짜, 종목코드 기준 중복 제거
    before_len = len(df_all)
    df_all.drop_duplicates(subset=["날짜", "종목코드"], keep="last", inplace=True)
    print(f"[3] 중복 제거: {before_len - len(df_all):,}건 제거")

    # --- 4. 정렬 및 저장 ---
    df_all.sort_values(["날짜", "종목코드"], inplace=True)
    df_all.reset_index(drop=True, inplace=True)

    df_all.to_parquet(output_file, index=False)
    print(f"[4] 저장 완료 → {output_file} (총 {len(df_all):,}행)")

# --- 실행부 ---
if __name__ == "__main__":
    merge_chunks(INPUT_DIR, OUTPUT_FILE)
