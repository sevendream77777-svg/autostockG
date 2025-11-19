# --- RAW 패치 병합기 V1 ---
# 위대하신호정님 위한 전용 버전 (15~17일 패치 적용)

import pandas as pd
import os

RAW_MAIN = r"F:\autostockG\all_stocks_cumulative.parquet"
DAILY_DIR = r"F:\autostockG\MODELENGINE\RAW\DAILY"

# 패치 대상 날짜
PATCH_DATES = ["20251115", "20251116", "20251117"]

def load_main():
    print("[LOAD] RAW 메인 로드 중...")
    df = pd.read_parquet(RAW_MAIN)
    print(f"  - 메인 RAW: {df['Date'].max()} 까지 {len(df):,}행")
    return df

def load_patch(date):
    path = os.path.join(DAILY_DIR, f"{date}.parquet")
    if not os.path.exists(path):
        print(f"[WARN] {path} 없음. 패치 불가.")
        return None
    print(f"[LOAD] 패치 로드: {path}")
    return pd.read_parquet(path)

def merge_all():
    df_main = load_main()
    frames = [df_main]

    for d in PATCH_DATES:
        df_patch = load_patch(d)
        if df_patch is not None:
            frames.append(df_patch)

    print("[MERGE] 병합 시작...")
    df_new = pd.concat(frames, ignore_index=True)

    print("[SORT] 날짜/코드 정렬...")
    df_new = df_new.sort_values(["Date", "Code"]).reset_index(drop=True)

    print("[SAVE] 메인 RAW 덮어쓰기...")
    df_new.to_parquet(RAW_MAIN)

    print(f"[DONE] RAW 업데이트 완료!")
    print(f"       최신 날짜: {df_new['Date'].max()}")
    print(f"       총 행수: {len(df_new):,}")

if __name__ == "__main__":
    merge_all()
