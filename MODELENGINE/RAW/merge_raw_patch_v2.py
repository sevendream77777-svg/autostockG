# merge_raw_patch_v2.py
# --- RAW 병합기 V2 ---
# 기존 RAW + DAILY 패치(YYYYMMDD) 파일 이어붙이기

import os
import pandas as pd

from safe_raw_builder_v2 import BASE_DIR, RAW_MAIN, DAILY_DIR, log

# 패치 대상 날짜 목록 (필요 시 수정)
PATCH_DATES = ["20251115", "20251116", "20251117"]


def main():
    log("===== RAW 병합기 시작 =====")

    if not os.path.exists(RAW_MAIN):
        log(f"[ERROR] 메인 RAW 파일 없음 → {RAW_MAIN}")
        return

    df_main = pd.read_parquet(RAW_MAIN)
    log(f"[LOAD] 메인 RAW: {df_main['Date'].min()} ~ {df_main['Date'].max()}, {len(df_main):,}행")

    frames = [df_main]

    for d in PATCH_DATES:
        path = os.path.join(DAILY_DIR, f"{d}.parquet")
        if not os.path.exists(path):
            log(f"[WARN] 패치 파일 없음: {path}")
            continue

        df_p = pd.read_parquet(path)
        if df_p is None or df_p.empty:
            log(f"[WARN] 패치 파일 비어 있음: {path}")
            continue

        log(f"[LOAD] 패치 {d}: {df_p['Date'].min()} ~ {df_p['Date'].max()}, {len(df_p):,}행")
        frames.append(df_p)

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.dropna(subset=["Date", "Code"])
    merged["Code"] = merged["Code"].astype(str).str.zfill(6)
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)

    merged.to_parquet(RAW_MAIN)
    log(f"[SAVE] RAW 병합 완료 → 최신 날짜: {merged['Date'].max()}, 총 행수: {len(merged):,}")
    log("===== RAW 병합 완료 =====")


if __name__ == "__main__":
    main()
