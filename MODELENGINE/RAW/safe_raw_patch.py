# safe_raw_patch.py
import os
import pandas as pd
import datetime
from safe_raw_builder import fetch_ohlcv, load_all_codes

BASE = r"F:\autostockG\MODELENGINE\RAW"
DAILY = os.path.join(BASE, "DAILY")
LOGS = os.path.join(BASE, "logs")

def patch_date(date_str):
    print(f"[PATCH] 재수집 날짜: {date_str}")

    codes = load_all_codes()
    rows = []

    for c in codes:
        df = fetch_ohlcv(c, date_str, date_str)
        if len(df) == 1:
            rows.append(df.iloc[0])

    ddf = pd.DataFrame(rows)
    path = os.path.join(DAILY, f"{date_str}.parquet")
    ddf.to_parquet(path)

    # 로그 저장
    log_path = os.path.join(LOGS, f"patch_{date_str}.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"{date_str} 패치 완료: {len(ddf)}행")

if __name__ == "__main__":
    # 예: 특정 날짜 패치
    patch_date("2025-11-13")
