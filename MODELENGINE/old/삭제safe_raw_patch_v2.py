# safe_raw_patch_v2.py
# --- RAW 패치기 V2 (safe_raw_builder_v2 기반) ---
# 특정 날짜의 KOSPI+KOSDAQ OHLCV 1일분만 받아서 DAILY에 저장
# 사용법:
#   python safe_raw_patch_v2.py --date 20251115

import os
import argparse
from datetime import datetime, timedelta

import pandas as pd

from safe_raw_builder_v2 import (
    BASE_DIR, DAILY_DIR,
    load_all_codes,
    fetch_ohlcv_multi_source,
    log,
)


def fetch_single_day(code: str, date_str: str) -> pd.DataFrame:
    """
    특정 날짜(1일치) OHLCV만 수집하여 DataFrame 반환.
    내부적으로는 start=당일, end=다음날 로 호출해서
    Yahoo/Naver/KRX에서 그 구간만 가져오고, 최종적으로 해당 날짜만 필터.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    start = date_obj.strftime("%Y-%m-%d")
    end = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    fail_log = []
    fb_log = []
    krx_log = []

    df_full = fetch_ohlcv_multi_source(code, start, end, fail_log, fb_log, krx_log)
    if df_full is None or df_full.empty:
        return pd.DataFrame()

    # 해당 날짜만 필터
    df_full["Date"] = pd.to_datetime(df_full["Date"])
    mask = df_full["Date"].dt.date == date_obj
    df_day = df_full[mask].copy()
    if df_day.empty:
        return pd.DataFrame()

    # 컬럼/타입 맞추기
    df_day = df_day[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]
    df_day["Code"] = df_day["Code"].astype(str).str.zfill(6)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df_day[col] = pd.to_numeric(df_day[col], errors="coerce")

    return df_day


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYYMMDD 또는 YYYY-MM-DD")
    args = parser.parse_args()

    raw_date = args.date.replace("-", "")
    if len(raw_date) != 8 or not raw_date.isdigit():
        raise ValueError("날짜는 YYYYMMDD 형식이어야 합니다.")

    date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    output_path = os.path.join(DAILY_DIR, f"{raw_date}.parquet")

    log(f"===== RAW 패치 시작: {date_str} =====")
    log(f"[OUTPUT] {output_path}")

    codes = load_all_codes()
    if not codes:
        log("[ERROR] 종목코드 수집 실패 → 종료")
        return

    all_rows = []
    total = len(codes)

    for i, code in enumerate(codes, start=1):
        log(f"[PATCH] ({i}/{total}) {code} 1일치 수집 시도...")
        df = fetch_single_day(code, date_str)
        if df is not None and not df.empty:
            all_rows.append(df)

    if not all_rows:
        log("[WARN] 모든 종목이 빈 데이터입니다. (해당 날짜에 거래가 없거나, 수집 실패)")
        return

    full = pd.concat(all_rows, ignore_index=True)
    full = full.dropna(subset=["Date", "Code"])
    full["Code"] = full["Code"].astype(str).str.zfill(6)
    full = full.sort_values(["Date", "Code"]).reset_index(drop=True)

    os.makedirs(DAILY_DIR, exist_ok=True)
    full.to_parquet(output_path)
    log(f"[SAVE] 패치 저장 완료: {output_path} (행 수: {len(full):,})")
    log("===== RAW 패치 완료 =====")


if __name__ == "__main__":
    main()
