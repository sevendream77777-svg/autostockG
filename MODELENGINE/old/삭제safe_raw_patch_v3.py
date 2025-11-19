# safe_raw_patch_v3.py
# RAW 하루치 패치기 (강력 교차검증 + v2 기반 안정화 완전판)

import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# v2 기반 핵심 함수 import
from safe_raw_builder_v2 import (
    BASE_DIR,
    DAILY_DIR,
    load_all_codes,
    fetch_ohlcv_multi_source,
    log,
)


def normalize_numeric_series(val):
    """
    숫자 컬럼에 들어오는 값이
    - 스칼라
    - 리스트
    - numpy 1D
    - numpy 2D (1×1)
    - pandas Series
    등 어떤 형태든 무조건 1D Series로 변환하는 안전 함수
    """
    # None → NaN
    if val is None:
        return pd.Series([pd.NA])

    # pandas Series이면 그대로
    if isinstance(val, pd.Series):
        return pd.to_numeric(val, errors="coerce")

    # numpy array 처리
    if isinstance(val, np.ndarray):
        val = val.flatten()  # 2D → 1D flatten

    # 스칼라 값 처리
    if isinstance(val, (int, float, str)):
        val = [val]

    # 리스트/tuple 처리
    if isinstance(val, (list, tuple)):
        return pd.to_numeric(pd.Series(val), errors="coerce")

    # 그 외 예상 못한 타입 → 한 번 더 강제로 감싸기
    return pd.to_numeric(pd.Series([val]), errors="coerce")


def fetch_single_day_multi(code: str, date_obj: datetime.date):
    """
    safe_raw_builder_v2 기반 → 1일치 only 가져오기.
    성공 시 df, "success"
    빈데이터 시 None, "empty"
    오류 시 None, "error"
    """
    date_str = date_obj.strftime("%Y-%m-%d")
    start = date_str
    end = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    fail_log = []
    fb_log = []
    krx_log = []

    try:
        df_full = fetch_ohlcv_multi_source(code, start, end, fail_log, fb_log, krx_log)
    except Exception as e:
        log(f"[PATCH] fetch error({code}): {e}")
        return None, "error"

    if df_full is None or df_full.empty:
        return None, "empty"

    # 날짜 정규화
    try:
        df_full["Date"] = pd.to_datetime(df_full["Date"])
    except Exception as e:
        log(f"[PATCH] Date 파싱 실패({code}): {e}")
        return None, "error"

    df_day = df_full[df_full["Date"].dt.date == date_obj].copy()
    if df_day.empty:
        return None, "empty"

    # 표준 컬럼 보장
    cols = ["Date", "Code", "Open", "High", "Low", "Close", "Volume"]
    for col in cols:
        if col not in df_day.columns:
            df_day[col] = pd.NA

    # Code 표준화
    try:
        df_day["Code"] = df_day["Code"].astype(str).str.zfill(6)
    except:
        df_day["Code"] = df_day["Code"].astype(str)

    # 숫자 컬럼 안정화(스칼라/배열 등 어떤 형태든 1D 변환)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df_day[col] = normalize_numeric_series(df_day[col])

    # 데이터가 여러 row 형태로 들어오도록 강제
    df_day = df_day[cols]
    return df_day, "success"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    args = parser.parse_args()

    raw_date = args.date.replace("-", "")
    if len(raw_date) != 8 or not raw_date.isdigit():
        raise ValueError("날짜는 YYYYMMDD 형식이어야 함 (예: 20251117)")

    date_obj = datetime.strptime(raw_date, "%Y%m%d").date()
    date_str = date_obj.strftime("%Y-%m-%d")

    output_path = os.path.join(DAILY_DIR, f"{raw_date}.parquet")
    os.makedirs(DAILY_DIR, exist_ok=True)

    log("====================================================")
    log(f"===== RAW 패치 V3 시작: {date_str} =====")
    log(f"[OUTPUT] {output_path}")

    codes = load_all_codes()
    log(f"[INFO] 전체 종목코드 수집 완료: {len(codes)}개")

    all_rows = []
    n_success = 0
    n_empty = 0
    n_error = 0

    for i, code in enumerate(codes, start=1):
        log(f"[PATCH] ({i}/{len(codes)}) {code} 1일치 수집 시도...")
        df_day, status = fetch_single_day_multi(code, date_obj)

        if status == "success" and df_day is not None and not df_day.empty:
            all_rows.append(df_day)
            n_success += 1
        elif status == "empty":
            n_empty += 1
        elif status == "error":
            n_error += 1

    log(f"[SUMMARY] 성공 종목: {n_success}, empty: {n_empty}, error: {n_error}")

    # 1개라도 성공 → 영업일로 판단 → 저장
    if n_success > 0 and all_rows:
        full = pd.concat(all_rows, ignore_index=True)
        full["Date"] = pd.to_datetime(full["Date"])
        full = full.sort_values(["Date", "Code"]).reset_index(drop=True)
        full.to_parquet(output_path)
        log(f"[SAVE] 저장 완료 → {output_path}, 행수: {len(full):,}")
        log("===== RAW 패치 V3 완료 (영업일) =====")
        return

    # 모든 종목이 empty + 오류 없음 → 휴장
    if n_success == 0 and n_error == 0:
        log("[INFO] 모든 종목 empty → 휴장일로 판단. 파일 생성 안 함.")
        log("===== RAW 패치 V3 종료 (휴장) =====")
        return

    # 모든 종목이 empty + 오류 있음 → 서버 문제
    if n_success == 0 and n_error > 0:
        log("[WARN] 데이터 없음 + 오류 다수 → 서버문제 가능성. 파일 생성/RAW 미수정.")
        log("===== RAW 패치 V3 종료 (서버 문제) =====")
        return


if __name__ == "__main__":
    main()
