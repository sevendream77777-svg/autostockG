# raw_patch_final.py (최종 통합 완성본)
# 역할: RAW 데이터의 최신 날짜 확인, 데이터 수집, 중복 제거 및 병합을 자동 수행합니다.

import os
import sys
import time
from datetime import datetime, timedelta, date
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import numpy as np

# --- 외부 라이브러리 (필수) ---
# pip install yfinance requests pykrx pandas numpy 가 필요합니다.
import requests
import yfinance as yf

try:
    from pykrx import stock as krx_stock
    HAS_KRX = True
except Exception:
    HAS_KRX = False

# ---------------------------------------------------------
# 1. 경로 설정 및 공통 변수
# ---------------------------------------------------------

RAW_MAIN = r"F:\autostockG\MODELENGINE\RAW\stocks\all_stocks_cumulative.parquet"
STOCKS_DIR = os.path.dirname(RAW_MAIN)

START_DATE = "2015-01-01"

# ---------------------------------------------------------
# 2. 로깅 함수
# ---------------------------------------------------------

def log(msg: str):
    """콘솔 로그용"""
    print(msg, flush=True)

# ---------------------------------------------------------
# 3. 데이터 수집 엔진 (Full Integration)
# ---------------------------------------------------------

def fetch_from_yahoo(code: str, start: str, end: str) -> pd.DataFrame:
    """1차: Yahoo Finance 수집"""
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            if df is not None and not df.empty:
                df = df.reset_index()
                df["Code"] = code
                df = df.rename(columns={
                    "Date": "Date", "Open": "Open", "High": "High",
                    "Low": "Low", "Close": "Close", "Volume": "Volume"
                })
                df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]
                df["Date"] = pd.to_datetime(df["Date"])
                log(f"  [YAHOO] {code} 성공.")
                return df
        except Exception:
            pass
    return pd.DataFrame()


def fetch_from_naver(code: str, start: str, end: str) -> pd.DataFrame:
    """2차: Naver 차트 API 수집"""
    url = f"https://api.stock.naver.com/stock/{code}/chart"
    params = {"period": "DAY", "count": "4000"} 
    try:
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code != 200:
            return pd.DataFrame()

        data = resp.json()
        if "chart" not in data or "tradePrice" not in data["chart"]:
            return pd.DataFrame()

        dates = data["chart"]["time"]
        opens = data["chart"]["openingPrice"]
        highs = data["chart"]["highPrice"]
        lows = data["chart"]["lowPrice"]
        closes = data["chart"]["tradePrice"]
        volumes = data["chart"]["candleAccTradeVolume"]

        df = pd.DataFrame({
            "Date": pd.to_datetime(dates).date,
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": volumes,
        })
        df["Code"] = code
        df["Date"] = pd.to_datetime(df["Date"])
        df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

        mask = (df["Date"] >= pd.to_datetime(start)) & (df["Date"] < pd.to_datetime(end))
        df = df.loc[mask].copy()

        if not df.empty:
            log(f"  [NAVER] {code} 성공.")
            return df
    except Exception:
        pass

    return pd.DataFrame()


def fetch_from_krx(code: str, start: str, end: str) -> pd.DataFrame:
    """3차: pykrx 사용한 KRX 수집"""
    if not HAS_KRX:
        return pd.DataFrame()

    try:
        start_krx = start.replace("-", "")
        end_krx = (pd.to_datetime(end) - pd.Timedelta(days=1)).strftime("%Y%m%d")

        df = krx_stock.get_market_ohlcv_by_date(start_krx, end_krx, code)
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        df = df.rename(columns={
            "날짜": "Date", "시가": "Open", "고가": "High",
            "저가": "Low", "종가": "Close", "거래량": "Volume"
        })
        df["Code"] = code
        df["Date"] = pd.to_datetime(df["Date"])
        df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

        mask = (df["Date"] >= pd.to_datetime(start)) & (df["Date"] < pd.to_datetime(end))
        df = df.loc[mask].copy()

        if not df.empty:
            log(f"  [KRX] {code} 성공.")
            return df
    except Exception:
        pass

    return pd.DataFrame()


def fetch_ohlcv_multi_source(code: str, start: str, end: str,
                             fail_log: list,
                             fb_log: list,
                             krx_log: list) -> pd.DataFrame:
    """
    3단계 Fallback: Yahoo → Naver → KRX 순으로 시도.
    """
    df = fetch_from_yahoo(code, start, end)
    if df is not None and not df.empty:
        return df

    fb_log.append(code)
    df = fetch_from_naver(code, start, end)
    if df is not None and not df.empty:
        return df

    krx_log.append(code)
    df = fetch_from_krx(code, start, end)
    if df is not None and not df.empty:
        return df

    fail_log.append(code)
    return pd.DataFrame()

# ---------------------------------------------------------
# 4. 데이터 안정화 로직 (safe_raw_patch_v3 기반)
# ---------------------------------------------------------

def normalize_numeric_series(val):
    """숫자 컬럼 안정화"""
    if val is None:
        return pd.Series([pd.NA])
    if isinstance(val, pd.Series):
        return pd.to_numeric(val, errors="coerce")
    if isinstance(val, np.ndarray):
        val = val.flatten()
    if isinstance(val, (int, float, str)):
        val = [val]
    if isinstance(val, (list, tuple)):
        return pd.to_numeric(pd.Series(val), errors="coerce")
    return pd.to_numeric(pd.Series([val]), errors="coerce")


def fetch_single_day_multi(code: str, date_obj: date):
    """1일치 OHLCV만 수집하여 DataFrame 반환."""
    date_str = date_obj.strftime("%Y-%m-%d")
    start = date_str
    end = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    fail_log, fb_log, krx_log = [], [], []
    df_full = fetch_ohlcv_multi_source(code, start, end, fail_log, fb_log, krx_log)

    if df_full is None or df_full.empty:
        return None, "empty"

    df_full["Date"] = pd.to_datetime(df_full["Date"])
    df_day = df_full[df_full["Date"].dt.date == date_obj]

    if df_day.empty:
        return None, "empty"

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df_day.columns:
            df_day[col] = normalize_numeric_series(df_day[col]).values

    df_day = df_day.dropna(subset=["Open", "High", "Low", "Close"])
    if df_day.empty:
        return None, "empty"

    return df_day, "success"

# ---------------------------------------------------------
# 5. RAW 최신 날짜 / 대상 날짜 계산
# ---------------------------------------------------------

def get_latest_raw_date(raw_path: str) -> Optional[date]:
    """RAW 파일에서 Date 컬럼의 최댓값(최신 날짜)을 반환"""
    if not os.path.exists(raw_path):
        return None
    df = pd.read_parquet(raw_path, columns=["Date"])
    if df.empty:
        return None
    return pd.to_datetime(df["Date"].max()).date()


def generate_missing_dates(latest: date, end_date: date) -> list:
    """
    latest+1일부터 end_date까지 중 영업일 후보들을 생성.
    (주말은 1차 필터에서 제외)
    """
    dates = []
    curr = latest + timedelta(days=1)
    while curr <= end_date:
        # 1차 필터: 토, 일 제외
        if curr.weekday() < 5:
            dates.append(curr)
        curr += timedelta(days=1)
    return dates

# ---------------------------------------------------------
# 6. 전체 RAW 자동 업데이트 로직
# ---------------------------------------------------------

def auto_update_raw(target_end: Optional[str] = None):
    """
    RAW_MAIN 기준으로 최신 날짜 이후의 데이터를 자동 수집/병합.

    1) RAW_MAIN에서 최신 날짜 읽음
    2) 최신 날짜 + 1일 ~ target_end (또는 오늘) 까지 날짜 목록 생성
    3) 각 날짜별로 모든 종목에 대해 하루치 데이터 수집
    4) 수집된 데이터만 기존 RAW와 병합
    """

    log("===== RAW_PATCH.PY: 자동 업데이트 및 병합 시작 =====")

    # [폴더 생성] stocks 폴더가 없으면 만듭니다.
    os.makedirs(STOCKS_DIR, exist_ok=True)

    # 1. RAW 파일 존재 확인 및 최신 날짜 확인
    if not os.path.exists(RAW_MAIN):
        log(f"[WARN] 메인 RAW 파일 없음 ({RAW_MAIN}). 전체 RAW 구축이 먼저 필요합니다.")
        return

    latest_date = get_latest_raw_date(RAW_MAIN)
    if latest_date is None:
        log("[FATAL] RAW 파일이 비었거나 날짜를 찾을 수 없습니다. 작업 종료.")
        return

    log(f"[INFO] 현재 RAW 최신 날짜: {latest_date}")

    # 2. target_end 날짜 결정 (기본: 어제까지)
    if target_end is None:
        today = date.today()
        target_end_date = today - timedelta(days=1)
    else:
        target_end_date = pd.to_datetime(target_end).date()

    if target_end_date <= latest_date:
        log(f"[INFO] 이미 최신입니다. (목표: {target_end_date}, 현재: {latest_date})")
        return

    fetch_dates = generate_missing_dates(latest_date, target_end_date)
    if not fetch_dates:
        log("[INFO] 수집할 추가 날짜가 없습니다.")
        return

    log(f"[INFO] 신규 수집 대상 날짜 수: {len(fetch_dates)}")
    log(f"      {fetch_dates[0]} ~ {fetch_dates[-1]}")

    # 3. 종목 코드 리스트 확보 (RAW 파일에서)
    df_main = pd.read_parquet(RAW_MAIN, columns=["Date", "Code"])
    codes = sorted(df_main["Code"].unique())
    log(f"[INFO] 종목 수: {len(codes)}개")

    all_new_data = []

    # 3-1. 날짜별로 반복 수집
    for date_obj in fetch_dates:
        # ... (이하 수집 로직은 동일) ...
        log(f"\n[FETCH] 날짜: {date_obj.strftime('%Y-%m-%d')} 데이터 수집 시작...")
        all_rows_for_day = []
        n_success = 0

        for code in codes:
            df_day, status = fetch_single_day_multi(code, date_obj)

            if status == "success" and df_day is not None and not df_day.empty:
                all_rows_for_day.append(df_day)
                n_success += 1

        if n_success > 0 and all_rows_for_day:
            full_day_df = pd.concat(all_rows_for_day, ignore_index=True)
            log(f"[SUCCESS] {date_obj.strftime('%Y-%m-%d')}: {n_success}개 종목 데이터 수집 성공.")
            all_new_data.append(full_day_df)
        elif n_success == 0:
            log(f"[INFO] {date_obj.strftime('%Y-%m-%d')}: 거래일 아님 또는 수집 실패로 건너뜀.")

    if not all_new_data:
        log("[INFO] 수집된 새로운 데이터가 없습니다. 작업 종료.")
        return

    # 4. 기존 RAW 로드 및 새로운 데이터와 병합
    df_main = pd.read_parquet(RAW_MAIN)
    frames = [df_main] + all_new_data

    merged = pd.concat(frames, ignore_index=True)

    # 5. 중복 제거 및 최종 정리
    if "Date" not in merged.columns or "Code" not in merged.columns:
        log("[FATAL] 병합 결과에 Date/Code 컬럼이 없습니다. 작업 종료.")
        return

    merged["Date"] = pd.to_datetime(merged["Date"])
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep="last")
    merged = merged.dropna(subset=["Date", "Code"])
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)

    # 6. 최종 저장
    merged.to_parquet(RAW_MAIN)
    log("\n🎉 [완료] RAW 최종 업데이트 완료.")
    log(f"       경로: {RAW_MAIN}")
    log(f"       최신 날짜: {merged['Date'].max().date()}, 총 행수: {len(merged):,}")
    log("===== RAW_PATCH.PY: 자동 업데이트 완료 =====")


if __name__ == "__main__":
    # 예시: python raw_patch.py  → 어제까지 자동 업데이트
    #       python raw_patch.py 2025-11-18  → 지정 날짜까지 업데이트
    if len(sys.argv) > 1:
        auto_update_raw(sys.argv[1])
    else:
        auto_update_raw()
