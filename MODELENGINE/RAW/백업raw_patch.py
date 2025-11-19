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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# [경로 설정] RAW 파일을 stocks 폴더 내에 저장 (요청에 따라 수정됨)
STOCKS_DIR = os.path.join(BASE_DIR, "stocks")
RAW_MAIN = os.path.join(STOCKS_DIR, "all_stocks_cumulative.parquet")

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

def load_all_codes() -> List[str]:
    """KOSPI + KOSDAQ 전체 종목코드 수집 (Naver + KRX)"""
    codes = set()
    urls = [
        "https://api.stock.naver.com/marketindex/marketStock/KOSPI",
        "https://api.stock.naver.com/marketindex/marketStock/KOSDAQ",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                js = r.json()
                stocks = js.get("stocks", [])
                for s in stocks:
                    c = str(s.get("code", "")).strip()
                    if len(c) == 6 and c.isdigit():
                        codes.add(c)
        except Exception:
            pass

    if HAS_KRX:
        try:
            df_kospi = krx_stock.get_market_ticker_list(market="KOSPI")
            df_kosdaq = krx_stock.get_market_ticker_list(market="KOSDAQ")
            for c in list(df_kospi) + list(df_kosdaq):
                c = str(c).zfill(6)
                if len(c) == 6 and c.isdigit():
                    codes.add(c)
        except Exception:
            pass

    codes = sorted(list(codes))
    log(f"[INFO] 전체 종목코드 수집 완료: {len(codes)}개")
    return codes

# ==========================================================
# 3-1. 3단계 Fallback 수집 함수 (fetch_from_...)
# ==========================================================

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
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200: return pd.DataFrame()
        js = r.json()
        rows = js.get("chart", {}).get("result", [])
        if not rows: return pd.DataFrame()

        data = []
        for d in rows:
            dt = d.get("date")
            if dt is None: continue
            if isinstance(dt, str) and len(dt) == 8 and dt.isdigit(): dt = f"{dt[0:4]}-{dt[4:6]}-{dt[6:8]}"
            data.append([dt, code, d.get("open", 0), d.get("high", 0), d.get("low", 0), d.get("close", 0), d.get("volume", 0)])

        df = pd.DataFrame(data, columns=["Date", "Code", "Open", "High", "Low", "Close", "Volume"])
        df["Date"] = pd.to_datetime(df["Date"])
        df = df[(df["Date"] >= pd.to_datetime(start)) & (df["Date"] <= pd.to_datetime(end))]

        if df.empty: return pd.DataFrame()
        log(f"  [NAVER] {code} 성공.")
        return df

    except Exception:
        return pd.DataFrame()


def fetch_from_krx(code: str, start: str, end: str) -> pd.DataFrame:
    """3차: KRX (pykrx) 수집"""
    if not HAS_KRX: return pd.DataFrame()

    try:
        s = start.replace("-", "")
        e = end.replace("-", "")
        df = krx_stock.get_market_ohlcv_by_date(s, e, code)
        if df is None or df.empty: return pd.DataFrame()

        df = df.reset_index()
        df["Code"] = code
        df = df.rename(columns={"날짜": "Date", "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"})
        df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]
        df["Date"] = pd.to_datetime(df["Date"])

        log(f"  [KRX] {code} 성공.")
        return df

    except Exception:
        return pd.DataFrame()


def fetch_ohlcv_multi_source(code: str, start: str, end: str, 
                             fail_log: List[str], fallback_log: List[str], krx_log: List[str]) -> pd.DataFrame:
    """3단계 fallback 포함한 통합 OHLCV 수집"""

    # 1) Yahoo
    df = fetch_from_yahoo(code, start, end)
    if not df.empty: return df

    # 2) Naver
    fallback_log.append(code)
    df = fetch_from_naver(code, start, end)
    if not df.empty: return df

    # 3) KRX
    krx_log.append(code)
    df = fetch_from_krx(code, start, end)
    if not df.empty: return df

    log(f"  [FAIL] {code} 3단계 모두 실패")
    fail_log.append(code)
    return pd.DataFrame()


# ==========================================================
# 4. 데이터 안정화 로직 (safe_raw_patch_v3.py 통합)
# ==========================================================

def normalize_numeric_series(val):
    """숫자 컬럼 안정화"""
    if val is None: return pd.Series([pd.NA])
    if isinstance(val, pd.Series): return pd.to_numeric(val, errors="coerce")
    if isinstance(val, np.ndarray): val = val.flatten()
    if isinstance(val, (int, float, str)): val = [val]
    if isinstance(val, (list, tuple)): return pd.to_numeric(pd.Series(val), errors="coerce")
    return pd.to_numeric(pd.Series([val]), errors="coerce")


def fetch_single_day_multi(code: str, date_obj: date):
    """1일치 OHLCV만 수집하여 DataFrame 반환."""
    date_str = date_obj.strftime("%Y-%m-%d")
    start = date_str
    end = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    fail_log, fb_log, krx_log = [], [], []

    df_full = fetch_ohlcv_multi_source(code, start, end, fail_log, fb_log, krx_log)

    if df_full is None or df_full.empty: return None, "empty"
    
    df_full["Date"] = pd.to_datetime(df_full["Date"])
    df_day = df_full[df_full["Date"].dt.date == date_obj].copy()
    
    if df_day.empty: return None, "empty"

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df_day[col] = normalize_numeric_series(df_day[col])

    df_day["Code"] = df_day["Code"].astype(str).str.zfill(6)
    
    return df_day, "success"


# ==========================================================
# 5. 메인 자동 업데이트 및 병합 함수 (RAW_PATCH 최종 로직)
# ==========================================================

def get_latest_raw_date(raw_path: str) -> Optional[date]:
    """메인 RAW 파일에서 가장 최신 날짜를 추출합니다."""
    if not os.path.exists(raw_path):
        return None
    try:
        df = pd.read_parquet(raw_path, columns=["Date"])
        df["Date"] = pd.to_datetime(df["Date"])
        return df["Date"].max().date()
    except Exception as e:
        log(f"[ERROR] RAW 파일({raw_path}) 읽기 실패: {e}")
        return None


# raw_patch_final.py 파일의 5번 섹션 (auto_update_raw 함수)

def auto_update_raw():
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

    # 2. 수집할 날짜 목록 생성 (최신 날짜의 다음 날부터 오늘까지)
    start_date_to_fetch = latest_date + timedelta(days=1)
    today = datetime.now().date()
    
    fetch_dates = []
    current_date = start_date_to_fetch
    while current_date < today:
        
        # ⬇️⬇️ [주말 건너뛰기 로직 통합] ⬇️⬇️
        if current_date.weekday() < 5: # 월(0) ~ 금(4)만 수집 대상에 포함
            fetch_dates.append(current_date)
        else:
            log(f"[SKIP] {current_date.strftime('%Y-%m-%d')} 주말이므로 건너뜁니다.")
        # ⬆️⬆️ [주말 건너뛰기 로직 통합] ⬆️⬆️
        
        current_date += timedelta(days=1)

    if not fetch_dates:
        log("[INFO] 업데이트할 새로운 날짜가 없습니다. 작업 종료.")
        return

    log(f"[INFO] 수집할 날짜 범위: {fetch_dates[0]} ~ {fetch_dates[-1]} ({len(fetch_dates)}일)")

    # 3. 데이터 수집 및 병합 준비 (이하 코드 동일)
    codes = load_all_codes()
    all_new_data = []
    
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
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged["Code"] = merged["Code"].astype(str).str.zfill(6)
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep='last')
    
    merged = merged.dropna(subset=["Date", "Code"])
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)

    # 6. 최종 저장
    merged.to_parquet(RAW_MAIN)
    log(f"\n🎉 [완료] RAW 최종 업데이트 완료.")
    log(f"       경로: {RAW_MAIN}")
    log(f"       최신 날짜: {merged['Date'].max().date()}, 총 행수: {len(merged):,}")
    log("===== RAW_PATCH.PY: 자동 업데이트 완료 =====")


if __name__ == "__main__":
    auto_update_raw()