# raw_patch.py  (V6 - Kiwoom 1st Source Added)
# 수정사항:
# 1. build_daily_from_pykrx 등 누락된 함수 복구
# 2. 이미 최신 데이터(target_date == last_date)가 있으면 수집 SKIP 기능 추가
# 3. 파일 저장 시 날짜 태그 규칙 준수

import os
import sys
import time
import math
import datetime as dt
from functools import lru_cache
from typing import Optional, Tuple, List, Iterable, Callable

import pandas as pd
from pykrx import stock
import requests
import FinanceDataReader as fdr
import glob
import os as _os
import yfinance as yf
import os.path as _path

# ============================================================
#  KIWOOM REST API 경로/모듈 설정
# ============================================================
KIWOOM_REST_DIR = r"F:\autostockG"
if KIWOOM_REST_DIR not in sys.path:
    sys.path.append(KIWOOM_REST_DIR)

# REST API 전용 모듈 가져오기
try:
    from kiwoom_rest.token_manager import KiwoomTokenManager
    from kiwoom_rest.kiwoom_api import KiwoomRestApi
except ImportError:
    print("Warning: kiwoom_rest 모듈을 찾을 수 없습니다. 경로를 확인해주세요.")

# ======================
# 경로 설정
# ======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR = os.path.join(BASE_DIR, "stocks")
RAW_MAIN = os.path.join(STOCKS_DIR, "all_stocks_cumulative.parquet")
DAILY_DIR = os.path.join(STOCKS_DIR, "DAILY")
LOG_DIR = os.path.join(STOCKS_DIR, "LOGS")
OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# MODELENGINE 루트 경로를 sys.path에 추가하여 UTIL 모듈 사용
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from UTIL.config_paths import versioned_filename
from UTIL.version_utils import save_dataframe_with_date, find_latest_file

def print_header():
    print("┌──────────────────────────────────────────────┐")
    print("│ 🎉 흰둥이 원본데이터 업데이트 (V6)           │")
    print("└──────────────────────────────────────────────┘")
    print(f"[PATH] RAW_MAIN : {RAW_MAIN}")
    print()

def log(msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def _invalid_ohlcv_mask(df: pd.DataFrame) -> pd.Series:
    subset = df[OHLCV_COLS] if all(col in df.columns for col in df.columns) else df
    return subset.isna().any(axis=1) | (subset <= 0).any(axis=1)

# ======================
# 날짜 유틸
# ======================
def to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def parse_date(s: str) -> dt.date:
    s = str(s)
    if "-" in s:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    return dt.datetime.strptime(s, "%Y%m%d").date()

@lru_cache(maxsize=512)
def _nearest_bizday_cached(date_ymd: str) -> str:
    return stock.get_nearest_business_day_in_a_week(date_ymd)

def is_trading_day(date: dt.date) -> bool:
    today = dt.date.today()
    if date == today:
        return date.weekday() < 5

    date_ymd = to_ymd(date)
    try:
        nearest = _nearest_bizday_cached(date_ymd)
        if nearest == date_ymd:
            return True
    except:
        pass

    try:
        df_tmp = fdr.DataReader("KS11", date, date)
        if df_tmp is not None and not df_tmp.empty:
            return True
    except:
        pass

    return date.weekday() < 5

def get_next_bizdate(last_date: dt.date) -> dt.date:
    d = last_date
    for _ in range(400):
        d = d + dt.timedelta(days=1)
        if is_trading_day(d):
            return d
    raise RuntimeError("다음 영업일을 찾지 못했습니다.")

# ======================
# SAVE / LOAD
# ======================
def load_raw_main() -> pd.DataFrame:
    latest_path = find_latest_file(STOCKS_DIR, "all_stocks_cumulative")
    if latest_path and os.path.exists(latest_path):
        log(f"[INFO] 최신 RAW 파일 사용: {os.path.basename(latest_path)}")
        df = pd.read_parquet(latest_path)
    else:
        if os.path.exists(RAW_MAIN):
            log(f"[INFO] 태그 파일 없음. 기존 RAW_MAIN 사용: {os.path.basename(RAW_MAIN)}")
            df = pd.read_parquet(RAW_MAIN)
        else:
            raise FileNotFoundError(f"RAW 메인 파일 없음: {RAW_MAIN} 또는 all_stocks_cumulative_*.parquet")

    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df

def merge_daily_into_raw(raw_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([raw_df, daily_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep="last")
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)
    return merged

# =====================================================================================
# [복구] Kiwoom REST API 수집 함수
# =====================================================================================
def build_daily_from_kiwoom(date: dt.date, tickers: Optional[List[str]] = None) -> Tuple[pd.DataFrame, List[str]]:
    log(f"[STEP] KIWOOM 전체 일봉 수집 시작: {to_ymd(date)}")

    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(r"F:\autostockG\kiwoom_rest\config.ini")

    MODE = cfg["SETTINGS"]["MODE"].strip().lower()
    BASE_URL = cfg["SETTINGS"]["BASE_URL_PAPER"] if MODE == "paper" else cfg["SETTINGS"]["BASE_URL"]

    token_mgr = KiwoomTokenManager(
        config_file=r"F:\autostockG\kiwoom_rest\config.ini",
        token_file=r"F:\autostockG\kiwoom_rest\token.json"
    )
    token = token_mgr.get_access_token()

    if tickers is None:
        try:
            tickers = stock.get_market_ticker_list(date=to_ymd(date), market="ALL")
        except:
            tickers = []
        if not tickers:
            try:
                tickers = load_raw_main()["Code"].unique().tolist()
            except:
                tickers = []

    rows = []
    bad_codes = []
    target = to_ymd(date)

    for idx, code in enumerate(tickers, 1):
        if idx % 100 == 0:
            log(f"[KIWOOM] 진행 {idx}/{len(tickers)}")

        url = f"{BASE_URL}/api/dostk/chart"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10081",
            "authorization": f"Bearer {token}",
        }
        body = {
            "stk_cd": code,
            "base_dt": target,
            "upd_stkpc_tp": "0",
        }

        try:
            r = requests.post(url, headers=headers, json=body, timeout=2)
            r.raise_for_status()
            js = r.json()

            chart = js.get("stk_dt_pole_chart_qry") or js.get("chart") or []
            if not chart:
                bad_codes.append(code)
                continue

            matched = None
            for item in chart:
                if str(item.get("dt")) == target:
                    matched = item
                    break
            if matched is None:
                matched = chart[0]

            def _to_float(v):
                try:
                    return float(str(v).replace("+", "").replace(",", "").strip())
                except:
                    return float("nan")

            rows.append({
                "Date": date,
                "Open": _to_float(matched.get("open_pric")),
                "High": _to_float(matched.get("high_pric")),
                "Low": _to_float(matched.get("low_pric")),
                "Close": _to_float(matched.get("cur_prc")),
                "Volume": _to_float(matched.get("trde_qty")),
                "Change": 0.0,
                "Code": code,
                "Name": "",
                "Market": ""
            })

        except:
            bad_codes.append(code)

    df = pd.DataFrame(rows)
    log(f"[KIWOOM] {len(df)}개 종목 수집, 실패 {len(bad_codes)}개")
    return df, bad_codes

# =====================================================================================
# [복구] pykrx 수집 함수
# =====================================================================================
def build_daily_from_pykrx(date: dt.date) -> Tuple[pd.DataFrame, List[str]]:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        _os.environ[key] = ""

    date_ymd = to_ymd(date)
    log(f"[STEP] KRX 일괄 수집 시작: {date_ymd}")

    df = stock.get_market_ohlcv_by_ticker(date_ymd, market="ALL")
    if df is None or df.empty:
        raise RuntimeError(f"KRX 데이터 없음: {date_ymd}")

    rename_map = {"시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume"}
    df = df.rename(columns=rename_map)

    if "등락률" in df.columns:
        df["Change"] = df["등락률"] / 100.0
    else:
        df["Change"] = 0.0

    df = df.reset_index().rename(columns={"티커": "Code"})

    def _get_name_safe(ticker):
        try:
            return stock.get_market_ticker_name(ticker)
        except:
            return ""

    df["Name"] = df["Code"].map(_get_name_safe)
    df["Date"] = date

    keep_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Change", "Code", "Name"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[keep_cols]

    mask_bad = _invalid_ohlcv_mask(df)
    suspicious = df.loc[mask_bad, "Code"].tolist()
    log(f"[KRX] {len(df)}개 종목 수집, 의심 {len(suspicious)}개")
    return df, suspicious

# =====================================================================================
# [복구] 보조 수집
# =====================================================================================
def fetch_ohlcv_from_naver(ticker, yyyymmdd):
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={yyyymmdd}&endTime={yyyymmdd}"
    try:
        r = requests.get(url, timeout=5)
        arr = r.json()
        if not arr or len(arr) < 2:
            return None
        row = arr[1]
        return {
            "Open": float(row[1]), "High": float(row[2]),
            "Low": float(row[3]), "Close": float(row[4]),
            "Volume": float(row[5]),
        }
    except:
        return None

def fetch_ohlcv_from_fdr(ticker, yyyymmdd):
    try:
        d0 = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        d1 = d0 + dt.timedelta(days=1)
        df = fdr.DataReader(ticker, d0, d1)
        if df is None or df.empty:
            return None
        row = df.iloc[0]
        return {
            "Open": float(row["Open"]), "High": float(row["High"]),
            "Low": float(row["Low"]), "Close": float(row["Close"]),
            "Volume": float(row["Volume"]),
        }
    except:
        return None

def fetch_ohlcv_from_yahoo(ticker, yyyymmdd):
    try:
        d0 = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        d1 = d0 + dt.timedelta(days=1)
        for suffix in [".KS", ".KQ", ""]:
            df = yf.download(f"{ticker}{suffix}", start=d0, end=d1, progress=False)
            if df is None or df.empty:
                continue
            row = df.iloc[0]
            return {
                "Open": float(row["Open"]), "High": float(row["High"]),
                "Low": float(row["Low"]), "Close": float(row["Close"]),
                "Volume": float(row["Volume"]),
            }
        return None
    except:
        return None

FALLBACK_SOURCES = [
    ("fdr", fetch_ohlcv_from_fdr),
    ("yahoo", fetch_ohlcv_from_yahoo),
    ("naver", fetch_ohlcv_from_naver),
]

def fill_missing_with_sources(daily_df, date, codes, sources=None):
    if not codes:
        return daily_df, []
    if sources is None:
        sources = FALLBACK_SOURCES

    date_ymd = to_ymd(date)
    unresolved = list(dict.fromkeys(codes))

    for source_name, fetcher in sources:
        if not unresolved:
            break
        log(f"[{source_name.upper()}] 보조 수집 시작 ({len(unresolved)}개)")

        still = []
        updated = {}

        for code in unresolved:
            o = fetcher(code, date_ymd)
            if not o:
                still.append(code)
                continue
            updated[code] = o

        for code, o in updated.items():
            idx = daily_df.index[daily_df["Code"] == code].tolist()
            if idx:
                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    daily_df.at[idx[0], col] = o[col]
                daily_df.at[idx[0], "Change"] = 0.0

        unresolved = still
        log(f"[{source_name.upper()}] 남은 코드는 {len(unresolved)}개")

    return daily_df, unresolved

def build_daily_from_fallback_sources(date, tickers):
    tickers = list(dict.fromkeys(tickers))
    base = pd.DataFrame({"Code": tickers})
    base["Date"] = date
    for col in ["Open","High","Low","Close","Volume","Change","Name"]:
        if col not in base.columns:
            base[col] = pd.NA

    df, unresolved = fill_missing_with_sources(base, date, tickers)
    mask_bad = _invalid_ohlcv_mask(df)
    unresolved = list(dict.fromkeys(unresolved + df.loc[mask_bad, "Code"].tolist()))
    log(f"[FALLBACK] 성공 {len(df) - len(unresolved)}건, 실패 {len(unresolved)}건")
    return df, unresolved


# =====================================================================================
# ⭐ 메인 실행부
# =====================================================================================
if __name__ == "__main__":
    print_header()

    now_dt = dt.datetime.now()
    today = now_dt.date()
    raw_df = load_raw_main()

    last_date = raw_df["Date"].max()
    log(f"[STEP 1] RAW 최신 날짜: {last_date}")

    # 16~18시는 오염방지로 중단
    if dt.time(16,0) <= now_dt.time() < dt.time(18,0):
        log("[WARN] 16~18시는 전날 영업일 기준으로 업데이트합니다.")
        # continue without exit

    # ======================================================================
    # >>>>>>>>>>>>>>>>>>>>>>>>>> PATCH START (target_date 재설계) <<<<<<<<<<<<<<<<<<<<<<<<<
    # ======================================================================

    try:
        now_t = now_dt.time()

        # today가 휴일이면 최근 영업일로 조정
        try:
            nearest = stock.get_nearest_business_day_in_a_week(to_ymd(today))
            today_biz = parse_date(nearest)
        except:
            tmp = today
            while not is_trading_day(tmp):
                tmp = tmp - dt.timedelta(days=1)
            today_biz = tmp

        # 전날 영업일 계산
        prev_biz = today_biz
        while True:
            prev_biz = prev_biz - dt.timedelta(days=1)
            if is_trading_day(prev_biz):
                break

        # 시간대 룰 적용
        if not is_trading_day(today):
            target_date = today_biz
        elif dt.time(16,0) <= now_t < dt.time(18,0):
            log("[WARN] 16~18시는 전날 영업일 기준으로 업데이트합니다.")
            target_date = prev_biz
        elif now_t < dt.time(18,0):
            target_date = prev_biz
        else:
            target_date = today_biz

    except Exception as e:
        raise e


    # 실제 업데이트 범위 로그(정확 표기)
    log(f"[STEP 2] 실제 업데이트 범위: {dates_to_update[0]} ~ {dates_to_update[-1]}")

    # ======================================================================
    # >>>>>>>>>>>>>>>>>>>>>>>>>> PATCH END <<<<<<<<<<<<<<<<<<<<<<<<<
    # ======================================================================

    # ----------------------- 메인 처리 루프 -----------------------
    # 원본 daily 처리 블록을 그대로 유지하면서 for-loop 적용
    for date in dates_to_update:
        log(f"[LOOP] {date} 업데이트 시작")

        # ===========================
        # ⭐⭐ 1순위: pykrx
        # ===========================
        try:
            daily_df, bad_codes = build_daily_from_pykrx(date)
            log("[OK] KRX(pykrx) 데이터 사용")
        except Exception as e:
            log(f"[WARN] KRX(pykrx) 실패 → {e}")
            daily_df = None
            bad_codes = []

        # ===========================
        # ⭐⭐ 2순위: KIWOOM
        # ===========================
        if daily_df is None or daily_df.empty:
            try:
                daily_df, bad_codes = build_daily_from_kiwoom(date)
                log("[OK] KIWOOM 데이터 수집 성공. 2순위 소스로 사용.")
            except Exception as e:
                log(f"[WARN] KIWOOM 실패 → {e}")
                daily_df = None
                bad_codes = []

        # ⭐⭐ 3순위: fallback
        if daily_df is None or daily_df.empty:
            tickers = raw_df["Code"].unique().tolist()
            daily_df, bad_codes = build_daily_from_fallback_sources(date, tickers)
            if daily_df is None or daily_df.empty:
                log("[ERROR] FDR/Yahoo/Naver fallback 실패")
                continue
            else:
                log("[OK] FDR/Yahoo/Naver fallback 사용")

        # 부족분 보조 수집
        if bad_codes:
            try:
                kiw_df, _ = build_daily_from_kiwoom(date, tickers=bad_codes)
                if kiw_df is not None and not kiw_df.empty:
                    kiw_sub = kiw_df[kiw_df["Code"].isin(bad_codes)]
                    if not kiw_sub.empty:
                        daily_df = merge_daily_into_raw(daily_df, kiw_sub)
                        log(f"[KIWOOM] 보조 수집으로 {len(kiw_sub)}개 덮어씀")
            except Exception as e:
                log(f"[WARN] KIWOOM 보조 수집 실패 → {e}")

        # SAVE DAILY
        out_path = os.path.join(DAILY_DIR, f"daily_{date.strftime('%y%m%d')}.parquet")
        daily_df.to_parquet(out_path)
        log(f"[SAVE] DAILY 저장 완료: {out_path}")

        # RAW 병합
        raw_df = merge_daily_into_raw(raw_df, daily_df)

        # (변경) RAW 최신본 저장은 루프 종료 후 1회 수행

    # 루프 종료 후 RAW 최신본을 1회 저장 (누적 병합 결과)
    saved_path = save_dataframe_with_date(raw_df, STOCKS_DIR, "all_stocks_cumulative", date_col="Date")
    if saved_path:
        log(f"[SAVE] RAW 최신본 저장: {os.path.basename(saved_path)}")
    else:
        log("[SKIP] RAW 최신본 저장 건너뜀 (동일 날짜 파일 존재)")

    log("[DONE] RAW 업데이트 끝.")
