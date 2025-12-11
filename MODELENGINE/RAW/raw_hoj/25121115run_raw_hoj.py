
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v48_nodart_36cols_safe.py — FINAL (No-DART) — 36 General Columns Collector
- 재무(DART) 관련 코드 전부 제거
- 36개 일반 컬럼만 수집 (가격/메타/수급/매크로/이벤트)
- 무결성 우선: ffill 안전화, yfinance 보정(^TNX/접미사), 품질가드 확장
- 메타 필드(pykrx) 보강, 영업일 커버리지 점검
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import pickle
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
import threading
import random
import ast

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# 외부 패키지
from pykrx import stock  # type: ignore
try:
    import yfinance as yf
except ImportError:
    yf = None

# --------------------------------------------------------------------------- #
# 경로 및 공용 설정
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[0]
CACHE_DIR = ROOT / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# 세션 & 리트라이
# --------------------------------------------------------------------------- #
SESSION = requests.Session()
try:
    from urllib3.util.retry import Retry  # type: ignore
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    SESSION.mount("http://", adapter)
    SESSION.mount("https://", adapter)
except Exception:
    pass

# --------------------------------------------------------------------------- #
# 간단 레이트 리밋
# --------------------------------------------------------------------------- #
_RATE_LOCK = threading.Lock()
_LAST_CALL: Dict[str, float] = {}
_MIN_INTERVAL = {"naver": float(os.environ.get("RATE_LIMIT_NAVER", "0.3"))}
_RATE_FILES = {"naver": CACHE_DIR / ".rate_naver"}

def _throttle(domain: str):
    interval = _MIN_INTERVAL.get(domain)
    if not interval:
        return
    rate_file = _RATE_FILES.get(domain)
    if rate_file:
        rate_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            if rate_file.exists():
                last_str = rate_file.read_text(encoding="utf-8").strip()
                try:
                    last = float(last_str)
                except Exception:
                    last = 0.0
            else:
                last = 0.0
            now = time.time()
            wait = interval - (now - last)
            if wait > 0:
                time.sleep(wait)
            rate_file.write_text(str(time.time()), encoding="utf-8")
        except Exception:
            pass
        return
    with _RATE_LOCK:
        last = _LAST_CALL.get(domain, 0.0)
        now = time.time()
        wait = interval - (now - last)
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[domain] = time.time()

# --------------------------------------------------------------------------- #
# 프록시 설정 (옵션)
# --------------------------------------------------------------------------- #
_PROXY_CONFIG: Optional[Dict[str, str]] = None
def set_proxy(proxy: Optional[str]):
    global _PROXY_CONFIG
    if proxy:
        _PROXY_CONFIG = {"http": proxy, "https": proxy}
        try:
            SESSION.proxies.update(_PROXY_CONFIG)
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
        except Exception:
            pass
    else:
        _PROXY_CONFIG = None
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
def get_proxy() -> Optional[Dict[str, str]]:
    return _PROXY_CONFIG

# --------------------------------------------------------------------------- #
# 안전 변환/요청 유틸
# --------------------------------------------------------------------------- #
def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except Exception:
        return None

def safe_request(method: str, url: str, **kwargs) -> requests.Response:
    proxy = get_proxy()
    if proxy:
        kwargs.setdefault("proxies", proxy)
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("User-Agent", "Mozilla/5.0 (raw_v48 nodart safe)")
    headers.setdefault("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    kwargs["headers"] = headers
    kwargs.setdefault("timeout", 10)
    if "finance.naver.com" in url:
        _throttle("naver")
    tries = 4
    for attempt in range(tries):
        try:
            resp = SESSION.request(method, url, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504, 403):
                if attempt == tries - 1:
                    return resp
                sleep = 0.5 * (2 ** attempt) + random.uniform(0, 0.3)
                time.sleep(sleep)
                continue
            return resp
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(0.5 * (2 ** attempt) + random.uniform(0, 0.3))

# --------------------------------------------------------------------------- #
# 스키마(36개) — 재무 제거
# --------------------------------------------------------------------------- #
V36_COLS: List[str] = [
    # 1) 기본 정보 (7)
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    # 2) 가격 & 상태 (10)
    "open", "high", "low", "close", "adjust_close", "volume", "amount", "adj_factor", "shares_out", "vwap",
    # 3) 거래정지/상하한 (2)
    "is_trading_suspended", "is_limit_reached",
    # 4) 수급/지분 (8)
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty",
    "frgn_hold_ratio",
    # 5) 매크로/이벤트 (9)
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix",
    "earnings_date",
]

# --------------------------------------------------------------------------- #
# 매크로 설정
# --------------------------------------------------------------------------- #
ECOS_API = "https://ecos.bok.or.kr/api/StatisticSearch"
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_KEY = "guest"

ECOS_CODES = {
    "usdkrw": ("036Y001", "M", None),
    "cnykrw": ("036Y015", "M", None),
    "kr10y_yield": ("817Y002", "D", "010210000"),
}
FRED_CODES = {
    "us10y_yield": "DGS10",
    "dxy": "DTWEXBGS",
    "wti": "DCOILWTICO",
    "gold": "GOLDAMGBD228NLBM",
    "vix": "VIXCLS",
}
YFINANCE_TICKERS = {
    "us10y_yield": "^TNX",  # /10 보정 필요
    "wti": "CL=F",
    "gold": "GC=F",
    "dxy": "DX-Y.NYB",
    "vix": "^VIX",
}

_MACRO_CACHE: Dict[str, pd.DataFrame] = {}

def _cache_read_df(path: Path) -> Optional[pd.DataFrame]:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            pass
    return None

def load_macro_yfinance(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    global _MACRO_CACHE
    cache_key = f"yf_{key}_{start_date}_{end_date}"
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key]
    if yf is None or key not in YFINANCE_TICKERS:
        return pd.DataFrame()

    cache_path = CACHE_DIR / f"yf_{key}.parquet"
    df = _cache_read_df(cache_path)
    if df is not None:
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        if key == "us10y_yield":
            out[key] = out[key] / 10.0
        _MACRO_CACHE[cache_key] = out
        return out

    try:
        data = yf.Ticker(YFINANCE_TICKERS[key])
        hist = data.history(period="11y")
        if hist.empty:
            return pd.DataFrame()
        df = hist[["Close"]].reset_index()
        df.columns = ["Date", key]
        df["Date"] = df["Date"].dt.strftime("%Y%m%d")
        if key == "us10y_yield":
            df[key] = df[key] / 10.0
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        _MACRO_CACHE[cache_key] = out
        return out
    except Exception:
        return pd.DataFrame()

def read_ecos_key() -> str:
    paths = [
        ROOT / "ecos_apikey.txt",
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\ecos_open_api.txt"),
    ]
    for p in paths:
        if p.exists():
            try:
                return p.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    return "5J2U4P96R1QKWL5F0Y7C"

def load_macro_ecos(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    global _MACRO_CACHE
    cache_key = f"ecos_{key}_{start_date}_{end_date}"
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key]

    if key not in ECOS_CODES:
        return pd.DataFrame()
    code, freq, item_code = ECOS_CODES[key]
    if freq == "M":
        start = start_date[:6]
        end = end_date[:6]
    else:
        start = start_date
        end = end_date

    cache_path = CACHE_DIR / f"ecos_{key}.parquet"
    df = _cache_read_df(cache_path)
    if df is not None:
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        _MACRO_CACHE[cache_key] = out
        return out

    url = f"{ECOS_API}/{read_ecos_key()}/json/kr/1/10000/{code}/{freq}/{start}/{end}"
    try:
        r = safe_request("get", url, timeout=30)
        rows = r.json().get("StatisticSearch", {}).get("row", [])
        data = []
        for it in rows:
            if item_code and it.get("ITEM_CODE1") != item_code:
                continue
            tm = it.get("TIME", "")
            val = safe_float(it.get("DATA_VALUE"))
            if tm and val is not None:
                if freq == "M" and len(tm) == 6:
                    tm = tm + "01"
                data.append((tm.replace("-", ""), val))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data, columns=["Date", key])
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        _MACRO_CACHE[cache_key] = out
        return out
    except Exception:
        return pd.DataFrame()

def load_macro_fred(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    global _MACRO_CACHE
    cache_key = f"fred_{key}_{start_date}_{end_date}"
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key]
    if key not in FRED_CODES:
        return pd.DataFrame()

    cache_path = CACHE_DIR / f"fred_{key}.parquet"
    df = _cache_read_df(cache_path)
    if df is not None:
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        _MACRO_CACHE[cache_key] = out
        return out

    params = {
        "series_id": FRED_CODES[key],
        "api_key": FRED_KEY,
        "file_type": "json",
        "observation_start": f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}",
        "observation_end": f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}",
    }
    try:
        r = safe_request("get", FRED_API, params=params, timeout=10)
        obs = r.json().get("observations", [])
        data = []
        for it in obs:
            dt = it.get("date", "").replace("-", "")
            val = safe_float(it.get("value"))
            if dt and val is not None:
                data.append((dt, val))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data, columns=["Date", key])
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        out = df[mask].copy()
        _MACRO_CACHE[cache_key] = out
        return out
    except Exception:
        return pd.DataFrame()

# --------------------------------------------------------------------------- #
# 가격/수급/메타 수집
# --------------------------------------------------------------------------- #
def _collect_price_pykrx(code: str, start: str, end: str) -> pd.DataFrame:
    df = stock.get_market_ohlcv_by_date(start, end, code)
    if df is None or df.empty:
        raise ValueError("pykrx_empty")
    df = df.reset_index()
    df.columns = ["date", "open", "high", "low", "close", "volume", "amount"]
    df["date"] = df["date"].astype(str).str.replace("-", "")
    df["code"] = code
    df["adj_factor"] = 1.0
    # vwap 우선 계산 시도
    df["vwap"] = df["amount"] / df["volume"].replace(0, pd.NA)

    # 0값 보정
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").replace(0, pd.NA)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # high/low 교차 보정
    swap_mask = (df["high"].notna()) & (df["low"].notna()) & (df["high"] < df["low"])
    df.loc[swap_mask, ["high", "low"]] = df.loc[swap_mask, ["low", "high"]].values

    df = df.sort_values("date")
    # 안전 ffill: 가격만
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].ffill()
    # 거래량/대금은 결측 유지
    # vwap 보정
    df["vwap"] = df["vwap"].fillna(df["amount"] / df["volume"].replace(0, pd.NA))
    df["vwap"] = df["vwap"].fillna(df["close"])
    df["adjust_close"] = df["close"]
    # 상태 기본값
    df["is_trading_suspended"] = False
    df["is_limit_reached"] = 0  # -1/0/1
    return df[["date", "code", "open", "high", "low", "close", "adjust_close", "volume", "amount", "adj_factor", "vwap"]]

def _collect_price_fallback(code: str, start: str, end: str) -> pd.DataFrame:
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={code}&requestType=1&timeframe=day&count=3000"
    r = safe_request("get", url, timeout=8)
    txt = (r.text or "").strip()
    try:
        data = ast.literal_eval(txt)
    except Exception:
        data = None
    if not data or len(data) < 2:
        return pd.DataFrame()
    header = data[0]; rows = data[1:]
    df = pd.DataFrame(rows, columns=header)
    col_map = {"날짜": "date", "시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume"}
    df = df.rename(columns=col_map)
    if "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame()
    df = df.dropna(subset=["date", "close"])
    df["date"] = df["date"].astype(str).str.replace(".", "", regex=False)
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")
    df = df.sort_values("date")
    df["amount"] = pd.NA
    df["adj_factor"] = 1.0
    df["code"] = code
    df["vwap"] = df["close"]
    df["adjust_close"] = df["close"]
    df["is_trading_suspended"] = False
    df["is_limit_reached"] = 0
    return df[["date", "code", "open", "high", "low", "close", "adjust_close", "volume", "amount", "adj_factor", "vwap"]]

def _detect_market(code: str) -> Optional[str]:
    try:
        kospi = set(stock.get_market_ticker_list(market="KOSPI"))
        if code in kospi:
            return "KS"
        kosdaq = set(stock.get_market_ticker_list(market="KOSDAQ"))
        if code in kosdaq:
            return "KQ"
    except Exception:
        pass
    return None

def collect_price(code: str, start: str, end: str) -> pd.DataFrame:
    try:
        return _collect_price_pykrx(code, start, end)
    except Exception:
        # yfinance fallback with market detection
        if yf is not None:
            suffixes = []
            mkt = _detect_market(code)
            if mkt:
                suffixes = [mkt]
            else:
                suffixes = ["KS", "KQ"]
            for suf in suffixes:
                try:
                    ticker = f"{code}.{suf}"
                    hist = yf.Ticker(ticker).history(period="11y")
                    if not hist.empty:
                        df = hist.reset_index()
                        df["date"] = df["Date"].dt.strftime("%Y%m%d")
                        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
                        df["amount"] = pd.NA
                        df["adj_factor"] = 1.0
                        df["code"] = code
                        df["vwap"] = df["close"]
                        df["adjust_close"] = df["close"]
                        df["is_trading_suspended"] = False
                        df["is_limit_reached"] = 0
                        return df[["date", "code", "open", "high", "low", "close", "adjust_close", "volume", "amount", "adj_factor", "vwap"]]
                except Exception:
                    continue
        return _collect_price_fallback(code, start, end)

def collect_flow(code: str, start: str, end: str) -> pd.DataFrame:
    try:
        df_val = stock.get_market_trading_value_by_date(start, end, code, detail=True)
        df_vol = stock.get_market_trading_volume_by_date(start, end, code, detail=True)
        if (df_val is None or df_val.empty) and (df_vol is None or df_vol.empty):
            return pd.DataFrame()

        result = pd.DataFrame()
        if df_val is not None and not df_val.empty:
            df_val = df_val.reset_index()
            df_val.columns = ["date"] + [str(c) for c in df_val.columns[1:]]
            df_val["date"] = df_val["date"].astype(str).str.replace("-", "")
            result = df_val[["date"]].copy()
            # 외국인 순매수 금액
            for col in ["외국인", "외국인합계"]:
                if col in df_val.columns:
                    result["frgn_net_amt"] = pd.to_numeric(df_val[col], errors="coerce")
                    break
            # 기관 합계 및 세부
            inst_cols = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금", "기타법인"]
            avail = [c for c in inst_cols if c in df_val.columns]
            if avail:
                result["inst_net_amt"] = pd.to_numeric(df_val[avail].sum(axis=1), errors="coerce")
                result["nps_net_amt"] = pd.to_numeric(df_val.get("연기금", pd.Series(0, index=df_val.index)), errors="coerce")
                result["tust_net_amt"] = pd.to_numeric(df_val.get("투신", pd.Series(0, index=df_val.index)), errors="coerce")
                result["dealer_net_amt"] = pd.to_numeric(df_val.get("금융투자", pd.Series(0, index=df_val.index)), errors="coerce")

        if df_vol is not None and not df_vol.empty:
            df_vol = df_vol.reset_index()
            df_vol.columns = ["date"] + [str(c) for c in df_vol.columns[1:]]
            df_vol["date"] = df_vol["date"].astype(str).str.replace("-", "")
            if result.empty:
                result = df_vol[["date"]].copy()
            else:
                result = result.merge(df_vol[["date"]], on="date", how="outer")
            for src, dst in [("외국인", "frgn_net_qty"), ("외국인합계", "frgn_net_qty"),
                             ("기관합계", "inst_net_qty"), ("연기금", "nps_net_qty")]:
                if src in df_vol.columns:
                    result[dst] = pd.to_numeric(df_vol[src], errors="coerce")

        result = result.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        return result
    except Exception:
        return pd.DataFrame()

def collect_meta_and_cap(code: str, start: str, end: str) -> pd.DataFrame:
    df = pd.DataFrame({"date": [start], "code": [code]})
    # 종목명
    try:
        df["name"] = stock.get_market_ticker_name(code)
    except Exception:
        df["name"] = None
    # 시장 구분
    mk = None
    try:
        if code in stock.get_market_ticker_list(market="KOSPI"):
            mk = "KOSPI"
        elif code in stock.get_market_ticker_list(market="KOSDAQ"):
            mk = "KOSDAQ"
    except Exception:
        mk = None
    df["market"] = mk
    df["listing_status"] = "Listed"
    df["sector_code"] = None
    df["sector_name"] = None
    # 상장주식수
    try:
        mc = stock.get_market_cap_by_date(start, end, code).reset_index()
        if not mc.empty:
            last = mc.sort_values("날짜").iloc[-1]
            df["shares_out"] = int(pd.to_numeric(last.get("상장주식수", None), errors="coerce"))
        else:
            df["shares_out"] = None
    except Exception:
        df["shares_out"] = None
    # 외국인 보유비중
    try:
        fr = stock.get_exhaustion_rates_of_foreign_investment_by_ticker(start, end)
        if code in fr.index:
            df["frgn_hold_ratio"] = float(pd.to_numeric(fr.loc[code, "외국인보유비중"], errors="coerce"))
        else:
            df["frgn_hold_ratio"] = None
    except Exception:
        df["frgn_hold_ratio"] = None
    return df

# --------------------------------------------------------------------------- #
# 매크로 병합
# --------------------------------------------------------------------------- #
def merge_macro(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    keys = ["usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix"]
    macro = pd.DataFrame({"date": df["date"].astype(str).copy()}).drop_duplicates(subset=["date"])

    def _merge_one(key: str) -> Optional[pd.DataFrame]:
        d = pd.DataFrame()
        if key in ("usdkrw", "cnykrw", "kr10y_yield"):
            d = load_macro_ecos(key, start, end)
        elif key in ("us10y_yield", "dxy", "wti", "gold", "vix"):
            d = load_macro_fred(key, start, end)
            if d is None or d.empty:
                d = load_macro_yfinance(key, start, end)
        if d is None or d.empty:
            return None
        out = d.copy()
        out["date"] = out["Date"].astype(str)
        out = out.drop(columns=["Date"], errors="ignore")
        return out

    for k in keys:
        d = _merge_one(k)
        if d is not None and not d.empty:
            macro = macro.merge(d, on="date", how="left")
    macro["earnings_date"] = None
    merged = df.merge(macro, on="date", how="left")
    return merged

# --------------------------------------------------------------------------- #
# 유틸: 상/하한 플래그(보수적 추정)
# --------------------------------------------------------------------------- #
def flag_limit_reached(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty or "close" not in df.columns:
        return pd.Series(0, index=df.index)
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df.get("high", pd.NA), errors="coerce")
    low  = pd.to_numeric(df.get("low", pd.NA), errors="coerce")
    prev = close.shift(1)
    chg = (close / prev.replace(0, pd.NA)) - 1
    # ±25% 이상이면서 종가=고가(상한 추정), 종가=저가(하한 추정)
    upper = (chg >= 0.25) & (close.notna()) & (high.notna()) & (close.eq(high))
    lower = (chg <= -0.25) & (close.notna()) & (low.notna()) & (close.eq(low))
    out = pd.Series(0, index=df.index)
    out[upper] = 1
    out[lower] = -1
    return out

# --------------------------------------------------------------------------- #
# 단일 종목 수집 엔진
# --------------------------------------------------------------------------- #
class VectorizedCollector:
    def __init__(self, code: str, start_date: str, end_date: str):
        self.code = str(code).zfill(6)
        self.start_date = start_date
        self.end_date = end_date

    def collect(self) -> pd.DataFrame:
        df_price = collect_price(self.code, self.start_date, self.end_date)
        if df_price is None or df_price.empty:
            return pd.DataFrame()
        df_flow = collect_flow(self.code, self.start_date, self.end_date)
        df_meta = collect_meta_and_cap(self.code, self.start_date, self.end_date)
        df = df_price.copy()
        if df_flow is not None and not df_flow.empty:
            df = df.merge(df_flow, on="date", how="left")
        # 메타 상수 채우기
        if df_meta is not None and not df_meta.empty:
            for col in df_meta.columns:
                if col == "date":
                    continue
                val = df_meta[col].iloc[0]
                if col not in df.columns:
                    df[col] = pd.NA
                df[col] = df[col].fillna(val)
        # vwap 재보정
        if "close" in df.columns:
            if "amount" in df.columns and "volume" in df.columns:
                df["vwap"] = df["vwap"].fillna(df["amount"] / df["volume"].replace(0, pd.NA))
            df["vwap"] = df["vwap"].fillna(df["close"])
        # 상/하한 추정 플래그
        try:
            df["is_limit_reached"] = flag_limit_reached(df)
        except Exception:
            pass
        # 매크로 병합
        df = merge_macro(df, self.start_date, self.end_date)
        # 스키마 강제
        for col in V36_COLS:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[[c for c in V36_COLS if c in df.columns]]
        return df

# --------------------------------------------------------------------------- #
# 검증/로깅/스트리밍 저장
# --------------------------------------------------------------------------- #
ESSENTIAL_COLS = ["date", "code", "close", "volume"]
QUALITY_LOG = "quality_report.jsonl"
STREAM_FILE = "raw_v48_nodart_all.csv"
METRIC_LOG = "run_metrics.jsonl"
STATUS_FILE = "status_summary.json"

# 품질 가드 범위
GUARD_RANGES = {
    "usdkrw": (900, 1500),
    "vix": (0, 100),
    "wti": (0, 200),
    "us10y_yield": (0, 20),
    "kr10y_yield": (0, 20),
    "dxy": (60, 130),
    "gold": (300, 3000),
}

def _write_status(out_dir: Path, payload: dict):
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / STATUS_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[상태파일 기록 실패] {e}")

def _log_quality(out_dir: Path, record: dict):
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / QUALITY_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[품질로그 실패] {e}")

def _log_metric(out_dir: Path, record: dict):
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / METRIC_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

def append_streaming(df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / STREAM_FILE
    header = not out_path.exists()
    cols = [c for c in V36_COLS if c in df.columns]
    missing = [c for c in V36_COLS if c not in df.columns]
    for col in missing:
        df[col] = None
    df = df[[c for c in V36_COLS if c in df.columns]]
    try:
        tmp_path = out_dir / f".tmp_{STREAM_FILE}"
        df.to_csv(tmp_path, mode="w", index=False, encoding="utf-8-sig", header=header)
        with open(out_path, "a", encoding="utf-8-sig", newline="") as fout, open(tmp_path, "r", encoding="utf-8-sig") as fin:
            for line in fin:
                fout.write(line)
        tmp_path.unlink(missing_ok=True)
    except Exception as e:
        print(f"[스트리밍 저장 실패] {out_path}: {e}")

def _bizday_count(start_date: str, end_date: str) -> int:
    try:
        rng = pd.bdate_range(
            start=pd.to_datetime(start_date, format="%Y%m%d"),
            end=pd.to_datetime(end_date, format="%Y%m%d"),
            freq="C"  # 주말 제외(한국 휴장일 반영 불가)
        )
        return int(len(rng))
    except Exception:
        return None

def validate_stock_df(code: str, df: pd.DataFrame, start_date: str, end_date: str) -> Tuple[bool, dict]:
    issues: List[str] = []
    warnings: List[str] = []

    if df is None or df.empty:
        issues.append("empty_dataframe")
        return False, {"issues": issues, "warnings": warnings, "rows": 0}

    # 필수 컬럼
    missing = [c for c in ESSENTIAL_COLS if c not in df.columns]
    if missing:
        issues.append(f"missing_columns:{','.join(missing)}")

    # 전체 스키마 누락 여부
    missing_all = [c for c in V36_COLS if c not in df.columns]
    if missing_all:
        issues.append(f"missing_fields:{len(missing_all)}")

    # 매크로 스케일 점검
    for col, (lo, hi) in GUARD_RANGES.items():
        if col in df.columns and df[col].notna().any():
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if ((vals < lo) | (vals > hi)).any():
                issues.append(f"macro_out_of_range:{col}")

    # NaN 비율 점검
    nan_ratios = {}
    for col in ["close", "volume"]:
        if col in df.columns:
            ratio = float(pd.to_numeric(df[col], errors="coerce").isna().mean())
            nan_ratios[col] = ratio
            if ratio > 0.5:
                warnings.append(f"high_nan:{col}:{ratio:.2f}")

    # 비정상 값 검증
    if "close" in df.columns:
        close = pd.to_numeric(df["close"], errors="coerce")
        if (close < 0).any():
            issues.append("negative_close")
        if (close == 0).any():
            warnings.append("zero_close")
        prev = close.shift(1)
        ratio = (close / prev.replace(0, pd.NA)).replace([pd.NA, pd.NaT], 1)
        if (ratio > 5).any():
            warnings.append("spike_close_gt5x")
        if (ratio < 0.2).any():
            warnings.append("drop_close_lt0.2x")

    if "volume" in df.columns:
        vol = pd.to_numeric(df["volume"], errors="coerce")
        if (vol < 0).any():
            issues.append("negative_volume")

    # 중복 제거
    if "date" in df.columns and "code" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["date", "code"], inplace=True)
        deduped = len(df)
        if deduped < before:
            warnings.append(f"dedup:{before-deduped}")

    # 커버리지(영업일 기준 근사) 점검
    try:
        total_days = _bizday_count(start_date, end_date)
        coverage_days = int(df["date"].astype(str).nunique())
        if total_days and total_days > 0:
            coverage_ratio = coverage_days / total_days
            if coverage_ratio < 0.2:
                issues.append(f"low_coverage:{coverage_ratio:.2f}")
            elif coverage_ratio < 0.4:
                warnings.append(f"low_coverage_warn:{coverage_ratio:.2f}")
        else:
            coverage_ratio = None
    except Exception:
        coverage_ratio = None

    ok = len(issues) == 0
    details = {
        "issues": issues,
        "warnings": warnings,
        "rows": int(len(df)),
        "nan_ratios": nan_ratios,
        "coverage_ratio": coverage_ratio,
    }
    return ok, details

# --------------------------------------------------------------------------- #
# SmartCollector (체크포인트/실패 큐)
# --------------------------------------------------------------------------- #
class SmartCollector:
    def __init__(self, out_dir: Path, checkpoint_file: str = "checkpoint.pkl"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.out_dir / checkpoint_file
        self.completed: Set[str] = set()
        self.failed: Set[str] = set()
        self.failed_info: Dict[str, dict] = {}
        self.load_checkpoint()

    def load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "rb") as f:
                    data = pickle.load(f)
                    self.completed = data.get("completed", set())
                    self.failed = data.get("failed", set())
                    self.failed_info = data.get("failed_info", {})
                print(f"[체크포인트 로드] 완료: {len(self.completed):,}개, 실패: {len(self.failed):,}개")
            except Exception as e:
                print(f"[체크포인트 로드 실패] {e}")

    def save_checkpoint(self):
        try:
            data = {
                "completed": self.completed,
                "failed": self.failed,
                "failed_info": self.failed_info,
                "timestamp": datetime.now().isoformat()
            }
            with open(self.checkpoint_file, "wb") as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"[체크포인트 저장 실패] {e}")

    def is_completed(self, code: str) -> bool:
        return code in self.completed

    def mark_completed(self, code: str):
        self.completed.add(code)
        self.failed.discard(code)
        self.failed_info.pop(code, None)
        if len(self.completed) % 10 == 0:
            self.save_checkpoint()

    def mark_failed(self, code: str, reason: Optional[dict] = None):
        self.failed.add(code)
        if reason:
            self.failed_info[code] = reason

# --------------------------------------------------------------------------- #
# 병렬 수집
# --------------------------------------------------------------------------- #
def collect_one_stock(args: Tuple[str, str, str, Optional[str], bool, Optional[str]]) -> Tuple[str, Optional[pd.DataFrame]]:
    code, start_date, end_date, proxy, calc_limit_flag, _ = args
    if proxy:
        try:
            set_proxy(proxy)
        except Exception:
            pass
    try:
        collector_obj = VectorizedCollector(code, start_date, end_date)
        df = collector_obj.collect()
        if calc_limit_flag and df is not None and not df.empty:
            # 이미 내부에서 계산하지만, 플래그 재확인 옵션
            try:
                df["is_limit_reached"] = flag_limit_reached(df)
            except Exception:
                pass
        return code, df
    except Exception as e:
        print(f"  [오류] {code}: {e}")
        return code, None

def collect_parallel(
    codes: List[str],
    start_date: str,
    end_date: str,
    collector: SmartCollector,
    num_workers: int = 16,
    out_dir: Optional[Path] = None,
    stream_save: bool = True,
    proxy: Optional[str] = None,
    failed_only: bool = False,
    max_tasks: Optional[int] = None,
    log_interval: int = 30,
    calc_limit_flag: bool = True
) -> Tuple[Dict[str, pd.DataFrame], int]:
    target_codes = collector.failed if failed_only else codes
    tasks = [(code, start_date, end_date, proxy, calc_limit_flag, None) for code in target_codes if not collector.is_completed(code)]
    if max_tasks is not None:
        tasks = tasks[:max_tasks]

    if not tasks:
        print(f"[모든 종목 완료] {len(codes):,}개 종목 모두 완료")
        return {}, 0

    print(f"[병렬 수집] {len(tasks):,}개 종목, {num_workers}개 워커")
    results: Dict[str, pd.DataFrame] = {}
    start_time = time.time()
    completed = 0
    total_tasks = len(tasks)
    last_metric_flush = time.time()

    try:
        with Pool(processes=num_workers) as pool:
            for result in pool.imap(collect_one_stock, tasks, chunksize=1):
                if result is None:
                    continue
                code, df = result

                if df is None or df.empty:
                    reason = {"issues": ["no_data_returned"], "warnings": []}
                    collector.mark_failed(code, reason=reason)
                    processed = completed + len(collector.failed)
                    print(f"[FAIL] code={code} progress={processed}/{total_tasks} fail={len(collector.failed)} elapsed={time.time()-start_time:.0f}s")
                    if out_dir:
                        fail_rec = {
                            "code": code, "ok": False, "status": "fail",
                            "details": reason, "timestamp": datetime.now().isoformat(),
                        }
                        _log_quality(out_dir, fail_rec)
                    continue

                ok, details = validate_stock_df(code, df, start_date, end_date)
                log_record = {
                    "code": code, "ok": ok, "details": details,
                    "timestamp": datetime.now().isoformat(),
                }

                if not ok:
                    collector.mark_failed(code, reason=details)
                    processed = completed + len(collector.failed)
                    print(f"[FAIL] code={code} progress={processed}/{total_tasks} fail={len(collector.failed)} elapsed={time.time()-start_time:.0f}s issues={';'.join(details.get('issues', []))}")
                    if out_dir:
                        _log_quality(out_dir, {**log_record, "status": "fail"})
                    continue

                if stream_save and out_dir:
                    append_streaming(df, out_dir)
                else:
                    results[code] = df

                collector.mark_completed(code)
                completed += 1
                processed = completed + len(collector.failed)
                print(f"[PASS] code={code} progress={processed}/{total_tasks} ok={completed} fail={len(collector.failed)}")

                if out_dir:
                    _log_quality(out_dir, {**log_record, "status": "pass"})

                if completed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                    print(f"  [진행] {completed:,}/{len(tasks):,} ({completed/len(tasks)*100:.1f}%) - 속도: {rate:.2f}종목/초 - 남은시간: {remaining/3600:.1f}시간")
                    collector.save_checkpoint()

                now = time.time()
                if out_dir and (now - last_metric_flush) >= log_interval:
                    elapsed = now - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else None
                    status_payload = {
                        "ts": datetime.now().isoformat(),
                        "completed": completed,
                        "total_tasks": total_tasks,
                        "failed": len(collector.failed),
                        "rate_per_sec": rate,
                        "eta_sec": remaining,
                    }
                    _log_metric(out_dir, status_payload)
                    _write_status(out_dir, {**status_payload, "running": True, "processed": processed})
                    last_metric_flush = now
    except KeyboardInterrupt:
        print("\n[중단] 사용자 요청으로 중단. 진행 상황 저장합니다.")
        collector.save_checkpoint()
    except Exception as e:
        print(f"[오류] 병렬 수집 중단: {e}")
        collector.save_checkpoint()

    elapsed = time.time() - start_time
    print(f"\n[완료] {completed:,}/{len(tasks):,} 종목 수집 완료 - 소요시간: {elapsed/3600:.2f}시간")
    collector.save_checkpoint()
    processed = completed + len(collector.failed)
    if out_dir:
        final_status = {
            "ts": datetime.now().isoformat(),
            "running": False,
            "completed": completed,
            "failed": len(collector.failed),
            "total_tasks": total_tasks,
            "processed": processed,
            "elapsed_sec": elapsed,
        }
        _write_status(out_dir, final_status)
    return results, completed

# --------------------------------------------------------------------------- #
# 코드/종목 로더
# --------------------------------------------------------------------------- #
def load_codes(codes_arg: str) -> List[str]:
    p = Path(codes_arg)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            codes = [line.strip() for line in f if line.strip()]
        print(f"[종목 로드] 파일에서 {len(codes):,}개 종목 로드")
    else:
        codes = [c.strip() for c in codes_arg.split(",") if c.strip()]
        print(f"[종목 로드] {len(codes):,}개 종목")
    # 숫자 6자리만 허용
    codes = [c for c in codes if len(c) == 6 and c.isdigit()]
    return codes

# --------------------------------------------------------------------------- #
# 실행 진입점
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="raw_v48_nodart_36cols_safe — 36 General Columns Collector")
    p.add_argument("--codes", type=str, required=True, help="종목코드 파일 또는 콤마 구분 리스트")
    p.add_argument("--start-date", type=str, default="20150102", help="시작일 (YYYYMMDD)")
    p.add_argument("--end-date", type=str, default=None, help="종료일 (YYYYMMDD, 기본: 오늘)")
    p.add_argument("--out-dir", type=str, default=str(ROOT / "out"), help="결과 저장 폴더")
    p.add_argument("--workers", type=int, default=None, help="워커 프로세스 수 (기본: CPU*2)")
    p.add_argument("--proxy", type=str, default=None, help="HTTP(S) 프록시 (옵션)")
    p.add_argument("--stream-save", action="store_true", help="CSV 스트리밍 저장 사용")
    p.add_argument("--failed-only", action="store_true", help="실패 종목만 재시도")
    p.add_argument("--max-tasks", type=int, default=None, help="최대 처리 종목 수 제한")
    p.add_argument("--no-limit-flag", action="store_true", help="상/하한 추정 플래그 계산 비활성화")
    return p.parse_args()

def main():
    args = parse_args()
    if not args.end_date:
        args.end_date = datetime.now().strftime("%Y%m%d")

    codes = load_codes(args.codes)

    # 사전 점검
    problems: List[str] = []
    if len(V36_COLS) != 36:
        problems.append(f"V36_COLS 개수 불일치: {len(V36_COLS)}개 (기대 36)")
    try:
        dt_start = datetime.strptime(args.start_date, "%Y%m%d")
        dt_end = datetime.strptime(args.end_date, "%Y%m%d")
        if dt_start > dt_end:
            problems.append("시작일이 종료일보다 늦음")
    except Exception as e:
        problems.append(f"날짜 형식 오류: {e}")
    out_dir = Path(args.out_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        problems.append(f"출력 디렉터리 생성 실패: {e}")
    if problems:
        print("[사전 점검 실패] 다음 항목을 확인하세요:")
        for p in problems:
            print(" -", p)
        sys.exit(1)

    # 워커 수
    workers = args.workers or max(2, min(cpu_count() * 2, 32))

    # Collector 준비
    sc = SmartCollector(out_dir)

    # 실행
    results, completed = collect_parallel(
        codes=codes,
        start_date=args.start_date,
        end_date=args.end_date,
        collector=sc,
        num_workers=workers,
        out_dir=out_dir,
        stream_save=bool(args.stream_save),
        proxy=args.proxy,
        failed_only=bool(args.failed_only),
        max_tasks=args.max_tasks,
        calc_limit_flag=not args.no_limit_flag,
    )

    # 통합 저장(옵션) — 스트리밍 미사용 시
    if results:
        dfs = [df for df in results.values() if df is not None and not df.empty]
        if dfs:
            df_all = pd.concat(dfs, ignore_index=True).sort_values(["date", "code"])
            output_file = out_dir / "raw_v48_nodart_all.csv"
            df_all.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"[저장] {output_file} ({len(df_all):,}행, {len(results):,}종목)")

if __name__ == "__main__":
    main()
