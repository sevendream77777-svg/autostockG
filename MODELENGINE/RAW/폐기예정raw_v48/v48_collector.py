#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v48 Vectorized 수집기 (대량 히스토리컬 데이터 구축용)
- 종목별 11년치 일괄 수집 (Point-wise → Vectorized)
- PyKRX 기간 일괄 조회
- DART 분기별 수집 + Merge & Fill
- 매크로 전범위 캐시
- 병렬화 지원
"""

from __future__ import annotations

# -----------------------
# Legacy DART account mapping (2011~2014 구계정 대응)
# 기존 코드/주석은 유지하고, 필요한 보강만 추가
# -----------------------
LEGACY_DART_ACCOUNT_MAP = {
    # 한국어 구계정 → 표준 키
    "매출액": "revenue",
    "영업수익": "revenue",
    "영업이익": "op_income",
    "영업손실": "op_income",  # 음수 처리
    "당기순이익": "net_income",
    "당기순손실": "net_income",      # 음수 처리
    "자본총계": "total_equity",
    "부채총계": "total_liabilities",
    "자산총계": "total_assets",
    # 영어/코드 유사 매핑 보강
    "ifrs_영업이익": "op_income",
    "ifrs_수익": "revenue",
    "ifrs-full_Revenue": "revenue",
    "ifrs-full_ProfitLoss": "net_income",
    "ifrs-full_Assets": "total_assets",
    "ifrs-full_Equity": "total_equity",
    "ifrs-full_Liabilities": "total_liabilities",
}

def _harmonize_legacy_dart(df):
    """
    2011~2014 구계정명/코드로 들어온 DART 재무를 표준 컬럼으로 맵핑.
    - 입력 df는 wide/long 상관없이 컬럼에 account_nm/account_id 혹은 표준 컬럼이 있을 수 있음.
    - 기존 코드에 영향 없이, 가능한 값만 채워 넣는다.
    """
    import numpy as _np
    import pandas as _pd

    if df is None or len(df) == 0:
        return df

    work = df.copy()
    # Long 형태 대응: account_nm/account_id + value → wide 피벗 시도
    cand_cols = set([c.lower() for c in work.columns])
    account_nm_col = None
    account_id_col = None
    value_col = None

    for c in work.columns:
        cl = c.lower()
        if cl in ("account_nm", "accountname", "account_name"):
            account_nm_col = c
        elif cl in ("account_id", "accountid"):
            account_id_col = c
        elif cl in ("value", "amount", "val"):
            value_col = c

    # Long → Wide
    if (account_nm_col or account_id_col) and value_col:
        keycol = account_id_col or account_nm_col
        pivot = work[[keycol, value_col]].copy()
        pivot.columns = ["key", "value"]
        # 키를 표준키로 변환
        def _to_std(k):
            if k is None:
                return None
            k = str(k).strip()
            if k in LEGACY_DART_ACCOUNT_MAP:
                return LEGACY_DART_ACCOUNT_MAP[k]
            # 유사 매칭: 공백/하이픈 제거 후 포함여부
            k2 = k.replace(" ", "").replace("-", "").lower()
            for kk, vv in LEGACY_DART_ACCOUNT_MAP.items():
                if k2 == kk.replace(" ", "").replace("-", "").lower():
                    return vv
            # ifrs-full_ 형태 등
            if k2.startswith("ifrsfull_"):
                k3 = "ifrs-full_" + k2[len("ifrsfull_"):]
                return LEGACY_DART_ACCOUNT_MAP.get(k3, None)
            return None

        pivot["std"] = pivot["key"].map(_to_std)
        pivot = pivot.dropna(subset=["std"])
        if len(pivot):
            wide = pivot.groupby("std", as_index=False)["value"].last()
            # 표준 컬럼으로 붙이기
            for _, row in wide.iterrows():
                std = row["std"]
                val = row["value"]
                # 음수 계정명 보정
                if std in ("op_income", "net_income"):
                    # '손실' 계정으로 들어오면 음수 가능 → 원본 숫자에 일관성 유지
                    pass
                work[std] = work.get(std, _np.nan)
                try:
                    # wide가 단일 값이면 전체 행 동일 채움(연결키가 없으므로 NaN → 값)
                    work[std] = work[std].fillna(val)
                except Exception:
                    pass
        return work

    # 이미 wide 형태인 경우: 컬럼명 직접 매핑
    for col in list(work.columns):
        std = LEGACY_DART_ACCOUNT_MAP.get(col, None)
        if std and std not in work.columns:
            try:
                work[std] = work[col]
            except Exception:
                pass

    return work


import os
import time
import json
import ast
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta
import threading
import random

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None

# 필수 외부 패키지
from pykrx import stock  # type: ignore
try:
    import yfinance as yf
except ImportError:
    yf = None


# --------------------------------------------------------------------------- #
# 설정
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
MODULE_DIR = Path(__file__).resolve().parent
CACHE_DIR = MODULE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# 세션 & 리트라이
# --------------------------------------------------------------------------- #
SESSION = requests.Session()
if Retry:
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    SESSION.mount("http://", adapter)
    SESSION.mount("https://", adapter)

# --------------------------------------------------------------------------- #
# 간단 레이트 리밋
# --------------------------------------------------------------------------- #
_RATE_LOCK = threading.Lock()
_LAST_CALL: Dict[str, float] = {}
_MIN_INTERVAL = {
    "dart": float(os.environ.get("RATE_LIMIT_DART", "0.25")),   # 기본 4 req/s
    "naver": float(os.environ.get("RATE_LIMIT_NAVER", "0.3")),  # 기본 3~4 req/s
}
_RATE_FILES = {
    "dart": CACHE_DIR / ".rate_dart",
    "naver": CACHE_DIR / ".rate_naver",
}

# --------------------------------------------------------------------------- #
# 구계정 매핑 외부 로드 (선택)
# --------------------------------------------------------------------------- #
LEGACY_ACCOUNT_MAP: Dict[str, str] = {}
LEGACY_ACCOUNT_PATHS = [
    ROOT / "MODELENGINE" / "RAW" / "raw_v48" / "account_map.json",
    ROOT / "MODELENGINE" / "RAW" / "raw_v48" / "account_map_backup.json",
]


def load_legacy_account_map():
    global LEGACY_ACCOUNT_MAP
    if LEGACY_ACCOUNT_MAP:
        return LEGACY_ACCOUNT_MAP
    for p in LEGACY_ACCOUNT_PATHS:
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                # 기대 형태: {account_id: target_col}
                if isinstance(data, dict):
                    LEGACY_ACCOUNT_MAP = data
                    return LEGACY_ACCOUNT_MAP
            except Exception:
                continue
    return LEGACY_ACCOUNT_MAP


def _throttle(domain: str):
    """도메인별 최소 간격 강제 (프로세스 간 파일 기반 간격 공유)"""
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

V48_COLS = [
    # Basic (7)
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    # Price & Liquidity (10)
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap", "market_cap", "shares_out",
    # Flow (8)
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty",
    # Finance (12)
    "announce_date", "revenue", "op_income", "net_income", "total_equity", "total_assets",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "roe",
    # Macro & Event (11)
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix",
    "earnings_date", "bps", "debt_ratio",
]

# 매크로 설정
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
DIV_KEYS = {
    "ifrs-full_Dividends": "div_amount",
    "dart_Dividends": "div_amount",
    "배당금": "div_amount",
    "배당": "div_amount",
}
DIV_KEYS = {
    "ifrs-full_Dividends": "div_amount",
    "dart_Dividends": "div_amount",
    "배당금": "div_amount",
    "배당": "div_amount",
}

YFINANCE_TICKERS = {
    "us10y_yield": "^TNX",
    "wti": "CL=F",
    "gold": "GC=F",
    "dxy": "DX-Y.NYB",
    "vix": "^VIX",
}


# --------------------------------------------------------------------------- #
# 유틸
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
    """프록시 지원 + 기본 UA/언어 + 세션 재사용 + 백오프 + 레이트리밋 + 403/429 대응"""
    proxy = get_proxy()
    if proxy:
        kwargs.setdefault("proxies", proxy)
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("User-Agent", "Mozilla/5.0 (raw_v48 collector)")
    headers.setdefault("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    kwargs["headers"] = headers
    
    # 기본 타임아웃
    kwargs.setdefault("timeout", 10)
    
    # 도메인별 레이트 리밋
    if "dart.fss.or.kr" in url:
        _throttle("dart")
    elif "finance.naver.com" in url:
        _throttle("naver")
    
    # 세션 재사용 + 백오프
    tries = 4
    for attempt in range(tries):
        try:
            resp = SESSION.request(method, url, **kwargs)
            # 403/429/5xx 시 재시도
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
# 프록시 설정
# --------------------------------------------------------------------------- #
_PROXY_CONFIG: Optional[Dict[str, str]] = None

def set_proxy(proxy: Optional[str]):
    """프록시 설정"""
    global _PROXY_CONFIG
    if proxy:
        _PROXY_CONFIG = {
            "http": proxy,
            "https": proxy
        }
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
    """현재 프록시 설정 반환"""
    return _PROXY_CONFIG


# --------------------------------------------------------------------------- #
# API 키 로드
# --------------------------------------------------------------------------- #
def read_ecos_key() -> str:
    """ECOS API 키 로드"""
    paths = [
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\ecos_open_api.txt"),
        ROOT / "ecos_apikey.txt",
    ]
    for p in paths:
        if p.exists():
            try:
                return p.read_text(encoding="utf-8").strip()
            except:
                pass
    return "5J2U4P96R1QKWL5F0Y7C"  # fallback

ECOS_KEY = read_ecos_key()

_DART_KEYS: List[str] = []
_DART_CALLS = 0
_DART_IDX = 0
_DART_LOCK = threading.Lock()
_DART_MAX_CALLS_PER_PROC = int(os.environ.get("DART_MAX_CALLS_PER_PROC", "5000"))
_DART_DAILY_LIMIT = int(os.environ.get("DART_DAILY_LIMIT", "10000"))  # 키당 일일 호출 제한
_DART_USAGE_FILE = CACHE_DIR / ".dart_usage.json"


def read_dart_keys() -> List[str]:
    """DART API 키 여러 개 로드(줄바꿈/콤마 구분)"""
    candidates: List[str] = []
    env_keys = os.environ.get("DART_API_KEYS", "")
    if env_keys:
        for it in env_keys.replace(",", "\n").splitlines():
            key = it.strip()
            if key:
                candidates.append(key)
    
    paths = [
        ROOT / "opendart_apikey.txt",
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\2slkdaum_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\3naver_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\4se77777gmail_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\5se1117gmail_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\8sevendrenaver_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\6109_kitchennaver_dart.txt"),
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\7109kitchen109naver_dart.txt"),
    ]
    for p in paths:
        if p.exists():
            try:
                txt = p.read_text(encoding="utf-8")
                for it in txt.replace(",", "\n").splitlines():
                    key = it.strip()
                    if key:
                        candidates.append(key)
            except:
                pass
    # 중복 제거
    uniq = []
    for k in candidates:
        if k and k not in uniq:
            uniq.append(k)
    return uniq


def init_dart_keys():
    global _DART_KEYS
    if not _DART_KEYS:
        _DART_KEYS = read_dart_keys()


def _load_usage() -> Dict[str, object]:
    try:
        if _DART_USAGE_FILE.exists():
            return json.loads(_DART_USAGE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"date": datetime.now().strftime("%Y%m%d"), "counts": {}}


def _save_usage(data: Dict[str, object]):
    try:
        _DART_USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _DART_USAGE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(_DART_USAGE_FILE)
    except Exception:
        pass


def _mark_key_exhausted(key: str):
    """키가 한도 초과(status=020 등)일 때 즉시 소진 처리"""
    try:
        usage = _load_usage()
        today = datetime.now().strftime("%Y%m%d")
        if usage.get("date") != today:
            usage = {"date": today, "counts": {}}
        counts: Dict[str, int] = usage.get("counts", {}) or {}
        counts[key] = max(counts.get(key, 0), _DART_DAILY_LIMIT)
        usage["counts"] = counts
        _save_usage(usage)
    except Exception:
        pass


class DartQuotaExhausted(RuntimeError):
    pass


def get_dart_key() -> str:
    """DART 키 라운드로빈 선택 (멀티프로세스 간 기본 분산)"""
    init_dart_keys()
    global _DART_CALLS, _DART_IDX
    if not _DART_KEYS:
        raise DartQuotaExhausted("no_dart_keys")
    with _DART_LOCK:
        _DART_CALLS += 1
        if _DART_CALLS >= _DART_MAX_CALLS_PER_PROC:
            raise RuntimeError(f"DART per-process call limit reached: {_DART_CALLS}")
        today = datetime.now().strftime("%Y%m%d")
        usage = _load_usage()
        if usage.get("date") != today:
            usage = {"date": today, "counts": {}}
        counts: Dict[str, int] = usage.get("counts", {}) or {}

        # 키별 남은 쿼터 확인 후 선택 (소진 키는 건너뜀)
        picked = None
        for i in range(len(_DART_KEYS)):
            idx = (_DART_IDX + i) % len(_DART_KEYS)
            k = _DART_KEYS[idx]
            if counts.get(k, 0) < _DART_DAILY_LIMIT:
                picked = k
                _DART_IDX = (idx + 1) % len(_DART_KEYS)
                break
        if not picked:
            raise DartQuotaExhausted("DART daily quota exhausted for all keys")

        counts[picked] = counts.get(picked, 0) + 1
        usage["counts"] = counts
        _save_usage(usage)

        return picked
    raise DartQuotaExhausted("DART daily quota exhausted for all keys")


# --------------------------------------------------------------------------- #
# DART corp_code 매핑
# --------------------------------------------------------------------------- #
_CORP_CODE_MAP: Dict[str, str] = {}
_CORP_LIST_PATHS = [
    MODULE_DIR / "dart_corp_list.xml",
]

def load_corp_code_map() -> Dict[str, str]:
    """dart_corp_list.xml에서 stock_code → corp_code 매핑 로드"""
    global _CORP_CODE_MAP
    if _CORP_CODE_MAP:
        return _CORP_CODE_MAP

    last_error = None
    for xml_path in _CORP_LIST_PATHS:
        if not xml_path.exists():
            last_error = f"not found: {xml_path}"
            continue
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                stock_code = (corp.findtext("stock_code", "") or "").strip()
                corp_code = (corp.findtext("corp_code", "") or "").strip()
                if stock_code and corp_code:
                    stock_code = stock_code.zfill(6)
                    _CORP_CODE_MAP[stock_code] = corp_code
            if _CORP_CODE_MAP:
                break
            last_error = f"parsed but empty: {xml_path}"
        except Exception as e:
            last_error = f"parse error {xml_path}: {e}"
            continue
    
    # 기본 하드코딩
    _CORP_CODE_MAP.setdefault("005930", "00126380")  # 삼성전자
    _CORP_CODE_MAP.setdefault("000660", "00164779")  # SK하이닉스

    if not _CORP_CODE_MAP:
        raise RuntimeError(f"dart_corp_list.xml load failed: {last_error}")
    
    return _CORP_CODE_MAP

def get_corp_code(stock_code: str) -> Optional[str]:
    """stock_code로 corp_code 조회"""
    mapping = load_corp_code_map()
    return mapping.get(stock_code.zfill(6))


# --------------------------------------------------------------------------- #
# 매크로 전범위 캐시 (Vectorized)
# --------------------------------------------------------------------------- #
_MACRO_CACHE: Dict[str, pd.DataFrame] = {}

def load_macro_yfinance(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """yfinance에서 매크로 데이터 로드 (기간 지정)"""
    global _MACRO_CACHE
    cache_key = f"{key}_{start_date}_{end_date}"
    
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key]
    
    # Parquet 캐시 확인
    cache_path = CACHE_DIR / f"yf_{key}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
            df_filtered = df[mask].copy()
            _MACRO_CACHE[cache_key] = df_filtered
            return df_filtered
        except Exception:
            pass
    
    # yfinance에서 다운로드
    if yf is None or key not in YFINANCE_TICKERS:
        return pd.DataFrame()
    
    ticker = YFINANCE_TICKERS[key]
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="11y")
        if hist.empty:
            return pd.DataFrame()
        
        df = hist[["Close"]].reset_index()
        df.columns = ["Date", key]
        df["Date"] = df["Date"].dt.strftime("%Y%m%d")
        
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        df_filtered = df[mask].copy()
        _MACRO_CACHE[cache_key] = df_filtered
        return df_filtered
    except Exception:
        return pd.DataFrame()


def load_macro_ecos(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """ECOS에서 매크로 데이터 로드 (기간 지정)"""
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
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
            df_filtered = df[mask].copy()
            _MACRO_CACHE[cache_key] = df_filtered
            return df_filtered
        except Exception:
            pass
    
    url = f"{ECOS_API}/{ECOS_KEY}/json/kr/1/10000/{code}/{freq}/{start}/{end}"
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
        df_filtered = df[mask].copy()
        _MACRO_CACHE[cache_key] = df_filtered
        return df_filtered
    except Exception:
        return pd.DataFrame()


def load_macro_fred(key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """FRED에서 매크로 데이터 로드 (기간 지정)"""
    global _MACRO_CACHE
    cache_key = f"fred_{key}_{start_date}_{end_date}"
    
    if cache_key in _MACRO_CACHE:
        return _MACRO_CACHE[cache_key]
    
    if key not in FRED_CODES:
        return pd.DataFrame()
    
    series = FRED_CODES[key]
    
    cache_path = CACHE_DIR / f"fred_{key}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
            df_filtered = df[mask].copy()
            _MACRO_CACHE[cache_key] = df_filtered
            return df_filtered
        except Exception:
            pass
    
    params = {
        "series_id": series,
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
            dt = it.get("date", "")
            val = safe_float(it.get("value"))
            if dt and val is not None:
                dt = dt.replace("-", "")
                data.append((dt, val))
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data, columns=["Date", key])
        
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        
        mask = (df["Date"] >= start_date) & (df["Date"] <= end_date)
        df_filtered = df[mask].copy()
        _MACRO_CACHE[cache_key] = df_filtered
        return df_filtered
    except Exception:
        return pd.DataFrame()


def _collect_price_naver(code: str, start: str, end: str):
    """간단한 네이버 시세 Fallback (HTML 테이블 파싱).
    - 기존 코드 보존, 실패 시 빈 DataFrame 반환
    """
    import pandas as _pd
    import datetime as _dt
    import time as _time
    try:
        dfs = []
        for page in range(1, 8):
            url = f"https://finance.naver.com/item/sise_day.nhn?code={code}&page={page}"
            _df = _pd.read_html(url, header=0)[0]
            dfs.append(_df)
            _time.sleep(0.2)
        df = _pd.concat(dfs, ignore_index=True)
        if df.empty:
            return _pd.DataFrame()
        col_map = {}
        for col in df.columns:
            col_str = str(col).strip()
            if '날짜' in col_str or 'date' in col_str.lower():
                col_map[col] = 'date'
            elif '종가' in col_str or 'close' in col_str.lower():
                col_map[col] = 'close'
            elif '시가' in col_str or 'open' in col_str.lower():
                col_map[col] = 'open'
            elif '고가' in col_str or 'high' in col_str.lower():
                col_map[col] = 'high'
            elif '저가' in col_str or 'low' in col_str.lower():
                col_map[col] = 'low'
            elif '거래량' in col_str or 'volume' in col_str.lower():
                col_map[col] = 'volume'
        if not col_map or 'date' not in col_map.values():
            return _pd.DataFrame()
        df = df.rename(columns=col_map)
        df = df.dropna(subset=['date'])
        if df.empty or 'close' not in df.columns:
            return _pd.DataFrame()
        try:
            df['date'] = _pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        except Exception:
            return _pd.DataFrame()
        df = df.sort_values('date').reset_index(drop=True)
        try:
            s = _dt.datetime.strptime(start, '%Y%m%d')
            e = _dt.datetime.strptime(end, '%Y%m%d')
            df['date_dt'] = _pd.to_datetime(df['date'], format='%Y%m%d')
            df = df[(df['date_dt']>=s)&(df['date_dt']<=e)].drop(columns=['date_dt'], errors='ignore')
        except Exception:
            pass
        if df.empty:
            return _pd.DataFrame()
        for col in ['open', 'high', 'low', 'volume']:
            if col not in df.columns:
                df[col] = _pd.NA
        df['amount'] = _pd.NA
        df['adj_factor'] = 1.0
        df['code'] = code.zfill(6)
        df['vwap'] = _pd.to_numeric(df['close'], errors='coerce')
        required_cols = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor', 'vwap']
        return df[[c for c in required_cols if c in df.columns]]
    except Exception:
        return _pd.DataFrame()


# --------------------------------------------------------------------------- #
# Vectorized 수집기
# --------------------------------------------------------------------------- #
@dataclass
class CollectConfig:
    code: str
    start_date: str  # YYYYMMDD
    end_date: str    # YYYYMMDD
    use_dart: bool = True  # 재무 수집 여부
    dart_mode: str = "annual"  # off/annual/full


class VectorizedCollector:
    """종목별 11년치 일괄 수집기"""
    
    def __init__(self, cfg: CollectConfig):
        self.cfg = cfg
        self.code = cfg.code.zfill(6)
        self.start_date = cfg.start_date
        self.end_date = cfg.end_date
        self.use_dart = getattr(cfg, "use_dart", True)
        self.dart_mode = getattr(cfg, "dart_mode", "annual")
        self.dart_key = None
        self.corp_code = get_corp_code(self.code)
        self.df: Optional[pd.DataFrame] = None
    
    def collect(self) -> pd.DataFrame:
        try:
            df_price = self._collect_pykrx_price()
            if df_price is None or df_price.empty:
                try:
                    df_price = _collect_price_naver(self.cfg.code, self.cfg.start_date, self.cfg.end_date)
                except Exception:
                    df_price = df_price
            df_flow = self._collect_pykrx_flow()
            df_meta = self._collect_pykrx_meta()
            df_cap = self._collect_pykrx_cap()
            
            if df_price is None or df_price.empty:
                return pd.DataFrame()
            
            self.df = df_price.copy()
            if not df_flow.empty and "date" in df_flow.columns:
                self.df = self.df.merge(df_flow, on="date", how="left")
            if not df_cap.empty and "date" in df_cap.columns:
                self.df = self.df.merge(df_cap, on="date", how="left")
            if not df_meta.empty:
                for col in df_meta.columns:
                    if col != "date":
                        val = df_meta[col].iloc[0] if len(df_meta) > 0 else None
                        self.df[col] = val
            
            if self.use_dart and self.dart_mode != "off":
                df_finance = self._collect_dart_finance()
                if not df_finance.empty:
                    self.df = self._merge_finance(df_finance)
            
            self._merge_macro()
            self._collect_naver_meta()
            self._calculate_derived()
            # 시가총액/상장주식수 계산 보강
            if "market_cap" in self.df.columns and "shares_out" in self.df.columns:
                if self.df["market_cap"].isna().any() and self.df["close"].notna().any():
                    self.df["market_cap"] = self.df["market_cap"].fillna(self.df["close"] * self.df["shares_out"])
                if self.df["shares_out"].isna().any():
                    self.df["shares_out"] = self.df["shares_out"].ffill().bfill()
            # 스키마 강제: 누락 컬럼은 NA로 채워 48개 컬럼을 항상 유지
            for col in V48_COLS:
                if col not in self.df.columns:
                    self.df[col] = pd.NA
            self.df = self.df[[col for col in V48_COLS if col in self.df.columns]]
            return self.df
        except Exception as e:
            print(f"  [오류] {self.code}: {e}")
            return pd.DataFrame()

    def collect_dart_only(self) -> pd.DataFrame:
        """가격/매크로 없이 DART 재무만 수집"""
        if not self.use_dart or self.dart_mode == "off":
            return pd.DataFrame()
        df_finance = self._collect_dart_finance()
        if df_finance.empty:
            raise RuntimeError("dart_empty_finance")
        df_finance = df_finance.copy()
        df_finance["code"] = self.code
        return df_finance
    
    def _collect_pykrx_price(self) -> pd.DataFrame:
        try:
            df = stock.get_market_ohlcv_by_date(self.start_date, self.end_date, self.code)
            if df.empty:
                raise ValueError("pykrx_empty")
            
            df = df.reset_index()
            df.columns = ["date", "open", "high", "low", "close", "volume", "amount"]
            df["date"] = df["date"].astype(str).str.replace("-", "")
            
            df["code"] = self.code
            df["adj_factor"] = 1.0
            df["vwap"] = df["amount"] / df["volume"].replace(0, pd.NA)
            df.loc[df["volume"] <= 0, "volume"] = pd.NA
            df.loc[df["amount"] <= 0, "amount"] = pd.NA
            df["close"] = df["close"].replace(0, pd.NA)
            df["open"] = df["open"].replace(0, pd.NA)
            df["high"] = df["high"].replace(0, pd.NA)
            df["low"] = df["low"].replace(0, pd.NA)
            swap_mask = df["high"] < df["low"]
            df.loc[swap_mask, ["high", "low"]] = df.loc[swap_mask, ["low", "high"]].values
            df = df.sort_values("date")
            df[["open", "high", "low", "close", "volume", "amount", "vwap"]] = df[["open", "high", "low", "close", "volume", "amount", "vwap"]].ffill()
            df["vwap"] = df["vwap"].fillna(df["close"])
            
            return df[["date", "code", "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap"]]
        except Exception:
            try:
                ticker = None
                if self.code.startswith("0"):
                    ticker = f"{self.code}.KS"
                else:
                    ticker = f"{self.code}.KQ"
                if yf is None:
                    return pd.DataFrame()
                data = yf.Ticker(ticker)
                hist = data.history(period="11y")
                if hist.empty:
                    raise ValueError("yfinance_empty")
                df = hist.reset_index()
                df["date"] = df["Date"].dt.strftime("%Y%m%d")
                df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
                df["amount"] = pd.NA
                df["adj_factor"] = 1.0
                df["code"] = self.code
                df["vwap"] = df["close"]
                return df[["date", "code", "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap"]]
            except Exception:
                try:
                    url = f"https://api.finance.naver.com/siseJson.naver?symbol={self.code}&requestType=1&timeframe=day&count=3000"
                    r = safe_request("get", url, timeout=8)
                    txt = (r.text or "").strip()
                    if not txt:
                        return pd.DataFrame()
                    try:
                        data = ast.literal_eval(txt)
                    except Exception:
                        return pd.DataFrame()
                    if not data or len(data) < 2:
                        return pd.DataFrame()
                    header = data[0]
                    rows = data[1:]
                    df = pd.DataFrame(rows, columns=header)
                    col_map = {
                        "날짜": "date",
                        "시가": "open",
                        "고가": "high",
                        "저가": "low",
                        "종가": "close",
                        "거래량": "volume",
                    }
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
                    df["code"] = self.code
                    df["vwap"] = df["close"]
                    return df[["date", "code", "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap"]]
                except Exception:
                    return pd.DataFrame()
    
    def _collect_pykrx_flow(self) -> pd.DataFrame:
        try:
            df_val = stock.get_market_trading_value_by_date(self.start_date, self.end_date, self.code, detail=True)
            df_vol = stock.get_market_trading_volume_by_date(self.start_date, self.end_date, self.code, detail=True)
            
            if df_val.empty and df_vol.empty:
                return pd.DataFrame()
            
            result = pd.DataFrame()
            
            if not df_val.empty:
                df_val = df_val.reset_index()
                df_val.columns = ["date"] + [str(c) for c in df_val.columns[1:]]
                df_val["date"] = df_val["date"].astype(str).str.replace("-", "")
                
                result = df_val[["date"]].copy()
                
                for col in ["외국인", "외국인합계"]:
                    if col in df_val.columns:
                        result["frgn_net_amt"] = df_val[col]
                        break
                
                inst_cols = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"]
                available_inst_cols = [c for c in inst_cols if c in df_val.columns]
                if available_inst_cols:
                    result["inst_net_amt"] = df_val[available_inst_cols].sum(axis=1)
                    result["nps_net_amt"] = df_val.get("연기금", pd.Series(0, index=df_val.index))
                    result["tust_net_amt"] = df_val.get("투신", pd.Series(0, index=df_val.index))
                    result["dealer_net_amt"] = df_val.get("금융투자", pd.Series(0, index=df_val.index))
            
            if not df_vol.empty:
                df_vol = df_vol.reset_index()
                df_vol.columns = ["date"] + [str(c) for c in df_vol.columns[1:]]
                df_vol["date"] = df_vol["date"].astype(str).str.replace("-", "")
                
                if result.empty:
                    result = df_vol[["date"]].copy()
                else:
                    result = result.merge(df_vol[["date"]], on="date", how="outer")
                
                for col in ["외국인", "외국인합계"]:
                    if col in df_vol.columns:
                        result["frgn_net_qty"] = df_vol[col]
                        break
                
                inst_cols = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"]
                available_inst_cols = [c for c in inst_cols if c in df_vol.columns]
                if available_inst_cols:
                    result["inst_net_qty"] = df_vol[available_inst_cols].sum(axis=1)
                    result["nps_net_qty"] = df_vol.get("연기금", pd.Series(0, index=df_vol.index))
                # 투신 수량 단독 컬럼
                if "투신" in df_vol.columns:
                    result["tust_net_qty"] = df_vol["투신"]
            
            flow_cols = ["date", "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
                        "frgn_net_qty", "inst_net_qty", "nps_net_qty", "tust_net_qty"]
            return result[[col for col in flow_cols if col in result.columns]]
        except Exception as e:
            return pd.DataFrame()
    
    def _collect_pykrx_cap(self) -> pd.DataFrame:
        """시가총액/상장주식수 일별 수집"""
        try:
            df_cap = stock.get_market_cap_by_date(self.start_date, self.end_date, self.code)
            if df_cap is None or df_cap.empty:
                return pd.DataFrame()
            df_cap = df_cap.reset_index()
            df_cap.columns = ["date"] + [str(c) for c in df_cap.columns[1:]]
            df_cap["date"] = df_cap["date"].astype(str).str.replace("-", "")
            col_map = {
                "시가총액": "market_cap",
                "상장주식수": "shares_out",
            }
            df_cap = df_cap.rename(columns=col_map)
            keep = ["date", "market_cap", "shares_out"]
            df_cap = df_cap[[c for c in keep if c in df_cap.columns]].copy()
            for c in ["market_cap", "shares_out"]:
                if c in df_cap.columns:
                    df_cap[c] = pd.to_numeric(df_cap[c], errors="coerce")
            return df_cap
        except Exception:
            return pd.DataFrame()
    
    def _collect_pykrx_meta(self) -> pd.DataFrame:
        try:
            name = stock.get_market_ticker_name(self.code)
            kospi = stock.get_market_ticker_list(self.start_date, market="KOSPI")
            market = "KOSPI" if self.code in kospi else "KOSDAQ"
            
            df = pd.DataFrame({
                "name": [name],
                "market": [market],
                "listing_status": ["Listed"],
            })
            return df
        except Exception:
            return pd.DataFrame()
    
    def _collect_dart_finance(self) -> pd.DataFrame:
        if not self.corp_code:
            return pd.DataFrame()
        self.last_dart_status = None
        self.last_dart_attempts = 0
        
        finance_data = []
        start_year = int(self.start_date[:4])
        end_year = int(self.end_date[:4])
        
        if getattr(self, "dart_mode", "annual") == "full":
            reprt_codes = ["11013", "11012", "11014", "11011"]
        else:
            reprt_codes = ["11011"]
        
        # 키 순환하며 연/보고서별 요청 (status=020이면 즉시 다른 키로 재시도)
        for year in range(start_year, end_year + 1):
            for rc in reprt_codes:
                url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                attempts = 0
                key_count = len(_DART_KEYS)
                if key_count == 0:
                    raise DartQuotaExhausted("no_dart_keys")
                max_attempts = max(2, key_count * 2)  # 키 수의 2배까지 시도(최소 2회)
                while attempts < max_attempts:
                    attempts += 1
                    self.last_dart_attempts = attempts
                    try:
                        params = {
                            "crtfc_key": get_dart_key(),
                            "corp_code": self.corp_code,
                            "bsns_year": str(year),
                            "reprt_code": rc,
                            "fs_div": "CFS",
                        }
                        r = safe_request("get", url, params=params, timeout=6)
                        if r.status_code == 429 or r.status_code >= 500:
                            time.sleep(0.5 * (2 ** (attempts % 3)) + random.uniform(0, 0.3))
                            continue
                        if r.status_code != 200:
                            continue
                        data = r.json()
                        if data.get("status") != "000" or not data.get("list"):
                            msg = ""
                            try:
                                msg = data.get("message", "")
                            except Exception:
                                pass
                            current_key = params.get("crtfc_key", "")
                            self.last_dart_status = data.get("status") or "no_status"
                            key_tail = current_key[-6:] if current_key else ""
                            print(f"[DART재시도] code={self.code} corp={self.corp_code} year={year} rc={rc} status={self.last_dart_status} attempt={attempts}/{max_attempts} key_tail={key_tail} msg={msg}")
                            if data.get("status") == "020":
                                _mark_key_exhausted(current_key)
                                # 다음 키로 강제 진전
                                try:
                                    global _DART_IDX
                                    _DART_IDX = (_DART_IDX + 1) % max(1, len(_DART_KEYS))
                                except Exception:
                                    pass
                                continue  # 즉시 다음 키로 재시도
                            # 데이터 없음/기타도 다음 키로 재시도 (두 바퀴까지)
                            continue
                        
                        amap = {
                            "ifrs-full_Revenue": "revenue",
                            "ifrs_Revenue": "revenue",
                            "ifrs-full_ProfitLossFromOperatingActivities": "op_income",
                            "dart_OperatingIncomeLoss": "op_income",
                            "ifrs-full_ProfitLoss": "net_income",
                            "ifrs_ProfitLoss": "net_income",
                            "ifrs-full_Equity": "total_equity",
                            "ifrs_Equity": "total_equity",
                            "ifrs-full_Assets": "total_assets",
                            "ifrs_Assets": "total_assets",
                            "ifrs-full_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
                            "ifrs_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
                            "ifrs-full_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
                            "ifrs_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
                            "ifrs-full_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
                            "ifrs_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
                            "ifrs-full_EarningsPerShare": "eps",
                            "ifrs_BasicEarningsLossPerShare": "eps",
                            "dart_TotalAssets": "total_assets",
                            "dart_TotalLiabilities": "total_liabilities",
                            "dart_TotalEquity": "total_equity",
                            "dart_Revenue": "revenue",
                            "dart_OperatingIncomeLoss": "op_income",
                            "dart_ProfitLoss": "net_income",
                            "dart_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
                            "dart_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
                            "dart_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
                        }
                        
                        row = {"year": year, "reprt_code": rc}
                        for item in data["list"]:
                            aid = item.get("account_id", "")
                            anm = item.get("account_nm", "") or ""
                            val = safe_float(item.get("thstrm_amount", ""))
                            key = None
                            if aid in amap:
                                key = amap[aid]
                            else:
                                if "매출" in anm or "수익" in anm:
                                    key = "revenue"
                                elif "영업이익" in anm:
                                    key = "op_income"
                                elif "당기순이익" in anm or "분기순이익" in anm:
                                    key = "net_income"
                                elif "자산총계" in anm or "총자산" in anm:
                                    key = "total_assets"
                                elif "자본총계" in anm or "총자본" in anm:
                                    key = "total_equity"
                                elif "영업활동" in anm and "현금" in anm:
                                    key = "cash_flow_op"
                                elif "투자활동" in anm and "현금" in anm:
                                    key = "cash_flow_inv"
                                elif "재무활동" in anm and "현금" in anm:
                                    key = "cash_flow_fin"
                                elif "주당이익" in anm or "EPS" in anm:
                                    key = "eps"
                                elif "배당" in anm:
                                    key = "div_amount"
                            if not key:
                                legacy = load_legacy_account_map()
                                if legacy and aid in legacy:
                                    key = legacy[aid]
                            if key and val is not None:
                                if key not in row or abs(val) > abs(row.get(key, 0)):
                                    row[key] = val
                        
                        if len(row) > 2:
                            finance_data.append(row)
                        self.last_dart_status = "000"
                        break  # 성공 시 다음 rc로
                    except DartQuotaExhausted:
                        raise
                    except Exception:
                        time.sleep(0.5 * (2 ** (attempts % 3)) + random.uniform(0, 0.3))
                        continue
                # end while
                if self.last_dart_status and self.last_dart_status != "000" and attempts >= max_attempts:
                    print(f"[DART중단] code={self.code} corp={self.corp_code} year={year} rc={rc} status={self.last_dart_status} attempts={attempts}/{max_attempts}")
                time.sleep(0.05)
        
        if not finance_data:
            # 키 한도(020)만 반복되면 키 소진 예외로 명확히 중단
            if self.last_dart_status == "020":
                raise DartQuotaExhausted("dart_quota_exhausted_all_keys")
            raise RuntimeError(f"dart_empty_finance status={self.last_dart_status}")
        
        df = pd.DataFrame(finance_data)
        
        def get_announce_date(year, reprt_code):
            if reprt_code == "11013":
                return f"{year}0515"
            elif reprt_code == "11012":
                return f"{year}0815"
            elif reprt_code == "11014":
                return f"{year}1115"
            else:
                return f"{year+1}0331"
        
        df["announce_date"] = df.apply(lambda x: get_announce_date(x["year"], x["reprt_code"]), axis=1)
        return df
    
    def _merge_finance(self, df_finance: pd.DataFrame) -> pd.DataFrame:
        if self.df.empty or df_finance.empty:
            return self.df
        
        df_finance = df_finance.copy()
        df_finance["announce_date"] = df_finance["announce_date"].astype(str)
        df_finance = df_finance.sort_values("announce_date")
        
        df_merged = self.df.copy()
        df_merged["date_int"] = df_merged["date"].astype(int)
        
        finance_cols = ["revenue", "op_income", "net_income", "total_equity", "total_assets",
                       "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "announce_date"]
        
        for idx, row in df_finance.iterrows():
            announce_date_int = int(row["announce_date"])
            mask = df_merged["date_int"] >= announce_date_int
            
            for col in finance_cols:
                if col in row and col != "announce_date":
                    df_merged.loc[mask, col] = row[col]
        
        for col in finance_cols:
            if col in df_merged.columns and col != "announce_date":
                df_merged[col] = df_merged[col].ffill()
        
        if "net_income" in df_merged.columns and "total_equity" in df_merged.columns:
            df_merged["roe"] = (df_merged["net_income"] / df_merged["total_equity"].replace(0, 1)) * 100
        if "total_equity" in df_merged.columns and "shares_out" in df_merged.columns:
            df_merged["bps"] = df_merged["total_equity"] / df_merged["shares_out"].replace(0, pd.NA)
        if "total_assets" in df_merged.columns and "total_equity" in df_merged.columns:
            df_merged["debt_ratio"] = (df_merged["total_assets"] - df_merged["total_equity"]) / df_merged["total_equity"].replace(0, pd.NA) * 100
        
        df_merged = df_merged.drop(columns=["date_int"], errors="ignore")
        return df_merged
    
    def _merge_macro(self):
        if self.df.empty:
            return
        
        for key in ["us10y_yield", "wti", "gold", "dxy", "vix"]:
            if key in YFINANCE_TICKERS:
                df_macro = load_macro_yfinance(key, self.start_date, self.end_date)
                if not df_macro.empty and "Date" in df_macro.columns:
                    self.df = self.df.merge(df_macro, left_on="date", right_on="Date", how="left")
                    if "Date" in self.df.columns:
                        self.df = self.df.drop(columns=["Date"])
        
        for key in ["usdkrw", "cnykrw", "kr10y_yield"]:
            if key in ECOS_CODES:
                df_macro = load_macro_ecos(key, self.start_date, self.end_date)
                if not df_macro.empty and "Date" in df_macro.columns:
                    if ECOS_CODES[key][1] == "M":
                        df_macro["month"] = df_macro["Date"].str[:6]
                        self.df["month"] = self.df["date"].str[:6]
                        self.df = self.df.merge(df_macro[["month", key]], on="month", how="left")
                        if "month" in self.df.columns:
                            self.df = self.df.drop(columns=["month"])
                    else:
                        self.df = self.df.merge(df_macro, left_on="date", right_on="Date", how="left")
                        if "Date" in self.df.columns:
                            self.df = self.df.drop(columns=["Date"])
        
        for key in FRED_CODES.keys():
            if key in self.df.columns and self.df[key].notna().any():
                continue
            df_macro = load_macro_fred(key, self.start_date, self.end_date)
            if not df_macro.empty and "Date" in df_macro.columns:
                self.df = self.df.merge(df_macro, left_on="date", right_on="Date", how="left")
                if "Date" in self.df.columns:
                    self.df = self.df.drop(columns=["Date"])
        
        # 매크로 결측은 앞/뒤 방향으로 보정
        macro_cols = ["usdkrw", "cnykrw", "kr10y_yield", "us10y_yield", "wti", "dxy", "gold", "vix"]
        for col in macro_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].ffill().bfill()
    
    def _collect_naver_meta(self):
        if self.df.empty:
            return
        
        try:
            url = f"https://m.stock.naver.com/api/stock/{self.code}/integration"
            resp = safe_request("get", url, timeout=8, headers={
                "User-Agent": "Mozilla/5.0 (raw_v48)",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                "Referer": "https://m.stock.naver.com/",
            })
            if resp.status_code != 200:
                return
            
            data = resp.json()

            def _fill(col, val):
                if val is None:
                    return
                if col not in self.df.columns or self.df[col].isna().all():
                    self.df[col] = val

            _fill("shares_out", data.get("accSharesOutstanding"))
            _fill("market_cap", data.get("marketValue"))
            _fill("sector_code", data.get("industryCode"))
            _fill("sector_name", data.get("industryName"))
            _fill("name", data.get("stockName"))
            _fill("market", data.get("market"))
            
            time.sleep(0.5)
        except Exception:
            pass
    
    def _calculate_derived(self):
        if self.df.empty:
            return
        
        if "market_cap" not in self.df.columns or self.df["market_cap"].isna().all():
            if "close" in self.df.columns and "shares_out" in self.df.columns:
                self.df["market_cap"] = self.df["close"] * self.df["shares_out"]
        
        if "shares_out" not in self.df.columns or self.df["shares_out"].isna().all():
            if "market_cap" in self.df.columns and "close" in self.df.columns:
                self.df["shares_out"] = self.df["market_cap"] / self.df["close"].replace(0, 1)
        
        if "vwap" not in self.df.columns or self.df["vwap"].isna().all():
            if "amount" in self.df.columns and "volume" in self.df.columns:
                self.df["vwap"] = self.df["amount"] / self.df["volume"].replace(0, 1)
        
        if "total_equity" not in self.df.columns or self.df["total_equity"].isna().all():
            if "bps" in self.df.columns and "shares_out" in self.df.columns:
                self.df["total_equity"] = self.df["bps"] * self.df["shares_out"]
            elif "close" in self.df.columns and "shares_out" in self.df.columns:
                self.df["total_equity"] = self.df["close"] * self.df["shares_out"]
        
        if "total_assets" not in self.df.columns or self.df["total_assets"].isna().all():
            if "total_equity" in self.df.columns and "debt_ratio" in self.df.columns:
                self.df["total_assets"] = self.df["total_equity"] * (1 + self.df["debt_ratio"] / 100)

        # bps 보충: total_equity / shares_out
        if "bps" not in self.df.columns or self.df["bps"].isna().all():
            if "total_equity" in self.df.columns and "shares_out" in self.df.columns:
                self.df["bps"] = self.df["total_equity"] / self.df["shares_out"].replace(0, pd.NA)

        # eps 보충: net_income / shares_out
        if "eps" not in self.df.columns or self.df["eps"].isna().all():
            if "net_income" in self.df.columns and "shares_out" in self.df.columns:
                self.df["eps"] = self.df["net_income"] / self.df["shares_out"].replace(0, pd.NA)

        # roe 보충: net_income / total_equity
        if "roe" not in self.df.columns or self.df["roe"].isna().all():
            if "net_income" in self.df.columns and "total_equity" in self.df.columns:
                self.df["roe"] = (self.df["net_income"] / self.df["total_equity"].replace(0, pd.NA)) * 100

        # debt_ratio 보충: (total_assets - total_equity) / total_equity
        if "debt_ratio" not in self.df.columns or self.df["debt_ratio"].isna().all():
            if "total_assets" in self.df.columns and "total_equity" in self.df.columns:
                self.df["debt_ratio"] = (self.df["total_assets"] - self.df["total_equity"]) / self.df["total_equity"].replace(0, pd.NA) * 100

