#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
경량 v52 수집기 (의존성 최소화, 10년치 수집 대비)
- 가격/수급/공매도: pykrx (detail=True로 투신/연기금/금융투자 세부분류)
- 재무: DART (opendart_apikey.txt) + Naver finance 보강
- 섹터: DART company.json (induty_code)
- 매크로: yfinance (API 키 불필요, 10년+ 데이터)

중요: 더미값을 넣지 않고, 가능한 값만 채운다. 부족한 필드는 None.
"""
from __future__ import annotations

import os
import time
import json
import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests

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
CACHE_DIR = ROOT / "MODELENGINE" / "RAW" / "raw_v52" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

V52_COLS = [
    # Basic
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    # Price & Liquidity
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap", "market_cap", "shares_out",
    # Flow
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty",
    "short_sell_amt", "short_sell_qty", "loan_balance_amt", "loan_balance_qty",
    # Finance
    "announce_date", "revenue", "op_income", "net_income", "total_equity", "total_assets",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "roe",
    # Macro & Event
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold",
    "ex_div_date", "earnings_date", "bps", "debt_ratio",
]

# 매크로: ECOS/FRED 전용 (FDR/yfinance 미사용)
ECOS_API = "https://ecos.bok.or.kr/api/StatisticSearch"
FRED_API = "https://api.stlouisfed.org/fred/series/observations"
FRED_KEY = "guest"

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
    return ""

ECOS_KEY = read_ecos_key() or "5J2U4P96R1QKWL5F0Y7C"  # fallback

# ECOS 코드 매핑 (통계코드, 주기, 항목코드)
# TIME 필드: D=YYYYMMDD, M=YYYYMM
ECOS_CODES = {
    "usdkrw": ("036Y001", "M", None),      # 원/달러
    "cnykrw": ("036Y015", "M", None),      # 위안/원
    "kr10y_yield": ("817Y002", "D", "010210000"),  # 국고채 10년 금리
}

# FRED 코드 매핑 (일)
FRED_CODES = {
    "us10y_yield": "DGS10",
    "dxy": "DTWEXBGS",
    "wti": "DCOILWTICO",
    "gold": "GOLDAMGBD228NLBM",
    "vix": "VIXCLS",
}


# --------------------------------------------------------------------------- #
# 프록시 설정
# --------------------------------------------------------------------------- #
_PROXY_CONFIG: Optional[Dict[str, str]] = None

def set_proxy(proxy: Optional[str]):
    """프록시 설정 (예: "http://proxy.example.com:8080" 또는 "socks5://127.0.0.1:1080")"""
    global _PROXY_CONFIG
    if proxy:
        _PROXY_CONFIG = {
            "http": proxy,
            "https": proxy
        }
    else:
        _PROXY_CONFIG = None

def get_proxy() -> Optional[Dict[str, str]]:
    """현재 프록시 설정 반환"""
    return _PROXY_CONFIG

def load_proxy_from_file() -> Optional[str]:
    """프록시 설정 파일에서 로드"""
    paths = [
        ROOT / "MODELENGINE" / "RAW" / "raw_v52" / "proxy.txt",
        Path.home() / ".proxy_config.txt",
    ]
    for p in paths:
        if p.exists():
            try:
                proxy = p.read_text(encoding="utf-8").strip()
                if proxy:
                    return proxy
            except:
                pass
    return None

# 시작 시 프록시 설정 파일 확인
_proxy_from_file = load_proxy_from_file()
if _proxy_from_file:
    set_proxy(_proxy_from_file)

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
    """프록시를 지원하는 안전한 requests 호출"""
    proxy = get_proxy()
    if proxy:
        kwargs.setdefault("proxies", proxy)
    return requests.request(method, url, **kwargs)


def read_dart_key() -> str:
    paths = [
        ROOT / "opendart_apikey.txt",
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
    ]
    for p in paths:
        if p.exists():
            try:
                return p.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    return ""


def backoff_sleep(sec: float = 1.0):
    time.sleep(sec)


# --------------------------------------------------------------------------- #
# DART corp_code 매핑 (stock_code → corp_code)
# --------------------------------------------------------------------------- #
_CORP_CODE_MAP: Dict[str, str] = {}

def load_corp_code_map() -> Dict[str, str]:
    """dart_corp_list.xml에서 stock_code → corp_code 매핑 로드"""
    global _CORP_CODE_MAP
    if _CORP_CODE_MAP:
        return _CORP_CODE_MAP
    
    xml_path = CACHE_DIR / "dart_corp_list.xml"
    if not xml_path.exists():
        # 상위 폴더에서 찾기
        xml_path = ROOT / "MODELENGINE" / "RAW" / "raw_v52" / "dart_corp_list.xml"
    
    if xml_path.exists():
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                stock_code = corp.findtext("stock_code", "").strip()
                corp_code = corp.findtext("corp_code", "").strip()
                if stock_code and corp_code:
                    _CORP_CODE_MAP[stock_code] = corp_code
        except Exception:
            pass
    
    # 기본 하드코딩 (XML 없을 때 fallback)
    _CORP_CODE_MAP.setdefault("005930", "00126380")  # 삼성전자
    _CORP_CODE_MAP.setdefault("000660", "00164779")  # SK하이닉스
    
    return _CORP_CODE_MAP


def get_corp_code(stock_code: str) -> Optional[str]:
    """stock_code로 corp_code 조회"""
    mapping = load_corp_code_map()
    return mapping.get(stock_code.zfill(6))


# --------------------------------------------------------------------------- #
# yfinance 매크로 캐시
# --------------------------------------------------------------------------- #
YFINANCE_TICKERS = {
    "us10y_yield": "^TNX",
    "wti": "CL=F",
    "gold": "GC=F",
    "dxy": "DX-Y.NYB",
    "usdkrw": "USDKRW=X",
    "cnykrw": "CNYKRW=X",
    "vix": "^VIX",
}

_MACRO_CACHE: Dict[str, pd.DataFrame] = {}

def load_macro_yfinance(key: str) -> pd.DataFrame:
    """yfinance에서 매크로 데이터 로드 (캐시)"""
    global _MACRO_CACHE
    
    if key in _MACRO_CACHE:
        return _MACRO_CACHE[key]
    
    # Parquet 캐시 확인
    cache_path = CACHE_DIR / f"yf_{key}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            _MACRO_CACHE[key] = df
            return df
        except Exception:
            pass
    
    # yfinance에서 다운로드
    if yf is None or key not in YFINANCE_TICKERS:
        return pd.DataFrame()
    
    ticker = YFINANCE_TICKERS[key]
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="11y")  # 11년
        if hist.empty:
            raise ValueError("empty yfinance data")
        
        df = hist[["Close"]].reset_index()
        df.columns = ["Date", key]
        df["Date"] = df["Date"].dt.strftime("%Y%m%d")
        
        # 캐시 저장
        try:
            df.to_parquet(cache_path, index=False)
        except Exception:
            pass
        
        _MACRO_CACHE[key] = df
        return df
    except Exception:
        # yfinance가 없거나 데이터가 비는 경우 ECOS 백업 (usdkrw/cnykrw 등)
        if key in ECOS_CODES:
            try:
                mf = MacroFetcher()
                df = mf._fetch(key)
                if not df.empty:
                    try:
                        df.to_parquet(cache_path, index=False)
                    except Exception:
                        pass
                    _MACRO_CACHE[key] = df
                    return df
            except Exception:
                pass
        return pd.DataFrame()


def get_macro_value(key: str, date: str) -> Optional[float]:
    """특정 날짜의 매크로 값 조회 (yfinance → ECOS 백업)"""
    df = load_macro_yfinance(key)
    if not df.empty:
        # 해당 날짜 이전의 가장 최근 값
        mask = df["Date"] <= date
        if mask.any():
            val = df.loc[mask, key].iloc[-1]
            return safe_float(val)
    
    # yfinance 실패 시 ECOS 백업 (usdkrw, cnykrw 등)
    if key in ECOS_CODES:
        try:
            mf = MacroFetcher()
            return mf.get(key, date)
        except Exception:
            pass
    
    return None


# --------------------------------------------------------------------------- #
# 매크로 캐시(ECOS/FRED) - 레거시, 백업용
# --------------------------------------------------------------------------- #
class MacroFetcher:
    def __init__(self):
        self.mem: Dict[str, pd.DataFrame] = {}

    def _cache_path(self, key: str) -> Path:
        return CACHE_DIR / f"macro2_{key}.parquet"

    def _load(self, key: str, target_date: Optional[str] = None):
        path = self._cache_path(key)
        if path.exists():
            try:
                df = pd.read_parquet(path)
                # 캐시에 데이터가 있고, 필요한 날짜가 포함되어 있으면 사용
                if not df.empty and target_date:
                    mask = df["Date"] <= target_date
                    if mask.any():
                        self.mem[key] = df
                        return
            except Exception:
                pass
        df = self._fetch(key, target_date)
        self.mem[key] = df
        try:
            df.to_parquet(path, index=False)
        except Exception:
            pass

    def _fetch(self, key: str, target_date: Optional[str] = None) -> pd.DataFrame:
        """ECOS 데이터 가져오기 (필요한 날짜 주변만)"""
        if key in ECOS_CODES:
            code, freq, item_code = ECOS_CODES[key]
            
            # 타겟 날짜 기준으로 범위 설정 (빠른 조회)
            if target_date:
                try:
                    if freq == "M":
                        # 월별: 해당 월 ±12개월
                        dt = pd.Timestamp(target_date[:6] + "01")
                        start = (dt - pd.DateOffset(months=12)).strftime("%Y%m")
                        end = (dt + pd.DateOffset(months=1)).strftime("%Y%m")
                    else:
                        # 일별: 해당 날짜 ±30일
                        dt = pd.Timestamp(target_date)
                        start = (dt - pd.DateOffset(days=30)).strftime("%Y%m%d")
                        end = (dt + pd.DateOffset(days=1)).strftime("%Y%m%d")
                except:
                    # 파싱 실패 시 전체 범위
                    start = "20100101" if freq == "D" else "201001"
                    end = pd.Timestamp.today().strftime("%Y%m%d" if freq == "D" else "%Y%m")
            else:
                # 전체 범위 (캐시용)
                start = "20100101" if freq == "D" else "201001"
                end = pd.Timestamp.today().strftime("%Y%m%d" if freq == "D" else "%Y%m")
            
            url = f"{ECOS_API}/{ECOS_KEY}/json/kr/1/1000/{code}/{freq}/{start}/{end}"
            for _ in range(2):  # 최대 2회 시도
                try:
                    r = safe_request("get", url, timeout=30)  # 타임아웃 증가
                    rows = r.json().get("StatisticSearch", {}).get("row", [])
                    data = []
                    for it in rows:
                        # 항목코드 필터링
                        if item_code and it.get("ITEM_CODE1") != item_code:
                            continue
                        tm = it.get("TIME", "")
                        val = safe_float(it.get("DATA_VALUE"))
                        if tm and val is not None:
                            if freq == "M" and len(tm) == 6:
                                tm = tm + "01"
                            data.append((tm.replace("-", ""), val))
                    df = pd.DataFrame(data, columns=["Date", key])
                    return df
                except Exception:
                    pass
            return pd.DataFrame(columns=["Date", key])
        if key in FRED_CODES:
            series = FRED_CODES[key]
            params = {
                "series_id": series,
                "api_key": FRED_KEY,
                "file_type": "json",
                "observation_start": "2010-01-01",
            }
            for _ in range(2):  # 최대 2회 시도
                try:
                    r = safe_request("get", FRED_API, params=params, timeout=3)
                    obs = r.json().get("observations", [])
                    data = []
                    for it in obs:
                        dt = it.get("date", "")
                        val = safe_float(it.get("value"))
                        if dt and val is not None:
                            dt = dt.replace("-", "")
                            data.append((dt, val))
                    df = pd.DataFrame(data, columns=["Date", key])
                    return df
                except Exception:
                    pass
            return pd.DataFrame(columns=["Date", key])
        return pd.DataFrame(columns=["Date", key])

    def get(self, key: str, date: str) -> Optional[float]:
        if key not in self.mem:
            self._load(key, date)  # 필요한 날짜 전달
        df = self.mem.get(key)
        if df is None or df.empty:
            return None
        row = df[df["Date"] <= date].tail(1)
        if row.empty:
            return None
        return safe_float(row.iloc[-1, 1])


# --------------------------------------------------------------------------- #
# 수집기
# --------------------------------------------------------------------------- #
@dataclass
class CollectConfig:
    code: str
    date: str  # YYYYMMDD


class V52Collector:
    def __init__(self, cfg: CollectConfig):
        self.cfg = cfg
        self.code = cfg.code.zfill(6)
        self.date = cfg.date
        self.res: Dict[str, Optional[float]] = {k: None for k in V52_COLS}
        self.res.update({
            "date": self.date,
            "code": self.code,
            "listing_status": "Listed",
            "adj_factor": 1.0,
        })
        self.macro = MacroFetcher()
        self.dart_key = read_dart_key()

    # --------------------------- PyKRX --------------------------- #
    def step_price_flow(self):
        dt = self.date
        try:
            df = stock.get_market_ohlcv_by_date(dt, dt, self.code)
            if not df.empty:
                r = df.iloc[0]
                self.res["open"] = safe_float(r.get("시가"))
                self.res["high"] = safe_float(r.get("고가"))
                self.res["low"] = safe_float(r.get("저가"))
                self.res["close"] = safe_float(r.get("종가"))
                self.res["volume"] = safe_float(r.get("거래량"))
                self.res["amount"] = safe_float(r.get("거래대금"))
                if self.res["amount"] and self.res["volume"]:
                    self.res["vwap"] = self.res["amount"] / self.res["volume"]
        except Exception:
            pass

        try:
            # detail=True: 투신/연기금/금융투자 세부 분류 포함
            df_val = stock.get_market_trading_value_by_date(dt, dt, self.code, detail=True)
            if not df_val.empty:
                r = df_val.iloc[0]
                self.res["frgn_net_amt"] = safe_float(r.get("외국인") or r.get("외국인합계"))
                # 기관합계 = 금융투자+보험+투신+사모+은행+기타금융+연기금
                inst_cols = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"]
                inst_sum = sum(safe_float(r.get(c)) or 0 for c in inst_cols)
                self.res["inst_net_amt"] = inst_sum if inst_sum != 0 else safe_float(r.get("기관합계"))
                self.res["nps_net_amt"] = safe_float(r.get("연기금"))
                self.res["tust_net_amt"] = safe_float(r.get("투신"))
                self.res["dealer_net_amt"] = safe_float(r.get("금융투자"))
            
            df_vol = stock.get_market_trading_volume_by_date(dt, dt, self.code, detail=True)
            if not df_vol.empty:
                r = df_vol.iloc[0]
                self.res["frgn_net_qty"] = safe_float(r.get("외국인") or r.get("외국인합계"))
                inst_cols = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금"]
                inst_sum = sum(safe_float(r.get(c)) or 0 for c in inst_cols)
                self.res["inst_net_qty"] = inst_sum if inst_sum != 0 else safe_float(r.get("기관합계"))
                self.res["nps_net_qty"] = safe_float(r.get("연기금"))
        except Exception:
            pass

        try:
            dfs = stock.get_shorting_status_by_date(dt, dt, self.code)
            if not dfs.empty:
                r = dfs.iloc[0]
                self.res["short_sell_amt"] = safe_float(r.get("거래대금"))
                self.res["short_sell_qty"] = safe_float(r.get("거래량"))
                self.res["loan_balance_amt"] = safe_float(r.get("잔고금액"))
                self.res["loan_balance_qty"] = safe_float(r.get("잔고수량"))
        except Exception:
            pass

    def step_meta_pykrx(self):
        try:
            name = stock.get_market_ticker_name(self.code)
            if name:
                self.res["name"] = name
        except Exception:
            pass
        try:
            # 시장 판단: 해당 날짜 KOSPI 리스트 조회
            kospi = stock.get_market_ticker_list(self.date, market="KOSPI")
            self.res["market"] = "KOSPI" if self.code in kospi else "KOSDAQ"
        except Exception:
            pass

    # --------------------------- Naver / FnGuide --------------------------- #
    def step_naver_meta(self):
        url = f"https://finance.naver.com/item/main.naver?code={self.code}"
        try:
            resp = safe_request("get", url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return
            html = resp.text
            import re
            
            # 상장주식수
            m_sh = re.search(r"상장주식수[^0-9]*([0-9,]+)", html)
            if m_sh:
                self.res["shares_out"] = safe_float(m_sh.group(1))
            # 시가총액
            m_mc = re.search(r"시가총액[^0-9]*([0-9,]+)", html)
            if m_mc:
                self.res["market_cap"] = safe_float(m_mc.group(1))
            # 섹터코드
            m_sec = re.search(r"sectorCode=([0-9]+)", html)
            if m_sec:
                self.res["sector_code"] = m_sec.group(1)
            # 섹터명
            m_sec_nm = re.search(r'([가-힣A-Za-z0-9\&\s\(\)\-\/]+)\s+업종', html)
            if m_sec_nm and not self.res.get("sector_name"):
                self.res["sector_name"] = m_sec_nm.group(1).strip()
            # 종목명 (HTML 내 stockName)
            m_nm = re.search(r"stockName\\s*=\\s*'([^']+)'", html)
            if m_nm:
                self.res["name"] = m_nm.group(1).strip()
            
            # 주당배당금 (여러 패턴 시도)
            m_div = re.search(r"주당배당금[^0-9]*([0-9,]+)\s*원", html)
            if m_div:
                self.res["div_amount"] = safe_float(m_div.group(1))
            
            # 배당락일 (여러 패턴 시도)
            m_ex = re.search(r"배당락[^0-9]*(\d{4})[.\-/]?(\d{2})[.\-/]?(\d{2})", html)
            if m_ex:
                self.res["ex_div_date"] = f"{m_ex.group(1)}{m_ex.group(2)}{m_ex.group(3)}"
            
            # 부채비율
            m_debt = re.search(r"부채비율[^0-9]*([0-9,.]+)\s*%", html)
            if m_debt:
                self.res["debt_ratio"] = safe_float(m_debt.group(1))
        except Exception:
            pass

    def step_fnguide_sector(self):
        try:
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{self.code}"
            resp = safe_request("get", url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
            if resp.status_code != 200:
                return
            import re
            m = re.search(r'<span class="stxt stxt2">([^<]+)</span>', resp.text)
            if m:
                self.res["sector_name"] = m.group(1).strip()
        except Exception:
            pass

    def step_naver_finance(self):
        """
        Naver finance JSON (mobile)에서 EPS/BPS/부채비율/배당금 등을 보완.
        """
        try:
            url = f"https://m.stock.naver.com/api/stock/{self.code}/finance"
            resp = safe_request("get", url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return
            j = resp.json()
            finance = j.get("finance", {})
            annual = finance.get("annual", [])
            if annual:
                row = annual[-1]
                def set_num(k_res, keys):
                    for k in keys:
                        if k in row:
                            v = safe_float(row[k])
                            if v is not None:
                                self.res[k_res] = v
                                return
                set_num("eps", ["EPS", "eps"])
                set_num("bps", ["BPS", "bps"])
                set_num("div_amount", ["DPS", "dps", "cashDividend", "cash_dvdn", "dpsAmt"])
                set_num("debt_ratio", ["debtRatio", "debt_ratio", "부채비율"])
                set_num("revenue", ["revenue", "sales", "매출액"])
                set_num("op_income", ["opIncome", "operatingIncome", "영업이익"])
                set_num("net_income", ["netIncome", "당기순이익"])
                set_num("total_equity", ["equity", "equityAmt"])
                set_num("total_assets", ["assets", "assetsAmt"])
        except Exception:
            pass

    def step_naver_finance_table(self):
        """
        Naver finance HTML 테이블(pandas.read_html)로 재무/현금흐름/배당/부채비율 보완.
        """
        url = f"https://finance.naver.com/item/main.naver?code={self.code}"
        try:
            dfs = pd.read_html(url, encoding="euc-kr")
        except Exception:
            return

        def set_if_empty(key: str, val: Optional[float]):
            if val is not None and (self.res.get(key) is None):
                self.res[key] = val

        for df in dfs:
            if df.empty:
                continue
            # '주요재무정보' 표 감지
            cols_str = " ".join(map(str, df.columns))
            if "주요재무정보" in cols_str:
                try:
                    # 첫 열을 항목명으로 사용
                    first_col = df.columns[0]
                    if isinstance(first_col, tuple):
                        first_col = first_col[0]
                    
                    # 항목명 → 행 매핑
                    item_map = {}
                    for idx, row in df.iterrows():
                        item_name = str(row.iloc[0])
                        # 마지막 숫자 열 (NaN 아닌 것) 찾기
                        for col_idx in range(len(row)-1, 0, -1):
                            val = safe_float(row.iloc[col_idx])
                            if val is not None:
                                item_map[item_name] = val
                                break
                    
                    # 매핑
                    set_if_empty("revenue", item_map.get("매출액"))
                    set_if_empty("op_income", item_map.get("영업이익"))
                    set_if_empty("net_income", item_map.get("당기순이익"))
                    set_if_empty("div_amount", item_map.get("주당배당금(원)"))
                    set_if_empty("eps", item_map.get("EPS(원)"))
                    set_if_empty("bps", item_map.get("BPS(원)"))
                    set_if_empty("debt_ratio", item_map.get("부채비율"))
                    set_if_empty("roe", item_map.get("ROE(지배주주)"))
                except Exception:
                    continue
        # announce_date / earnings_date 보완 (가장 최근 재무연도 추정)
        try:
            if not self.res.get("announce_date") and self.res.get("revenue"):
                # 재무 데이터가 있는 경우 해당 연도 0331로 가정
                yr = int(self.date[:4])
                self.res["announce_date"] = f"{yr}0331"
            if not self.res.get("earnings_date") and self.res.get("revenue"):
                yr = int(self.date[:4])
                self.res["earnings_date"] = f"{yr}0331"
        except Exception:
            pass

    # --------------------------- DART --------------------------- #
    def step_dart_sector(self):
        """DART company.json에서 sector_code (induty_code) 조회"""
        if not self.dart_key:
            return
        
        corp_code = get_corp_code(self.code)
        if not corp_code:
            return
        
        try:
            url = "https://opendart.fss.or.kr/api/company.json"
            params = {"crtfc_key": self.dart_key, "corp_code": corp_code}
            r = safe_request("get", url, params=params, timeout=6)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "000":
                    induty_code = data.get("induty_code")
                    if induty_code:
                        self.res["sector_code"] = induty_code
        except Exception:
            pass
    
    def step_dart_finance(self):
        """DART 재무제표 API (corp_code 동적 매핑)"""
        if not self.dart_key:
            return
        
        corp_code = get_corp_code(self.code)
        if not corp_code:
            return
        
        year = int(self.date[:4])
        # 최근 2개년 x (1,2,3,4분기/사업보고서) 순회
        for y in range(year, year - 2, -1):
            for rc in ["11013", "11012", "11014", "11011"]:
                try:
                    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                    params = {
                        "crtfc_key": self.dart_key,
                        "corp_code": corp_code,
                        "bsns_year": str(y),
                        "reprt_code": rc,
                        "fs_div": "CFS",
                    }
                    r = safe_request("get", url, params=params, timeout=6).json()
                    if r.get("status") != "000" or not r.get("list"):
                        continue
                    # 2024+ 버전 (ifrs-full_) + 2015 이전 버전 (ifrs_) 모두 매핑
                    amap = {
                        # Revenue
                        "ifrs-full_Revenue": "revenue",
                        "ifrs_Revenue": "revenue",
                        # Operating Income
                        "ifrs-full_ProfitLossFromOperatingActivities": "op_income",
                        "dart_OperatingIncomeLoss": "op_income",
                        # Net Income
                        "ifrs-full_ProfitLoss": "net_income",
                        "ifrs_ProfitLoss": "net_income",
                        # Equity
                        "ifrs-full_Equity": "total_equity",
                        "ifrs_Equity": "total_equity",
                        # Assets
                        "ifrs-full_Assets": "total_assets",
                        "ifrs_Assets": "total_assets",
                        # Cash Flow - Operating
                        "ifrs-full_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
                        "ifrs_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
                        # Cash Flow - Investing
                        "ifrs-full_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
                        "ifrs_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
                        # Cash Flow - Financing
                        "ifrs-full_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
                        "ifrs_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
                        # EPS
                        "ifrs-full_EarningsPerShare": "eps",
                        "ifrs_BasicEarningsLossPerShare": "eps",
                    }
                    # DART는 동일 계정에 여러 값 반환 (연결/별도/자회사)
                    # 가장 큰 값 = 연결재무제표 기준
                    for item in r["list"]:
                        aid = item.get("account_id", "")
                        val = safe_float(item.get("thstrm_amount", ""))
                        if aid in amap and val is not None:
                            key = amap[aid]
                            # 기존값보다 큰 경우만 업데이트 (연결재무제표 우선)
                            existing = self.res.get(key)
                            if existing is None or abs(val) > abs(existing):
                                self.res[key] = val
                    # 파생
                    if self.res.get("net_income") and self.res.get("total_equity"):
                        te = self.res["total_equity"]
                        if te and te != 0:
                            self.res["roe"] = (self.res["net_income"] / te) * 100
                    return
                except Exception:
                    backoff_sleep(0.5)

    # --------------------------- 매크로 (yfinance + ECOS) --------------------------- #
    def step_macro(self):
        """yfinance에서 매크로 데이터 수집 (API 키 불필요, 10년+ 가능)"""
        macro_keys = ["us10y_yield", "wti", "gold", "dxy", "usdkrw", "cnykrw", "vix"]
        for key in macro_keys:
            self.res[key] = get_macro_value(key, self.date)
        
        # kr10y_yield는 ECOS에서 직접 조회 (yfinance 미지원)
        self.res["kr10y_yield"] = self._fetch_kr10y_yield()
    
    def _fetch_kr10y_yield(self) -> Optional[float]:
        """ECOS API에서 국고채 10년 금리 직접 조회"""
        if not ECOS_KEY:
            return None
        
        # 해당 날짜 기준 최근 30일 범위 조회
        import datetime
        try:
            dt = datetime.datetime.strptime(self.date, "%Y%m%d")
            start = (dt - datetime.timedelta(days=30)).strftime("%Y%m%d")
            end = self.date
        except:
            return None
        
        url = f"{ECOS_API}/{ECOS_KEY}/json/kr/1/100/817Y002/D/{start}/{end}"
        try:
            r = safe_request("get", url, timeout=15)
            rows = r.json().get("StatisticSearch", {}).get("row", [])
            # 국고채 10년 (010210000) 필터링
            for row in reversed(rows):
                if row.get("ITEM_CODE1") == "010210000":
                    return safe_float(row.get("DATA_VALUE"))
        except Exception:
            pass
        return None

    # --------------------------- 실행 --------------------------- #
    def run(self) -> Dict[str, Optional[float]]:
        self.step_price_flow()
        self.step_meta_pykrx()
        self.step_naver_meta()
        self.step_fnguide_sector()
        self.step_dart_sector()      # DART sector_code
        self.step_dart_finance()
        self.step_macro()            # yfinance 매크로
        self.step_naver_finance()
        self.step_naver_finance_table()

        # 기본값: amount / shares_out / market_cap / vwap 보정
        if (self.res.get("amount") is None) and self.res.get("close") and self.res.get("volume"):
            self.res["amount"] = self.res["close"] * self.res["volume"]
        if not self.res.get("market_cap") and self.res.get("close") and self.res.get("shares_out"):
            self.res["market_cap"] = self.res["close"] * self.res["shares_out"]
        if not self.res.get("shares_out") and self.res.get("close") and self.res.get("market_cap"):
            c = self.res["close"]
            if c:
                self.res["shares_out"] = self.res["market_cap"] / c
        if not self.res.get("vwap") and self.res.get("amount") and self.res.get("volume"):
            vol = self.res["volume"]
            if vol:
                self.res["vwap"] = self.res["amount"] / vol
        
        # 자본총계 = BPS * 주식수 (DART 실패 시 백업)
        if not self.res.get("total_equity") and self.res.get("bps") and self.res.get("shares_out"):
            self.res["total_equity"] = self.res["bps"] * self.res["shares_out"]
        
        # 자산총계 = 자본 * (1 + 부채비율/100) (DART 실패 시 백업)
        if not self.res.get("total_assets") and self.res.get("total_equity") and self.res.get("debt_ratio"):
            dr = self.res["debt_ratio"]
            if dr:
                self.res["total_assets"] = self.res["total_equity"] * (1 + dr / 100)

        # 이름 깨짐 보정: Naver/FnGuide 우선
        if not self.res.get("name") or "�" in str(self.res.get("name")):
            # FnGuide sector_name에서 코드가 들어갈 수 있어 name만은 Naver 우선
            try:
                url = f"https://m.stock.naver.com/api/stock/{self.code}/basic"
                r = requests.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code == 200:
                    nm = r.json().get("stockName")
                    if nm:
                        self.res["name"] = nm
            except Exception:
                pass
        # 이벤트 기본값 최소화
        return self.res

    # --------------------------- 매크로 API (ECOS/FRED) --------------------------- #
    def _macro_api(self, key: str, date: str) -> Optional[float]:
        # ECOS 일/월 자료
        if key in ECOS_CODES:
            code, freq = ECOS_CODES[key]
            # ECOS는 월/일 데이터; 요청 범위를 date 포함으로 설정
            start = end = date if freq == "D" else date[:6]
            url = f"{ECOS_API}/{ECOS_KEY}/json/kr/1/10/{code}/{freq}/{start}/{end}"
            r = safe_request("get", url, timeout=6)
            if r.status_code == 200:
                try:
                    rows = r.json().get("StatisticSearch", {}).get("row", [])
                    if rows:
                        val = safe_float(rows[-1].get("DATA_VALUE"))
                        if val is not None:
                            return val
                except Exception:
                    pass
        # FRED 일 자료
        if key in FRED_CODES:
            series = FRED_CODES[key]
            params = {
                "series_id": series,
                "api_key": FRED_KEY,
                "file_type": "json",
                "limit": 1,
                "sort_order": "desc",
                "observation_start": f"{date[:4]}-{date[4:6]}-{date[6:]}",
            }
            r = safe_request("get", FRED_API, params=params, timeout=6)
            if r.status_code == 200:
                try:
                    obs = r.json().get("observations", [])
                    if obs:
                        return safe_float(obs[0].get("value"))
                except Exception:
                    pass
        return None

