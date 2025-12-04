#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
collector_full.py
HOJ_DB 확장 수집기 (PER/PBR/수급/섹터/거시) - Resume/Fallback/연도확장 지원

실행 예시:
  python collector_full.py --from-year 2015 --to-year 2015 --root "F:\autostockG" --resume --prefer kiwoom,kis,pykrx,macro --universe auto

필요 패키지:
  pip install pandas pyarrow requests tqdm yfinance pykrx

설계 요약:
- 하루(영업일) 단위로 Date=YYYY-MM-DD 별 parquet 저장 → 재실행 시 존재하면 스킵(Resume).
- 소스 우선순위(Fallback): kiwoom → kis → pykrx → macro.
- 유니버스: pykrx로 해당일 상장리스트 자동, 실패 시 CSV 제공 (--universe file --universe-file path).
- 스키마: PER/PBR/EPS/BPS/ROE/시총/주식수 + 수급(기관/외국인/연기금/누적) + 섹터 + 거시(KOSDAQ/USDKRW/WTI/KR10Y/VIX).
- 각 어댑터는 가능한 항목만 채우고, 없는 컬럼은 NaN으로 유지(후속 파이프라인에서 ffill/0 규칙 적용).

중요:
- Kiwoom/KIS 어댑터는 실제 API 연동부를 작성해야 함(주석의 TODO 위치). 이 파일은 구조/흐름/예외/재시작을 포함한 완전 수집기.
- PyKRX/매크로는 즉시 동작 가능한 보조 수집 경로를 포함.
"""

import os
import sys
import json
import time
import argparse
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import pandas as pd
from tqdm import tqdm

# -----------------------------
# 로깅 & 파일 유틸
# -----------------------------

def log(msg: str):
    print(time.strftime("[%Y-%m-%d %H:%M:%S] "), msg, flush=True)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def atomic_save_parquet(df: pd.DataFrame, path: str):
    ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        log(f"SKIP (exists): {path}")
        return
    tmp = path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    log(f"SAVED: {path}")

def business_days_kr(start: str, end: str) -> List[pd.Timestamp]:
    # 주말만 제외. 한국 휴장일 달력 필요 시 교체.
    return list(pd.bdate_range(start=start, end=end))

# -----------------------------
# 공통 인터페이스
# -----------------------------

class SourceAdapter:
    name: str = "base"
    def available(self) -> bool:
        return True
    def fetch_fundamental(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        return None
    def fetch_supply(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        return None
    def fetch_sector(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        return None
    def fetch_macro(self, date: str) -> Optional[pd.DataFrame]:
        return None
    def fetch_universe(self, date_yyyymmdd: str) -> Optional[List[str]]:
        return None

# -----------------------------
# 키움 어댑터 (자리표시자 + 구조 완비)
# -----------------------------

class KiwoomAdapter(SourceAdapter):
    name = "kiwoom"
    def __init__(self):
        # TODO: 실제 토큰/엔드포인트 설정
        self.base_url = os.environ.get("KIWOOM_BASE_URL", "")  # 예: https://api.kiwoom.com
        self.token = os.environ.get("KIWOOM_TOKEN", "")
    def available(self) -> bool:
        return bool(self.base_url and self.token)

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def fetch_universe(self, date_yyyymmdd: str) -> Optional[List[str]]:
        # TODO: 해당일 상장 종목 리스트 API가 있으면 구현
        return None

    def fetch_fundamental(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: PER/PBR/EPS/BPS/ROE/시총/주식수 일자 스냅샷 불러오기
        # 반환 스키마: Date, Code, PER, PBR, EPS, BPS, ROE, MktCap, SharesOut
        return None

    def fetch_supply(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: 투자자별 수급(기관/외국인/연기금) 일자별 종목 데이터
        # 반환: Date, Code, Inst_Net_Qty, Frgn_Net_Qty, NPS_Net_Qty, Inst_Net_Amt, Frgn_Net_Amt
        return None

    def fetch_sector(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: 종목→섹터 매핑 + 섹터 1일 수익률/평균 PER·PBR/거래대금
        # 반환: Date, Code, SectorCode, SectorRet_1D, Sector_PER_Avg, Sector_PBR_Avg, Sector_Turnover
        return None

# -----------------------------
# 한국투자증권 어댑터 (자리표시자 + 구조 완비)
# -----------------------------

class KISAdapter(SourceAdapter):
    name = "kis"
    def __init__(self):
        self.base_url = os.environ.get("KIS_BASE_URL", "")   # 예: https://openapi.koreainvestment.com
        self.appkey = os.environ.get("KIS_APPKEY", "")
        self.appsecret = os.environ.get("KIS_APPSECRET", "")
        self.token = os.environ.get("KIS_TOKEN", "")
    def available(self) -> bool:
        return bool(self.base_url and self.appkey and self.appsecret and self.token)

    def _headers(self):
        return {
            "authorization": f"Bearer {self.token}",
            "appkey": self.appkey,
            "appsecret": self.appsecret,
            "content-type": "application/json"
        }

    def fetch_universe(self, date_yyyymmdd: str) -> Optional[List[str]]:
        # TODO: 상장/거래가능 종목 조회 구현 (있을 경우)
        return None

    def fetch_fundamental(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: PER/PBR/EPS/BPS/ROE/시총/주식수
        return None

    def fetch_supply(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: 투자자별 거래/수급 (종목)
        return None

    def fetch_sector(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # TODO: 섹터 메타/수익률/평균 밸류/거래대금
        return None

# -----------------------------
# PyKRX 어댑터 (즉시 사용 가능한 보조 구현)
# -----------------------------

class PyKRXAdapter(SourceAdapter):
    name = "pykrx"
    def __init__(self):
        try:
            from pykrx import stock as _S  # noqa
            self._S = _S
            self.ok = True
        except Exception:
            self._S = None
            self.ok = False

    def available(self) -> bool:
        return self.ok

    def fetch_universe(self, date_yyyymmdd: str) -> Optional[List[str]]:
        try:
            lst = self._S.get_market_ticker_list(date_yyyymmdd, market="ALL")
            # 티커는 '005930' 같은 6자리
            return lst
        except Exception:
            return None

    def fetch_fundamental(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # pykrx의 시장 전체 Fundamental을 불러와서 Codes 필터링
        # 반환 컬럼은 환경에 따라 다름: PER, PBR, EPS, BPS, DIV, DPS 등
        try:
            yyyymmdd = date.replace("-", "")
            kospi = self._S.get_market_fundamental_by_ticker(yyyymmdd, market="KOSPI")
            kosdaq = self._S.get_market_fundamental_by_ticker(yyyymmdd, market="KOSDAQ")
            df = pd.concat([kospi, kosdaq], axis=0)
            df = df.reset_index().rename(columns={"티커":"Code"})
            df["Date"] = date
            # 필요한 컬럼만 추출(없는 경우 NaN 유지)
            keep_map = {
                "PER": ["PER"],
                "PBR": ["PBR"],
                "EPS": ["EPS"],
                "BPS": ["BPS"],
                # 시총/주식수는 pykrx 별도 함수 필요(보조로 채움)
            }
            out = df[["Date","Code"] + [c for c in keep_map if any(k in df.columns for k in keep_map[c])]].copy()
            # 시총/주식수 보조 채움
            try:
                cap = self._S.get_market_cap_by_ticker(yyyymmdd, market="ALL").reset_index().rename(columns={"티커":"Code"})
                cap = cap[["Code","시가총액","상장주식수"]].rename(columns={"시가총액":"MktCap","상장주식수":"SharesOut"})
                out = out.merge(cap, on="Code", how="left")
                out["Date"] = date
            except Exception:
                out["MktCap"] = pd.NA
                out["SharesOut"] = pd.NA
            # ROE는 직접 계산 불가 → NaN 유지
            return out
        except Exception:
            return None

    def fetch_supply(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # 투자자별 수급 (종목별): pykrx는 함수 지원이 제한적. 가능한 범위에서 작성.
        # 여기서는 '거래대금/거래량'으로 대체 지표 일부 생성 시도, 없으면 NaN.
        try:
            yyyymmdd = date.replace("-", "")
            # 시장별 거래대금 by ticker
            val_kospi = self._S.get_market_trading_value_by_ticker(yyyymmdd, market="KOSPI").reset_index().rename(columns={"티커":"Code"})
            val_kosdaq = self._S.get_market_trading_value_by_ticker(yyyymmdd, market="KOSDAQ").reset_index().rename(columns={"티커":"Code"})
            val = pd.concat([val_kospi, val_kosdaq], axis=0)
            val["Date"] = date
            # pykrx 결과 컬럼은 '개인','외국인','기관합계' 등. 존재 여부에 따라 유연 처리.
            cols = val.columns.tolist()
            map_cols = {}
            if "기관합계" in cols:
                map_cols["Inst_Net_Amt"] = "기관합계"
            if "외국인" in cols:
                map_cols["Frgn_Net_Amt"] = "외국인"
            # 순매수량은 부재 → 금액만 우선 채움, 수량/연기금은 NaN
            out = val[["Date","Code"] + list(map_cols.values())].rename(columns={v:k for k,v in map_cols.items()})
            if "Inst_Net_Amt" not in out.columns: out["Inst_Net_Amt"] = pd.NA
            if "Frgn_Net_Amt" not in out.columns: out["Frgn_Net_Amt"] = pd.NA
            out["NPS_Net_Qty"] = pd.NA
            out["Inst_Net_Qty"] = pd.NA
            out["Frgn_Net_Qty"] = pd.NA
            # 누적(NaN 유지; 후처리 파이프라인에서 rolling 계산 권장)
            out["Cum5_Net_Qty"] = pd.NA
            out["Cum20_Net_Qty"] = pd.NA
            out["Cum60_Net_Qty"] = pd.NA
            return out
        except Exception:
            return None

    def fetch_sector(self, date: str, codes: List[str]) -> Optional[pd.DataFrame]:
        # pykrx 산업지수 기반으로 근사 섹터 수익률 구성 (정교한 매핑은 별도 테이블 필요)
        try:
            yyyymmdd = date.replace("-", "")
            # 산업지수 티커 리스트
            idx = self._S.get_index_ticker_list(date=yyyymmdd, market="KOSPI")
            if not idx:
                return None
            # 산업지수 수익률 (전일 대비)
            pr = self._S.get_index_price_change_by_ticker(fromdate=yyyymmdd, todate=yyyymmdd)
            pr = pr.reset_index().rename(columns={"티커":"SectorCode", "등락률":"SectorRet_1D"})
            pr["Date"] = date
            # Code 매핑이 없으므로, 일단 종목 단위로 브로드캐스트(후속 정교화 필요)
            out = pd.DataFrame({"Date":[date]*len(codes), "Code":codes})
            # 대표 산업지수 수익률만 공통 적용(임시) → 실제 운영 시 종목→섹터코드 매핑 테이블과 join
            if not pr.empty:
                # 임시: 첫 번째 산업지수의 수익률을 브로드캐스트
                sector_ret = float(pr["SectorRet_1D"].iloc[0]) if not pd.isna(pr["SectorRet_1D"].iloc[0]) else pd.NA
            else:
                sector_ret = pd.NA
            out["SectorCode"] = pd.NA
            out["SectorRet_1D"] = sector_ret
            out["Sector_PER_Avg"] = pd.NA
            out["Sector_PBR_Avg"] = pd.NA
            out["Sector_Turnover"] = pd.NA
            return out
        except Exception:
            return None

# -----------------------------
# 매크로 어댑터 (yfinance 기반 캐시형)
# -----------------------------

class MacroAdapter(SourceAdapter):
    name = "macro"
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        ensure_dir(self.cache_dir)
        try:
            import yfinance as yf  # noqa
            self.yf_ok = True
        except Exception:
            self.yf_ok = False

    def available(self) -> bool:
        return self.yf_ok

    def _cache_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.parquet")

    def _load_or_download(self, ticker: str, key: str, start="2010-01-01", end=None) -> Optional[pd.DataFrame]:
        path = self._cache_path(key)
        if os.path.exists(path):
            try:
                return pd.read_parquet(path)
            except Exception:
                pass
        try:
            import yfinance as yf
            df = yf.download(ticker, start=start, end=end or dt.date.today().isoformat(), progress=False)
            if df is None or df.empty:
                return None
            # 일자/종가만
            df = df.reset_index()[["Date","Adj Close"]].rename(columns={"Adj Close": key})
            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
            df.to_parquet(path, index=False)
            return df
        except Exception:
            return None

    def fetch_macro(self, date: str) -> Optional[pd.DataFrame]:
        # KOSDAQ 수익률(1D): KOSDAQ 지수 대체 티커를 찾기 어려워, 일단 NaN.
        # 환율 USDKRW: "KRW=X"
        # WTI: "CL=F"
        # VIX: "^VIX"
        # KR10Y: 한국 10년 금리는 YF에서 "^KS10Y" 등의 티커가 없을 수 있음 → NaN
        usdkrw = self._load_or_download("KRW=X", "USDKRW")
        wti    = self._load_or_download("CL=F",  "WTI")
        vix    = self._load_or_download("^VIX",  "VIX")
        # KOSDAQ, KR10Y는 일단 결측 처리(사용 환경에서 대체 소스 연결 권장)
        rows = {"Date":[date], "KOSDAQ_Ret_1D":[pd.NA], "USDKRW":[pd.NA], "WTI":[pd.NA], "KR10Y":[pd.NA], "VIX":[pd.NA]}
        out = pd.DataFrame(rows)
        for key, df in [("USDKRW", usdkrw), ("WTI", wti), ("VIX", vix)]:
            if df is None or df.empty:
                continue
            ser = df.loc[df["Date"]==date, key]
            if not ser.empty:
                out.loc[0, key] = ser.iloc[0]
        return out

# -----------------------------
# 유니버스 결정
# -----------------------------

def load_universe(date: str, mode: str, path: Optional[str], adapters: Dict[str, SourceAdapter], prefer: List[str]) -> List[str]:
    if mode == "file" and path and os.path.exists(path):
        s = pd.read_csv(path)
        return s.iloc[:,0].astype(str).str.zfill(6).tolist()
    # 우선순위에 따라 소스에서 유니버스 조회
    yyyymmdd = date.replace("-", "")
    for src in prefer:
        ad = adapters.get(src)
        if ad is None:
            continue
        try:
            u = ad.fetch_universe(yyyymmdd)
            if u:
                return [str(x).zfill(6) for x in u]
        except Exception:
            continue
    # 최후: 샘플
    return ["005930","000660","035420"]

# -----------------------------
# 표준 스키마
# -----------------------------

STD_COLS = [
    "Date","Code",
    # 가치
    "PER","PBR","EPS","BPS","ROE","MktCap","SharesOut",
    # 수급
    "Inst_Net_Qty","Frgn_Net_Qty","NPS_Net_Qty",
    "Inst_Net_Amt","Frgn_Net_Amt",
    "Cum5_Net_Qty","Cum20_Net_Qty","Cum60_Net_Qty",
    # 섹터
    "SectorCode","SectorRet_1D","Sector_PER_Avg","Sector_PBR_Avg","Sector_Turnover",
    # 거시
    "KOSDAQ_Ret_1D","USDKRW","WTI","KR10Y","VIX"
]

def empty_day_frame(date: str, codes: List[str]) -> pd.DataFrame:
    base = pd.DataFrame({"Date":[date]*len(codes), "Code":codes})
    for c in STD_COLS[2:]:
        base[c] = pd.NA
    return base

def safe_merge(left: pd.DataFrame, right: Optional[pd.DataFrame], on=["Date","Code"]) -> pd.DataFrame:
    if right is None or (hasattr(right, "empty") and right.empty):
        return left
    # 보정: 필요한 컬럼만 유지
    keep = set(["Date","Code"]).union(set([c for c in STD_COLS if c not in ["Date","Code"]]))
    cols = [c for c in right.columns if c in keep]
    if "Date" not in cols: right["Date"] = left["Date"].iloc[0]
    if "Code" not in cols and "Code" in left.columns: pass
    r = right[["Date","Code"] + [c for c in cols if c not in ["Date","Code"]]].copy()
    return left.merge(r, on=on, how="left")

# -----------------------------
# 수집기
# -----------------------------

@dataclass
class CollectorConfig:
    root: str
    year_from: int
    year_to: int
    prefer: List[str]
    resume: bool
    universe_mode: str
    universe_file: Optional[str]

class Collector:
    def __init__(self, cfg: CollectorConfig):
        self.cfg = cfg
        macro_cache = os.path.join(cfg.root, "MODELENGINE", "CACHE", "MACRO")
        self.adapters: Dict[str, SourceAdapter] = {
            "kiwoom": KiwoomAdapter(),
            "kis": KISAdapter(),
            "pykrx": PyKRXAdapter(),
            "macro": MacroAdapter(macro_cache),
        }
        self.active_order = [s for s in cfg.prefer if s in self.adapters]
        self.base_out = os.path.join(cfg.root, "MODELENGINE", "RAW", "EXTERNAL")
        ensure_dir(self.base_out)

    def run(self):
        for y in range(self.cfg.year_from, self.cfg.year_to + 1):
            self._run_year(y)

    def _run_year(self, year: int):
        out_dir = os.path.join(self.base_out, str(year))
        ensure_dir(out_dir)
        state_path = os.path.join(out_dir, "_state.json")
        state = self._load_state(state_path)

        start = f"{year}-01-02"
        end   = f"{year}-12-31"
        days = business_days_kr(start, end)
        log(f"[YEAR {year}] {days[0].date()} ~ {days[-1].date()} | ~{len(days)} days")

        # 소스 가용성 로그
        for s in self.active_order:
            ok = self.adapters[s].available()
            log(f"Source [{s}] available={ok}")

        for dts in tqdm(days, desc=f"Collect {year}"):
            d = dts.strftime("%Y-%m-%d")
            out_path = os.path.join(out_dir, f"{d}.parquet")
            if self.cfg.resume and (state.get("done", {}).get(d) or os.path.exists(out_path)):
                continue

            # 유니버스
            codes = load_universe(d, self.cfg.universe_mode, self.cfg.universe_file, self.adapters, self.active_order)
            if not codes:
                log(f"NO UNIVERSE on {d}, skip")
                continue

            df = empty_day_frame(d, codes)

            # Fundamental
            df = self._collect_block(df, d, codes, kind="fundamental")

            # Supply
            df = self._collect_block(df, d, codes, kind="supply")

            # Sector
            df = self._collect_block(df, d, codes, kind="sector")

            # Macro (Date 단위 조인)
            mdf = self._collect_macro(d)
            if mdf is not None and not mdf.empty:
                df = df.merge(mdf, on="Date", how="left")

            atomic_save_parquet(df, out_path)
            state.setdefault("done", {})[d] = True
            self._save_state(state_path, state)

        log(f"[YEAR {year}] Done.")

    def _collect_block(self, base: pd.DataFrame, date: str, codes: List[str], kind: str) -> pd.DataFrame:
        merged = base.copy()
        for src in self.active_order:
            ad = self.adapters[src]
            if not ad.available():
                continue
            try:
                if kind == "fundamental":
                    part = ad.fetch_fundamental(date, codes)
                elif kind == "supply":
                    part = ad.fetch_supply(date, codes)
                elif kind == "sector":
                    part = ad.fetch_sector(date, codes)
                else:
                    part = None
                if part is not None and not part.empty:
                    log(f"COLLECT {kind} from {src} ({len(part)} rows)")
                    merged = safe_merge(merged, part)
                    return merged
            except Exception as e:
                log(f"ERR {src}:{kind}@{date} -> {e}")
                continue
        # 아무 소스도 못 채우면 그대로 반환(스키마 유지)
        return merged

    def _collect_macro(self, date: str) -> Optional[pd.DataFrame]:
        for src in self.active_order:
            ad = self.adapters[src]
            try:
                part = ad.fetch_macro(date)
                if part is not None and not part.empty:
                    log(f"COLLECT macro from {src}")
                    need = ["Date","KOSDAQ_Ret_1D","USDKRW","WTI","KR10Y","VIX"]
                    for c in need:
                        if c not in part.columns:
                            part[c] = pd.NA
                    return part[need]
            except Exception as e:
                log(f"ERR {src}:macro@{date} -> {e}")
                continue
        # 최소 스키마 반환
        return pd.DataFrame({"Date":[date], "KOSDAQ_Ret_1D":[pd.NA], "USDKRW":[pd.NA], "WTI":[pd.NA], "KR10Y":[pd.NA], "VIX":[pd.NA]})

    def _load_state(self, path: str) -> Dict[str, Any]:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self, path: str, state: Dict[str, Any]):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

# -----------------------------
# CLI
# -----------------------------

def parse_args():
    p = argparse.ArgumentParser(description="HOJ_DB 확장 수집기 (PER/PBR/수급/섹터/거시) - Resume/Fallback")
    p.add_argument("--root", type=str, default="F:\\autostockG", help="프로젝트 루트")
    p.add_argument("--from-year", type=int, default=2015, help="시작 연도")
    p.add_argument("--to-year", type=int, default=2015, help="종료 연도")
    p.add_argument("--prefer", type=str, default="kiwoom,kis,pykrx,macro", help="소스 우선순위, 콤마 구분")
    p.add_argument("--resume", action="store_true", help="재시작 모드")
    p.add_argument("--universe", type=str, default="auto", choices=["auto","file"], help="유니버스 결정 방식")
    p.add_argument("--universe-file", type=str, default=None, help="CSV 경로(universe=file)")
    return p.parse_args()

def main():
    args = parse_args()
    cfg = CollectorConfig(
        root=args.root,
        year_from=args.from_year,
        year_to=args.to_year,
        prefer=[s.strip() for s in args.prefer.split(",") if s.strip()],
        resume=args.resume,
        universe_mode=args.universe,
        universe_file=args.universe_file
    )
    log(f"CFG: years={cfg.year_from}..{cfg.year_to}, root={cfg.root}, prefer={cfg.prefer}, resume={cfg.resume}, universe={cfg.universe_mode}")
    Collector(cfg).run()
    log("DONE.")

if __name__ == "__main__":
    main()
