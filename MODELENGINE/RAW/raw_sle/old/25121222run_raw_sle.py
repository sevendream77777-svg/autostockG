# -*- coding: utf-8 -*-
"""
run_raw_sle.py
- SLE 재무 11컬럼 정밀 수집기 (2016년 1Q 이후)

**데이터 무결성 확보 최종 수정:**
1. **[FIXED] EPS Diff 처리 제외:** 'eps'를 FLOW_COLS_TO_DIFF에서 제거하여 데이터 왜곡을 방지.
2. **[FIXED] 병합 키 (선행 0) 유지:** 'code', 'corp_code'를 CSV 저장 전에 6자리 문자열로 변환하여 선행 0 유실을 방지.
3. Flow Variables의 누적/분기 데이터 형태에 따른 '선택적 차분' 로직 유지.
4. 배당금 미지급 분기는 NaN 유지.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import multiprocessing as mp

import pandas as pd
import requests

# -------------------- 컬럼 정의 -------------------- #
META_COLS = ["corp_code", "bsns_year", "reprt_code"]
FINANCE_COLS = [
    "announce_date",
    "revenue",
    "op_income",
    "net_income",
    "eps",
    "total_assets",
    "total_equity",
    "cash_flow_op",
    "cash_flow_inv",
    "cash_flow_fin",
    "div_amount",
]
# 누적 -> 분기 변환 대상 컬럼 (Flow Variables)
# [FIXED 1: EPS 제외] EPS를 제외하여 분기별 지표가 Diff 처리되는 데이터 오염을 방지.
FLOW_COLS_TO_DIFF = [
    "revenue", "op_income", "net_income",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin"
]

KEEP_COLS = ["date", "code", "period"] + META_COLS + FINANCE_COLS
VERBOSE = os.environ.get("DART_VERBOSE", "0") == "1"

# corp_code XML 후보
DEFAULT_XML_CANDIDATES = [
    Path(__file__).resolve().parent / "dart_corp_list.xml",
    Path(__file__).resolve().parents[2] / "MODELENGINE" / "RAW" / "raw_v48" / "dart_corp_list.xml",
    Path.cwd() / "dart_corp_list.xml",
]

# DART 키 후보 (기존 유지)
DEFAULT_KEY_PATHS = [
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
    Path(__file__).resolve().parents[2] / "opendart_apikey.txt",
]

# -------- 유틸 -------- #
def safe_float(v) -> Optional[float]:
    """문자열에서 쉼표, 퍼센트 기호를 제거하고 float으로 안전하게 변환"""
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none", "-", "nan,"}: # 'nan,' 같은 오류 값 처리 추가
            return None
        return float(s)
    except Exception:
        return None

def _extract_leading_int(name: str) -> int:
    m = re.match(r"^(\d{1,2})", name)
    if m:
        try:
            return int(m.group(1))
        except:
            return 999
    return 999

def _read_lines(path: Path) -> List[str]:
    try:
        if path.exists():
            txt = path.read_text(encoding="utf-8", errors="ignore")
            return [ln.strip() for ln in txt.replace(",", "\n").splitlines() if ln.strip()]
    except:
        pass
    return []

def read_dart_keys_ordered() -> List[str]:
    """환경변수 및 파일에서 DART API 키를 우선순위에 따라 정렬하여 읽어옴"""
    env = os.environ.get("DART_API_KEYS", "")
    candidates = []
    if env.strip():
        for k in env.replace(",", "\n").splitlines():
            k = k.strip()
            if k:
                candidates.append((0, k))
    for p in DEFAULT_KEY_PATHS:
        if p.exists():
            leading = _extract_leading_int(p.name)
            for k in _read_lines(p):
                candidates.append((leading, k))
    candidates_sorted = sorted(enumerate(candidates), key=lambda x: (x[1][0], x[0]))
    uniq = []
    for _, (_, key) in candidates_sorted:
        if key and key not in uniq:
            uniq.append(key)
    return uniq

def ensure_corp_xml_exists() -> bool:
    """corp_code XML 파일 존재 여부 확인"""
    for p in DEFAULT_XML_CANDIDATES:
        if p.exists(): return True
    return False

def http_get_json(url: str, params: dict, tries: int = 3, timeout: int = 8):
    """지정된 URL로 GET 요청을 보내고 JSON 응답을 반환 (재시도 및 오류 처리 포함)"""
    for a in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            # API 제한 또는 서버 오류 시 재시도
            if r.status_code in (429, 500, 502, 503, 504, 403):
                if a == tries - 1: return r.status_code, None
                time.sleep(0.5 * (2 ** (a % 3)))
                continue
            if r.status_code != 200:
                if a == tries - 1: return r.status_code, None
                continue
            return r.status_code, r.json()
        except Exception:
            # 네트워크 오류 시 재시도
            if a == tries - 1: return -1, None
            time.sleep(0.5 * (2 ** (a % 3)))
    return -1, None

_CORP_CODE_MAP: Dict[str, str] = {}
def get_corp_code(stock_code: str) -> Optional[str]:
    """주식 코드로부터 고유 기업 코드 (corp_code)를 조회"""
    sc = stock_code.zfill(6)
    if _CORP_CODE_MAP: return _CORP_CODE_MAP.get(sc)
    import xml.etree.ElementTree as ET
    for xml_path in DEFAULT_XML_CANDIDATES:
        try:
            if not xml_path.exists(): continue
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                s = (corp.findtext("stock_code", "") or "").strip().zfill(6)
                c = (corp.findtext("corp_code", "") or "").strip()
                if s and c: _CORP_CODE_MAP[s] = c
            if _CORP_CODE_MAP: break
        except: continue
    _CORP_CODE_MAP.setdefault("005930", "00126380")
    _CORP_CODE_MAP.setdefault("000660", "00164779")
    return _CORP_CODE_MAP.get(sc)

_STOCK_NAME_MAP: Dict[str, str] = {}
def get_stock_name(stock_code: str) -> Optional[str]:
    """주식 코드로부터 기업명을 조회"""
    sc = stock_code.zfill(6)
    if _STOCK_NAME_MAP: return _STOCK_NAME_MAP.get(sc)
    import xml.etree.ElementTree as ET
    for xml_path in DEFAULT_XML_CANDIDATES:
        try:
            if not xml_path.exists(): continue
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                s = (corp.findtext("stock_code", "") or "").strip().zfill(6)
                n = (corp.findtext("corp_name", "") or "").strip()
                if s and n: _STOCK_NAME_MAP.setdefault(s, n)
            if _STOCK_NAME_MAP: break
        except: continue
    _STOCK_NAME_MAP.setdefault("005930", "삼성전자")
    _STOCK_NAME_MAP.setdefault("000660", "SK하이닉스")
    return _STOCK_NAME_MAP.get(sc)

# --- 매핑 및 블랙리스트 ---
# AMAP: account_id -> FINANCE_COLS
AMAP = {
    "ifrs-full_Revenue": "revenue",
    "ifrs_Revenue": "revenue",
    "dart_Sales": "revenue",
    "dart_OperatingRevenue": "revenue",
    "dart_OrdinaryRevenue": "revenue",
    "ifrs-full_ProfitLossFromOperatingActivities": "op_income",
    "ifrs_ProfitLossFromOperatingActivities": "op_income",
    "dart_OperatingIncomeLoss": "op_income",
    "ifrs-full_ProfitLoss": "net_income",
    "ifrs_ProfitLoss": "net_income",
    "dart_ProfitLoss": "net_income",
    "dart_ProfitLossAttributableToOwnersOfParent": "net_income",
    "ifrs-full_Equity": "total_equity",
    "ifrs_Equity": "total_equity",
    "dart_TotalEquity": "total_equity",
    "ifrs_EquityAttributableToOwnersOfParent": "total_equity",
    "ifrs_full_EquityAttributableToOwnersOfParent": "total_equity",
    "ifrs-full_Assets": "total_assets",
    "ifrs_Assets": "total_assets",
    "dart_TotalAssets": "total_assets",
    "ifrs-full_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
    "ifrs_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
    "dart_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
    "ifrs-full_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
    "ifrs_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
    "dart_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
    "ifrs-full_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
    "ifrs_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
    "dart_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
    "ifrs-full_EarningsPerShare": "eps",
    "ifrs_BasicEarningsLossPerShare": "eps",
    "ifrs_BasicEarningsPerShare": "eps",
    "ifrs_EarningsPerShare": "eps",
    "dart_EarningsPerShare": "eps",
    "dart_BasicEarningsLossPerShare": "eps",
}

# KMAP: account_nm (한글 계정명) -> FINANCE_COLS
KMAP = {
    "revenue": [],
    "op_income": ["영업이익", "영업이익손실"],
    "net_income": ["당기순이익", "분기순이익", "반기순이익", "당기순이익손실", "분기순이익손실"],
    "total_assets": ["자산총계", "총자산"],
    "total_equity": ["자본총계", "총자본"],
    "cash_flow_op": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름", "투자활동으로인한현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름", "재무활동으로인한현금흐름"],
    "eps": ["주당순이익", "주당이익"],
}

# BLACKLIST: 계정명에 포함되면 제외할 키워드
BLACKLIST = {
    "eps": ["희석", "수정", "재평가", "중단"],
    "op_income": ["기타", "금융", "중단", "지분법"],
    "net_income": ["비지배", "포괄", "중단", "법인세비용차감전"],
    "revenue": [], 
    "total_assets": [],
    "total_equity": [],
    "cash_flow_op": [],
    "cash_flow_inv": [],
    "cash_flow_fin": [],
}

# DART 보고서 코드 및 월별 버킷
ANN_REPORT_CODES = {"11011": "사업보고서", "11012": "반기보고서", "11013": "1분기보고서", "11014": "3분기보고서"}
MONTH_BUCKETS = {
    "11013": {"04", "05", "06"},
    "11012": {"07", "08", "09"},
    "11014": {"10", "11", "12"},
    "11011": {"02", "03", "04"},
}
ANN_KEYWORDS_BASE = ["분기보고서", "반기보고서", "3분기보고서", "사업보고서", "잠정실적"]
LIST_CACHE: Dict[tuple, Optional[List[dict]]] = {}


def parse_dart_list(list_rows: List[dict]) -> Dict[str, Optional[float]]:
    """
    DART API 응답 리스트를 파싱하여 재무 항목을 추출.
    Flow 변수의 '누적' 여부를 판단하여 값을 추출하는 로직 포함.
    """
    row: Dict[str, Optional[float]] = {}
    row_prio: Dict[str, int] = {}
    
    eq_owner = None
    eq_nonctl = None
    eq_total = None
    
    # [FIXED 1 반영] eps를 flow_cols에서 제외하여 차분 로직이 적용되지 않도록 함.
    flow_cols = {"revenue", "op_income", "net_income", "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount"}
    priority_map = {
        "revenue": ["dart_OperatingRevenue", "dart_Sales", "ifrs-full_Revenue"],
        "op_income": ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        "net_income": ["ifrs-full_ProfitLoss", "dart_ProfitLossAttributableToOwnersOfParent"],
        "eps": ["ifrs-full_EarningsPerShare", "ifrs_BasicEarningsPerShare"], # EPS는 Diff 대상이 아니지만, 우선순위 매핑은 유지
    }

    def _priority_idx(key, aid):
        cand = priority_map.get(key)
        if not cand: return 999
        try: return cand.index(aid)
        except: return 900

    for item in list_rows:
        aid = item.get("account_id", "") or ""
        anm_raw = item.get("account_nm", "") or ""
        # thstrm_add_amount: 당기 누적 금액 (일반적으로 분기/반기 누적)
        val_add = safe_float(item.get("thstrm_add_amount", ""))
        # thstrm_amount: 당기 금액 (해당 기간 또는 잔액)
        val_amt = safe_float(item.get("thstrm_amount", ""))
        
        norm = re.sub(r"[\s\(\)\[\]\{\}\-_/\.]", "", anm_raw)
        detail = (item.get("account_detail") or "").strip()
        
        key_hint = AMAP.get(aid)
        is_flow_diff_target = key_hint in FLOW_COLS_TO_DIFF
        is_eps = key_hint == "eps"
        
        # [Fix 5 적용] Flow Variables의 누적/분기 판단 로직
        val = None
        is_cumulative = False
        
        if is_flow_diff_target:
            # 1. 누적 금액(val_add)이 있다면 누적일 가능성이 높음 (Diff 대상)
            if val_add is not None:
                val = val_add
                is_cumulative = True
            # 2. 누적 금액이 없고 val_amt만 있다면 해당 기간 금액으로 간주
            elif val_amt is not None:
                val = val_amt
                is_cumulative = False 
        elif is_eps or key_hint in ["total_assets", "total_equity"]:
            # EPS 및 Stock Variables (Diff 대상 아님)
            val = val_amt
        else:
            val = val_amt # 기타 컬럼은 val_amt 사용

        # Equity (자본) 처리: 지배/비지배 합산 로직 유지 (재무상태표 항목)
        if aid in {"ifrs-full_EquityAttributableToOwnersOfParent", "ifrs_EquityAttributableToOwnersOfParent", "dart_OwnersEquity"}:
            eq_owner = val if val is not None else eq_owner
            continue
        if aid in {"ifrs-full_NoncontrollingInterests", "ifrs_NoncontrollingInterests", "dart_NonControllingInterests"}:
            eq_nonctl = val if val is not None else eq_nonctl
            continue
        if aid in {"ifrs-full_Equity", "ifrs_Equity", "dart_TotalEquity"}:
            if "[" in detail or "member" in detail:
                if eq_total is None and val is not None: eq_total = val
            else:
                if eq_total is None and val is not None: eq_total = val
                elif val is not None and abs(val) > abs(eq_total or 0): eq_total = val
            continue

        manual_prio = None
        key = AMAP.get(aid)
        
        # account_id로 매핑되지 않은 경우, 한글 계정명 KMAP을 통해 매핑 시도
        if not key:
            for tgt, patterns in KMAP.items():
                if tgt == "revenue": continue
                if any(bad in norm for bad in BLACKLIST.get(tgt, [])): continue
                if any(p in norm for p in patterns):
                    key = tgt
                    manual_prio = 999
                    break
        
        if key and val is not None:
            if aid == "dart_ProfitLoss" and "법인세비용차감전" in norm: continue
            
            prio = manual_prio if manual_prio is not None else _priority_idx(key, aid)
            
            # Flow Diff Target이면서 누적일 가능성이 높을 경우, 누적 플래그 저장
            if is_flow_diff_target and is_cumulative:
                item["__is_cumulative"] = True
            
            # 총자본 외의 일반적인 우선순위 결정 로직
            prev_prio = row_prio.get(key, 1000)
            if (key not in row) or (prio < prev_prio):
                row[key] = val
                row_prio[key] = prio
                # 누적 플래그 저장 (차분 로직에서 사용)
                if is_flow_diff_target:
                    row[f"__is_cumulative_{key}"] = is_cumulative

    if eq_total is None and (eq_owner is not None or eq_nonctl is not None):
        eq_total = (eq_owner or 0) + (eq_nonctl or 0)
    if eq_total is not None:
        row["total_equity"] = eq_total
    return row

def _select_announce_date_from_rows(list_rows, year, reprt_code):
    """공시 목록에서 보고서 코드를 기반으로 공시일을 추출"""
    bgn, end = f"{year}0101", f"{year+1}0430"
    rows = [it for it in list_rows if bgn <= (it.get("rcept_dt") or "") <= end]
    if not rows: rows = list_rows
    
    def _in_bucket(dt):
        if not dt or len(dt)<6: return True
        return dt[4:6] in MONTH_BUCKETS.get(reprt_code, set())
    
    keywords = [ANN_REPORT_CODES.get(reprt_code, "")] + ANN_KEYWORDS_BASE
    kw_cands = [it for it in rows if any((kw and kw in (it.get("report_nm") or "").replace(" ","")) for kw in keywords)]
    if kw_cands:
        filtered = [it for it in kw_cands if _in_bucket(it.get("rcept_dt", ""))]
        kw_cands = filtered or kw_cands
        kw_cands = sorted(kw_cands, key=lambda x: x.get("rcept_dt", "") or "99999999")
        return kw_cands[0].get("rcept_dt")
    
    cands = [it for it in rows if it.get("reprt_code") == reprt_code and _in_bucket(it.get("rcept_dt", ""))]
    if not cands: cands = [it for it in rows if it.get("reprt_code") == reprt_code]
    if not cands: cands = rows
    if not cands: return None
    cands = sorted(cands, key=lambda x: x.get("rcept_dt", "") or "99999999")
    return cands[0].get("rcept_dt")

def load_list_cache(stock_code, corp_code, cache_dir):
    """공시 목록 캐시 파일 로드"""
    key = (str(stock_code).zfill(6), str(corp_code))
    if key in LIST_CACHE: return LIST_CACHE[key]
    if not cache_dir.exists(): return None
    path = cache_dir / f"{key[0]}_{key[1]}.json"
    if not path.exists():
        path = cache_dir / f"{key[1]}.json"
        if not path.exists(): return None
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        if isinstance(data, list):
            LIST_CACHE[key] = data
            return data
    except: pass
    return None

def resolve_announce_date(corp_code, stock_code, year, reprt_code, key, mode, cache_dir):
    """공시일 결정 (캐시 또는 API 호출)"""
    if mode == "none": return None
    if mode in {"cache", "hybrid"}:
        rows = load_list_cache(stock_code, corp_code, cache_dir)
        if rows: return _select_announce_date_from_rows(rows, year, reprt_code)
        if mode == "cache": return None
    
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {"crtfc_key": key, "corp_code": corp_code, "bgn_de": f"{year}0101", "end_de": f"{year+1}0430", "page_no": 1, "page_count": 100}
    status, data = http_get_json(url, params, tries=2)
    if status==200 and data and data.get("status")=="000":
        return _select_announce_date_from_rows(data.get("list") or [], year, reprt_code)
    return None

# --- [Fix] 배당금 공공데이터포털 API (무결성 강화 버전) ---
def get_dividend_public_api(stock_name: str, base_date: str) -> Optional[float]:
    """
    공공데이터포털 기업활력제고 배당정보 API에서 보통주 현금배당금 추출
    """
    API_KEY = "9c3cf7dd64c3f256bc2533ea8698751579ccbd7df0bf5489c5493abce4a99f7b"
    URL = "https://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo"

    # [예외처리] 예탁원 API는 영문 약어가 아닌 한글 정식 명칭을 요구함
    KSD_NAME_MAP = {
        "SK하이닉스": "에스케이하이닉스",
        "SK": "에스케이",
        "LG전자": "엘지전자",
        "LG화학": "엘지화학",
        "CJ제일제당": "씨제이제일제당",
        "POSCO홀딩스": "포스코홀딩스",
        "KT": "케이티",
        "KT&G": "케이티앤지"
    }
    
    query_name = KSD_NAME_MAP.get(stock_name, stock_name)

    params = {
        "serviceKey": API_KEY,
        "numOfRows": "50",
        "pageNo": "1",
        "resultType": "json",
        "stckIssuCmpyNm": query_name,
        "dvdnBasDt": base_date # 배당기준일 (YYYYMMDD)
    }

    try:
        res = requests.get(URL, params=params, timeout=5)
        try:
            data = res.json()
        except:
            return None
        
        body = data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", [])

        if isinstance(items, dict):
            items = [items]
        
        if not items:
            return None

        for it in items:
            # [필터링 1] '보통주' 확인 
            se_val = (it.get("se") or "").strip()
            kind_nm = (it.get("scrsItmsKcdNm") or "").strip() # 예: '보통주'
            
            if "보통주" not in se_val and "보통주" not in kind_nm:
                continue

            # [필터링 2] 현금배당금 추출
            amt_str = it.get("stckGenrDvdnAmt")
            if amt_str and str(amt_str).replace(".", "").isdigit():
                val = float(amt_str)
                if val >= 0: # 배당금이 0원이거나 양수일 경우만 반환
                    return val
                    
    except Exception:
        return None

    return None

# --- 핵심 수집 로직 ---
def collect_one_code(args):
    code, start_date, end_date, dart_mode, out_dir, ordered_keys, announce_mode, list_cache_dir = args
    code6 = str(code).zfill(6)
    print(f"\n[수집 시작] 종목 {code6}")
    
    corp_code = get_corp_code(code6)
    if not corp_code:
        print(f"[WARN] corp_code 없음: {code6}")
        return code6, False, {"issues": ["no_corp_code"]}
    
    stock_name = get_stock_name(code6)

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    reprt_codes = ["11013", "11012", "11014", "11011"] if dart_mode != "annual" else ["11011"]
    
    collected_rows = []

    for year in range(start_year, end_year + 1):
        if year < 2016: continue # 2016년 이전 데이터는 수집하지 않음
        print(f"  [연도] {year}년")
        
        prev_cumulative = {c: None for c in FLOW_COLS_TO_DIFF} # None으로 초기화
        
        # 4개 분기 순환 (1Q, 2Q, 3Q, 4Q)
        for rc_idx, rc in enumerate(reprt_codes):
            got_fin = False
            fin_row = {}
            announce_dt = None
            
            # 1. 분기별 기준일 계산
            q_map = {"11013": "0331", "11012": "0630", "11014": "0930", "11011": "1231"}
            quarter_date = f"{year}{q_map.get(rc, '1231')}"
            
            # 2. 배당금 데이터 독립 수집
            # 배당 기준일 날짜로 '예탁원 확정 데이터' 조회. 배당이 없는 경우 None 반환 (NaN 처리)
            div_val = get_dividend_public_api(stock_name, quarter_date)

            # 3. 재무제표 수집
            for k in ordered_keys:
                # 공시일 결정
                announce_dt = resolve_announce_date(corp_code, code6, year, rc, k, announce_mode, list_cache_dir)
                
                params = {"crtfc_key": k, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rc, "fs_div": "CFS"}
                
                # 전체 재무제표 (All)
                status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=2)
                if status == 200 and data and data.get("status") == "000" and data.get("list"):
                    fin_row = parse_dart_list(data["list"])
                    got_fin = True
                else:
                    # 단일 재무제표 (Single) - 백업
                    status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params, tries=2)
                    if status == 200 and data and data.get("status") == "000" and data.get("list"):
                        fin_row = parse_dart_list(data["list"])
                        got_fin = True

                if got_fin:
                    break
            
            # 4. 데이터 병합 및 차분 계산
            # 재무 데이터 또는 배당금 데이터 중 하나라도 수집되었다면 처리
            if got_fin or div_val is not None:
                # [FIXED 3 반영]: code와 corp_code는 수집 시점에서 6자리로 변환될 준비를 함.
                row_out = {
                    "code": code6, # code6는 zfill(6)이 적용된 상태
                    "corp_code": corp_code,
                    "bsns_year": year,
                    "reprt_code": rc,
                    "announce_date": announce_dt,
                    "period": "Q1" if rc=="11013" else "Q2" if rc=="11012" else "Q3" if rc=="11014" else "Q4"
                }
                
                pd_map = {"Q1":"0331", "Q2":"0630", "Q3":"0930", "Q4":"1231"}
                row_out["date"] = f"{year}{pd_map[row_out['period']]}"
                
                # 배당금 강제 병합 (NaN 유지)
                row_out["div_amount"] = div_val
                
                # [Fix 5 적용] 누적 -> 분기 변환 (Diff) 및 값 매핑
                current_cumulative_snapshot = {} # 현재 분기의 누적값 저장용
                
                for col in FINANCE_COLS:
                    if col in ["announce_date", "div_amount"]: continue # 제외
                    val = fin_row.get(col)
                    
                    # EPS는 Diff 대상이 아니며, is_cumulative 플래그를 확인하지 않음.
                    # FLOW_COLS_TO_DIFF에 있는 컬럼에 대해서만 is_cumulative 확인.
                    is_cumulative = fin_row.get(f"__is_cumulative_{col}", False) if col in FLOW_COLS_TO_DIFF else False
                    
                    if col in FLOW_COLS_TO_DIFF and val is not None:
                        if is_cumulative and prev_cumulative[col] is not None:
                            # 누적 값으로 판단되고, 이전 누적 값이 있을 경우에만 Diff 계산 (무결성 확보)
                            discrete_val = val - prev_cumulative[col]
                            row_out[col] = discrete_val
                            # 다음 분기를 위해 현재 (Diff 계산의 기준이 된) 누적값 저장
                            current_cumulative_snapshot[col] = val
                        else:
                            # 누적 값이 아니거나 (Discrete), 이전 누적 값이 None일 경우 그대로 사용
                            row_out[col] = val
                            # 현재 값이 누적 값이므로 다음 차분 계산에 사용될 수 있도록 저장
                            current_cumulative_snapshot[col] = val
                    else:
                        # EPS, 재무상태표 항목 (Assets, Equity) 등 Diff 비대상 항목은 그대로 저장
                        row_out[col] = val

                # 다음 분기 Loop를 위해 prev_cumulative 업데이트
                for col in FLOW_COLS_TO_DIFF:
                    if col in current_cumulative_snapshot:
                         prev_cumulative[col] = current_cumulative_snapshot[col]
                
                collected_rows.append(row_out)
        # [Loop 내부 끝]

    df = pd.DataFrame(collected_rows)
    if not df.empty:
        final_cols = [c for c in KEEP_COLS if c in df.columns]
        df = df[final_cols]
        
        # [FIXED 3 반영]: code, corp_code를 6자리 문자열로 명시적으로 변환하여 선행 0 유지
        df['code'] = df['code'].astype(str).str.zfill(6)
        df['corp_code'] = df['corp_code'].astype(str).str.zfill(8) # corp_code는 8자리
        
        base = out_dir / f"{code6}_sle.csv"
        save_path = base
        suffix = 1
        while save_path.exists():
            save_path = out_dir / f"{code6}_sle_{suffix}.csv"
            suffix += 1
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"  [저장 완료] {save_path}")
        return code6, True, None
    else:
        return code6, False, {"issues": ["empty"]}

def run_single_task(t):
    try: collect_one_code(t)
    except Exception as e: print(f"[ERROR] {t[0]} : {e}")

def _load_codes_from_file(path: str) -> List[str]:
    try:
        p = Path(path)
        if not p.exists(): return []
        return [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    except Exception:
        return []

DEFAULT_CODE = "000660"  # UI에서 누락될 때 기본값 (삼성 고정 방지)

def _resolve_codes(args, default_code: str = DEFAULT_CODE) -> List[str]:
    # 1) 별도 codes-file이 주어지면 최우선
    if getattr(args, "codes_file", ""):
        codes = _load_codes_from_file(args.codes_file)
        if codes: return codes
    # 2) codes 인자가 파일 경로라면 자동 인식
    if args.codes and Path(args.codes).exists():
        codes = _load_codes_from_file(args.codes)
        if codes: return codes
    # 3) 일반 쉼표/개행 구분 코드 목록
    codes = [x.strip() for x in args.codes.replace(",", "\n").splitlines() if x.strip()]
    if codes:
        return codes
    # 4) 모든 경로가 비었으면 기본 코드로 대체 (UI 미전달 방지)
    return [default_code]

if __name__ == "__main__":
    mp.freeze_support()
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--codes-file", type=str, default="")
    parser.add_argument("--start", type=str, default="20160101")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--out", type=str, default=r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_sle_11date")
    parser.add_argument("--mode", type=str, default="quarter")
    parser.add_argument("--announce-mode", type=str, default="hybrid")
    parser.add_argument("--list-cache", type=str, default="DART_LIST_CACHE")
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()

    codes = _resolve_codes(args)
    if codes == [DEFAULT_CODE] and not args.codes and not args.codes_file:
        print(f"[WARN] UI에서 코드가 전달되지 않아 기본 코드로 대체: {DEFAULT_CODE}")
    print(f"[INFO] 대상 종목: {', '.join(codes)}")
    if not ensure_corp_xml_exists(): exit(1)
    
    ordered_keys = read_dart_keys_ordered()
    if not ordered_keys: exit(1)
    
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    list_cache_dir = Path(args.list_cache)
    
    tasks = [(c, args.start, args.end, args.mode, out_dir, ordered_keys, args.announce_mode, list_cache_dir) for c in codes]
    
    if args.workers <= 1:
        for t in tasks: run_single_task(t)
    else:
        with mp.Pool(args.workers) as pool: pool.map(run_single_task, tasks)