# -*- coding: utf-8 -*-
"""
run_raw_sle.py
- SLE 재무 11컬럼 정밀 수집기 (2016년 1Q 이후)

**최종 수정 사항 (v47_Fix):**
1. **[FALLBACK]** CFS(연결) 실패 시 OFS(별도) 자동 시도 로직 추가.
2. **[MAPPING]** 지주사/금융사 대응 '영업수익', '수익' 키워드 추가.
3. **[SPEED]** 공시일(announce_date) 조회용 Loop 제거 (Default: none).
4. **[RETRY]** API Key 에러(020, 800) 시 즉시 키 교체, 데이터 없음(013)은 즉시 스킵.
5. **[SAVE]** 재무 데이터가 없어도(NaN) 메타 데이터 보존을 위해 파일 저장 허용.
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
from threading import Thread
from queue import Empty

import pandas as pd
import requests

# -------------------- Windows console freeze guard -------------------- #
def disable_quick_edit():
    if os.name != 'nt':
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        hStdIn = kernel32.GetStdHandle(-10)  # STD_INPUT_HANDLE = -10
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(hStdIn, ctypes.byref(mode)):
            return
        # Disable QUICK_EDIT(0x0040), enable EXTENDED_FLAGS(0x0080)
        new_mode = (mode.value & ~0x0040) | 0x0080
        kernel32.SetConsoleMode(hStdIn, new_mode)
    except:
        pass

def _ui_printer(_q, _ctr, _total):
    try:
        import sys, time
        buf = []
        max_lines = 10
        while True:
            try:
                # drain queue quickly
                for _ in range(20):
                    lvl, msg = _q.get_nowait()
                    if lvl == "STOP":
                        raise KeyboardInterrupt
                    buf.append(msg)
            except Empty:
                pass
            if len(buf) > max_lines:
                buf = buf[-max_lines:]
            done = int(_ctr.get('done', 0))
            suc  = int(_ctr.get('success', 0))
            fail = int(_ctr.get('fail', 0))
            header = f"총 {_total} / 성공 {suc} / 실패 {fail}"
            # Move cursor to home and clear screen from cursor down
            sys.stdout.write("\033[H\033[J")
            sys.stdout.write(header + "\n")
            for line in buf:
                sys.stdout.write(line + "\n")
            sys.stdout.flush()
            if done >= _total:
                break
            time.sleep(0.25)
    except KeyboardInterrupt:
        pass

    if os.name != 'nt':
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        hStdIn = kernel32.GetStdHandle(-10)  # STD_INPUT_HANDLE = -10
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(hStdIn, ctypes.byref(mode)):
            return
        # Disable QUICK_EDIT (0x0040), enable EXTENDED_FLAGS (0x0080)
        new_mode = (mode.value & ~0x0040) | 0x0080
        kernel32.SetConsoleMode(hStdIn, new_mode)
    except Exception:
        pass


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

# DART 키 후보
DEFAULT_KEY_PATHS = [
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
    Path(__file__).resolve().parents[2] / "opendart_apikey.txt",
]

# -------- 유틸 -------- #
def safe_float(v) -> Optional[float]:
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none", "-", "nan,"}:
            return None
        return float(s)
    except Exception:
        return None

def _extract_leading_int(name: str) -> int:
    m = re.match(r"^(\d{1,2})", name)
    if m:
        try: return int(m.group(1))
        except: return 999
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
    env = os.environ.get("DART_API_KEYS", "")
    candidates = []
    if env.strip():
        for k in env.replace(",", "\n").splitlines():
            k = k.strip()
            if k: candidates.append((0, k))
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
    for p in DEFAULT_XML_CANDIDATES:
        if p.exists(): return True
    return False

def http_get_json(url: str, params: dict, tries: int = 3, timeout: int = 5):
    for a in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504, 403):
                if a == tries - 1: return r.status_code, None
                time.sleep(0.5 * (2 ** (a % 3)))
                continue
            if r.status_code != 200:
                if a == tries - 1: return r.status_code, None
                continue
            return r.status_code, r.json()
        except Exception:
            if a == tries - 1: return -1, None
            time.sleep(0.5 * (2 ** (a % 3)))
    return -1, None

_CORP_CODE_MAP: Dict[str, str] = {}
def get_corp_code(stock_code: str) -> Optional[str]:
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

# KMAP: [Fix 2] 매출에 '영업수익', '수익' 추가
KMAP = {
    "revenue": ["영업수익", "수익", "매출액"],
    "op_income": ["영업이익", "영업이익손실"],
    "net_income": ["당기순이익", "분기순이익", "반기순이익", "당기순이익손실", "분기순이익손실"],
    "total_assets": ["자산총계", "총자산"],
    "total_equity": ["자본총계", "총자본"],
    "cash_flow_op": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름", "투자활동으로인한현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름", "재무활동으로인한현금흐름"],
    "eps": ["주당순이익", "주당이익"],
}

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
    row: Dict[str, Optional[float]] = {}
    row_prio: Dict[str, int] = {}
    eq_owner = None
    eq_nonctl = None
    eq_total = None
    
    flow_cols = {"revenue", "op_income", "net_income", "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount"}
    priority_map = {
        "revenue": ["dart_OperatingRevenue", "dart_Sales", "ifrs-full_Revenue"],
        "op_income": ["dart_OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
        "net_income": ["ifrs-full_ProfitLoss", "dart_ProfitLossAttributableToOwnersOfParent"],
        "eps": ["ifrs-full_EarningsPerShare", "ifrs_BasicEarningsPerShare"],
    }

    def _priority_idx(key, aid):
        cand = priority_map.get(key)
        if not cand: return 999
        try: return cand.index(aid)
        except: return 900

    for item in list_rows:
        aid = item.get("account_id", "") or ""
        anm_raw = item.get("account_nm", "") or ""
        val_add = safe_float(item.get("thstrm_add_amount", ""))
        val_amt = safe_float(item.get("thstrm_amount", ""))
        
        norm = re.sub(r"[\s\(\)\[\]\{\}\-_/\.]", "", anm_raw)
        detail = (item.get("account_detail") or "").strip()
        
        key_hint = AMAP.get(aid)
        is_flow_diff_target = key_hint in FLOW_COLS_TO_DIFF
        is_eps = key_hint == "eps"
        
        val = None
        is_cumulative = False
        
        if is_flow_diff_target:
            if val_add is not None:
                val = val_add
                is_cumulative = True
            elif val_amt is not None:
                val = val_amt
                is_cumulative = False 
        elif is_eps or key_hint in ["total_assets", "total_equity"]:
            val = val_amt
        else:
            val = val_amt

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
        
        if not key:
            for tgt, patterns in KMAP.items():
                if tgt == "revenue" and not patterns: continue # Skip if empty (handled by fix 2)
                if any(bad in norm for bad in BLACKLIST.get(tgt, [])): continue
                if any(p in norm for p in patterns):
                    key = tgt
                    manual_prio = 999
                    break
        
        if key and val is not None:
            if aid == "dart_ProfitLoss" and "법인세비용차감전" in norm: continue
            
            prio = manual_prio if manual_prio is not None else _priority_idx(key, aid)
            
            if is_flow_diff_target and is_cumulative:
                item["__is_cumulative"] = True
            
            prev_prio = row_prio.get(key, 1000)
            if (key not in row) or (prio < prev_prio):
                row[key] = val
                row_prio[key] = prio
                if is_flow_diff_target:
                    row[f"__is_cumulative_{key}"] = is_cumulative

    if eq_total is None and (eq_owner is not None or eq_nonctl is not None):
        eq_total = (eq_owner or 0) + (eq_nonctl or 0)
    if eq_total is not None:
        row["total_equity"] = eq_total
    return row

def _select_announce_date_from_rows(list_rows, year, reprt_code):
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

def get_dividend_public_api(stock_name: str, base_date: str) -> Optional[float]:
    API_KEY = "9c3cf7dd64c3f256bc2533ea8698751579ccbd7df0bf5489c5493abce4a99f7b"
    URL = "https://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo"
    KSD_NAME_MAP = {
        "SK하이닉스": "에스케이하이닉스", "SK": "에스케이", "LG전자": "엘지전자", "LG화학": "엘지화학",
        "CJ제일제당": "씨제이제일제당", "POSCO홀딩스": "포스코홀딩스", "KT": "케이티", "KT&G": "케이티앤지"
    }
    query_name = KSD_NAME_MAP.get(stock_name, stock_name)
    params = {"serviceKey": API_KEY, "numOfRows": "50", "pageNo": "1", "resultType": "json", "stckIssuCmpyNm": query_name, "dvdnBasDt": base_date}
    try:
        res = requests.get(URL, params=params, timeout=5)
        try: data = res.json()
        except: return None
        body = data.get("response", {}).get("body", {})
        items = body.get("items", {}).get("item", [])
        if isinstance(items, dict): items = [items]
        if not items: return None
        for it in items:
            se_val = (it.get("se") or "").strip()
            kind_nm = (it.get("scrsItmsKcdNm") or "").strip()
            if "보통주" not in se_val and "보통주" not in kind_nm: continue
            amt_str = it.get("stckGenrDvdnAmt")
            if amt_str and str(amt_str).replace(".", "").isdigit():
                val = float(amt_str)
                if val >= 0: return val
    except Exception: return None
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
        if year < 2016: continue
        print(f"  [연도] {year}년")
        
        prev_cumulative = {c: None for c in FLOW_COLS_TO_DIFF}
        
        for rc_idx, rc in enumerate(reprt_codes):
            got_fin = False
            fin_row = {}
            announce_dt = None
            data_status_code = None # 000: 성공, 013: 데이터없음
            
            q_map = {"11013": "0331", "11012": "0630", "11014": "0930", "11011": "1231"}
            quarter_date = f"{year}{q_map.get(rc, '1231')}"
            
            div_val = get_dividend_public_api(stock_name, quarter_date)

            # [Fix 4 & 5] 키 로테이션 및 폴백 강화
            for k in ordered_keys:
                if announce_mode != "none":
                    if announce_dt is None:
                        announce_dt = resolve_announce_date(corp_code, code6, year, rc, k, announce_mode, list_cache_dir)
                
                # [Fix 1] CFS 시도 -> 실패시 OFS 시도
                for fs_div in ["CFS", "OFS"]:
                    params = {"crtfc_key": k, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rc, "fs_div": fs_div}
                    
                    # 1. Try All
                    status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=2)
                    d_status = data.get("status") if data else "999"
                    
                    if d_status == "000" and data.get("list"):
                        fin_row = parse_dart_list(data["list"])
                        got_fin = True
                        data_status_code = "000"
                        break
                    elif d_status in ["020", "800"]: # Key Error -> Break fs_div loop to swap key
                        data_status_code = d_status
                        break 
                    elif d_status == "013": # No Data -> Continue to OFS
                        data_status_code = "013"
                        # Try Single Backup
                        status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params, tries=2)
                        d_status_single = data.get("status") if data else "999"
                        if d_status_single == "000" and data.get("list"):
                             fin_row = parse_dart_list(data["list"])
                             got_fin = True
                             data_status_code = "000"
                             break
                        elif d_status_single in ["020", "800"]:
                             data_status_code = d_status_single
                             break
                        # If 013, loop continues to OFS

                # Loop Exit Logic
                if got_fin:
                    break # Success -> Stop Key Loop
                
                # [Fix 5] 키 에러면 다음 키 시도, 데이터 없음(013)이면 키 바꿔도 소용 없으니 Stop
                if data_status_code == "013":
                    break
                # If 020/800, continue to next key
            
            # 4. 데이터 병합 (Fix 4: got_fin이 False여도 013(데이터없음)이거나 배당이 있으면 저장 시도)
            # --> "데이터 없음"으로 확인된 경우에도 빈 행을 생성하여 누락인지 없는건지 구별 가능하게 함
            force_save = (data_status_code == "013")
            
            if got_fin or div_val is not None or force_save:
                row_out = {
                    "code": code6,
                    "corp_code": corp_code,
                    "bsns_year": year,
                    "reprt_code": rc,
                    "announce_date": announce_dt,
                    "period": "Q1" if rc=="11013" else "Q2" if rc=="11012" else "Q3" if rc=="11014" else "Q4"
                }
                
                pd_map = {"Q1":"0331", "Q2":"0630", "Q3":"0930", "Q4":"1231"}
                row_out["date"] = f"{year}{pd_map[row_out['period']]}"
                row_out["div_amount"] = div_val
                
                current_cumulative_snapshot = {}
                
                for col in FINANCE_COLS:
                    if col in ["announce_date", "div_amount"]: continue
                    val = fin_row.get(col)
                    
                    is_cumulative = fin_row.get(f"__is_cumulative_{col}", False) if col in FLOW_COLS_TO_DIFF else False
                    
                    if col in FLOW_COLS_TO_DIFF and val is not None:
                        if is_cumulative and prev_cumulative[col] is not None:
                            discrete_val = val - prev_cumulative[col]
                            row_out[col] = discrete_val
                            current_cumulative_snapshot[col] = val
                        else:
                            row_out[col] = val
                            current_cumulative_snapshot[col] = val
                    else:
                        row_out[col] = val

                for col in FLOW_COLS_TO_DIFF:
                    if col in current_cumulative_snapshot:
                         prev_cumulative[col] = current_cumulative_snapshot[col]
                
                collected_rows.append(row_out)

    df = pd.DataFrame(collected_rows)
    if not df.empty:
        final_cols = [c for c in KEEP_COLS if c in df.columns]
        df = df[final_cols]
        
        df['code'] = df['code'].astype(str).str.zfill(6)
        df['corp_code'] = df['corp_code'].astype(str).str.zfill(8)
        
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


def run_single_task(t, _ctr=None, _q=None):
    try:
        ret = collect_one_code(t)
        # ret: (code6, ok:bool, info)
        if isinstance(ret, tuple) and len(ret) >= 2:
            code6, ok = ret[0], bool(ret[1])
            if _ctr is not None:
                _ctr['done'] = int(_ctr.get('done',0)) + 1
                if ok:
                    _ctr['success'] = int(_ctr.get('success',0)) + 1
                else:
                    _ctr['fail'] = int(_ctr.get('fail',0)) + 1
            if _q is not None:
                status = "OK" if ok else "FAIL"
                _q.put(("INFO", f"[{status}] {code6}"))
        else:
            if _ctr is not None:
                _ctr['done'] = int(_ctr.get('done',0)) + 1
                _ctr['fail'] = int(_ctr.get('fail',0)) + 1
            if _q is not None:
                _q.put(("INFO", "[FAIL] <unknown>"))
    except Exception as e:
        if _ctr is not None:
            _ctr['done'] = int(_ctr.get('done',0)) + 1
            _ctr['fail'] = int(_ctr.get('fail',0)) + 1
        if _q is not None:
            _q.put(("ERROR", f"[ERROR] {t[0]} : {e}"))

def _load_codes_from_file(path: str) -> List[str]:
    try:
        p = Path(path)
        if not p.exists(): return []
        return [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    except Exception:
        return []

DEFAULT_CODE = "000660"

def _resolve_codes(args, default_code: str = DEFAULT_CODE) -> List[str]:
    if getattr(args, "codes_file", ""):
        codes = _load_codes_from_file(args.codes_file)
        if codes: return codes
    if args.codes and Path(args.codes).exists():
        codes = _load_codes_from_file(args.codes)
        if codes: return codes
    codes = [x.strip() for x in args.codes.replace(",", "\n").splitlines() if x.strip()]
    if codes: return codes
    return [default_code]


if __name__ == "__main__":
    mp.freeze_support()
    disable_quick_edit()
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--codes-file", type=str, default=r"F:\autostockG\MODELENGINE\RAW\all_codes.txt")
    parser.add_argument("--start", type=str, default="20160101")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--out", type=str, default=r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_sle_11date")
    parser.add_argument("--mode", type=str, default="quarter")
    parser.add_argument("--announce-mode", type=str, default="none")
    parser.add_argument("--list-cache", type=str, default="DART_LIST_CACHE")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    codes = _resolve_codes(args)
    if codes == [DEFAULT_CODE] and not args.codes and not args.codes_file:
        print(f"[WARN] UI에서 코드가 전달되지 않아 기본 코드로 대체: {DEFAULT_CODE}")
    print(f"[INFO] 대상 종목: {len(codes)}개")
    if not ensure_corp_xml_exists(): exit(1)

    ordered_keys = read_dart_keys_ordered()
    if not ordered_keys: exit(1)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    list_cache_dir = Path(args.list_cache)

    tasks = [(c, args.start, args.end, args.mode, out_dir, ordered_keys, args.announce_mode, list_cache_dir) for c in codes]

    manager = mp.Manager()
    ctr = manager.dict({'done': 0, 'success': 0, 'fail': 0})
    q = manager.Queue()

    # UI thread
    total = len(tasks)
    ui_th = Thread(target=_ui_printer, args=(q, ctr, total), daemon=True)
    ui_th.start()

    if args.workers <= 1:
        for t in tasks: run_single_task(t, ctr, q)
    else:
        # partial function to pass shared objects
        def _wrapper(arg):
            return run_single_task(arg, ctr, q)
        with mp.Pool(args.workers) as pool:
            pool.map(_wrapper, tasks)

    q.put(("STOP","."))
    ui_th.join()
