# -*- coding: utf-8 -*-
"""
run_raw_sle_final_v2.py
- Base: 25121223run_raw_sle.py (Logic & Integrity)
- Engine: Multiprocessing (Workers=10)
- UI: Fixed Dashboard (No Scroll)

**복구 및 설정 내역:**
1. [LOGIC] 25121223 버전의 정밀 배당/공시일/캐시 로직 전면 원복.
2. [CONFIG] 공시일 조회(announce-mode) 기본값 'none' (로직은 존재).
3. [PATH] 대상 파일 기본 경로 하드코딩 완료.
4. [UI] 콘솔 멈춤 방지 및 진행상황 제자리 갱신 적용.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
import sys
import ctypes
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from queue import Empty

import pandas as pd
import requests

# -------------------- 윈도우 콘솔 멈춤 방지 -------------------- #
def disable_quick_edit():
    if os.name != 'nt': return
    try:
        kernel32 = ctypes.windll.kernel32
        hStdIn = kernel32.GetStdHandle(-10)
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(hStdIn, ctypes.byref(mode)): return
        new_mode = (mode.value & ~0x0040) | 0x0080
        kernel32.SetConsoleMode(hStdIn, new_mode)
    except: pass

# -------------------- 상수 및 설정 -------------------- #
META_COLS = ["corp_code", "bsns_year", "reprt_code"]
FINANCE_COLS = [
    "announce_date", "revenue", "op_income", "net_income", "eps",
    "total_assets", "total_equity",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    "div_amount",
]
# [25121223 기준] EPS 제외된 Diff 타겟
FLOW_COLS_TO_DIFF = [
    "revenue", "op_income", "net_income",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin"
]
KEEP_COLS = ["date", "code", "period"] + META_COLS + FINANCE_COLS

DEFAULT_XML_CANDIDATES = [
    Path(__file__).resolve().parent / "dart_corp_list.xml",
    Path(__file__).resolve().parents[2] / "MODELENGINE" / "RAW" / "raw_v48" / "dart_corp_list.xml",
    Path.cwd() / "dart_corp_list.xml",
]
DEFAULT_KEY_PATHS = [
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
    Path(__file__).resolve().parents[2] / "opendart_apikey.txt",
]
# [요청하신 하드코딩 경로]
HARDCODED_CODE_PATH = Path(r"F:\autostockG\MODELENGINE\RAW\all_codes.txt")

ANN_REPORT_CODES = {"11011": "사업보고서", "11012": "반기보고서", "11013": "1분기보고서", "11014": "3분기보고서"}
MONTH_BUCKETS = {
    "11013": {"04", "05", "06"},
    "11012": {"07", "08", "09"},
    "11014": {"10", "11", "12"},
    "11011": {"02", "03", "04"},
}
ANN_KEYWORDS_BASE = ["분기보고서", "반기보고서", "3분기보고서", "사업보고서", "잠정실적"]
LIST_CACHE: Dict[tuple, Optional[List[dict]]] = {}

# -------------------- 유틸리티 -------------------- #
def safe_float(v) -> Optional[float]:
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none", "-", "nan,"}: return None
        return float(s)
    except: return None

def _extract_leading_int(name: str) -> int:
    m = re.match(r"^(\d{1,2})", name)
    return int(m.group(1)) if m else 999

def _read_lines(path: Path) -> List[str]:
    try:
        if path.exists():
            return [ln.strip() for ln in path.read_text(encoding="utf-8", errors="ignore").replace(",", "\n").splitlines() if ln.strip()]
    except: pass
    return []

def read_dart_keys_ordered() -> List[str]:
    env = os.environ.get("DART_API_KEYS", "")
    candidates = []
    if env.strip():
        for k in env.replace(",", "\n").splitlines():
            if k.strip(): candidates.append((0, k.strip()))
    for p in DEFAULT_KEY_PATHS:
        if p.exists():
            leading = _extract_leading_int(p.name)
            for k in _read_lines(p): candidates.append((leading, k))
    candidates_sorted = sorted(enumerate(candidates), key=lambda x: (x[1][0], x[0]))
    uniq = []
    for _, (_, key) in candidates_sorted:
        if key and key not in uniq: uniq.append(key)
    return uniq

def ensure_corp_xml_exists() -> bool:
    for p in DEFAULT_XML_CANDIDATES:
        if p.exists(): return True
    return False

def http_get_json(url: str, params: dict, tries: int = 3, timeout: int = 8):
    for a in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504, 403):
                time.sleep(0.5 * (2 ** (a % 3)))
                continue
            if r.status_code != 200: continue
            return r.status_code, r.json()
        except: time.sleep(0.5 * (2 ** (a % 3)))
    return -1, None

# -------------------- 메타데이터 (XML) -------------------- #
_CORP_CODE_MAP: Dict[str, str] = {}
_STOCK_NAME_MAP: Dict[str, str] = {}

def load_xml_global():
    import xml.etree.ElementTree as ET
    for xml_path in DEFAULT_XML_CANDIDATES:
        try:
            if not xml_path.exists(): continue
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                s = (corp.findtext("stock_code", "") or "").strip().zfill(6)
                c = (corp.findtext("corp_code", "") or "").strip()
                n = (corp.findtext("corp_name", "") or "").strip()
                if s and c: _CORP_CODE_MAP[s] = c
                if s and n: _STOCK_NAME_MAP[s] = n
            _CORP_CODE_MAP.setdefault("005930", "00126380")
            _CORP_CODE_MAP.setdefault("000660", "00164779")
            _STOCK_NAME_MAP.setdefault("005930", "삼성전자")
            _STOCK_NAME_MAP.setdefault("000660", "SK하이닉스")
            if _CORP_CODE_MAP: break
        except: continue

def get_corp_code(stock_code: str) -> Optional[str]:
    return _CORP_CODE_MAP.get(stock_code.zfill(6))

def get_stock_name(stock_code: str) -> Optional[str]:
    return _STOCK_NAME_MAP.get(stock_code.zfill(6))

# -------------------- 매핑 정의 -------------------- #
AMAP = {
    "ifrs-full_Revenue": "revenue", "ifrs_Revenue": "revenue", "dart_Sales": "revenue",
    "dart_OperatingRevenue": "revenue", "dart_OrdinaryRevenue": "revenue",
    "ifrs-full_ProfitLossFromOperatingActivities": "op_income", "ifrs_ProfitLossFromOperatingActivities": "op_income",
    "dart_OperatingIncomeLoss": "op_income",
    "ifrs-full_ProfitLoss": "net_income", "ifrs_ProfitLoss": "net_income", "dart_ProfitLoss": "net_income",
    "dart_ProfitLossAttributableToOwnersOfParent": "net_income",
    "ifrs-full_Equity": "total_equity", "ifrs_Equity": "total_equity", "dart_TotalEquity": "total_equity",
    "ifrs_EquityAttributableToOwnersOfParent": "total_equity", "ifrs_full_EquityAttributableToOwnersOfParent": "total_equity",
    "ifrs-full_Assets": "total_assets", "ifrs_Assets": "total_assets", "dart_TotalAssets": "total_assets",
    "ifrs-full_CashFlowsFromUsedInOperatingActivities": "cash_flow_op", "dart_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
    "ifrs_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
    "ifrs-full_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv", "dart_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
    "ifrs_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
    "ifrs-full_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin", "dart_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
    "ifrs_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin",
    "ifrs-full_EarningsPerShare": "eps", "ifrs_BasicEarningsLossPerShare": "eps", "ifrs_BasicEarningsPerShare": "eps",
    "ifrs_EarningsPerShare": "eps", "dart_EarningsPerShare": "eps", "dart_BasicEarningsLossPerShare": "eps",
}
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
    "revenue": [], "total_assets": [], "total_equity": [], 
    "cash_flow_op": [], "cash_flow_inv": [], "cash_flow_fin": [],
}

# -------------------- [복구] 공시일 조회 & 캐시 로직 -------------------- #
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
    # 파일 기반 캐시 로드 (25121223 로직 원복)
    if not cache_dir.exists(): return None
    path = cache_dir / f"{str(stock_code).zfill(6)}_{corp_code}.json"
    if not path.exists():
        path = cache_dir / f"{corp_code}.json"
        if not path.exists(): return None
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        if isinstance(data, list): return data
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

# -------------------- [복구] 배당금 정밀 조회 -------------------- #
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
    except: pass
    return None

# -------------------- 파싱 로직 (25121223) -------------------- #
def parse_dart_list(list_rows: List[dict]) -> Dict[str, Optional[float]]:
    row = {}
    row_prio = {}
    eq_owner, eq_nonctl, eq_total = None, None, None
    
    for item in list_rows:
        aid = item.get("account_id", "") or ""
        anm_raw = item.get("account_nm", "") or ""
        val_add = safe_float(item.get("thstrm_add_amount", ""))
        val_amt = safe_float(item.get("thstrm_amount", ""))
        
        norm = re.sub(r"[\s\(\)\[\]\{\}\-_/\.]", "", anm_raw)
        detail = (item.get("account_detail") or "").strip()
        key = AMAP.get(aid)
        
        # 자본 처리
        if aid in {"ifrs-full_EquityAttributableToOwnersOfParent", "dart_OwnersEquity"}:
            eq_owner = val_amt if val_amt is not None else eq_owner; continue
        if aid in {"ifrs-full_NoncontrollingInterests", "dart_NonControllingInterests"}:
            eq_nonctl = val_amt if val_amt is not None else eq_nonctl; continue
        if aid in {"ifrs-full_Equity", "dart_TotalEquity"}:
            if eq_total is None and val_amt is not None: eq_total = val_amt
            continue

        # 키워드 매핑
        prio = 0
        if not key:
            for tgt, patterns in KMAP.items():
                if tgt == "revenue" and not patterns: continue 
                if any(bad in norm for bad in BLACKLIST.get(tgt, [])): continue
                if any(p in norm for p in patterns):
                    key = tgt; prio = 1; break
        
        if not key: continue
        
        val = None
        is_cum = False
        if key in FLOW_COLS_TO_DIFF:
            if val_add is not None: val = val_add; is_cum = True
            elif val_amt is not None: val = val_amt; is_cum = False
        else:
            val = val_amt
            
        if val is None: continue
        
        old_prio = row_prio.get(key, 99)
        if prio <= old_prio:
            row[key] = val
            row_prio[key] = prio
            if key in FLOW_COLS_TO_DIFF:
                row[f"__is_cumulative_{key}"] = is_cum

    if eq_total is None and (eq_owner is not None or eq_nonctl is not None):
        eq_total = (eq_owner or 0) + (eq_nonctl or 0)
    if eq_total is not None: row["total_equity"] = eq_total
    return row

# -------------------- 워커 (로직 통합) -------------------- #
def collect_one_code(args):
    code, start_date, end_date, out_dir, keys, log_queue, counter_dict, announce_mode, list_cache_dir = args
    
    def log(msg, level="INFO"):
        if log_queue: log_queue.put((level, msg))
    
    code6 = str(code).zfill(6)
    corp_code = get_corp_code(code6)
    stock_name = get_stock_name(code6)
    
    if not corp_code:
        counter_dict['fail'] += 1
        return

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    reprt_codes = ["11013", "11012", "11014", "11011"]
    
    collected_rows = []
    
    for year in range(start_year, end_year + 1):
        prev_cum = {c: None for c in FLOW_COLS_TO_DIFF}
        
        for rc in reprt_codes:
            fin_row = {}
            got_fin = False
            announce_dt = None
            status_code = None 
            
            q_map = {"11013": "0331", "11012": "0630", "11014": "0930", "11011": "1231"}
            quarter_date = f"{year}{q_map.get(rc, '1231')}"
            
            # [복구] 배당금 조회 (정밀 버전)
            div_val = get_dividend_public_api(stock_name, quarter_date)

            for key in keys:
                # [복구] 공시일 조회 (옵션에 따름)
                if announce_mode != "none" and announce_dt is None:
                    announce_dt = resolve_announce_date(corp_code, code6, year, rc, key, announce_mode, list_cache_dir)
                
                # 재무 수집 (CFS -> OFS Fallback)
                for fs_div in ["CFS", "OFS"]:
                    params = {"crtfc_key": key, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rc, "fs_div": fs_div}
                    s, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=2)
                    
                    if not data: continue
                    d_stat = data.get("status")
                    if d_stat == "000" and data.get("list"):
                        fin_row = parse_dart_list(data["list"])
                        got_fin = True; status_code = "000"
                        break
                    elif d_stat == "013":
                        status_code = "013"
                        if fs_div == "OFS": break
                    elif d_stat in ["020", "800"]:
                        status_code = d_stat
                        break 
                
                if got_fin or status_code == "013": break
            
            # 저장 로직 (25121223 무결성 유지)
            if got_fin or div_val is not None or status_code == "013":
                row = {
                    "code": code6, "corp_code": corp_code, "bsns_year": year, "reprt_code": rc,
                    "announce_date": announce_dt, "div_amount": div_val,
                    "period": {"11013":"Q1", "11012":"Q2", "11014":"Q3", "11011":"Q4"}[rc],
                    "date": f"{year}{q_map[rc]}"
                }
                
                curr_cum_snap = {}
                for col in FINANCE_COLS:
                    if col in ["announce_date", "div_amount"]: continue
                    val = fin_row.get(col)
                    
                    if col in FLOW_COLS_TO_DIFF:
                        is_cum = fin_row.get(f"__is_cumulative_{col}", False)
                        if val is None:
                            row[col] = None
                        elif row["period"] == "Q1":
                            row[col] = val
                            curr_cum_snap[col] = val
                        else:
                            if is_cum and prev_cum[col] is not None:
                                row[col] = val - prev_cum[col]
                                curr_cum_snap[col] = val
                            elif is_cum and prev_cum[col] is None:
                                row[col] = None # 계산불가
                                curr_cum_snap[col] = val
                            else:
                                row[col] = val
                                prev = prev_cum[col]
                                if prev is not None: curr_cum_snap[col] = prev + val
                                else: curr_cum_snap[col] = val
                    else:
                        row[col] = val
                
                for k, v in curr_cum_snap.items(): prev_cum[k] = v
                collected_rows.append(row)

    if collected_rows:
        df = pd.DataFrame(collected_rows)
        final_cols = [c for c in KEEP_COLS if c in df.columns]
        df = df[final_cols]
        df['code'] = df['code'].astype(str).str.zfill(6)
        df['corp_code'] = df['corp_code'].astype(str).str.zfill(8)
        
        save_path = out_dir / f"{code6}_sle.csv"
        idx = 1
        while save_path.exists():
            save_path = out_dir / f"{code6}_sle_{idx}.csv"
            idx += 1
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        log(f"[{code6}] Saved {len(df)} rows.", "INFO")
        counter_dict['success'] += 1
    else:
        log(f"[{code6}] No Data", "WARN")
        counter_dict['fail'] += 1
    
    counter_dict['done'] += 1

def worker_wrapper(args):
    try: collect_one_code(args)
    except Exception as e:
        if len(args) >= 6 and args[5]: args[5].put(("ERROR", f"Worker Error: {e}"))

# -------------------- UI 스레드 -------------------- #
def ui_listener(log_queue, counter_dict, total_count):
    logs_buffer = []
    max_buffer = 5
    if os.name == 'nt': os.system('cls')
    
    while True:
        try:
            level, msg = log_queue.get(timeout=0.1)
            if level == "STOP": break
            time_str = datetime.now().strftime("%H:%M:%S")
            log_line = f"[{time_str}] {msg}"
            if len(log_line) > 75: log_line = log_line[:72] + "..."
            logs_buffer.append(log_line)
            if len(logs_buffer) > max_buffer: logs_buffer.pop(0)
        except Empty: pass
        
        done = counter_dict['done']
        pct = (done / total_count * 100) if total_count > 0 else 0
        status = f" [STATUS] Total: {total_count} | Done: {done} (Suc:{counter_dict['success']}/Fail:{counter_dict['fail']}) | Progress: {pct:.1f}% "
        print(f"\033[H\n{status.center(80, '=')}\n")
        for i in range(max_buffer):
            line = logs_buffer[i] if i < len(logs_buffer) else ""
            print(f"\033[K{line}")
        
        if done >= total_count and log_queue.empty(): break

# -------------------- MAIN -------------------- #
def main():
    mp.freeze_support()
    disable_quick_edit()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--codes-file", type=str, default=str(HARDCODED_CODE_PATH))
    parser.add_argument("--start", type=str, default="20160101")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--out", type=str, default=r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_sle_11date")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--announce-mode", type=str, default="none") # Default none logic maintained
    parser.add_argument("--list-cache", type=str, default="DART_LIST_CACHE")
    args = parser.parse_args()
    
    # 코드 로드 우선순위: args.codes -> args.codes_file -> Hardcoded
    codes = []
    if args.codes:
        codes = [c.strip() for c in args.codes.replace(",", "\n").splitlines() if c.strip()]
    elif args.codes_file and Path(args.codes_file).exists():
        codes = [ln.strip() for ln in Path(args.codes_file).read_text(encoding="utf-8").splitlines() if ln.strip()]
    
    if not codes:
        print(f"[ERR] No codes found in {args.codes_file}")
        return

    load_xml_global()
    keys = read_dart_keys_ordered()
    if not keys: return
    
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    list_cache_dir = Path(args.list_cache)

    manager = mp.Manager()
    log_queue = manager.Queue()
    counter_dict = manager.dict({'done': 0, 'success': 0, 'fail': 0})
    
    ui_process = mp.Process(target=ui_listener, args=(log_queue, counter_dict, len(codes)))
    ui_process.start()
    
    task_args = []
    for code in codes:
        task_args.append((code, args.start, args.end, out_dir, keys, log_queue, counter_dict, args.announce_mode, list_cache_dir))
    
    with mp.Pool(args.workers) as pool:
        pool.map(worker_wrapper, task_args)
    
    log_queue.put(("STOP", "Finished"))
    ui_process.join()

if __name__ == "__main__":
    main()