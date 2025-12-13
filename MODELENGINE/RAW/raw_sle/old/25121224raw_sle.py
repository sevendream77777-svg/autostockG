# -*- coding: utf-8 -*-
"""
run_raw_sle_final.py
- SLE 재무 데이터 고속 수집기 (UI 개선 & 멈춤 방지 버전)

**기능 요약:**
1. [UI] 콘솔 스크롤 방지, 진행바 상단 고정, 하단 로그 5줄 롤링.
2. [LOG] 모든 로그는 'log_execution_YYYYMMDD.txt'에 자동 저장.
3. [SYS] Windows 콘솔 'QuickEdit Mode' 강제 비활성화 (클릭 시 멈춤 현상 해결).
4. [CORE] 기존 정밀 수집 로직(CFS->OFS, 키 로테이션, 매핑) 100% 유지.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
import sys
import logging
import ctypes
import threading
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from queue import Empty

import pandas as pd
import requests

# -------------------- 윈도우 콘솔 멈춤 방지 -------------------- #
def disable_quick_edit():
    """윈도우 CMD/PowerShell에서 마우스 클릭 시 실행 멈춤(QuickEdit) 방지"""
    if os.name != 'nt': return
    try:
        kernel32 = ctypes.windll.kernel32
        hStdIn = kernel32.GetStdHandle(-10)
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(hStdIn, ctypes.byref(mode)): return
        # ENABLE_QUICK_EDIT_MODE (0x0040) 끄기, ENABLE_EXTENDED_FLAGS (0x0080) 켜기
        new_mode = (mode.value & ~0x0040) | 0x0080
        kernel32.SetConsoleMode(hStdIn, new_mode)
    except Exception:
        pass

# -------------------- 설정 및 상수 -------------------- #
META_COLS = ["corp_code", "bsns_year", "reprt_code"]
FINANCE_COLS = [
    "announce_date", "revenue", "op_income", "net_income", "eps",
    "total_assets", "total_equity",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    "div_amount",
]
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

# -------------------- 유틸리티 함수 -------------------- #
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

# -------------------- XML 매퍼 (전역) -------------------- #
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

# -------------------- 매핑 및 상수 -------------------- #
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

# -------------------- 파싱 로직 -------------------- #
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
        
        # 자본 보정
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
                if any(bad in norm for bad in BLACKLIST.get(tgt, [])): continue
                if any(p in norm for p in patterns):
                    key = tgt; prio = 1; break
        
        if not key: continue
        
        # 값 결정
        val = None
        is_cum = False
        if key in FLOW_COLS_TO_DIFF:
            if val_add is not None: val = val_add; is_cum = True
            elif val_amt is not None: val = val_amt; is_cum = False
        else:
            val = val_amt
            
        if val is None: continue
        
        # 우선순위 (ID Match(0) > Keyword(1))
        old_prio = row_prio.get(key, 99)
        if prio <= old_prio:
            # ID 매치끼리 혹은 키워드끼리 경쟁시, 더 구체적인 처리는 생략(단순 덮어쓰기)
            row[key] = val
            row_prio[key] = prio
            if key in FLOW_COLS_TO_DIFF:
                row[f"__is_cumulative_{key}"] = is_cum

    if eq_total is None and (eq_owner is not None or eq_nonctl is not None):
        eq_total = (eq_owner or 0) + (eq_nonctl or 0)
    if eq_total is not None: row["total_equity"] = eq_total
    return row

def get_dividend_public_api(stock_name: str, base_date: str) -> Optional[float]:
    API_KEY = "9c3cf7dd64c3f256bc2533ea8698751579ccbd7df0bf5489c5493abce4a99f7b"
    URL = "https://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo"
    name_map = {"SK하이닉스": "에스케이하이닉스", "SK": "에스케이", "LG전자": "엘지전자", "LG화학": "엘지화학", "KT": "케이티", "POSCO홀딩스": "포스코홀딩스"}
    q_name = name_map.get(stock_name, stock_name)
    params = {"serviceKey": API_KEY, "numOfRows": "50", "pageNo": "1", "resultType": "json", "stckIssuCmpyNm": q_name, "dvdnBasDt": base_date}
    try:
        r = requests.get(URL, params=params, timeout=5)
        items = r.json().get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if isinstance(items, dict): items = [items]
        for it in items:
            if "보통주" in (it.get("se") or "") or "보통주" in (it.get("scrsItmsKcdNm") or ""):
                amt = safe_float(it.get("stckGenrDvdnAmt"))
                if amt is not None and amt >= 0: return amt
    except: pass
    return None

# -------------------- 워커 프로세스 -------------------- #
def collect_one_code(args):
    """
    args: (code, start_date, end_date, out_dir, keys, log_queue, counter_dict)
    """
    code, start_date, end_date, out_dir, keys, log_queue, counter_dict = args
    
    # 로깅 헬퍼
    def log(msg, level="INFO"):
        if log_queue: log_queue.put((level, msg))
    
    code6 = str(code).zfill(6)
    corp_code = get_corp_code(code6)
    stock_name = get_stock_name(code6)
    
    if not corp_code:
        log(f"[{code6}] CorpCode Not Found", "WARN")
        counter_dict['fail'] += 1
        return

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    reprt_codes = ["11013", "11012", "11014", "11011"]
    
    collected_rows = []
    
    for year in range(start_year, end_year + 1):
        prev_cum = {c: None for c in FLOW_COLS_TO_DIFF}
        
        for rc in reprt_codes:
            # 1. API Call Loop (Key Rotation & Fallback)
            fin_row = {}
            got_fin = False
            status_code = None # 000, 013, 020...
            
            for key in keys:
                for fs_div in ["CFS", "OFS"]:
                    params = {"crtfc_key": key, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rc, "fs_div": fs_div}
                    s, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=2, timeout=5)
                    
                    if not data: continue
                    d_stat = data.get("status")
                    
                    if d_stat == "000" and data.get("list"):
                        fin_row = parse_dart_list(data["list"])
                        got_fin = True; status_code = "000"
                        break
                    elif d_stat == "013":
                        status_code = "013"
                        # OFS까지 013이면 진짜 없는 것. CFS 013이면 OFS 시도.
                        if fs_div == "OFS": break
                    elif d_stat in ["020", "800"]:
                        status_code = d_stat
                        break # Key Error -> Break fs_div -> Next Key
                        
                if got_fin or status_code == "013": break
            
            # 2. 배당 조회
            q_map = {"11013": "0331", "11012": "0630", "11014": "0930", "11011": "1231"}
            div_val = get_dividend_public_api(stock_name, f"{year}{q_map[rc]}")
            
            # 3. 저장 여부 (데이터 없음(013)이라도 행 생성)
            if got_fin or div_val is not None or status_code == "013":
                row = {
                    "code": code6, "corp_code": corp_code, "bsns_year": year, "reprt_code": rc,
                    "period": {"11013":"Q1", "11012":"Q2", "11014":"Q3", "11011":"Q4"}[rc],
                    "div_amount": div_val, "announce_date": None
                }
                row["date"] = f"{year}{q_map[rc]}"
                
                # Flow Variable 누적 차감
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
                            if is_cum:
                                prev = prev_cum[col]
                                if prev is not None:
                                    row[col] = val - prev
                                    curr_cum_snap[col] = val
                                else:
                                    row[col] = None # 계산 불가
                                    curr_cum_snap[col] = val
                            else: # 이미 분기값
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
        
        save_path = out_dir / f"{code6}_sle.csv"
        # 덮어쓰기 방지 (v1, v2...)
        idx = 1
        while save_path.exists():
            save_path = out_dir / f"{code6}_sle_{idx}.csv"
            idx += 1
            
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        log(f"[{code6}] Saved {len(df)} rows.", "INFO")
        counter_dict['success'] += 1
    else:
        log(f"[{code6}] No Data Collected.", "WARN")
        counter_dict['fail'] += 1
    
    counter_dict['done'] += 1

def worker_wrapper(args):
    try:
        collect_one_code(args)
    except Exception as e:
        # 큐가 있을 경우 에러 전송
        if len(args) >= 6 and args[5]:
            args[5].put(("ERROR", f"Worker Error: {e}"))

# -------------------- UI/Logging 리스너 -------------------- #
def ui_listener(log_queue, counter_dict, total_count, log_file_path):
    """
    메인 UI 업데이트 및 파일 로깅 담당 스레드
    """
    logs_buffer = [] # 화면에 보여줄 최근 로그 (5줄)
    max_buffer = 5
    
    # 콘솔 초기화 (Clear)
    if os.name == 'nt': os.system('cls')
    else: os.system('clear')

    with open(log_file_path, 'a', encoding='utf-8') as f:
        while True:
            try:
                # 1. 큐에서 메시지 꺼내기 (0.1초 대기)
                level, msg = log_queue.get(timeout=0.1)
                if level == "STOP": break
                
                # 2. 파일 쓰기
                time_str = datetime.now().strftime("%H:%M:%S")
                f.write(f"[{time_str}] [{level}] {msg}\n")
                f.flush()
                
                # 3. 화면 버퍼 업데이트
                log_line = f"[{time_str}] {msg}"
                if len(log_line) > 75: log_line = log_line[:72] + "..." # 길이 자르기
                logs_buffer.append(log_line)
                if len(logs_buffer) > max_buffer: logs_buffer.pop(0)
                
            except Empty:
                pass
            
            # 4. 화면 그리기 (ANSI Escape Code 사용)
            done = counter_dict['done']
            success = counter_dict['success']
            fail = counter_dict['fail']
            pct = (done / total_count * 100) if total_count > 0 else 0
            
            # 커서를 맨 위로 이동 (\033[H) 후 내용 덮어쓰기
            # 상단 상태바
            status_bar = f" [STATUS] Total: {total_count} | Done: {done} (Suc:{success}/Fail:{fail}) | Progress: {pct:.1f}% "
            print(f"\033[H\n{status_bar.center(80, '=')}\n")
            
            # 로그 영역 (5줄 고정)
            for i in range(max_buffer):
                line = logs_buffer[i] if i < len(logs_buffer) else ""
                # 줄 지우기(\033[K) 후 출력
                print(f"\033[K{line}")
            
            # 완료 체크
            if done >= total_count and log_queue.empty():
                break

# -------------------- Main -------------------- #
def main():
    mp.freeze_support()
    disable_quick_edit() # 멈춤 방지 적용
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--start", type=str, default="20160101")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--out", type=str, default=r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_sle_11date")
    parser.add_argument("--workers", type=int, default=10) # Default 10
    args = parser.parse_args()
    
    # 1. 대상 코드 로드
    codes = []
    if args.codes:
        codes = [c.strip() for c in args.codes.replace(",", "\n").splitlines() if c.strip()]
    if not codes:
        # 기본 코드 (테스트용)
        codes = ["000660"]
        print("[WARN] No codes provided. Using default: 000660")

    # 2. 메타데이터 로드 (전역 공유를 위해 Main에서 로드)
    load_xml_global()
    
    # 3. API 키 로드
    keys = read_dart_keys_ordered()
    if not keys:
        print("[ERR] API Key Not Found!")
        return

    # 4. 디렉토리 및 로그 파일 준비
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"log_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_path = Path.cwd() / log_filename
    print(f"Logs will be saved to: {log_path}")
    
    # 5. Multiprocessing Manager 설정
    manager = mp.Manager()
    log_queue = manager.Queue()
    counter_dict = manager.dict({'done': 0, 'success': 0, 'fail': 0})
    
    # 6. UI 리스너 스레드 시작
    # (메인 프로세스는 UI 업데이트, 자식 프로세스는 수집)
    # 별도 프로세스로 띄울 수도 있지만, 여기선 메인 스레드나 별도 스레드로 처리
    # 화면 갱신을 위해 메인 스레드에서 UI를 그리는게 낫지만,
    # Pool.map이 블로킹되므로 UI를 별도 프로세스로 분리하거나 비동기로 해야 함.
    # 여기서는 간단하게 UI를 별도 Process로 띄우고 Main은 Pool 대기.
    
    ui_process = mp.Process(target=ui_listener, args=(log_queue, counter_dict, len(codes), log_path))
    ui_process.start()
    
    # 7. 워커 실행
    task_args = []
    for code in codes:
        task_args.append((code, args.start, args.end, out_dir, keys, log_queue, counter_dict))
    
    with mp.Pool(args.workers) as pool:
        pool.map(worker_wrapper, task_args)
    
    # 8. 종료 처리
    log_queue.put(("STOP", "Finished"))
    ui_process.join()
    print("\n[Complete] All tasks finished.")

if __name__ == "__main__":
    main()