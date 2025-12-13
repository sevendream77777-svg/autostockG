# -*- coding: utf-8 -*-
"""
run_raw_sle.py
- SLE 재무 11컬럼 단독 수집기 (2016년 1Q 이후)
- 외부 의존 없이 단일 파일로 실행
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

# -------------------- 컬럼 정의 (11) -------------------- #
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
KEEP_COLS = ["date", "code", "period"] + META_COLS + FINANCE_COLS
VERBOSE = os.environ.get("DART_VERBOSE", "0") == "1"

STATUS_FILE = "status.json"

# corp_code XML 후보
DEFAULT_XML_CANDIDATES = [
    Path(__file__).resolve().parent / "dart_corp_list.xml",
    Path(__file__).resolve().parents[2] / "MODELENGINE" / "RAW" / "raw_v48" / "dart_corp_list.xml",
    Path.cwd() / "dart_corp_list.xml",
]

# DART 키 후보
DEFAULT_KEY_PATHS = [
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\2slkdaum_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\3naver_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\4se77777gmail_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\5se1117gmail_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\8sevendrenaver_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\6109_kitchennaver_dart.txt"),
    Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\7109kitchen109naver_dart.txt"),
    Path(__file__).resolve().parents[2] / "opendart_apikey.txt",
]

# -------- 유틸 -------- #
def safe_float(v) -> Optional[float]:
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except Exception:
        return None


def _extract_leading_int(name: str) -> int:
    m = re.match(r"^(\d{1,2})", name)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 999
    return 999


def _read_lines(path: Path) -> List[str]:
    try:
        if path.exists():
            txt = path.read_text(encoding="utf-8", errors="ignore")
            return [ln.strip() for ln in txt.replace(",", "\n").splitlines() if ln.strip()]
    except Exception:
        pass
    return []


def read_dart_keys_ordered() -> List[str]:
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
    for p in DEFAULT_XML_CANDIDATES:
        if p.exists():
            return True
    return False


def log_info(msg: str):
    print(msg)


def log_warn(msg: str):
    print(f"[WARN] {msg}")


def log_error(msg: str):
    print(f"[ERROR] {msg}")


def http_get_json(url: str, params: dict, tries: int = 3, timeout: int = 8):
    for a in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504, 403):
                if a == tries - 1:
                    return r.status_code, None
                time.sleep(0.5 * (2 ** (a % 3)))
                continue
            if r.status_code != 200:
                if a == tries - 1:
                    return r.status_code, None
                continue
            return r.status_code, r.json()
        except Exception:
            if a == tries - 1:
                return -1, None
            time.sleep(0.5 * (2 ** (a % 3)))
    return -1, None


_CORP_CODE_MAP: Dict[str, str] = {}


def get_corp_code(stock_code: str) -> Optional[str]:
    sc = stock_code.zfill(6)
    if _CORP_CODE_MAP:
        return _CORP_CODE_MAP.get(sc)
    import xml.etree.ElementTree as ET
    for xml_path in DEFAULT_XML_CANDIDATES:
        try:
            if not xml_path.exists():
                continue
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for corp in root.findall(".//list"):
                s = (corp.findtext("stock_code", "") or "").strip().zfill(6)
                c = (corp.findtext("corp_code", "") or "").strip()
                if s and c:
                    _CORP_CODE_MAP[s] = c
            if _CORP_CODE_MAP:
                break
        except Exception:
            continue
    _CORP_CODE_MAP.setdefault("005930", "00126380")
    _CORP_CODE_MAP.setdefault("000660", "00164779")
    return _CORP_CODE_MAP.get(sc)


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

KMAP = {
    # 매출: 과매칭 방지 → KMAP에서 제외 (AMAP 매칭만 사용)
    "revenue": [],
    "op_income": ["영업이익", "영업이익손실"],
    "net_income": ["당기순이익", "분기순이익", "반기순이익", "당기순이익손실", "분기순이익손실"],
    "total_assets": ["자산총계", "총자산"],
    "total_equity": ["자본총계", "총자본"],
    "cash_flow_op": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름", "투자활동으로인한현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름", "재무활동으로인한현금흐름"],
    # 배당은 과매칭 위험 → KMAP에서 제외, 아래에서 별도 처리
    "eps": ["주당순이익", "주당이익"],
}

ANN_REPORT_CODES = {
    "11011": "사업보고서",
    "11012": "반기보고서",
    "11013": "1분기보고서",
    "11014": "3분기보고서",
}

# 공시일 계산 보조 상수
MONTH_BUCKETS = {
    "11013": {"04", "05", "06"},  # 1분기
    "11012": {"07", "08", "09"},  # 반기
    "11014": {"10", "11", "12"},  # 3분기
    "11011": {"02", "03", "04"},  # 사업보고서 (다음 해)
}
ANN_KEYWORDS_BASE = ["분기보고서", "반기보고서", "3분기보고서", "사업보고서", "잠정실적", "손익구조", "영업실적"]

# list.json 캐시
LIST_CACHE: Dict[tuple, Optional[List[dict]]] = {}


def parse_dart_list(list_rows: List[dict]) -> Dict[str, Optional[float]]:
    row: Dict[str, Optional[float]] = {}
    row_prio: Dict[str, int] = {}
    # 임시 보관: 지배주주지분/비지배주주지분 등
    eq_owner = None
    eq_nonctl = None
    eq_total = None
    flow_cols = {
        "revenue",
        "op_income",
        "net_income",
        "eps",
        "cash_flow_op",
        "cash_flow_inv",
        "cash_flow_fin",
        "div_amount",
    }

    # 계정별 우선순위 (낮은 index가 더 높은 우선순위)
    priority_map = {
        "revenue": [
            "ifrs-full_Revenue",
            "ifrs_Revenue",
            "dart_Sales",
            "dart_OperatingRevenue",
            "dart_OrdinaryRevenue",
        ],
        "op_income": [
            "dart_OperatingIncomeLoss",  # 실무에서 가장 자주 사용됨
            "ifrs-full_ProfitLossFromOperatingActivities",
            "ifrs_ProfitLossFromOperatingActivities",
        ],
        "net_income": [
            "ifrs-full_ProfitLoss",
            "ifrs_ProfitLoss",
            "dart_ProfitLossAttributableToOwnersOfParent",
            "dart_ProfitLoss",
        ],
    }

    def _priority_idx(key: str, aid: str) -> int:
        cand = priority_map.get(key)
        if not cand:
            return 999
        try:
            return cand.index(aid)
        except ValueError:
            # 우선순위 테이블에 없는 값은 낮은 우선순위로 취급
            return 900

    for item in list_rows:
        aid = item.get("account_id", "") or ""
        anm_raw = item.get("account_nm", "") or ""
        val_add = safe_float(item.get("thstrm_add_amount", ""))
        val_amt = safe_float(item.get("thstrm_amount", ""))
        norm = re.sub(r"[\s\(\)\[\]\{\}\-_/\.]", "", anm_raw)
        detail = (item.get("account_detail") or "").strip()
        key_hint = AMAP.get(aid)
        is_flow = key_hint in flow_cols
        val = val_add if is_flow and val_add is not None else val_amt

        # Equity 세부
        if aid in {"ifrs-full_EquityAttributableToOwnersOfParent", "ifrs_EquityAttributableToOwnersOfParent", "dart_OwnersEquity"}:
            eq_owner = val if val is not None else eq_owner
            continue
        if aid in {"ifrs-full_NoncontrollingInterests", "ifrs_NoncontrollingInterests", "dart_NonControllingInterests"}:
            eq_nonctl = val if val is not None else eq_nonctl
            continue
        if aid in {"ifrs-full_Equity", "ifrs_Equity", "dart_TotalEquity"}:
            # 멤버 분할(주식발행초과금 등)일 경우 총계 덮어쓰지 않음
            if "[" in detail or "member" in detail:
                if eq_total is None and val is not None:
                    eq_total = val
            else:
                if eq_total is None and val is not None:
                    eq_total = val
                elif val is not None and abs(val) > abs(eq_total or 0):
                    eq_total = val
            continue

        # 배당: DPS만 수집, 현금배당 지급 총액은 제외
        if ("주당배당" in norm) or ("배당금주당" in norm) or ("배당금1주당" in norm) or ("배당금1주당" in norm.replace(" ", "")) or ("dividendsper" in norm.lower() and "share" in norm.lower()):
            if val is not None:
                row["div_amount"] = val
            continue
        if aid and "DividendsPerShare" in aid:
            if val is not None:
                row["div_amount"] = val
            continue

        # 일반 매핑
        manual_prio = None
        key = AMAP.get(aid)
        # dart_ProfitLoss가 "법인세비용차감전순이익"에 붙는 경우는 제외
        if aid == "dart_ProfitLoss" and "법인세비용차감전" in norm:
            continue
        if not key:
            for tgt, patterns in KMAP.items():
                if tgt == "revenue":
                    continue  # revenue는 KMAP 매칭 금지
                if any(p in norm for p in patterns):
                    key = tgt
                    # 텍스트 매칭은 우선순위 최하로 둔다
                    manual_prio = 999
                    break
        if key and val is not None:
            prio = manual_prio if manual_prio is not None else _priority_idx(key, aid)
            if key == "total_equity":
                # 멤버 분할로 인한 덮어쓰기 방지
                if "[" in detail or "member" in detail:
                    if key not in row:
                        row[key] = val
                        row_prio[key] = prio
                else:
                    if (key not in row) or (prio < row_prio.get(key, 1000)) or (abs(val) > abs(row.get(key) or 0)):
                        row[key] = val
                        row_prio[key] = prio
            else:
                prev_prio = row_prio.get(key, 1000)
                if (key not in row) or (prio < prev_prio):
                    row[key] = val
                    row_prio[key] = prio

    # Equity 최종 계산: 지배주주+비지배 → total_equity
    if eq_total is None and (eq_owner is not None or eq_nonctl is not None):
        eq_total = (eq_owner or 0) + (eq_nonctl or 0)
    if eq_total is not None:
        row["total_equity"] = eq_total
    return row


def _select_announce_date_from_rows(list_rows: List[dict], year: int, reprt_code: str) -> Optional[str]:
    # year 범위 필터: 당해 1월1일 ~ 다음 해 4월30일
    bgn = f"{year}0101"
    end = f"{year + 1}0430"
    rows = [it for it in list_rows if bgn <= (it.get("rcept_dt") or "") <= end]
    if not rows:
        rows = list_rows

    def _in_bucket(rcept_dt: str) -> bool:
        if not rcept_dt or len(rcept_dt) < 6:
            return True
        mm = rcept_dt[4:6]
        return mm in MONTH_BUCKETS.get(reprt_code, set())

    def _match_kw(it, keywords):
        nm = (it.get("report_nm") or "").replace(" ", "")
        return any(kw in nm for kw in keywords)

    label = ANN_REPORT_CODES.get(reprt_code, "")
    keywords = []
    if label:
        keywords.append(label)
    keywords += ANN_KEYWORDS_BASE

    # 1순위: 보고서명 키워드 매칭(잠정/손익/영업 포함). 월 필터 유지, 없으면 완화.
    kw_candidates = [it for it in rows if _match_kw(it, keywords)]
    if kw_candidates:
        filtered = [it for it in kw_candidates if _in_bucket(it.get("rcept_dt", ""))]
        kw_candidates = filtered or kw_candidates
        kw_candidates = sorted(kw_candidates, key=lambda x: x.get("rcept_dt", "") or "99999999")
        return kw_candidates[0].get("rcept_dt")

    # 2순위: reprt_code 일치 + 월 범위 필터
    candidates = [it for it in rows if it.get("reprt_code") == reprt_code and _in_bucket(it.get("rcept_dt", ""))]

    # 3순위: reprt_code 일치 (월 필터 없이)
    if not candidates:
        candidates = [it for it in rows if it.get("reprt_code") == reprt_code]

    # 4순위: 아무 조건 없이 가장 빠른 공시일
    if not candidates:
        candidates = rows

    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda x: x.get("rcept_dt", "") or "99999999")
    return candidates[0].get("rcept_dt")


def load_list_cache(stock_code: str, corp_code: str, cache_dir: Path) -> Optional[List[dict]]:
    key = (str(stock_code).zfill(6), str(corp_code))
    if key in LIST_CACHE:
        return LIST_CACHE[key]
    if not cache_dir.exists():
        LIST_CACHE[key] = None
        return None
    path = cache_dir / f"{key[0]}_{key[1]}.json"
    if not path.exists():
        # 백업 네이밍(혹시 corp_code만 있을 경우)
        alt = cache_dir / f"{key[1]}.json"
        if alt.exists():
            path = alt
        else:
            LIST_CACHE[key] = None
            return None
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        data = json.loads(txt)
        if isinstance(data, list):
            LIST_CACHE[key] = data
            return data
    except Exception:
        pass
    LIST_CACHE[key] = None
    return None


def fetch_announce_date_live(corp_code: str, year: int, reprt_code: str, key: str) -> Optional[str]:
    url = "https://opendart.fss.or.kr/api/list.json"
    end_de = f"{year + 1}0430"
    params = {
        "crtfc_key": key,
        "corp_code": corp_code,
        "bgn_de": f"{year}0101",
        "end_de": end_de,
        "page_no": 1,
        "page_count": 100,
    }
    status, data = http_get_json(url, params, tries=2, timeout=8)
    if status != 200 or data is None:
        log_warn(f"[list] status={status} key={key[:8]} rc={reprt_code} corp={corp_code}")
        return None
    if data.get("status") != "000":
        log_warn(f"[list] status={data.get('status')} msg={data.get('message')} key={key[:8]} rc={reprt_code} corp={corp_code}")
        return None
    rows = data.get("list") or []
    return _select_announce_date_from_rows(rows, year, reprt_code)


def fetch_announce_date_cache(corp_code: str, stock_code: str, year: int, reprt_code: str, cache_dir: Path) -> Optional[str]:
    rows = load_list_cache(stock_code, corp_code, cache_dir)
    if not rows:
        return None
    return _select_announce_date_from_rows(rows, year, reprt_code)


def resolve_announce_date(
    corp_code: str,
    stock_code: str,
    year: int,
    reprt_code: str,
    key: str,
    mode: str,
    cache_dir: Path,
) -> Optional[str]:
    """
    mode:
      - none   : announce_date 비움
      - cache  : 캐시(by_corp)만 사용
      - live   : list.json 호출만 사용
      - hybrid : 캐시 우선, 없으면 live
    """
    if mode == "none":
        return None
    if mode in {"cache", "hybrid"}:
        ann = fetch_announce_date_cache(corp_code, stock_code, year, reprt_code, cache_dir)
        if ann:
            return ann
        if mode == "cache":
            return None
    # live/hybrid fallback
    return fetch_announce_date_live(corp_code, year, reprt_code, key)


def dump_accounts(list_rows: List[dict], out_dir: Path, code: str, year: int, rc: str, fs_div: str, api: str):
    if not VERBOSE:
        return
    try:
        uniq = {}
        for it in list_rows:
            aid = it.get("account_id")
            anm = it.get("account_nm")
            uniq[aid] = anm
        payload = [{"account_id": k, "account_nm": v} for k, v in uniq.items()]
        dbg_dir = Path(out_dir) / "debug_accounts"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        path = dbg_dir / f"{code}_{year}_{rc}_{fs_div}_{api}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[DEBUG] account dump: {path}")
    except Exception as e:
        print(f"[DEBUG] account dump failed: {e}")

def dump_raw_response(status: int, data: Optional[dict], out_dir: Path, code: str, year: int, rc: str, fs_div: str, api: str):
    try:
        dbg_dir = Path(out_dir) / "debug_accounts"
        dbg_dir.mkdir(parents=True, exist_ok=True)
        path = dbg_dir / f"{code}_{year}_{rc}_{fs_div}_{api}_raw.json"
        payload = {"status": status, "data": data}
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[DEBUG] raw dump: {path}")
    except Exception as e:
        print(f"[DEBUG] raw dump failed: {e}")

def collect_one_code(args):
    code, start_date, end_date, dart_mode, out_dir, ordered_keys, announce_mode, list_cache_dir = args
    code6 = str(code).zfill(6)
    print(f"\n[수집 시작] 종목 {code6}")

    corp_code = get_corp_code(code6)
    if not corp_code:
        print(f"[WARN] corp_code 없음: {code6}")
        return code6, False, {"issues": ["no_corp_code"]}

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    if dart_mode == "annual":
        reprt_codes = ["11011"]
    else:
        reprt_codes = ["11013", "11012", "11014", "11011"]

    collected_rows = []

    for year in range(start_year, end_year + 1):
        if year < 2016:
            continue  # 2015 이전은 수집하지 않음
        print(f"  [연도] {year}년")
        seen_rc_for_year = set()
        year_rows: Dict[str, dict] = {}

        for rc in reprt_codes:
            print(f"    [보고서] {rc} …")
            got_fin = False
            fin_row = None
            announce_dt = None
            used_key_for_rc = None

            for use_key in ordered_keys:
                # announce_date + 사용할 키 선택
                announce_dt = resolve_announce_date(corp_code, code6, year, rc, use_key, announce_mode, list_cache_dir)
                if not announce_dt and VERBOSE and announce_mode != "none":
                    print(f"      announce_date 없음 ({code6}, {year}, rc={rc}) key={use_key[:6]} mode={announce_mode}")

                def _try_all(fs_div: Optional[str]):
                    params = {
                        "crtfc_key": use_key,
                        "corp_code": corp_code,
                        "bsns_year": str(year),
                        "reprt_code": rc,
                    }
                    if fs_div:
                        params["fs_div"] = fs_div
                    status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=2, timeout=8)
                    if status != 200 or data is None:
                        log_warn(f"[All] status={status} key={use_key[:8]} rc={rc} year={year} fs={fs_div}")
                    else:
                        log_info(f"[All] status={data.get('status')} msg={data.get('message')} key={use_key[:8]} rc={rc} year={year} fs={fs_div}")
                    return status, data

                def _try_singl(fs_div: Optional[str]):
                    params = {
                        "crtfc_key": use_key,
                        "corp_code": corp_code,
                        "bsns_year": str(year),
                        "reprt_code": rc,
                    }
                    if fs_div:
                        params["fs_div"] = fs_div
                    status, data = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params, tries=2, timeout=8)
                    if status != 200 or data is None:
                        log_warn(f"[Singl] status={status} key={use_key[:8]} rc={rc} year={year} fs={fs_div}")
                    else:
                        log_info(f"[Singl] status={data.get('status')} msg={data.get('message')} key={use_key[:8]} rc={rc} year={year} fs={fs_div}")
                    return status, data

                # 1) CFS All
                for fs_div_opt in ("CFS",):
                    status_all, data_all = _try_all(fs_div_opt)
                    dump_raw_response(status_all, data_all, out_dir, code6, year, rc, fs_div_opt or "None", "All")
                    if status_all == 200 and data_all is not None:
                        st = data_all.get("status")
                        if st == "000":
                            if data_all.get("list"):
                                dump_accounts(data_all["list"], out_dir, code6, year, rc, fs_div_opt or "None", "All")
                                fin_row = parse_dart_list(data_all["list"])
                                got_fin = True
                                used_key_for_rc = use_key
                                break
                            else:
                                log_warn(f"[All] CFS list empty rc={rc} year={year} key={use_key[:8]}")
                        if st == "020":
                            continue
                        if VERBOSE:
                            print(f"      [rc={rc} fs={fs_div_opt} api=All] status={st} msg={data_all.get('message')}")
                if got_fin:
                    break

                # 2) CFS Singl
                for fs_div_opt in ("CFS",):
                    status_s, data_s = _try_singl(fs_div_opt)
                    dump_raw_response(status_s, data_s, out_dir, code6, year, rc, fs_div_opt or "None", "Singl")
                    if status_s == 200 and data_s is not None:
                        st = data_s.get("status")
                        if st == "000" and data_s.get("list"):
                            dump_accounts(data_s["list"], out_dir, code6, year, rc, fs_div_opt or "None", "Singl")
                            fin_row = parse_dart_list(data_s["list"])
                            got_fin = True
                            used_key_for_rc = use_key
                            break
                        if st != "020" and VERBOSE:
                            print(f"      [rc={rc} fs={fs_div_opt} api=Singl] status={st} msg={data_s.get('message')}")
                if got_fin:
                    break

            # announce_date가 비어 있으면 live/hybrid 모드에서 다른 키로 재시도
            if announce_dt is None and announce_mode in {"live", "hybrid"}:
                for k in ordered_keys:
                    announce_dt = resolve_announce_date(corp_code, code6, year, rc, k, "live", list_cache_dir)
                    if announce_dt:
                        break

            # 3) OFS (계정 id 수집용 덤프; 데이터는 사용하지 않음)
            for fs_div_opt in ("OFS",):
                status_all, data_all = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", {
                    "crtfc_key": ordered_keys[0],
                    "corp_code": corp_code,
                    "bsns_year": str(year),
                    "reprt_code": rc,
                    "fs_div": fs_div_opt,
                }, tries=2, timeout=8)
                dump_raw_response(status_all, data_all, out_dir, code6, year, rc, fs_div_opt or "None", "All")
                if status_all == 200 and data_all is not None and data_all.get("list"):
                    dump_accounts(data_all["list"], out_dir, code6, year, rc, fs_div_opt or "None", "All")
                status_s, data_s = http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", {
                    "crtfc_key": ordered_keys[0],
                    "corp_code": corp_code,
                    "bsns_year": str(year),
                    "reprt_code": rc,
                    "fs_div": fs_div_opt,
                }, tries=2, timeout=8)
                dump_raw_response(status_s, data_s, out_dir, code6, year, rc, fs_div_opt or "None", "Singl")
                if status_s == 200 and data_s is not None and data_s.get("list"):
                    dump_accounts(data_s["list"], out_dir, code6, year, rc, fs_div_opt or "None", "Singl")

            if got_fin and fin_row:
                if rc in seen_rc_for_year:
                    if VERBOSE:
                        print(f"      [중복] reprt_code={rc} 스킵")
                    continue
                fin_row["corp_code"] = corp_code
                fin_row["bsns_year"] = str(year)
                fin_row["reprt_code"] = rc
                fin_row["period"] = {
                    "11013": "Q1",
                    "11012": "Q2_ACC",
                    "11014": "Q3_ACC",
                    "11011": "FY",
                }.get(rc, "")
                fin_row["announce_date"] = announce_dt
                if rc == "11011":
                    fin_row["date"] = f"{year}1231"
                elif rc == "11012":
                    fin_row["date"] = f"{year}0630"
                elif rc == "11013":
                    fin_row["date"] = f"{year}0331"
                elif rc == "11014":
                    fin_row["date"] = f"{year}0930"
                else:
                    fin_row["date"] = f"{year}1231"
                fin_row["code"] = code6
                fin_row["used_key"] = used_key_for_rc
                year_rows[rc] = fin_row
                seen_rc_for_year.add(rc)
            else:
                log_warn(f"      재무 없음 ({code6}, {year}, rc={rc})")

        # 누적→분기 변환 (11013=Q1, 11012=반기누적, 11014=3Q누적, 11011=연간)
        def _diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
            if a is None or b is None:
                return None
            return a - b

        flow_cols = {
            "revenue",
            "op_income",
            "net_income",
            "eps",
            "cash_flow_op",
            "cash_flow_inv",
            "cash_flow_fin",
            "div_amount",
        }

        def _apply_diff(target: dict, minuend: dict, subtrahend: dict):
            for col in FINANCE_COLS:
                if col == "announce_date":
                    continue
                if col in flow_cols:
                    target[col] = _diff(minuend.get(col), subtrahend.get(col))
                else:
                    # 재무상태표 항목은 시점 값 그대로 사용
                    target[col] = minuend.get(col)

        q1 = year_rows.get("11013")
        h1 = year_rows.get("11012")
        q3c = year_rows.get("11014")
        ann = year_rows.get("11011")

        q2 = None
        q3 = None
        q4 = None

        if h1 and q1:
            q2 = dict(h1)
            _apply_diff(q2, h1, q1)
            q2["date"] = f"{year}0630"
            q2["reprt_code"] = "11012"
            q2["period"] = "Q2"
        if q3c and h1:
            q3 = dict(q3c)
            _apply_diff(q3, q3c, h1)
            q3["date"] = f"{year}0930"
            q3["reprt_code"] = "11014"
            q3["period"] = "Q3"
        if ann and q3c:
            q4 = dict(ann)
            _apply_diff(q4, ann, q3c)
            q4["date"] = f"{year}1231"
            q4["reprt_code"] = "11011"
            q4["period"] = "Q4"

        for row in [q1, q2, q3, q4]:
            if row:
                if "period" not in row or not row.get("period"):
                    row["period"] = {
                        "11013": "Q1",
                        "11012": "Q2_ACC",
                        "11014": "Q3_ACC",
                        "11011": "FY",
                    }.get(row.get("reprt_code", ""), "")
                collected_rows.append(row)

    return code6, True, {"rows": collected_rows}


def finalize_one_code(code, collected_rows, out_dir):
    code6 = str(code).zfill(6)
    if not collected_rows:
        print(f"[WARN] 수집결과 없음: {code6}")
        return False
    df = pd.DataFrame(collected_rows)
    for col in KEEP_COLS:
        if col not in df.columns:
            df[col] = None
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["corp_code"] = df["corp_code"].astype(str)
    df = df[KEEP_COLS]
    out_csv_dir = Path(out_dir) / "csv"
    out_csv_dir.mkdir(parents=True, exist_ok=True)
    # 파일명에 수집 시각(YYMMDDHH) 추가
    ts = datetime.now().strftime("%y%m%d%H")
    csv_path = out_csv_dir / f"{code6}_{ts}.csv"
    if csv_path.exists():
        idx = 1
        cand = csv_path
        while cand.exists():
            cand = out_csv_dir / f"{code6}_{ts}_{idx}.csv"
            idx += 1
        csv_path = cand
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[CSV 저장 완료] {csv_path}")
    return True


def _write_status(out_dir: Path, payload: dict):
    try:
        path = Path(out_dir) / STATUS_FILE
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def run_single_task(task_args):
    idx, total, code, start_date, end_date, dart_mode, out_dir, ordered_keys, announce_mode, list_cache_dir = task_args
    print(f"[진행] {idx}/{total} code={code}")
    code6, ok, info = collect_one_code((code, start_date, end_date, dart_mode, out_dir, ordered_keys, announce_mode, list_cache_dir))
    if ok:
        finalize_one_code(code, info.get("rows", []), out_dir)
    _write_status(out_dir, {
        "ts": datetime.now().isoformat(),
        "idx": idx,
        "total": total,
        "code": code,
        "ok": ok,
        "rows": len(info.get("rows", [])) if info else 0,
    })
    return code6, ok, info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--start", type=str, default="20160101")
    parser.add_argument("--end", type=str, default="20251205")
    parser.add_argument("--mode", type=str, default="full", choices=["annual", "full"])
    parser.add_argument("--out", type=str, default=str(Path(__file__).resolve().parent / "out"))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--announce-mode", type=str, default="live", choices=["live", "cache", "hybrid", "none"])
    parser.add_argument("--list-cache", type=str, default=str(Path(__file__).resolve().parent / "out" / "list" / "by_corp"))
    args = parser.parse_args()

    codes: List[str] = []
    if args.codes.strip():
        for x in args.codes.replace(",", "\n").splitlines():
            x = x.strip()
            if x:
                codes.append(x)
    if not codes:
        print("[ERROR] --codes 종목코드 필요")
        return

    if not ensure_corp_xml_exists():
        log_error("dart_corp_list.xml을 찾을 수 없습니다. DEFAULT_XML_CANDIDATES 경로를 확인하세요.")
        return

    ordered_keys = read_dart_keys_ordered()
    if not ordered_keys:
        print("[ERROR] DART 키 없음")
        return
    print(f"[키 로드됨] 총 {len(ordered_keys)}개")
    for i, k in enumerate(ordered_keys, 1):
        print(f"  {i}. {k[:10]}...")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    list_cache_dir = Path(args.list_cache)

    tasks = []
    total = len(codes)
    for i, c in enumerate(codes, 1):
        tasks.append((i, total, c, args.start, args.end, args.mode, out_dir, ordered_keys, args.announce_mode, list_cache_dir))

    if args.workers <= 1:
        for t in tasks:
            run_single_task(t)
        return
    with mp.Pool(args.workers) as pool:
        pool.map(run_single_task, tasks)


if __name__ == "__main__":
    try:
        mp.freeze_support()
    except Exception:
        pass
    print("\n============================================")
    print("   SLE Financial Collector (11 cols, 2016+)")
    print("============================================\n")
    main()
    print("\n[완료] 모든 작업 종료\n")


