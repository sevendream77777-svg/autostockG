# -*- coding: utf-8 -*-
# ==============================================
# run_dart_standalone_v48.py
# DART 전용 14컬럼 수집 + announce_date 확보 버전
# v48 독립 실행기 (가격/수급/매크로 제거)
# ==============================================

from __future__ import annotations
import argparse
import json
import os
import sys
import time
import logging
import pickle
import re
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
import requests
import multiprocessing as mp

# ----------------------------------------------
# 수집 컬럼 정의
# ----------------------------------------------
META_COLS = ["corp_code", "bsns_year", "reprt_code"]

FINANCE_COLS = [
    "announce_date",
    "revenue",
    "op_income",
    "net_income",
    "total_equity",
    "total_assets",
    "cash_flow_op",
    "cash_flow_inv",
    "cash_flow_fin",
    "div_amount",
    "eps",
    "roe",
    "bps",
    "debt_ratio",
]

KEEP_COLS = ["date", "code"] + META_COLS + FINANCE_COLS

# ----------------------------------------------
# 경로 / 로그 / 체크포인트
# ----------------------------------------------
CHECKPOINT = "checkpoint.pkl"
STATUS_FILE = "status.json"
QUALITY_LOG = "quality_dart.jsonl"
FAIL_QUEUE_LOG = "fail_queue.jsonl"

# DART corp_code XML 후보 경로
DEFAULT_XML_CANDIDATES = [
    Path(__file__).resolve().parent / "dart_corp_list.xml",
    Path(__file__).resolve().parents[2] / "MODELENGINE" / "RAW" / "raw_v48" / "dart_corp_list.xml",
    Path.cwd() / "dart_corp_list.xml",
]

# DART 키 파일 후보 (1~8순)
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

# ----------------------------------------------
# 안전 숫자 변환
# ----------------------------------------------
def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except:
        return None
# ----------------------------------------------
# 숫자 앞자리 추출 (키 정렬용)
# ----------------------------------------------
def _extract_leading_int(name: str) -> int:
    m = re.match(r"^(\d{1,2})", name)
    if m:
        try:
            return int(m.group(1))
        except:
            return 999
    return 999

# ----------------------------------------------
# 키 파일 읽기
# ----------------------------------------------
def _read_lines(path: Path) -> List[str]:
    try:
        if path.exists():
            txt = path.read_text(encoding="utf-8", errors="ignore")
            lines = [ln.strip() for ln in txt.replace(",", "\n").splitlines() if ln.strip()]
            return lines
    except:
        pass
    return []

# ----------------------------------------------
# DART 키 순서대로 읽기 (1→8)
# ----------------------------------------------
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

# ----------------------------------------------
# HTTP GET with retries
# ----------------------------------------------
def http_get_json(url: str, params: dict, tries: int = 4, timeout: int = 8):
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

        except:
            if a == tries - 1:
                return -1, None
            time.sleep(0.5 * (2 ** (a % 3)))

    return -1, None
# ----------------------------------------------
# corp_code XML 파싱
# ----------------------------------------------
_CORP_CODE_MAP = {}

def get_corp_code(stock_code: str) -> Optional[str]:
    global _CORP_CODE_MAP
    sc = stock_code.zfill(6)

    if _CORP_CODE_MAP:
        return _CORP_CODE_MAP.get(sc)

    last_err = None
    import xml.etree.ElementTree as ET

    for xml_path in DEFAULT_XML_CANDIDATES:
        try:
            if not xml_path.exists():
                last_err = f"not found: {xml_path}"
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

            last_err = f"parsed but empty: {xml_path}"

        except Exception as e:
            last_err = f"parse error {xml_path}: {e}"
            continue

    # fallback 기본값
    _CORP_CODE_MAP.setdefault("005930", "00126380")
    _CORP_CODE_MAP.setdefault("000660", "00164779")

    return _CORP_CODE_MAP.get(sc)

# ----------------------------------------------
# 계정명 → 재무컬럼 매핑
# ----------------------------------------------
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

    "ifrs-full_Equity": "total_equity",
    "ifrs_Equity": "total_equity",
    "dart_TotalEquity": "total_equity",

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
    "dart_EarningsPerShare": "eps",
}

# 전환기(2014~2015) 계정명 패턴: 공백/기호 제거 후 contains
KMAP = {
    "revenue": ["매출", "매출액", "영업수익", "수익", "수익매출액"],
    "op_income": ["영업이익", "영업이익손실"],
    "net_income": ["당기순이익", "분기순이익", "반기순이익", "당기순이익손실", "분기순이익손실"],
    "total_assets": ["자산총계", "총자산"],
    "total_equity": ["자본총계", "총자본"],
    "cash_flow_op": ["영업활동현금흐름", "영업활동으로인한현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름", "투자활동으로인한현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름", "재무활동으로인한현금흐름"],
    "div_amount": ["배당"],
    "eps": ["주당순이익", "주당이익"],
}

DIV_HINTS = ("배당", "Dividends", "배당금")

# ----------------------------------------------
# DART list → dict(row) 변환
# ----------------------------------------------
def parse_dart_list(list_rows: List[dict]) -> Dict[str, Optional[float]]:
    row = {}

    for item in list_rows:
        aid = item.get("account_id", "") or ""
        anm_raw = item.get("account_nm", "") or ""
        anm = anm_raw.replace(" ", "").replace("\t", "")
        val = safe_float(item.get("thstrm_amount", ""))

        key = AMAP.get(aid)

        if not key:
            norm = re.sub(r"[\s\(\)\[\]\{\}\-_/\.]", "", anm_raw)
            for tgt, patterns in KMAP.items():
                if any(p in norm for p in patterns):
                    key = tgt
                    break

        if key and val is not None:
            row[key] = val

    return row
# ----------------------------------------------
# DART 공시목록(list.json) → announce_date 확보
# ----------------------------------------------


def fetch_announce_date(corp_code: str, year: int, reprt_code: str, key: str) -> Optional[str]:
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": key,
        "corp_code": corp_code,
        "bgn_de": f"{year}0101",
        "end_de": f"{year}1231",
        "page_no": 1,
        "page_count": 100,
    }
    status, data = http_get_json(url, params, tries=3, timeout=8)
    if status != 200 or data is None or data.get("status") != "000":
        return None
    rows = data.get("list") or []
    candidates = [it for it in rows if it.get("reprt_code") == reprt_code]
    if not candidates:
        label = ANN_REPORT_CODES.get(reprt_code, "")
        if label:
            candidates = [it for it in rows if label in (it.get("report_nm") or "")]
    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda x: x.get("rcept_dt", ""), reverse=True)
    return candidates[0].get("rcept_dt")


def collect_one_code(args):
    (
        code,
        start_date,
        end_date,
        dart_mode,
        out_dir,
        sample_csv_limit,
        ordered_keys,
    ) = args

    start_t = time.time()
    code6 = str(code).zfill(6)

    print(f"\n[수집 시작] 종목 {code6}")

    # ------------------------------
    # corp_code 확보
    # ------------------------------
    corp_code = get_corp_code(code6)
    if not corp_code:
        print(f"[WARN] corp_code 없음: {code6}")
        return code6, False, {"issues": ["no_corp_code"]}

    # ------------------------------
    # 연도 범위 설정
    # ------------------------------
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    # dart_mode=full → 1Q, 2Q, 3Q, 연간 전체 가져옴
    if dart_mode == "annual":
        reprt_codes = ["11011"]
    else:
        reprt_codes = ["11013", "11012", "11014", "11011"]  # 1Q,2Q,3Q,사업

    # ------------------------------
    # 누적 저장 구조
    # ------------------------------
    collected_rows = []
    all_keys_exhausted = False

    # ------------------------------
    # 연도별 수집 시작
    # ------------------------------
    for year in range(start_year, end_year + 1):
        print(f"  [연도] {year}년")

        for rc in reprt_codes:
            print(f"    [보고서] {rc} …")

            # --------------------------
            # announce_date 먼저 확보
            # --------------------------
            announce_dt = None

            for key in ordered_keys:
                announce_dt = fetch_announce_date(corp_code, year, rc, key)
                if announce_dt:
                    break  # announce_date 확보됨

            # announce_date를 못 구했어도 재무는 계속 시도함
            if not announce_dt:
                print(f"      announce_date 없음 ({code6}, {year}, rc={rc})")

            
            # --------------------------
            # 재무 데이터 확보
            # --------------------------
            got_fin = False
            fin_row = None

            def _try_all(fs_div: Optional[str]):
                params = {
                    "crtfc_key": key,
                    "corp_code": corp_code,
                    "bsns_year": str(year),
                    "reprt_code": rc,
                }
                if fs_div:
                    params["fs_div"] = fs_div
                return http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json", params, tries=3, timeout=8)

            def _try_singl(fs_div: Optional[str]):
                params = {
                    "crtfc_key": key,
                    "corp_code": corp_code,
                    "bsns_year": str(year),
                    "reprt_code": rc,
                }
                if fs_div:
                    params["fs_div"] = fs_div
                return http_get_json("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params, tries=3, timeout=8)

            # 1) All: CFS → OFS
            for fs_div_opt in ("CFS", "OFS"):
                status_all, data_all = _try_all(fs_div_opt)
                if status_all != 200 or data_all is None:
                    continue
                st = data_all.get("status")
                if st == "000" and data_all.get("list"):
                    fin_row = parse_dart_list(data_all["list"])
                    got_fin = True
                    break
                else:
                    print(f"      [rc={rc} fs={fs_div_opt} api=All] status={st} msg={data_all.get('message')}")
                if st == "020":
                    continue
            # 2) Singl: CFS → OFS
            if not got_fin:
                for fs_div_opt in ("CFS", "OFS"):
                    status_s, data_s = _try_singl(fs_div_opt)
                    if status_s != 200 or data_s is None:
                        continue
                    st = data_s.get("status")
                    if st == "000" and data_s.get("list"):
                        fin_row = parse_dart_list(data_s["list"])
                        got_fin = True
                        break
                    else:
                        print(f"      [rc={rc} fs={fs_div_opt} api=Singl] status={st} msg={data_s.get('message')}")
                    if st == "020":
                        continue

            # --------------------------
            if got_fin and fin_row:
                fin_row["corp_code"] = corp_code
                fin_row["bsns_year"] = str(year)
                fin_row["reprt_code"] = rc
                fin_row["announce_date"] = announce_dt
                # 대표 날짜 매핑
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
                collected_rows.append(fin_row)
            else:
                print(f"      재무 없음 ({code6}, {year}, rc={rc})")
    # --------------------------
    # (추가) 수집 완료 후 return
    # --------------------------
    info = {
        "rows": collected_rows,
        "issues": [],
    }
    return code6, True, info

# ----------------------------------------------
# 수집 결과 마무리 (DF로 변환 후 CSV 저장)
# ----------------------------------------------
def finalize_one_code(code, collected_rows, out_dir, sample_csv_limit):
    code6 = str(code).zfill(6)

    if not collected_rows:
        print(f"[WARN] 수집결과 없음: {code6}")
        return False

    # DataFrame 생성
    df = pd.DataFrame(collected_rows)

    # 누락 컬럼 NA 채우기
    for col in KEEP_COLS:
        if col not in df.columns:
            df[col] = None

    # 최종 컬럼 순서 강제
    df = df[KEEP_COLS]

    # ------------------------------------------
    # CSV 저장 (내용 확인용)
    # ------------------------------------------
    out_csv_dir = Path(out_dir) / "csv"
    out_csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_csv_dir / f"{code6}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"[CSV 저장 완료] {csv_path}")

    return True
# ----------------------------------------------
# 메인 엔트리포인트
# ----------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", type=str, default="")
    parser.add_argument("--start", type=str, default="20180101")
    parser.add_argument("--end", type=str, default="20250101")
    parser.add_argument("--mode", type=str, default="full", choices=["annual", "full"])
    parser.add_argument("--out", type=str, default="out_dart_test")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--csv-limit", type=int, default=0)
    args = parser.parse_args()

    codes = []
    if args.codes.strip():
        for x in args.codes.replace(",", "\n").splitlines():
            x = x.strip()
            if x:
                codes.append(x)

    if not codes:
        print("[ERROR] --codes 종목코드 필요")
        return

    # ------------------------------------------
    # DART 키 로드 (1→8 정방향)
    # ------------------------------------------
    ordered_keys = read_dart_keys_ordered()
    if not ordered_keys:
        print("[ERROR] DART 키 없음")
        return

    print(f"[키 로드됨] 총 {len(ordered_keys)}개")
    for i, k in enumerate(ordered_keys, 1):
        print(f"  {i}. {k[:10]}...")

    # ------------------------------------------
    # 실행할 out 디렉토리 생성
    # ------------------------------------------
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------
    # 멀티프로세싱 준비
    # ------------------------------------------
    tasks = []
    total = len(codes)
    for i, c in enumerate(codes, 1):
        tasks.append((i, total, c, args.start, args.end, args.mode, out_dir, args.csv_limit, ordered_keys))

    # worker 1이면 단일 실행
    if args.workers <= 1:
        for t in tasks:
            _, ok, _ = run_single_task(t)
        return

    # 병렬 처리
    with mp.Pool(args.workers) as pool:
        pool.map(run_single_task, tasks)


# ----------------------------------------------
# 상태 기록
# ----------------------------------------------
def _write_status(out_dir: Path, payload: dict):
    try:
        path = Path(out_dir) / STATUS_FILE
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# ----------------------------------------------
# 멀티프로세싱용 래퍼
# ----------------------------------------------
def run_single_task(task_args):
    (
        idx, total,
        code, start_date, end_date, dart_mode,
        out_dir, sample_csv_limit, ordered_keys
    ) = task_args

    print(f"[진행] {idx}/{total} code={code}")
    print(f"[Task 시작] {code}")

    ok = False
    collected_rows = []

    # 단일 종목 수집
    (code6, ok, info) = collect_one_code((code, start_date, end_date,
                                          dart_mode, out_dir,
                                          sample_csv_limit, ordered_keys))

    # 성공 시 CSV 작성
    if ok:
        finalize_one_code(code, info.get("rows", []), out_dir, sample_csv_limit)

    try:
        _write_status(out_dir, {
            "ts": datetime.now().isoformat(),
            "idx": idx,
            "total": total,
            "code": code,
            "ok": ok,
            "rows": len(info.get("rows", [])) if info else 0,
        })
    except Exception:
        pass

    return code6, ok, info
# ----------------------------------------------
# 프로그램 시작점 (Windows 멀티프로세싱 안전)
# ----------------------------------------------
if __name__ == "__main__":
    try:
        mp.freeze_support()   # Windows에서 필요
    except:
        pass

    print("\n============================================")
    print("   DART Standalone Collector v48  (독립버전)")
    print("============================================\n")

    main()

    print("\n[완료] 모든 작업 종료\n")
