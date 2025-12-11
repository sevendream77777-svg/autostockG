# -*- coding: utf-8 -*-
"""
fetch_and_build_announce_date_full_v3_QUARTER.py

수정사항:
1. DART API 정책(검색기간 3개월 제한) 준수를 위해 연도별 루프 내부에서 분기별(Q1~Q4)로 쪼개서 수집
2. 진행 상황(Progress) 실시간 출력 추가
3. 키 파일 로딩 개선 (콤마 분리, BOM 제거) 및 100번 에러 시 자동 로테이션

출력 구조 변경:
out/raw/YYYY/list_YYYYMMDD_YYYYMMDD_pNNNNN.json 형태로 저장됨 (분기별 분리)
"""

from __future__ import annotations
import os, sys, json, time, argparse
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import csv
import re
import requests
from collections import Counter

# ============================ 고정 키 파일 경로 ============================
KEY_FILES = [
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\8sevendrenaver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\6109_kitchennaver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\7109kitchen109naver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\2slkdaum_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\3naver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\4se77777gmail_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\5se1117gmail_dart.txt",
]

# ============================ 설정 ============================
DEFAULT_START = "20160101"
DEFAULT_END   = "20251205"
PAGE_COUNT    = 100
CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 20
RAW_MIN_BYTES   = 50
SLEEP_BASE      = 0.15
SLEEP_BACKOFF_MAX = 2.0
TOTAL_KEY_CYCLES  = 3  # 모든 키 실패 시 재시도 횟수

VALID_RC = {"11011", "11012", "11013", "11014"}
KW_RE = re.compile(r"(사업보고서|반기보고서|분기보고서)")

# ============================ 유틸 ============================
def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_text_lines(p: Path) -> List[str]:
    # 콤마 분리 + BOM 제거 + 에러 무시 (run_raw_sle.py 방식)
    try:
        if p.exists():
            txt = p.read_text(encoding="utf-8-sig", errors="ignore")
            return [ln.strip() for ln in txt.replace(",", "\n").splitlines() if ln.strip()]
    except Exception:
        pass
    return []

def load_keys_from_files(paths: List[str]) -> List[str]:
    keys: List[str] = []
    for p in paths:
        lines = read_text_lines(Path(p))
        for k in lines:
            if k and k not in keys:
                keys.append(k)
    if not keys:
        raise ValueError("DART API 키를 찾을 수 없습니다. KEY_FILES 경로들을 확인하세요.")
    return keys

class KeyPool:
    def __init__(self, keys: List[str]):
        self.keys = keys[:]
        self.idx = 0
    def current(self) -> str:
        return self.keys[self.idx]
    def rotate(self) -> None:
        self.idx = (self.idx + 1) % len(self.keys)

# ============================ HTTP ============================
def _classify_dart_status(dart_status: str) -> str:
    if dart_status == "000": return "SUCCESS"
    if dart_status == "013": return "NO_DATA"
    # 100(유효하지 않은 필드) 포함하여 키/쿼터 에러는 로테이션
    if dart_status in {"100", "020", "300", "320", "334", "336"}: return "KEY_BLOCK"
    if dart_status in {"101", "102", "103", "104"}: return "BAD_PARAM"
    return "RETRY"

def http_get_json(url: str, params: dict) -> Tuple[int, Optional[dict]]:
    try:
        r = requests.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.status_code == 200:
            try:
                return 200, r.json()
            except Exception:
                return 200, None
        return r.status_code, None
    except requests.exceptions.Timeout:
        return 408, None
    except Exception:
        return -1, None

# ============================ 수집 (기간 분할) ============================
def fetch_period_chunk(year: int, bgn: str, end: str, key_pool: KeyPool, out_raw_dir: Path, verbose: bool) -> int:
    """특정 기간(3개월 이내)에 대해 페이지네이션 수집"""
    year_dir = out_raw_dir / str(year)
    ensure_dir(year_dir)
    
    page_no = 1
    saved_count = 0

    while True:
        # 파일명에 기간 포함: list_20160101_20160331_p00001.json
        fname = f"list_{bgn}_{end}_p{page_no:05d}.json"
        out_file = year_dir / fname

        # 이어받기: 파일 존재하면 스킵
        if out_file.exists():
            if out_file.stat().st_size >= RAW_MIN_BYTES:
                if verbose and page_no % 10 == 0:
                    print(f"\r  [{bgn}~{end}] Skip page {page_no} (exists)", end="", flush=True)
                page_no += 1
                continue
            else:
                try: out_file.unlink()
                except: pass

        attempt_ok = False
        last_msg = ""
        
        # 키 사이클링
        for _cycle in range(TOTAL_KEY_CYCLES):
            for _ in range(len(key_pool.keys)):
                params = {
                    "crtfc_key": key_pool.current(),
                    "bgn_de": bgn,
                    "end_de": end,
                    "page_no": page_no,
                    "page_count": PAGE_COUNT,
                }
                
                # 진행 상황 표시
                if verbose:
                    print(f"\r  [{bgn}~{end}] Fetching page {page_no} (key: {key_pool.current()[:5]}...)", end="", flush=True)

                status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params)
                
                if status != 200 or data is None:
                    key_pool.rotate()
                    time.sleep(SLEEP_BASE)
                    continue

                d_status = str(data.get("status") or "")
                d_msg = str(data.get("message") or "")
                cls = _classify_dart_status(d_status)

                if cls == "SUCCESS":
                    out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                    saved_count += 1
                    
                    # 마지막 페이지 체크
                    items = data.get("list") or []
                    tot = int(data.get("total_count") or 0)
                    if len(items) < PAGE_COUNT:
                        # 끝
                        if verbose:
                            print(f" -> Done. (Total {tot} items)")
                        return saved_count
                    else:
                        # 다음 페이지로
                        attempt_ok = True
                        break

                elif cls == "NO_DATA":
                    if verbose:
                        print(" -> No Data.")
                    return saved_count

                elif cls == "KEY_BLOCK":
                    # 키 에러/제한/100 -> 로테이션
                    last_msg = f"{d_status}/{d_msg}"
                    key_pool.rotate()
                    time.sleep(SLEEP_BASE)
                    continue

                elif cls == "BAD_PARAM":
                    print(f"\n[FATAL] Param Error: {d_status} {d_msg}")
                    return saved_count
                
                else: # RETRY
                    key_pool.rotate()
                    time.sleep(SLEEP_BASE)
                    continue
            
            if attempt_ok:
                break
        
        if not attempt_ok:
            print(f"\n[ERROR] All keys failed at {bgn}~{end} p{page_no}. Last: {last_msg}")
            # 여기서 멈추지 않고 다음 쿼터로 넘어갈지 결정. 
            # 일단 이 구간은 포기하고 리턴
            return saved_count

        page_no += 1
        time.sleep(SLEEP_BASE)

    return saved_count

def fetch_year_quarters(year: int, key_pool: KeyPool, out_raw_dir: Path, start_arg: str, end_arg: str, verbose: bool):
    """연도를 4분기로 나누어 3개월 제한을 우회"""
    # 쿼터 정의
    quarters = [
        (f"{year}0101", f"{year}0331"),
        (f"{year}0401", f"{year}0630"),
        (f"{year}0701", f"{year}0930"),
        (f"{year}1001", f"{year}1231"),
    ]
    
    total_saved = 0
    for q_start, q_end in quarters:
        # 사용자 지정 기간(start_arg~end_arg)과 겹치는지 확인
        # 문자열 비교로 교집합 구하기
        actual_start = max(q_start, start_arg)
        actual_end = min(q_end, end_arg)
        
        if actual_start <= actual_end:
            # 유효한 구간이면 수집
            s = fetch_period_chunk(year, actual_start, actual_end, key_pool, out_raw_dir, verbose)
            total_saved += s
    
    return total_saved

# ============================ 병합 & 필터 ============================
def merge_year_raw_to_jsonl(year: int, out_raw_dir: Path, merged_fp) -> int:
    year_dir = out_raw_dir / f"{year}"
    if not year_dir.exists():
        return 0
    merged = 0
    # 패턴 변경: list_*.json (모든 list 파일)
    files = sorted(year_dir.glob("list_*.json"))
    for p in files:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        items = obj.get("list") or []
        for it in items:
            merged_fp.write(json.dumps(it, ensure_ascii=False) + "\n")
            merged += 1
    return merged

def rc_to_quarter_label(reprt_code: str) -> str:
    if reprt_code == "11014": return "Q1"
    if reprt_code == "11012": return "Q2"
    if reprt_code == "11013": return "Q3"
    if reprt_code == "11011": return "Q4"
    return ""

def calc_bsns_year_from_rcept(reprt_code: str, rcept_dt: str) -> Optional[int]:
    try:
        y = int(rcept_dt[:4])
    except Exception:
        return None
    if reprt_code == "11011":
        return y - 1
    return y

def filter_and_build_csv(jsonl_path: Path, out_csv_path: Path, start: str, end: str, qc_dir: Path) -> int:
    ensure_dir(out_csv_path.parent)
    ensure_dir(qc_dir)

    seen_rcept = set()
    join_key_counter = Counter()
    by_year_code = Counter()
    dup_rcept_rows = []
    dup_join_rows = []
    out_rows: List[List[str]] = []

    print("[BUILD] Filtering and building CSV...")
    with jsonl_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            try: it = json.loads(line)
            except: continue

            report_nm = (it.get("report_nm") or "").strip()
            reprt_code = (it.get("reprt_code") or "").strip()
            if not (reprt_code in VALID_RC or KW_RE.search(report_nm)):
                continue

            corp_code = (it.get("corp_code") or "").strip()
            stock_code = (it.get("stock_code") or "").strip()
            corp_name = (it.get("corp_name") or "").strip()
            rcept_no  = (it.get("rcept_no")  or "").strip()
            rcept_dt  = (it.get("rcept_dt")  or "").strip()

            if not rcept_dt or len(rcept_dt) != 8 or not reprt_code:
                continue

            if rcept_no in seen_rcept:
                dup_rcept_rows.append([corp_code, reprt_code, rcept_no, rcept_dt])
                continue
            seen_rcept.add(rcept_no)

            byear = calc_bsns_year_from_rcept(reprt_code, rcept_dt)
            if byear is None:
                continue

            qlabel = rc_to_quarter_label(reprt_code)
            join_key = (corp_code, reprt_code, byear)
            join_key_counter[join_key] += 1
            if join_key_counter[join_key] > 1:
                dup_join_rows.append([corp_code, reprt_code, byear, rcept_no, rcept_dt])

            by_year_code[(byear, reprt_code)] += 1

            out_rows.append([
                corp_code, stock_code, corp_name, report_nm, reprt_code,
                rcept_no, rcept_dt, str(byear), qlabel, start, end
            ])

    with out_csv_path.open("w", newline="", encoding="utf-8") as fw:
        w = csv.writer(fw)
        w.writerow([
            "corp_code","stock_code","corp_name","report_nm",
            "reprt_code","rcept_no","announce_date","bsns_year_calc","quarter_label",
            "src_start","src_end"
        ])
        w.writerows(out_rows)

    # QC Files
    with (qc_dir / "qc_counts_by_year_code.csv").open("w", newline="", encoding="utf-8") as fw:
        w = csv.writer(fw)
        w.writerow(["bsns_year_calc","reprt_code","count"])
        for (yy, rc), cnt in sorted(by_year_code.items()):
            w.writerow([yy, rc, cnt])

    if dup_rcept_rows:
        with (qc_dir / "qc_duplicates_rcept_no.csv").open("w", newline="", encoding="utf-8") as fw:
            w = csv.writer(fw)
            w.writerow(["corp_code","reprt_code","rcept_no","announce_date"])
            w.writerows(dup_rcept_rows)

    if dup_join_rows:
        with (qc_dir / "qc_join_key_dupes.csv").open("w", newline="", encoding="utf-8") as fw:
            w = csv.writer(fw)
            w.writerow(["corp_code","reprt_code","bsns_year_calc","rcept_no","announce_date"])
            w.writerows(dup_join_rows)

    summary = [
        f"total_rows={len(out_rows)}",
        f"dup_rcept_no={len(dup_rcept_rows)}",
        f"dup_join_keys={len(dup_join_rows)}",
        f"distinct_join_keys={len(join_key_counter)}"
    ]
    (qc_dir / "summary.txt").write_text("\n".join(summary), encoding="utf-8")

    return len(out_rows)

# ============================ 메인 ============================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default=DEFAULT_START, help="YYYYMMDD")
    ap.add_argument("--end",   type=str, default=DEFAULT_END,   help="YYYYMMDD")
    ap.add_argument("--out",   type=str, default=str(Path.cwd() / "dart_list_full_v3_quarter"), help="출력 루트")
    ap.add_argument("--quiet", action="store_true", help="로그 최소화")
    args = ap.parse_args()

    start = args.start
    end   = args.end
    out_root = Path(args.out).resolve()
    out_raw     = out_root / "out" / "raw"
    out_merged  = out_root / "out" / "merged"
    out_final   = out_root / "out" / "final"
    out_qc      = out_root / "out" / "qc"
    
    for d in (out_raw, out_merged, out_final, out_qc):
        ensure_dir(d)

    # 키 로드
    keys = load_keys_from_files(KEY_FILES)
    key_pool = KeyPool(keys)
    if not args.quiet:
        print(f"[KEY] {len(keys)} keys loaded. first={key_pool.current()[:10]}...")

    d0 = parse_yyyymmdd(start)
    d1 = parse_yyyymmdd(end)
    if d0 > d1:
        raise ValueError("start > end")
    years = list(range(d0.year, d1.year + 1))

    # 1) 연도별 -> 분기별 수집
    for y in years:
        if not args.quiet:
            print(f"\n[FETCH] Processing Year {y}...")
        try:
            pages = fetch_year_quarters(y, key_pool, out_raw, start, end, verbose=not args.quiet)
            if not args.quiet:
                print(f" -> Year {y} saved pages count: {pages}")
        except KeyboardInterrupt:
            print("\n[STOP] 사용자에 의해 중단됨")
            sys.exit(1)
        except Exception as e:
            print(f"\n[ERROR] Year {y} failed: {e}")
            # 계속 진행

    # 2) 병합(JSONL)
    merged_jsonl = out_merged / "all_list.jsonl"
    print(f"\n[MERGE] Merging into {merged_jsonl} ...")
    with merged_jsonl.open("w", encoding="utf-8") as mfp:
        merged_total = 0
        for y in years:
            merged = merge_year_raw_to_jsonl(y, out_raw, mfp)
            merged_total += merged
            if not args.quiet and merged > 0:
                print(f"  Year {y}: {merged} items merged")
    
    # 3) 필터 & 산출물
    out_csv = out_final / f"announce_fin_reports_{start}_{end}.csv"
    selected = filter_and_build_csv(merged_jsonl, out_csv, start, end, out_qc)
    
    print(f"\n[DONE] Finished. Total Financial Reports: {selected}")
    print(f"File: {out_csv}")

if __name__ == "__main__":
    main()