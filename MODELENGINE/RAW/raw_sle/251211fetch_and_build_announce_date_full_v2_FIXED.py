# -*- coding: utf-8 -*-
"""
fetch_and_build_announce_date_full_v2_FIXED.py
A안: v2 구조 유지 + 안전장치 강화 / B안: 키 파일 경로를 코드에 고정

요약
- 기간 전체 DART 공시(list.json) 연도별 페이지네이션 수집
- 재무보고서 4종(11011,11012,11013,11014)만 필터
- announce_date = rcept_dt
- 분기/연간 조인용 bsns_year_calc 자동 생성
- 원본 보존 + 무결성 QC 리포트 생성
- 0byte/raw page 자동 삭제 후 재수집, 이어받기(연도·페이지 단위)
- 키 파일 8개 경로를 코드에 고정하여 로딩 실패 방지
- 키 오류/429/타임아웃 시 즉시 로테이션(페이지 증가 금지) → 전 키 소진 시 즉시 종료 + 상태기록(이어받기 가능)

실행 예시(파워쉘):
python fetch_and_build_announce_date_full_v2_FIXED.py --start 20160101 --end 20251205 --out "F:\\autostockG\\MODELENGINE\\RAW\\raw_sle\\DART_ANNOUNCE"

출력 구조(기본 out 루트):
out/
  raw/YYYY/list_YYYY_pageNNNNN.json        ← DART 원본(페이지 단위)
  merged/all_list.jsonl                    ← 전체 병합(JSON Lines)
  final/announce_fin_reports_START_END.csv ← 최종 산출물
  qc/qc_counts_by_year_code.csv            ← 연도×reprt_code 카운트
  qc/qc_duplicates_rcept_no.csv            ← rcept_no 중복 목록(있을 때만)
  qc/qc_join_key_dupes.csv                 ← (corp_code,reprt_code,bsns_year_calc) 중복(있을 때만)
  qc/summary.txt                           ← 요약 통계
state/
  year_{YYYY}.json                         ← 이어받기 체크포인트(마지막 성공 페이지)
  FAIL_year{YYYY}_page{PPPPP}.json         ← 전 키 소진 후 실패 기록(해당 지점부터 재시작)

주의
- 본 스크립트는 인터넷 연결 환경에서 실행해야 합니다(로컬 PC에서).
- DART API 정책 변경 시 파라미터 확인이 필요할 수 있습니다.
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

# ============================ 고정 키 파일 경로(B안) ============================
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
CONNECT_TIMEOUT = 8
READ_TIMEOUT    = 15
RAW_MIN_BYTES   = 50  # 0byte/에러페이지 방지: 이보다 작으면 삭제 후 재시도
SLEEP_BASE      = 0.12
SLEEP_BACKOFF_MAX = 2.0
MAX_FAIL_EACH_KEY = 1        # 키당 즉시 로테이션(1A)
TOTAL_KEY_CYCLES  = 1        # 전 키 1회 소진 후 실패 시 즉시 종료(요청대로)

VALID_RC = {"11011", "11012", "11013", "11014"}
KW_RE = re.compile(r"(사업보고서|반기보고서|분기보고서)")

# ============================ 유틸 ============================
def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_text_lines(p: Path) -> List[str]:
    try:
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except Exception:
        return []

def load_keys_from_files(paths: List[str]) -> List[str]:
    keys: List[str] = []
    for p in paths:
        try:
            lines = read_text_lines(Path(p))
            for k in lines:
                if k and k not in keys:
                    keys.append(k)
        except Exception:
            continue
    if not keys:
        raise ValueError("DART API 키를 찾을 수 없습니다. KEY_FILES 경로들을 확인하세요.")
    return keys

class KeyPool:
    def __init__(self, keys: List[str]):
        self.keys = keys[:]  # copy
        self.idx = 0
    def current(self) -> str:
        return self.keys[self.idx]
    def rotate(self) -> None:
        self.idx = (self.idx + 1) % len(self.keys)

# ============================ HTTP ============================
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

# ============================ 체크포인트 ============================
def state_file_for_year(state_dir: Path, year: int) -> Path:
    return state_dir / f"year_{year}.json"

def write_year_state(state_dir: Path, year: int, last_ok_page: int) -> None:
    ensure_dir(state_dir)
    sf = state_file_for_year(state_dir, year)
    sf.write_text(json.dumps({"year": year, "last_ok_page": last_ok_page}, ensure_ascii=False, indent=2), encoding="utf-8")

def read_year_state(state_dir: Path, year: int) -> int:
    sf = state_file_for_year(state_dir, year)
    if not sf.exists():
        return 0
    try:
        obj = json.loads(sf.read_text(encoding="utf-8"))
        return int(obj.get("last_ok_page") or 0)
    except Exception:
        return 0

def write_fail_marker(state_dir: Path, year: int, page_no: int, info: dict) -> None:
    ensure_dir(state_dir)
    f = state_dir / f"FAIL_year{year}_page{page_no:05d}.json"
    f.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")

# ============================ 수집 ============================
def fetch_year_full_list(year: int, key_pool: KeyPool, out_raw_dir: Path, state_dir: Path, bgn_de: str, end_de: str, verbose: bool=True) -> int:
    """
    연도 범위(bgn_de ~ end_de)에 대해 list.json을 페이지 끝까지 수집.
    - 이어받기: state/year_{year}.json 의 last_ok_page 이후부터 재개
    - 0byte 파일: 즉시 삭제 후 재시도
    - 키 교체: 요청 실패/429/100 등 즉시 로테이션(페이지 증가 금지)
    - 전 키 소진 후에도 실패 시: FAIL 마커 기록 후 즉시 종료(sys.exit)
    반환: 저장한 페이지 수(이번 실행에서 신규 저장 개수)
    """
    ensure_dir(out_raw_dir / f"{year}")
    last_ok = read_year_state(state_dir, year)
    page_no = max(1, last_ok + 1)
    saved_pages = 0

    while True:
        out_file = out_raw_dir / f"{year}" / f"list_{year}_page{page_no:05d}.json"

        # 이어받기: 정상 파일 있으면 건너뛴다
        if out_file.exists():
            sz = out_file.stat().st_size
            if sz >= RAW_MIN_BYTES:
                if verbose and page_no % 50 == 0:
                    print(f"[{year}] resume-skip page={page_no}")
                # 체크포인트도 갱신해 둔다(정상 파일이 존재하는 경우)
                write_year_state(state_dir, year, page_no)
                page_no += 1
                continue
            else:
                # 깨진 파일 → 삭제 후 재수집
                try:
                    out_file.unlink()
                except Exception:
                    pass

        # 이 페이지를 성공시킬 때까지 키를 회전하며 시도
        attempt_ok = False
        for _cycle in range(TOTAL_KEY_CYCLES):
            for _ in range(len(key_pool.keys)):
                params = {
                    "crtfc_key": key_pool.current(),
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "page_no": page_no,
                    "page_count": PAGE_COUNT,
                }
                status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params)

                # 즉시 로테이션 조건(페이지 증가 금지)
                rotate_needed = False
                if status == 429 or status in (-1, 408) or data is None:
                    rotate_needed = True
                else:
                    dart_status = str(data.get("status") or "")
                    # '000'만 정상이므로 그 외(특히 100)도 즉시 회전
                    if dart_status != "000":
                        rotate_needed = True

                if rotate_needed:
                    key_pool.rotate()
                    # 짧은 백오프
                    time.sleep(min(SLEEP_BACKOFF_MAX, SLEEP_BASE))
                    continue

                # 정상 응답 처리
                items = data.get("list") or []
                out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                saved_pages += 1

                # 체크포인트 기록(이 페이지 성공)
                write_year_state(state_dir, year, page_no)

                # 마지막 페이지 판단
                if len(items) < PAGE_COUNT:
                    if verbose:
                        tc = int(data.get("total_count") or 0)
                        print(f"[{year}] done. total_count={tc} saved_pages+={saved_pages}")
                    attempt_ok = True
                    break  # cycle 종료
                else:
                    attempt_ok = True
                    break  # 이 페이지 성공했으니 다음 페이지로

            if attempt_ok:
                break

        if not attempt_ok:
            # 전 키 소진 실패 → 즉시 종료 + FAIL 마커
            info = {
                "year": year,
                "page_no": page_no,
                "message": "All keys exhausted or persistent API failure",
                "hint": "재실행 시 이 지점부터 이어받기 됩니다."
            }
            write_fail_marker(state_dir, year, page_no, info)
            print(f"[FATAL] year={year} page={page_no} 전 키 실패 → 즉시 종료")
            sys.exit(2)

        # 다음 페이지로
        page_no += 1
        time.sleep(SLEEP_BASE)

    return saved_pages

# ============================ 병합 & 필터 ============================
def merge_year_raw_to_jsonl(year: int, out_raw_dir: Path, merged_fp) -> int:
    year_dir = out_raw_dir / f"{year}"
    if not year_dir.exists():
        return 0
    merged = 0
    pages = sorted(year_dir.glob(f"list_{year}_page*.json"))
    for p in pages:
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
    if reprt_code == "11014":
        return "Q1"
    if reprt_code == "11012":
        return "Q2"  # 반기
    if reprt_code == "11013":
        return "Q3"
    if reprt_code == "11011":
        return "Q4"  # 사업보고서(연간)
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

    with jsonl_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            try:
                it = json.loads(line)
            except Exception:
                continue

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

    qc_counts = qc_dir / "qc_counts_by_year_code.csv"
    with qc_counts.open("w", newline="", encoding="utf-8") as fw:
        w = csv.writer(fw)
        w.writerow(["bsns_year_calc","reprt_code","count"])
        for (yy, rc), cnt in sorted(by_year_code.items()):
            w.writerow([yy, rc, cnt])

    if dup_rcept_rows:
        qc_dup_rcept = qc_dir / "qc_duplicates_rcept_no.csv"
        with qc_dup_rcept.open("w", newline="", encoding="utf-8") as fw:
            w = csv.writer(fw)
            w.writerow(["corp_code","reprt_code","rcept_no","announce_date"])
            w.writerows(dup_rcept_rows)

    if dup_join_rows:
        qc_dup_join = qc_dir / "qc_join_key_dupes.csv"
        with qc_dup_join.open("w", newline="", encoding="utf-8") as fw:
            w = csv.writer(w)
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
    ap.add_argument("--out",   type=str, default=str(Path.cwd() / "dart_list_full_v2_fixed"), help="출력 루트")
    ap.add_argument("--quiet", action="store_true", help="로그 최소화")
    args = ap.parse_args()

    start = args.start
    end   = args.end
    out_root = Path(args.out).resolve()
    out_raw     = out_root / "out" / "raw"
    out_merged  = out_root / "out" / "merged"
    out_final   = out_root / "out" / "final"
    out_qc      = out_root / "out" / "qc"
    out_state   = out_root / "state"

    for d in (out_raw, out_merged, out_final, out_qc, out_state):
        ensure_dir(d)

    # 키 로드(파일 내용 라인별)
    keys = load_keys_from_files(KEY_FILES)
    key_pool = KeyPool(keys)
    if not args.quiet:
        print(f"[KEY] {len(keys)} keys loaded. first={key_pool.current()[:10]}...")

    # 연도 범위
    d0 = parse_yyyymmdd(start)
    d1 = parse_yyyymmdd(end)
    if d0 > d1:
        raise ValueError("start > end")
    years = list(range(d0.year, d1.year + 1))

    # 1) 연도별 전체 공시 수집
    for y in years:
        y_bgn = f"{y}0101" if y != d0.year else start
        y_end = f"{y}1231" if y != d1.year else end
        if not args.quiet:
            print(f"\n[FETCH] year={y}  range={y_bgn}..{y_end}")
        try:
            pages = fetch_year_full_list(y, key_pool, out_raw, out_state, y_bgn, y_end, verbose=not args.quiet)
            if not args.quiet:
                print(f"[FETCH] year={y} saved_pages={pages}")
        except SystemExit as e:
            # FAIL 마커 기록되어 즉시 종료된 경우 → 그대로 전파
            raise
        except Exception as e:
            print(f"[ERROR] year={y} fetch failed: {e}")
            raise

    # 2) 병합(JSONL)
    merged_jsonl = out_merged / "all_list.jsonl"
    with merged_jsonl.open("w", encoding="utf-8") as mfp:
        merged_total = 0
        for y in years:
            merged = merge_year_raw_to_jsonl(y, out_raw, mfp)
            merged_total += merged
            if not args.quiet:
                print(f"[MERGE] year={y} merged={merged}")
    if not args.quiet:
        print(f"[MERGE] total merged items = {merged_total} → {merged_jsonl}")

    # 3) 필터 & 산출물 + QC
    out_csv = out_final / f"announce_fin_reports_{start}_{end}.csv"
    selected = filter_and_build_csv(merged_jsonl, out_csv, start, end, out_qc)
    if not args.quiet:
        print(f"[BUILD] selected(financial reports) = {selected} → {out_csv}")
        print("[DONE] 완성")

if __name__ == "__main__":
    main()
