# -*- coding: utf-8 -*-
"""
올인원 v2 (학습데이터 무결성 강화판)
- 기간 전체 DART 공시(list.json) 연도별 페이지네이션 수집
- 재무보고서 4종(11011,11012,11013,11014)만 필터
- announce_date = rcept_dt
- 분기/연간 조인용 bsns_year_calc 자동 생성
- 원본 보존 + 무결성 QC 리포트 생성
- 0byte/raw page 재시도, 키 로테이션, 이어받기

출력 구조(기본 out 루트 = ./dart_list_full_v2):
out/
  raw/YYYY/list_YYYY_pageNN.json           ← DART 원본(페이지 단위)
  merged/all_list.jsonl                    ← 전체 병합(JSON Lines)
  final/announce_fin_reports_START_END.csv ← 최종 산출물
  qc/qc_counts_by_year_code.csv            ← 연도×reprt_code 카운트
  qc/qc_duplicates_rcept_no.csv            ← rcept_no 중복 목록(있을 때만)
  qc/qc_join_key_dupes.csv                 ← (corp_code,reprt_code,bsns_year_calc) 중복(있을 때만)
  qc/summary.txt                           ← 요약 통계

실행 예시:
python fetch_and_build_announce_date_full_v2.py --start 20160101 --end 20251205 --out F:\autostockG\DART_ANNOUNCE
"""

from __future__ import annotations
import os, sys, json, time, argparse
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import csv
import re
import requests
from collections import Counter, defaultdict

# ============================ 설정 ============================
DEFAULT_START = "20160101"
DEFAULT_END   = "20251205"
PAGE_COUNT    = 100
CONNECT_TIMEOUT = 8
READ_TIMEOUT    = 15
MAX_FAIL        = 6
SLEEP_BASE      = 0.12
SLEEP_BACKOFF_MAX = 2.0
TOTAL_RETRY_PAGES = 3
RAW_MIN_BYTES   = 50  # 0byte/에러페이지 방지: 이보다 작으면 삭제 후 재시도

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

def load_keys() -> List[str]:
    keys_env = os.environ.get("DART_API_KEYS", "").strip()
    keys: List[str] = []
    if keys_env:
        for k in keys_env.replace(",", "\n").splitlines():
            k = k.strip()
            if k:
                keys.append(k)
    for p in DEFAULT_KEY_PATHS:
        try:
            if p.exists():
                for k in read_text_lines(p):
                    if k and k not in keys:
                        keys.append(k)
        except Exception:
            pass
    if not keys:
        raise ValueError("DART API 키를 찾을 수 없습니다. 환경변수 DART_API_KEYS 또는 기본 경로에 키 파일을 두세요.")
    return keys

class KeyPool:
    def __init__(self, keys: List[str]):
        self.keys = keys
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

# ============================ 수집 ============================
def fetch_year_full_list(year: int, key_pool: KeyPool, out_raw_dir: Path, bgn_de: str, end_de: str, verbose: bool=True) -> int:
    ensure_dir(out_raw_dir / f"{year}")
    page_no = 1
    saved_pages = 0
    backoff = SLEEP_BASE
    consecutive_fail = 0

    while True:
        out_file = out_raw_dir / f"{year}" / f"list_{year}_page{page_no:05d}.json"

        # 이어받기: 0byte 같은 비정상 파일은 삭제 후 재시작
        if out_file.exists():
            sz = out_file.stat().st_size
            if sz >= RAW_MIN_BYTES:
                if verbose and page_no % 50 == 0:
                    print(f"[{year}] resume-skip page={page_no}")
                page_no += 1
                continue
            else:
                try:
                    out_file.unlink()
                except Exception:
                    pass  # 다음 루프에서 그냥 덮어씀

        params = {
            "crtfc_key": key_pool.current(),
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": PAGE_COUNT,
        }
        status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params)

        if status == 429:
            key_pool.rotate()
            consecutive_fail += 1
            backoff = min(SLEEP_BACKOFF_MAX, backoff * 2)
            if verbose:
                print(f"[{year}] 429 → rotate key, backoff {backoff:.2f}s (fail={consecutive_fail})")
            time.sleep(backoff)
            if consecutive_fail >= MAX_FAIL:
                time.sleep(2.0)
                consecutive_fail = 0
            continue

        if status in (-1, 408) or data is None:
            consecutive_fail += 1
            backoff = min(SLEEP_BACKOFF_MAX, backoff * 2)
            if verbose:
                print(f"[{year}] transient HTTP {status} → backoff {backoff:.2f}s (fail={consecutive_fail})")
            time.sleep(backoff)
            if consecutive_fail >= MAX_FAIL:
                retried = 0
                while retried < TOTAL_RETRY_PAGES:
                    time.sleep(1.0 + retried)
                    status2, data2 = http_get_json("https://opendart.fss.or.kr/api/list.json", params)
                    if status2 == 200 and data2 is not None:
                        data = data2
                        break
                    retried += 1
                if data is None:
                    raise RuntimeError(f"[{year}] page {page_no} 지속 실패")
            else:
                continue

        consecutive_fail = 0
        backoff = SLEEP_BASE

        dart_status = data.get("status")
        if dart_status and dart_status != "000":
            if verbose:
                print(f"[{year}] DART status={dart_status} → stop this year")
            break

        items = data.get("list") or []

        out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        saved_pages += 1

        if len(items) < PAGE_COUNT:
            if verbose:
                tc = int(data.get("total_count") or 0)
                print(f"[{year}] done. total_count={tc} saved_pages={saved_pages}")
            break

        page_no += 1
        time.sleep(SLEEP_BASE)

    return saved_pages

# ============================ 병합 & 필터 ============================
VALID_RC = {"11011", "11012", "11013", "11014"}
KW_RE = re.compile(r"(사업보고서|반기보고서|분기보고서)")

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
    ap.add_argument("--out",   type=str, default=str(Path.cwd() / "dart_list_full_v2"), help="출력 루트")
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

    keys = load_keys()
    key_pool = KeyPool(keys)
    if not args.quiet:
        print(f"[KEY] {len(keys)} keys loaded. first={key_pool.current()[:10]}...")

    d0 = parse_yyyymmdd(start)
    d1 = parse_yyyymmdd(end)
    if d0 > d1:
        raise ValueError("start > end")
    years = list(range(d0.year, d1.year + 1))

    for y in years:
        y_bgn = f"{y}0101" if y != d0.year else start
        y_end = f"{y}1231" if y != d1.year else end
        if not args.quiet:
            print(f"\n[FETCH] year={y}  range={y_bgn}..{y_end}")
        try:
            pages = fetch_year_full_list(y, key_pool, out_raw, y_bgn, y_end, verbose=not args.quiet)
            if not args.quiet:
                print(f"[FETCH] year={y} saved_pages={pages}")
        except Exception as e:
            print(f"[ERROR] year={y} fetch failed: {e}")
            continue

    merged_jsonl = out_merged / "all_list.jsonl"
    with merged_jsonl.open("a", encoding="utf-8") as mfp:
        merged_total = 0
        for y in years:
            merged = merge_year_raw_to_jsonl(y, out_raw, mfp)
            merged_total += merged
            if not args.quiet:
                print(f"[MERGE] year={y} merged={merged}")
    if not args.quiet:
        print(f"[MERGE] total merged items = {merged_total} → {merged_jsonl}")

    out_csv = out_final / f"announce_fin_reports_{start}_{end}.csv"
    selected = filter_and_build_csv(merged_jsonl, out_csv, start, end, out_qc)
    if not args.quiet:
        print(f"[BUILD] selected(financial reports) = {selected} → {out_csv}")
        print("[DONE] 완성")

if __name__ == "__main__":
    main()
