# -*- coding: utf-8 -*-
"""
올인원: DART 연도별 전체 공시 수집 + 재무보고서 announce_date 테이블 생성
- 기간: 20160101 ~ 20251205 (기본값, 인자 변경 가능)
- 방식: DART /api/list.json (corp_code 없이) 연도별 호출 + 페이지네이션
- 안정성: 키 로테이션, 429/타임아웃 재시도, 이어받기, 원본 JSON 보존
- 산출물:
  1) 원본 백업(JSON Lines): out/raw/YYYY/list_YYYY_pageNN.json
  2) 병합 중간(JSON Lines): out/merged/all_list.jsonl
  3) 최종 CSV: out/final/announce_fin_reports_YYYYMMDD_YYYYMMDD.csv
    - 컬럼: corp_code, stock_code, corp_name, report_nm, reprt_code, rcept_no, announce_date
- 실행 예:
  python fetch_and_build_announce_date_full_v1.py
  python fetch_and_build_announce_date_full_v1.py --start 20160101 --end 20251205 --out ./dart_list_full
"""

from __future__ import annotations
import os, sys, json, time, argparse, math
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import requests

# ============================ 설정 ============================
DEFAULT_START = "20160101"
DEFAULT_END   = "20251205"
PAGE_COUNT    = 100          # DART 최대 100
CONNECT_TIMEOUT = 8
READ_TIMEOUT    = 15
MAX_FAIL        = 6
SLEEP_BASE      = 0.12       # 기본 sleep (초)
SLEEP_BACKOFF_MAX = 2.0
TOTAL_RETRY_PAGES = 3        # 페이지 재시도 허용 횟수

# 키 검색 경로(라인별로 키 적힌 텍스트 파일)
DEFAULT_KEY_PATHS = [
    # 사용자의 기존 SAFE_v3에서 쓰던 경로 패턴을 그대로 지원
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
def yyyymmdd(dt: date) -> str:
    return dt.strftime("%Y%m%d")

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
    # 우선순위 1: 환경변수 DART_API_KEYS="k1,k2,k3"
    keys_env = os.environ.get("DART_API_KEYS", "").strip()
    keys: List[str] = []
    if keys_env:
        for k in keys_env.replace(",", "\n").splitlines():
            k = k.strip()
            if k:
                keys.append(k)

    # 우선순위 2: 기본 경로들에 있는 텍스트 파일에서 라인별 로드
    for p in DEFAULT_KEY_PATHS:
        try:
            if p.exists():
                for k in read_text_lines(p):
                    if k and k not in keys:
                        keys.append(k)
        except Exception:
            pass

    return keys

class KeyPool:
    def __init__(self, keys: List[str]):
        if not keys:
            raise ValueError("DART API 키를 찾을 수 없습니다. 환경변수 DART_API_KEYS 또는 DEFAULT_KEY_PATHS에 키를 넣으세요.")
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

# ============================ 핵심 로직 ============================
def fetch_year_full_list(year: int, key_pool: KeyPool, out_raw_dir: Path, bgn_de: str, end_de: str, verbose: bool=True) -> int:
    """
    주어진 연도 범위(bgn_de ~ end_de)에 대해서 corp_code 없이 list.json 전량을 페이지네이션으로 수집.
    결과는 raw/YYYY/list_YYYY_pageNN.json 로 저장(JSON Lines 아님; 페이지별 JSON 원본).
    반환: 저장한 페이지 수
    """
    ensure_dir(out_raw_dir / f"{year}")
    page_no = 1
    saved_pages = 0
    backoff = SLEEP_BASE
    consecutive_fail = 0

    while True:
        out_file = out_raw_dir / f"{year}" / f"list_{year}_page{page_no:05d}.json"
        if out_file.exists() and out_file.stat().st_size > 2:
            # 이어받기: 이미 있으면 스킵하고 다음 페이지 시도
            if verbose and page_no % 50 == 0:
                print(f"[{year}] resume-skip page={page_no}")
            page_no += 1
            continue

        params = {
            "crtfc_key": key_pool.current(),
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": PAGE_COUNT,
        }
        status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params)

        if status == 429:
            # Rate limit → 키 교체 + 백오프
            key_pool.rotate()
            consecutive_fail += 1
            backoff = min(SLEEP_BACKOFF_MAX, backoff * 2)
            if verbose:
                print(f"[{year}] 429 Too Many Requests → rotate key, backoff {backoff:.2f}s (fail={consecutive_fail})")
            time.sleep(backoff)
            if consecutive_fail >= MAX_FAIL:
                # 키 전체가 막혔을 가능성 → 잠시 대기 후 재시도
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
                # 페이지 재시도 상한 도달: 다음 페이지로 넘어가면 데이터가 끊기므로,
                # 안전하게 몇 번(최대 TOTAL_RETRY_PAGES) 더 재시도
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

        # 정상 응답 처리
        consecutive_fail = 0
        backoff = SLEEP_BASE

        dart_status = data.get("status")
        if dart_status and dart_status != "000":
            # 전체 조회에서는 013/020 같은 코드가 뜨는 경우가 드묾.
            # 그래도 안전하게 종료 조건 처리
            msg = data.get("message", "")
            if verbose:
                print(f"[{year}] DART status={dart_status} msg={msg} → stop this year")
            break

        total_count = int(data.get("total_count") or 0)
        items = data.get("list") or []

        # 저장
        out_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        saved_pages += 1

        # 종료 조건: 마지막 페이지
        if len(items) < PAGE_COUNT:
            if verbose:
                print(f"[{year}] done. total_count={total_count} saved_pages={saved_pages}")
            break

        page_no += 1
        # polite sleep
        time.sleep(SLEEP_BASE)

    return saved_pages

def merge_year_raw_to_jsonl(year: int, out_raw_dir: Path, merged_fp) -> int:
    """
    raw/YYYY/list_YYYY_pageNN.json들을 순회하며 list[] 항목을 merged_fp(JSON Lines)에 씀.
    반환: 병합한 항목 수
    """
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

def filter_financial_reports_from_jsonl(jsonl_path: Path, out_csv_path: Path, start: str, end: str) -> int:
    """
    JSONL에서 재무보고서(사업/반기/분기)만 필터링해 최종 CSV 생성.
    - 조건: report_nm 포함키워드 OR reprt_code in {'11011','11012','11013','11014'}
    - announce_date = rcept_dt
    CSV 컬럼: corp_code, stock_code, corp_name, report_nm, reprt_code, rcept_no, announce_date
    """
    import csv, re

    ensure_dir(out_csv_path.parent)
    kw = re.compile(r"(사업보고서|반기보고서|분기보고서)")
    valid_rc = {"11011", "11012", "11013", "11014"}

    n_in = 0
    n_out = 0

    with jsonl_path.open("r", encoding="utf-8") as fp, out_csv_path.open("w", newline="", encoding="utf-8") as fw:
        w = csv.writer(fw)
        w.writerow(["corp_code", "stock_code", "corp_name", "report_nm", "reprt_code", "rcept_no", "announce_date", "src_start", "src_end"])
        for line in fp:
            n_in += 1
            try:
                it = json.loads(line)
            except Exception:
                continue
            report_nm = (it.get("report_nm") or "").strip()
            reprt_code = (it.get("reprt_code") or "").strip()
            if not (kw.search(report_nm) or (reprt_code in valid_rc)):
                continue

            corp_code = (it.get("corp_code") or "").strip()
            stock_code = (it.get("stock_code") or "").strip()
            corp_name = (it.get("corp_name") or "").strip()
            rcept_no  = (it.get("rcept_no")  or "").strip()
            rcept_dt  = (it.get("rcept_dt")  or "").strip()

            if not rcept_dt or len(rcept_dt) != 8:
                continue

            w.writerow([corp_code, stock_code, corp_name, report_nm, reprt_code, rcept_no, rcept_dt, start, end])
            n_out += 1

    return n_out

# ============================ 메인 ============================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default=DEFAULT_START, help="YYYYMMDD")
    ap.add_argument("--end",   type=str, default=DEFAULT_END,   help="YYYYMMDD")
    ap.add_argument("--out",   type=str, default=str(Path.cwd() / "dart_list_full"), help="출력 폴더 루트")
    ap.add_argument("--quiet", action="store_true", help="로그 최소화")
    args = ap.parse_args()

    start = args.start
    end   = args.end
    out_root = Path(args.out).resolve()
    out_raw     = out_root / "out" / "raw"
    out_merged  = out_root / "out" / "merged"
    out_final   = out_root / "out" / "final"
    ensure_dir(out_raw); ensure_dir(out_merged); ensure_dir(out_final)

    # 키 로드
    keys = load_keys()
    key_pool = KeyPool(keys)
    if not args.quiet:
        print(f"[KEY] {len(keys)} keys loaded. first={key_pool.current()[:10]}...")

    # 연도 범위 산출
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
            pages = fetch_year_full_list(y, key_pool, out_raw, y_bgn, y_end, verbose=not args.quiet)
            if not args.quiet:
                print(f"[FETCH] year={y} saved_pages={pages}")
        except Exception as e:
            print(f"[ERROR] year={y} fetch failed: {e}")
            # 실패해도 다음 연도로 진행 (필요 시 재실행로 보강)
            continue

    # 2) 병합(JSONL)
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

    # 3) 재무보고서만 필터 → CSV 생성
    out_csv = out_final / f"announce_fin_reports_{start}_{end}.csv"
    selected = filter_financial_reports_from_jsonl(merged_jsonl, out_csv, start, end)
    if not args.quiet:
        print(f"[BUILD] selected(financial reports) = {selected} → {out_csv}")
        print("[DONE] 완성")

if __name__ == "__main__":
    main()
