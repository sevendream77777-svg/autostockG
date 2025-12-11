# -*- coding: utf-8 -*-
"""
DART 공시 목록(list.json) 수집기 — SAFE+STATS (무결성+실시간 카운터)
- 원자적 저장(temporary → atomic replace)
- 0건(013) 센티널 파일 기록 및 스킵
- 레거시 파일명(000000_{corp}.json) 포함 스킵
- 실패 JSONL 로그(failures_*.jsonl)로 이어받기 지원
- 타임아웃/백오프/키로테이션 강화
- 실시간 요약 통계 출력(013/000/실패/스킵) 및 간단 Per-item 로그 제어
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from pathlib import Path
from threading import Lock

# -------------------- 상수 --------------------
DEFAULT_START = "20160101"
DEFAULT_END = "20251205"
PAGE_COUNT = 100
REQ_SLEEP_BASE = 0.12  # 기본 간격(초)
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 7
MAX_FAIL = 5
TOTAL_TIME_CAP_SEC = 90  # 한 corp 처리 상한(초)

# -------------------- 파일명 규칙 --------------------
def bycorp_main_path(dir_by_corp: Path, stock_code: str, corp_code: str) -> Path:
    return dir_by_corp / f"{stock_code}_{corp_code}.json"

def bycorp_alt_path(dir_by_corp: Path, corp_code: str) -> Path:
    return dir_by_corp / f"{corp_code}.json"

def bycorp_zero_path(dir_by_corp: Path, stock_code: str, corp_code: str) -> Path:
    return dir_by_corp / f"{stock_code}_{corp_code}.zero.json"

def bycorp_legacy000_path(dir_by_corp: Path, corp_code: str) -> Path:
    # 레거시 파일 패턴: 000000_{corp}.json
    return dir_by_corp / f"000000_{corp_code}.json"

# -------------------- 유틸 --------------------
def _read_lines(p: Path) -> List[str]:
    try:
        if not p.exists():
            return []
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except Exception:
        return []

def _extract_leading_int(name: str) -> int:
    import re
    m = re.match(r"^(\d{1,2})", name)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 9999
    return 9999

# -------------------- 키 로드 --------------------
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
    # stable sort by (prefix number, original order), then unique
    uniq: List[str] = []
    for _, key in sorted(enumerate(candidates), key=lambda x: (x[1][0], x[0])):
        k = key[1]
        if k and k not in uniq:
            uniq.append(k)
    return uniq

# -------------------- 법인 목록 --------------------
DEFAULT_XML_PATHS = [Path(__file__).resolve().parent / "dart_corp_list.xml"]

def load_corp_list() -> List[Dict[str, str]]:
    xml_path = None
    for p in DEFAULT_XML_PATHS:
        if p.exists():
            xml_path = p
            break
    if not xml_path:
        raise FileNotFoundError("dart_corp_list.xml 파일을 찾을 수 없습니다.")
    import xml.etree.ElementTree as ET
    root = ET.parse(xml_path).getroot()
    corps: List[Dict[str, str]] = []
    for el in root.findall(".//list"):
        corp_code = (el.findtext("corp_code") or "").strip()
        stock_code = (el.findtext("stock_code") or "").strip()
        if corp_code and stock_code:
            corps.append({"corp_code": corp_code, "stock_code": stock_code})
    return corps

# -------------------- HTTP --------------------
def http_get_json(url: str, params: dict, timeout: Tuple[int, int]=(CONNECT_TIMEOUT, READ_TIMEOUT)) -> Tuple[int, Optional[dict]]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
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

# -------------------- 키 풀 --------------------
class KeyPool:
    def __init__(self, keys: List[str]):
        if not keys:
            raise ValueError("DART 키 없음")
        self.keys = keys
        self.idx = 0
    def current(self) -> str:
        return self.keys[self.idx]
    def rotate(self) -> None:
        self.idx = (self.idx + 1) % len(self.keys)

# -------------------- 파일 원자성 --------------------
def atomic_write_json(out_path: Path, payload) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # validation
    obj = json.loads(tmp.read_text(encoding="utf-8"))
    if not isinstance(obj, (list, dict)):
        raise ValueError("Invalid JSON payload type")
    os.replace(tmp, out_path)

# -------------------- 스킵 판단 --------------------
def exists_any(dir_by_corp: Path, stock_code: str, corp_code: str) -> bool:
    candidates = [
        bycorp_main_path(dir_by_corp, stock_code, corp_code),
        bycorp_alt_path(dir_by_corp, corp_code),
        bycorp_zero_path(dir_by_corp, stock_code, corp_code),
        bycorp_legacy000_path(dir_by_corp, corp_code),
    ]
    for p in candidates:
        try:
            if p.exists() and p.stat().st_size > 0:
                return True
        except Exception:
            continue
    return False

# -------------------- 수집 --------------------
def fetch_list_for_corp(
    corp_code: str,
    stock_code: str,
    start: str,
    end: str,
    key_pool: KeyPool,
    out_corp_dir: Path,
    skip_existing: bool = True,
) -> Tuple[str, int, List[str]]:
    """
    반환: (corp_code, 건수, issues)
    issues:
      - 'dart_status_013' : 정상 0건(센티널 생성됨)
      - 'skipped_existing' : 선행 스킵
      - 'http_status_*' / 'dart_status_*' / 'total_time_cap' 등 실패 사유
    """
    t0 = time.time()
    issues: List[str] = []
    rows: List[Dict[str, str]] = []
    page_no = 1
    fail_count = 0
    sleep = REQ_SLEEP_BASE

    # 선행 스킵
    if skip_existing and exists_any(out_corp_dir, stock_code, corp_code):
        return corp_code, -1, ["skipped_existing"]

    while True:
        if time.time() - t0 > TOTAL_TIME_CAP_SEC:
            issues.append("total_time_cap")
            break

        params = {
            "crtfc_key": key_pool.current(),
            "corp_code": corp_code,
            "bgn_de": start,
            "end_de": end,
            "page_no": page_no,
            "page_count": PAGE_COUNT,
        }
        status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params)
        if status == 429:
            issues.append("http_status_429")
            key_pool.rotate()
            time.sleep(min(1.0, sleep * 2))
            fail_count += 1
            if fail_count >= MAX_FAIL:
                break
            continue
        if status in (-1, 408) or data is None:
            issues.append(f"http_status_{status}")
            time.sleep(min(1.0, sleep * 2))
            fail_count += 1
            if fail_count >= MAX_FAIL:
                break
            continue

        # DART status
        dart_status = data.get("status")
        if dart_status == "013":
            zero = {
                "corp_code": corp_code,
                "stock_code": stock_code,
                "count": 0,
                "status": "013",
                "start": start,
                "end": end,
                "ts": datetime.now().isoformat(timespec="seconds"),
            }
            atomic_write_json(bycorp_zero_path(out_corp_dir, stock_code, corp_code), zero)
            return corp_code, 0, ["dart_status_013"]

        if dart_status not in (None, "000"):
            issues.append(f"dart_status_{dart_status}")
            time.sleep(min(1.2, sleep * 2))
            fail_count += 1
            if fail_count >= MAX_FAIL:
                break
            key_pool.rotate()
            continue

        # 정상
        fail_count = 0
        items = data.get("list") or []
        for it in items:
            rows.append(
                {
                    "corp_code": it.get("corp_code"),
                    "stock_code": stock_code,
                    "corp_name": it.get("corp_name"),
                    "report_nm": it.get("report_nm"),
                    "rcept_no": it.get("rcept_no"),
                    "rcept_dt": it.get("rcept_dt"),
                    "reprt_code": it.get("reprt_code"),
                    "flr_nm": it.get("flr_nm"),
                }
            )

        if len(items) < PAGE_COUNT:
            break
        page_no += 1
        time.sleep(sleep)

    # 저장
    if rows:
        atomic_write_json(bycorp_main_path(out_corp_dir, stock_code, corp_code), rows)
        return corp_code, len(rows), issues

    return corp_code, 0, issues if issues else ["unknown_empty"]

# -------------------- 메인 --------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=DEFAULT_START)
    parser.add_argument("--end", type=str, default=DEFAULT_END)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out", type=str, default=str(Path(__file__).resolve().parent / "out" / "list"))
    parser.add_argument("--no-skip-existing", action="store_true")
    parser.add_argument("--stats-interval", type=int, default=1, help="요약 출력 간격(N개 완료마다 1줄)")
    parser.add_argument("--verbose-items", action="store_true", help="각 항목 라인별 출력 허용")
    args = parser.parse_args()

    start = args.start
    end = args.end
    out_dir = Path(args.out)
    out_corp_dir = out_dir / "by_corp"
    skip_existing = not args.no_skip_existing

    keys = read_dart_keys_ordered()
    if not keys:
        print("[ERROR] DART 키를 찾을 수 없습니다.")
        sys.exit(1)
    key_pool = KeyPool(keys)
    print(f"[INFO] 키 {len(keys)}개 로드, 첫 키={key_pool.current()[:10]}...")

    # 법인 목록
    corps = load_corp_list()
    print(f"[INFO] 수집 대상 법인수: {len(corps)}")

    # 사전 스킵
    filtered: List[Dict[str, str]] = []
    skipped = 0
    out_corp_dir.mkdir(parents=True, exist_ok=True)
    for c in corps:
        stock, corp = c["stock_code"], c["corp_code"]
        if skip_existing and exists_any(out_corp_dir, stock, corp):
            skipped += 1
            continue
        filtered.append(c)
    print(f"[INFO] 사전 스킵: {skipped}건, 실제 수집: {len(filtered)}건")

    # 통계
    lock = Lock()
    total_done = 0
    cnt_zero_013 = 0
    cnt_success_000 = 0
    cnt_failed = 0
    cnt_skipped_runtime = 0  # 런타임 스킵

    from concurrent.futures import ThreadPoolExecutor, as_completed
    summary: List[Dict[str, str]] = []
    failures_path = out_dir / f"failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    failures_fp = failures_path.open("w", encoding="utf-8")

    def print_stats(optional_note: str = ""):
        nonlocal total_done, cnt_zero_013, cnt_success_000, cnt_failed, skipped
        note = f" {optional_note}" if optional_note else ""
        print(f"[STATS]{note} done={total_done} | 000>0={cnt_success_000} | 013={cnt_zero_013} | failed={cnt_failed} | pre-skip={skipped}")

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {}
        for c in filtered:
            fut = ex.submit(
                fetch_list_for_corp,
                c["corp_code"],
                c["stock_code"],
                start,
                end,
                key_pool,
                out_corp_dir,
                skip_existing,
            )
            fut_map[fut] = c

        i = 0
        for fut in as_completed(fut_map):
            c = fut_map[fut]
            try:
                corp_code, cnt, issues = fut.result()
                with lock:
                    total_done += 1
                    if "skipped_existing" in (issues or []):
                        cnt_skipped_runtime += 1
                    elif "dart_status_013" in (issues or []):
                        cnt_zero_013 += 1
                    elif cnt > 0 and not issues:
                        cnt_success_000 += 1
                    else:
                        cnt_failed += 1

                if args.verbose_items:
                    if issues:
                        print(f"[ITEM] {c['stock_code']}({c['corp_code']}) cnt={cnt} issues={issues}")
                    else:
                        print(f"[ITEM] {c['stock_code']}({c['corp_code']}) cnt={cnt}")

                if cnt == 0 and issues and "dart_status_013" not in issues:
                    failures_fp.write(json.dumps({
                        "corp_code": corp_code,
                        "stock_code": c["stock_code"],
                        "start": start, "end": end,
                        "issues": issues,
                        "ts": datetime.now().isoformat(timespec="seconds"),
                    }, ensure_ascii=False) + "\n")

            except Exception as e:
                with lock:
                    total_done += 1
                    cnt_failed += 1
                if args.verbose_items:
                    print(f"[ITEM] {c['corp_code']} EXC: {e}")
                failures_fp.write(json.dumps({
                    "corp_code": c["corp_code"],
                    "stock_code": c["stock_code"],
                    "start": start, "end": end,
                    "issues": [str(e)],
                    "ts": datetime.now().isoformat(timespec="seconds"),
                }, ensure_ascii=False) + "\n")

            i += 1
            if i % max(1, args.stats_interval) == 0:
                print_stats()

    failures_fp.close()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"summary_{ts}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SUMMARY] {summary_path} 저장")
    print(f"[FAILURES] {failures_path} 저장")
    print_stats("final")

if __name__ == "__main__":
    main()
