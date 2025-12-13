# -*- coding: utf-8 -*-
"""
DART list.json 수집기 — SAFE_v3 (단일 파일 완전체)
무결성 보장 / 이어받기 / 013 오판 방지 / zero 덮어쓰기 금지
"""
from __future__ import annotations

import argparse, json, os, sys, time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from threading import Lock
import requests

# -------------------- 상수 --------------------
DEFAULT_START = "20160101"
DEFAULT_END = "20251205"
PAGE_COUNT = 100
REQ_SLEEP_BASE = 0.12
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 7
MAX_FAIL = 5
TOTAL_TIME_CAP_SEC = 90
CONFIRM_013_RETRY = 2

# -------------------- 경로 유틸 --------------------
def bycorp_main_path(dir_by_corp: Path, stock_code: str, corp_code: str) -> Path:
    return dir_by_corp / f"{stock_code}_{corp_code}.json"

def bycorp_zero_path(dir_by_corp: Path, stock_code: str, corp_code: str) -> Path:
    return dir_by_corp / f"{stock_code}_{corp_code}.zero.json"

def bycorp_legacy000_path(dir_by_corp: Path, corp_code: str) -> Path:
    return dir_by_corp / f"000000_{corp_code}.json"

def file_is_valid_json(p: Path) -> bool:
    try:
        if not p.exists() or p.stat().st_size <= 2:
            return False
        json.loads(p.read_text(encoding="utf-8"))
        return True
    except Exception:
        return False

def atomic_write_json(out_path: Path, payload) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _ = json.loads(tmp.read_text(encoding="utf-8"))
    os.replace(tmp, out_path)

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
def _read_lines(p: Path) -> List[str]:
    try:
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except Exception:
        return []
def _extract_leading_int(name: str) -> int:
    import re
    m = re.match(r"^(\d{1,2})", name)
    return int(m.group(1)) if m else 9999
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
                if k:
                    candidates.append((leading, k))
    uniq: List[str] = []
    for _, (lead, key) in sorted(enumerate(candidates), key=lambda x: (x[1][0], x[0])):
        if key not in uniq:
            uniq.append(key)
    return uniq
class KeyPool:
    def __init__(self, keys: List[str]):
        if not keys:
            raise ValueError("DART 키 없음")
        self.keys = keys; self.idx = 0
    def current(self) -> str: return self.keys[self.idx]
    def rotate(self) -> None: self.idx = (self.idx + 1) % len(self.keys)

# -------------------- corp 리스트 로드 --------------------
DEFAULT_XML_PATHS = [Path(__file__).resolve().parent / "dart_corp_list.xml"]
def load_corp_list() -> List[Dict[str, str]]:
    xml_path = None
    for p in DEFAULT_XML_PATHS:
        if p.exists():
            xml_path = p; break
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
            try: return 200, r.json()
            except Exception: return 200, None
        return r.status_code, None
    except requests.exceptions.Timeout:
        return 408, None
    except Exception:
        return -1, None

# -------------------- 스킵 판정 --------------------
def decide_skip(out_dir_by_corp: Path, stock_code: str, corp_code: str, respect_zero: bool) -> Tuple[bool, str]:
    main_p = bycorp_main_path(out_dir_by_corp, stock_code, corp_code)
    zero_p = bycorp_zero_path(out_dir_by_corp, stock_code, corp_code)
    legacy_p = bycorp_legacy000_path(out_dir_by_corp, corp_code)
    if file_is_valid_json(main_p):
        return True, "have_main_valid"
    if respect_zero and zero_p.exists() and zero_p.stat().st_size > 0:
        return True, "have_zero_respected"
    if legacy_p.exists() and legacy_p.stat().st_size > 0:
        try:
            json.loads(legacy_p.read_text(encoding="utf-8"))
            return True, "have_legacy000"
        except Exception:
            pass
    return False, ""

# -------------------- 한 법인 수집 --------------------
def fetch_list_for_corp(
    corp_code: str,
    stock_code: str,
    start: str,
    end: str,
    key_pool: KeyPool,
    out_corp_dir: Path,
    respect_zero: bool,
    skip_existing: bool = True,
) -> Tuple[str, int, List[str]]:
    t0 = time.time()
    issues: List[str] = []
    rows: List[Dict[str, str]] = []
    page_no = 1
    fail_count = 0
    sleep = REQ_SLEEP_BASE

    if skip_existing:
        skip, reason = decide_skip(out_corp_dir, stock_code, corp_code, respect_zero)
        if skip:
            return corp_code, -1, [f"skipped_existing:{reason}"]

    seen_013 = 0
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
            issues.append("http_status_429"); key_pool.rotate()
            time.sleep(min(1.0, sleep * 2)); fail_count += 1
            if fail_count >= MAX_FAIL: break
            continue
        if status in (-1, 408) or data is None:
            issues.append(f"http_status_{status}")
            time.sleep(min(1.0, sleep * 2)); fail_count += 1
            if fail_count >= MAX_FAIL: break
            continue

        dart_status = data.get("status")
        if dart_status == "013":
            seen_013 += 1
            if seen_013 >= CONFIRM_013_RETRY:
                if file_is_valid_json(bycorp_main_path(out_corp_dir, stock_code, corp_code)):
                    return corp_code, 0, ["dart_status_013_but_main_exists"]
                zero = {
                    "corp_code": corp_code, "stock_code": stock_code,
                    "count": 0, "status": "013",
                    "start": start, "end": end,
                    "ts": datetime.now().isoformat(timespec="seconds"),
                }
                atomic_write_json(bycorp_zero_path(out_corp_dir, stock_code, corp_code), zero)
                return corp_code, 0, ["dart_status_013"]
            else:
                time.sleep(min(0.5, sleep)); continue

        if dart_status not in (None, "000"):
            issues.append(f"dart_status_{dart_status}")
            time.sleep(min(1.2, sleep * 2)); fail_count += 1
            if fail_count >= MAX_FAIL: break
            key_pool.rotate(); continue

        fail_count = 0
        total_count = int(data.get("total_count") or 0)
        items = data.get("list") or []
        if page_no == 1 and total_count > 0 and not items:
            issues.append("000_but_empty_first_page")
            fail_count += 1
            if fail_count >= MAX_FAIL: break
            time.sleep(min(1.0, sleep * 2)); continue

        for it in items:
            rows.append({
                "corp_code": it.get("corp_code"),
                "stock_code": stock_code,
                "corp_name": it.get("corp_name"),
                "report_nm": it.get("report_nm"),
                "rcept_no": it.get("rcept_no"),
                "rcept_dt": it.get("rcept_dt"),
                "reprt_code": it.get("reprt_code"),
                "flr_nm": it.get("flr_nm"),
            })
        if len(items) < PAGE_COUNT: break
        page_no += 1; time.sleep(sleep)

    if rows:
        main_p = bycorp_main_path(out_corp_dir, stock_code, corp_code)
        if not file_is_valid_json(main_p):
            atomic_write_json(main_p, rows)
        return corp_code, len(rows), issues

    return corp_code, 0, issues if issues else ["empty_without_013"]

# -------------------- 메인 --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default=DEFAULT_START)
    ap.add_argument("--end", type=str, default=DEFAULT_END)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--out", type=str, default=str(Path(__file__).resolve().parent / "out" / "list"))
    ap.add_argument("--no-skip-existing", action="store_true")
    ap.add_argument("--respect-zero", action="store_true")
    ap.add_argument("--stats-interval", type=int, default=1)
    ap.add_argument("--verbose-items", action="store_true")
    args = ap.parse_args()

    start, end = args.start, args.end
    out_dir = Path(args.out); out_corp_dir = out_dir / "by_corp"
    skip_existing = not args.no_skip_existing
    respect_zero = args.respect_zero

    keys = read_dart_keys_ordered()
    if not keys:
        print("[ERROR] DART 키 없음"); sys.exit(1)
    key_pool = KeyPool(keys)
    print(f"[INFO] 키 {len(keys)}개 로드, 첫 키={key_pool.current()[:10]}...")

    corps = load_corp_list()
    print(f"[INFO] 수집 대상 법인수: {len(corps)}")

    filtered: List[Dict[str, str]] = []
    pre_skipped = 0
    out_corp_dir.mkdir(parents=True, exist_ok=True)
    for c in corps:
        stock, corp = c["stock_code"], c["corp_code"]
        if skip_existing:
            skip, _ = decide_skip(out_corp_dir, stock, corp, respect_zero)
            if skip:
                pre_skipped += 1; continue
        filtered.append(c)
    print(f"[INFO] 사전 스킵: {pre_skipped}건, 실제 수집: {len(filtered)}건")

    lock = Lock()
    total_done = 0; cnt_zero_013 = 0; cnt_success_000 = 0; cnt_failed = 0

    from concurrent.futures import ThreadPoolExecutor, as_completed
    failures_path = out_dir / f"failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    failures_fp = failures_path.open("w", encoding="utf-8")

    def print_stats(note: str = ""):
        nonlocal total_done, cnt_zero_013, cnt_success_000, cnt_failed, pre_skipped
        tail = f" {note}" if note else ""
        print(f"[STATS]{tail} done={total_done} | 000>0={cnt_success_000} | 013={cnt_zero_013} | failed={cnt_failed} | pre-skip={pre_skipped}")

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {}
        for c in filtered:
            fut = ex.submit(
                fetch_list_for_corp, c["corp_code"], c["stock_code"],
                start, end, key_pool, out_corp_dir, respect_zero, skip_existing
            )
            fut_map[fut] = c

        i = 0
        for fut in as_completed(fut_map):
            c = fut_map[fut]
            try:
                corp_code, cnt, issues = fut.result()
                with lock:
                    total_done += 1
                    if "dart_status_013" in (issues or []):
                        cnt_zero_013 += 1
                    elif cnt > 0 and not issues:
                        cnt_success_000 += 1
                    else:
                        cnt_failed += 1

                if args.verbose_items:
                    print(f"[ITEM] {c['stock_code']}({c['corp_code']}) cnt={cnt} issues={issues}")

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
                    total_done += 1; cnt_failed += 1
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
    print(f"[FAILURES] {failures_path} 저장")
    print_stats("final")

if __name__ == "__main__":
    main()
