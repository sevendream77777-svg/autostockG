# -*- coding: utf-8 -*-
"""
전 종목 공시목록(list.json) 수집기
- 입력: dart_corp_list.xml (stock_code 있는 법인만)
- 범위: 기본 20160101 ~ 20251205 (옵션 --start/--end)
- 출력: out/list/by_corp/{stock}_{corp}.json (UTF-8), 전체 로그
- 키 사용: 한 키로 시도, 오류/429 시에만 다음 키로 회전 → 최소 키 사용
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import requests


# -------------------- 기본 설정 -------------------- #
DEFAULT_START = "20160101"
DEFAULT_END = "20251205"
PAGE_COUNT = 100
REQ_SLEEP = 0.12  # 키 소모 최소화를 위한 짧은 간격

DEFAULT_XML_PATHS = [
    Path(__file__).resolve().parent / "dart_corp_list.xml",
]

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


# -------------------- 유틸 -------------------- #
def _extract_leading_int(name: str) -> int:
    import re

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


def load_corp_list() -> List[Dict[str, str]]:
    xml_path = None
    for p in DEFAULT_XML_PATHS:
        if p.exists():
            xml_path = p
            break
    if xml_path is None:
        raise FileNotFoundError("dart_corp_list.xml을 찾을 수 없습니다.")

    tree = ET.parse(xml_path)
    root = tree.getroot()
    corps = []
    for corp in root.findall(".//list"):
        stock_raw = (corp.findtext("stock_code", "") or "").strip()
        stock = stock_raw.zfill(6)
        corp_code = (corp.findtext("corp_code", "") or "").strip()
        corp_name = (corp.findtext("corp_name", "") or "").strip()
        if not stock or len(stock) != 6:
            continue
        if stock == "000000":  # 비상장/미사용 코드는 제외해 키 소모를 줄임
            continue
        if not corp_code:
            continue
        corps.append({"stock_code": stock, "corp_code": corp_code, "corp_name": corp_name})
    # 중복 제거 (stock_code 기준 우선)
    dedup = {}
    for c in corps:
        dedup[c["corp_code"]] = c
    return list(dedup.values())


class KeyPool:
    def __init__(self, keys: List[str]):
        if not keys:
            raise ValueError("DART 키가 필요합니다.")
        self.keys = keys
        self.idx = 0

    def current(self) -> str:
        return self.keys[self.idx % len(self.keys)]

    def rotate(self):
        self.idx = (self.idx + 1) % len(self.keys)


def http_get_json(url: str, params: dict, timeout: int = 8) -> Tuple[int, Optional[dict]]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return 200, r.json()
        return r.status_code, None
    except Exception:
        return -1, None


# -------------------- 수집 로직 -------------------- #
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
    """
    issues: List[str] = []
    rows: List[Dict[str, str]] = []
    page_no = 1
    fail_count = 0
    MAX_FAIL = 5

    # 이미 수집된 파일이 있으면 스킵해 키/시간 낭비 방지
    out_corp_dir.mkdir(parents=True, exist_ok=True)
    target_path = out_corp_dir / f"{stock_code}_{corp_code}.json"
    alt_path = out_corp_dir / f"{corp_code}.json"
    existing_path = target_path if target_path.exists() else (alt_path if alt_path.exists() else None)
    if skip_existing and existing_path:
        try:
            size_ok = existing_path.stat().st_size > 0
        except Exception:
            size_ok = True
        if size_ok:
            return corp_code, -1, ["skipped_existing"]

    while True:
        params = {
            "crtfc_key": key_pool.current(),
            "corp_code": corp_code,
            "bgn_de": start,
            "end_de": end,
            "page_no": page_no,
            "page_count": PAGE_COUNT,
        }
        status, data = http_get_json("https://opendart.fss.or.kr/api/list.json", params, timeout=8)
        if status == 429:
            key_pool.rotate()
            time.sleep(0.3)
            continue
        if status != 200 or data is None:
            issues.append(f"http_status_{status}")
            fail_count += 1
            key_pool.rotate()
            time.sleep(0.2)
            if fail_count >= MAX_FAIL:
                break
            continue

        dstatus = data.get("status")
        if dstatus == "013":  # 조회 데이터 없음 → 종료
            break
        if dstatus != "000":
            issues.append(f"dart_status_{dstatus}")
            fail_count += 1
            key_pool.rotate()
            time.sleep(0.2)
            if fail_count >= MAX_FAIL:
                break
            continue
        fail_count = 0  # 성공하면 실패 카운터 초기화

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
        time.sleep(REQ_SLEEP)

    # 저장
    if rows:
        out_corp_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_corp_dir / f"{stock_code}_{corp_code}.json"
        out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return corp_code, len(rows), issues


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=DEFAULT_START)
    parser.add_argument("--end", type=str, default=DEFAULT_END)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument(
        "--out", type=str, default=str(Path(__file__).resolve().parent / "out" / "list")
    )
    parser.add_argument("--no-skip-existing", action="store_true", help="이미 수집된 by_corp 파일이 있어도 다시 수집")
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

    corps = load_corp_list()
    print(f"[INFO] 수집 대상 법인수: {len(corps)}")

    out_dir.mkdir(parents=True, exist_ok=True)
    summary: List[Dict[str, str]] = []

    # 미리 존재 파일 스킵 처리 → 불필요 호출 최소화
    filtered_corps: List[Dict[str, str]] = []
    skipped = 0
    for c in corps:
        stock = c["stock_code"]
        corp = c["corp_code"]
        tgt = out_corp_dir / f"{stock}_{corp}.json"
        alt = out_corp_dir / f"{corp}.json"
        if skip_existing and (tgt.exists() or alt.exists()):
            summary.append({"corp_code": corp, "stock_code": stock, "count": -1, "issues": ["skipped_existing"]})
            skipped += 1
            continue
        filtered_corps.append(c)

    if skip_existing:
        print(f"[INFO] 기존 파일 스킵 {skipped}건, 실제 수집 {len(filtered_corps)}건")

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {}
        for c in filtered_corps:
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

        for fut in as_completed(fut_map):
            corp = fut_map[fut]
            try:
                corp_code, cnt, issues = fut.result()
                summary.append(
                    {"corp_code": corp_code, "stock_code": corp["stock_code"], "count": cnt, "issues": issues}
                )
                if issues:
                    print(f"[DONE] {corp['stock_code']} ({corp_code}) count={cnt} issues={issues}")
                else:
                    print(f"[DONE] {corp['stock_code']} ({corp_code}) count={cnt}")
            except Exception as e:
                print(f"[ERROR] {corp['corp_code']} 실패: {e}")
                summary.append(
                    {"corp_code": corp["corp_code"], "stock_code": corp["stock_code"], "count": 0, "issues": [str(e)]}
                )

    # 요약 저장
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"summary_{ts}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SUMMARY] {summary_path} 저장 (총 {len(summary)} 법인)")


if __name__ == "__main__":
    main()

