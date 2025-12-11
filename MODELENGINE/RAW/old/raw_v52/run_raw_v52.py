#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v52 수집기 (콘솔용)
- 새 경량 수집기(v52_collector)로 52개 컬럼 일별 수집
- 출력: 날짜별 CSV (default: MODELENGINE/RAW/raw_v52/out/raw_v52_YYYYMMDD.csv)

빠른 스팟 테스트(3일 x 2종목) 후 전체 수집으로 확장하는 워크플로를 목표로 한다.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# --------------------------------------------------------------------------- #
# 경로 설정: 리포지토리 루트를 sys.path에 추가하여 내부 모듈 import
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig, V52_COLS  # noqa: E402

V52_COLUMNS: List[str] = V52_COLS

# ui.sources 우선순위를 보완 fallback 용도로 사용
try:
    for p in (ROOT, ROOT / "ui"):
        sp = str(p)
        if sp not in sys.path:
            sys.path.insert(0, sp)
    from ui.sources import collect_all  # type: ignore  # noqa: E402
    from ui.sources.common.schema import SOURCE_PRIORITY  # type: ignore  # noqa: E402
    _UI_AVAILABLE = True
    _BASE_PRIORITY_IDX: Dict[str, int] = {src: i for i, src in enumerate(SOURCE_PRIORITY)}
except Exception:
    _UI_AVAILABLE = False
    _BASE_PRIORITY_IDX = {}


def collect_one(code: str, date: str) -> Tuple[Dict[str, object], Dict[str, Dict[str, object]]]:
    """
    단일 종목/일자 수집 (경량 수집기)
    """
    collector = V52Collector(CollectConfig(code=code, date=date))
    row = collector.run()
    return row, {"raw": row}


def fill_missing_with_ui(row: Dict[str, object], code: str, date: str) -> Dict[str, object]:
    """
    ui.sources.collect_all 결과로 비어 있는 필드를 보강한다.
    매크로/이벤트 키는 제외한다.
    """
    if not _UI_AVAILABLE:
        return row
    try:
        payload = collect_all(code, date)
        merged = payload.get("by_field", {})
    except Exception:
        return row

    exclude_keys = {
        "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix",
        "ex_div_date", "earnings_date", "split_announce_date", "split_effective_date",
        "rights_issue_announce_date", "rights_issue_effective_date", "mna_announce_date",
    }

    def pick_best(values: List[Tuple[str, object]]) -> object:
        if not values:
            return None
        ranked = sorted(values, key=lambda x: _BASE_PRIORITY_IDX.get(x[0], -1), reverse=True)
        return ranked[0][1]

    for col in V52_COLUMNS:
        if row.get(col) is None and col not in exclude_keys:
            row[col] = pick_best(merged.get(col, []))
    return row


def save_by_date(rows: List[Dict[str, object]], out_dir: Path) -> None:
    """
    날짜별 CSV로 저장. 파일명: raw_v52_YYYYMMDD.csv
    """
    if not rows:
        return
    by_date: Dict[str, List[Dict[str, object]]] = {}
    for r in rows:
        dt = str(r.get("date") or "")
        by_date.setdefault(dt, []).append(r)

    out_dir.mkdir(parents=True, exist_ok=True)
    for dt, items in by_date.items():
        df = pd.DataFrame(items, columns=V52_COLUMNS)
        fname = out_dir / f"raw_v52_{dt}.csv"
        df.to_csv(fname, index=False, encoding="utf-8-sig")
        print(f"[save] {fname} ({len(items)} rows)")


def save_raw_debug(raw_map: Dict[Tuple[str, str], Dict[str, Dict[str, object]]], out_dir: Path) -> None:
    """
    소스별 raw payload를 JSON으로 별도 저장 (디버그용)
    파일명: raw_debug_{code}_{date}.json
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for (code, date), payload in raw_map.items():
        fname = out_dir / f"raw_debug_{code}_{date}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[debug] {fname}")


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """날짜 범위 생성 (영업일만)"""
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    dates = []
    current = start
    while current <= end:
        # 주말 제외
        if current.weekday() < 5:  # 월~금
            dates.append(current.strftime("%Y%m%d"))
        current += pd.Timedelta(days=1)
    return dates


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="raw_v52 spot collector")
    p.add_argument("--codes", type=str, default="005930,000660", help="콤마로 구분된 종목코드들")
    p.add_argument("--dates", type=str, default=None, help="콤마로 구분된 일자(YYYYMMDD)")
    p.add_argument("--date-range", type=str, default=None, help="날짜 범위: START:END (예: 20150102:20251205)")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "out"), help="결과 저장 폴더")
    p.add_argument("--no-debug", action="store_true", help="raw 디버그 JSON 저장 안 함")
    p.add_argument("--ui-fallback", action="store_true", help="ui.sources 기반 보강 활성화(매크로/이벤트는 제외)")
    p.add_argument("--headless", action="store_true", help="조용히 실행(로그 최소화)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    
    # 날짜 처리: --date-range 우선, 없으면 --dates
    if args.date_range:
        start, end = args.date_range.split(":")
        dates = generate_date_range(start.strip(), end.strip())
        print(f"[info] 날짜 범위: {start} ~ {end} ({len(dates)}일)")
    elif args.dates:
        dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    else:
        dates = ["20150102", "20200615", "20251205"]  # 기본값
        print(f"[info] 기본 날짜 사용: {dates}")

    rows: List[Dict[str, object]] = []
    raw_debug: Dict[Tuple[str, str], Dict[str, Dict[str, object]]] = {}

    total = len(codes) * len(dates)
    current = 0
    
    for date in dates:
        for code in codes:
            current += 1
            if not args.headless:
                print(f"[run] ({current}/{total}) code={code}, date={date}")
            row, raw = collect_one(code, date)
            # 기본은 ui fallback OFF. --ui-fallback 옵션을 켜면 macro/event 제외 보강.
            if args.ui_fallback:
                row = fill_missing_with_ui(row, code, date)
            rows.append(row)
            if not args.no_debug:
                raw_debug[(code, date)] = raw

    save_by_date(rows, Path(args.out_dir))
    if not args.no_debug:
        save_raw_debug(raw_debug, Path(args.out_dir) / "debug")
    
    print(f"\n[완료] 총 {len(rows)}개 행 수집 완료")


if __name__ == "__main__":
    main()

