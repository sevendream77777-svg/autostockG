#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v52 병렬 수집기 (하루만에 대량 수집)
- 멀티프로세싱으로 여러 종목/날짜를 동시에 수집
- 하루만에 2800종목 × 11년 수집 가능
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from multiprocessing import Pool, cpu_count, Manager
from functools import partial
import pandas as pd

# --------------------------------------------------------------------------- #
# 경로 설정
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig, V52_COLS  # noqa: E402

V52_COLUMNS: List[str] = V52_COLS


def collect_one_worker(args: Tuple[str, str]) -> Tuple[str, str, Dict[str, object], Dict[str, Dict[str, object]]]:
    """워커 함수: 단일 종목/날짜 수집"""
    code, date = args
    try:
        collector = V52Collector(CollectConfig(code=code, date=date))
        row = collector.run()
        return code, date, row, {"raw": row}
    except Exception as e:
        # 에러 발생 시 빈 데이터 반환
        row = {k: None for k in V52_COLS}
        row.update({"date": date, "code": code})
        return code, date, row, {"raw": row, "error": str(e)}


def collect_parallel(
    codes: List[str],
    dates: List[str],
    num_workers: int = None,
    chunk_size: int = 10
) -> Tuple[List[Dict[str, object]], Dict[Tuple[str, str], Dict[str, Dict[str, object]]]]:
    """
    병렬 수집 (하루 내 대량 수집 최적화)
    """
    if num_workers is None:
        # 하루 내 수집을 위해 더 많은 워커 사용
        num_workers = min(cpu_count() * 10, 500)  # CPU 코어의 10배, 최대 500개
    
    # 모든 조합 생성
    tasks = [(code, date) for date in dates for code in codes]
    total = len(tasks)
    
    print(f"[병렬 수집] 총 {total:,}개 작업, {num_workers}개 워커 사용")
    print(f"[병렬 수집] 예상 시간: 약 {total * 25 / num_workers / 3600:.1f}시간")
    
    rows: List[Dict[str, object]] = []
    raw_debug: Dict[Tuple[str, str], Dict[str, Dict[str, object]]] = {}
    
    start_time = time.time()
    completed = 0
    
    # 프로세스 풀 생성 및 실행
    with Pool(processes=num_workers) as pool:
        # 결과를 순차적으로 받기 (진행률 표시)
        for result in pool.imap(collect_one_worker, tasks, chunksize=chunk_size):
            code, date, row, raw = result
            rows.append(row)
            raw_debug[(code, date)] = raw
            
            completed += 1
            if completed % 100 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (total - completed) / rate if rate > 0 else 0
                print(f"[진행] {completed:,}/{total:,} ({completed/total*100:.1f}%) - "
                      f"속도: {rate:.1f}건/초 - 남은시간: {remaining/3600:.1f}시간")
    
    elapsed = time.time() - start_time
    print(f"\n[완료] 총 {total:,}개 작업 완료 - 소요시간: {elapsed/3600:.2f}시간 - 속도: {total/elapsed:.1f}건/초")
    
    return rows, raw_debug


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """날짜 범위 생성 (영업일만)"""
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 월~금
            dates.append(current.strftime("%Y%m%d"))
        current += pd.Timedelta(days=1)
    return dates


def save_by_date(rows: List[Dict[str, object]], out_dir: Path) -> None:
    """날짜별 CSV로 저장"""
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
        print(f"[저장] {fname} ({len(items):,} rows)")


def save_raw_debug(raw_map: Dict[Tuple[str, str], Dict[str, Dict[str, object]]], out_dir: Path) -> None:
    """디버그 JSON 저장"""
    out_dir.mkdir(parents=True, exist_ok=True)
    for (code, date), payload in raw_map.items():
        fname = out_dir / f"raw_debug_{code}_{date}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="raw_v52 병렬 수집기")
    p.add_argument("--codes", type=str, required=True, help="콤마로 구분된 종목코드들 또는 파일 경로")
    p.add_argument("--date-range", type=str, required=True, help="날짜 범위: START:END (예: 20150102:20251205)")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "out"), help="결과 저장 폴더")
    p.add_argument("--workers", type=int, default=None, help="워커 프로세스 수 (기본: CPU*2, 최대 200)")
    p.add_argument("--chunk-size", type=int, default=10, help="청크 크기 (기본: 10)")
    p.add_argument("--no-debug", action="store_true", help="raw 디버그 JSON 저장 안 함")
    return p.parse_args()


def load_codes(codes_arg: str) -> List[str]:
    """종목코드 로드 (콤마 구분 또는 파일)"""
    if Path(codes_arg).exists():
        # 파일에서 로드
        with open(codes_arg, "r", encoding="utf-8") as f:
            codes = [line.strip() for line in f if line.strip()]
        print(f"[종목 로드] 파일에서 {len(codes):,}개 종목 로드")
    else:
        # 콤마 구분
        codes = [c.strip() for c in codes_arg.split(",") if c.strip()]
        print(f"[종목 로드] {len(codes):,}개 종목")
    return codes


def main() -> None:
    args = parse_args()
    
    # 종목 코드 로드
    codes = load_codes(args.codes)
    
    # 날짜 범위 생성
    start, end = args.date_range.split(":")
    dates = generate_date_range(start.strip(), end.strip())
    print(f"[날짜 범위] {start} ~ {end} ({len(dates):,}일)")
    
    # 워커 수 설정
    num_workers = args.workers
    if num_workers is None:
        num_workers = min(cpu_count() * 2, 200)
    
    print(f"[설정] 워커: {num_workers}개, 청크 크기: {args.chunk_size}")
    print(f"[설정] 총 작업: {len(codes):,}종목 × {len(dates):,}일 = {len(codes) * len(dates):,}건")
    
    # 병렬 수집
    rows, raw_debug = collect_parallel(
        codes=codes,
        dates=dates,
        num_workers=num_workers,
        chunk_size=args.chunk_size
    )
    
    # 저장
    print("\n[저장 중...]")
    save_by_date(rows, Path(args.out_dir))
    if not args.no_debug:
        save_raw_debug(raw_debug, Path(args.out_dir) / "debug")
    
    print(f"\n[완료] 총 {len(rows):,}개 행 수집 완료")


if __name__ == "__main__":
    main()

