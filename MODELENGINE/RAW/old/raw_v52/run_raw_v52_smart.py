#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v52 스마트 수집기 (안전한 대량 수집)
- 하루 단위로 차곡차곡 수집
- 중단 시 이어서 진행
- 부분 재수집 지원
- 데이터 검증 및 자동 재수집
- 오염된 데이터 필터링
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import pickle
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from multiprocessing import Pool, cpu_count
from datetime import datetime
import pandas as pd

# --------------------------------------------------------------------------- #
# 경로 설정
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig, V52_COLS, set_proxy  # noqa: E402

V52_COLUMNS: List[str] = V52_COLS


class SmartCollector:
    """스마트 수집기: 진행 상황 저장, 검증, 재수집 지원"""
    
    def __init__(self, out_dir: Path, checkpoint_file: str = "checkpoint.pkl"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.out_dir / checkpoint_file
        self.completed: Set[Tuple[str, str]] = set()  # (code, date) 완료된 작업
        self.failed: Set[Tuple[str, str]] = set()  # 실패한 작업
        self.load_checkpoint()
    
    def load_checkpoint(self):
        """체크포인트 로드"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "rb") as f:
                    data = pickle.load(f)
                    self.completed = data.get("completed", set())
                    self.failed = data.get("failed", set())
                print(f"[체크포인트 로드] 완료: {len(self.completed):,}건, 실패: {len(self.failed):,}건")
            except Exception as e:
                print(f"[체크포인트 로드 실패] {e}")
    
    def save_checkpoint(self):
        """체크포인트 저장"""
        try:
            data = {
                "completed": self.completed,
                "failed": self.failed,
                "timestamp": datetime.now().isoformat()
            }
            with open(self.checkpoint_file, "wb") as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"[체크포인트 저장 실패] {e}")
    
    def is_completed(self, code: str, date: str) -> bool:
        """작업 완료 여부 확인"""
        return (code, date) in self.completed
    
    def mark_completed(self, code: str, date: str):
        """작업 완료 표시"""
        self.completed.add((code, date))
        if (code, date) in self.failed:
            self.failed.remove((code, date))
        # 주기적으로 저장 (100건마다)
        if len(self.completed) % 100 == 0:
            self.save_checkpoint()
    
    def mark_failed(self, code: str, date: str):
        """작업 실패 표시"""
        self.failed.add((code, date))
    
    def get_existing_data(self, date: str) -> Optional[pd.DataFrame]:
        """기존 데이터 로드"""
        csv_file = self.out_dir / f"raw_v52_{date}.csv"
        if csv_file.exists():
            try:
                return pd.read_csv(csv_file, dtype=str)
            except:
                return None
        return None
    
    def save_data(self, date: str, rows: List[Dict]):
        """데이터 저장 (기존 데이터와 병합)"""
        existing_df = self.get_existing_data(date)
        new_df = pd.DataFrame(rows, columns=V52_COLUMNS)
        
        if existing_df is not None and not existing_df.empty:
            # 기존 데이터와 병합 (중복 제거)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["code", "date"], keep="last")
        else:
            combined = new_df
        
        csv_file = self.out_dir / f"raw_v52_{date}.csv"
        combined.to_csv(csv_file, index=False, encoding="utf-8-sig")
        return len(combined)


def collect_one_worker(args: Tuple[str, str, Set]) -> Optional[Tuple[str, str, Dict, bool]]:
    """워커 함수: 단일 종목/날짜 수집"""
    code, date, completed_set = args
    
    # 이미 완료된 작업 스킵
    if (code, date) in completed_set:
        return None
    
    try:
        collector_obj = V52Collector(CollectConfig(code=code, date=date))
        row = collector_obj.run()
        
        # 기본 검증
        is_valid = validate_row(row, code, date)
        return code, date, row, is_valid
    except Exception as e:
        row = {k: None for k in V52_COLS}
        row.update({"date": date, "code": code})
        return code, date, row, False


def safe_float(v) -> Optional[float]:
    """안전한 float 변환"""
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "").strip()
            if v == "" or v.lower() in ["nan", "none", ""]:
                return None
        return float(v)
    except:
        return None


def validate_row(row: Dict, code: str, date: str) -> bool:
    """행 데이터 검증"""
    # 필수 필드 확인
    row_date = str(row.get("date", "")).strip()
    row_code = str(row.get("code", "")).strip().zfill(6)
    if row_date != date or row_code != code.zfill(6):
        return False
    
    # 기본 필드 확인 (가격 데이터)
    close_val = safe_float(row.get("close"))
    if close_val is None or close_val <= 0:
        return False
    
    # 가격 일관성 확인
    open_val = safe_float(row.get("open"))
    high_val = safe_float(row.get("high"))
    low_val = safe_float(row.get("low"))
    
    if all(v is not None and v > 0 for v in [open_val, high_val, low_val, close_val]):
        if not (high_val >= low_val and high_val >= open_val and high_val >= close_val and
                low_val <= open_val and low_val <= close_val):
            return False
    
    # NaN 체크 (너무 많은 NaN이면 실패)
    nan_count = sum(1 for v in row.values() 
                   if v is None or 
                   (isinstance(v, str) and v.lower() in ["nan", "none", ""]) or
                   (isinstance(v, float) and pd.isna(v)))
    if nan_count > len(row) * 0.5:  # 50% 이상 NaN이면 실패
        return False
    
    return True


def collect_by_date(
    codes: List[str],
    date: str,
    collector: SmartCollector,
    num_workers: int = 10
) -> Tuple[List[Dict], int, int]:
    """하루치 데이터 수집"""
    # 미완료 작업만 필터링
    tasks = [(code, date, collector.completed) for code in codes 
             if not collector.is_completed(code, date)]
    
    if not tasks:
        print(f"  [{date}] 모든 종목 완료 (스킵)")
        return [], 0, 0
    
    print(f"  [{date}] {len(tasks):,}개 종목 수집 시작...")
    
    rows = []
    success = 0
    failed = 0
    
    start_time = time.time()
    
    with Pool(processes=num_workers) as pool:
        results = pool.imap(collect_one_worker, tasks, chunksize=10)
        for result in results:
            if result is None:
                continue
            
            code, date, row, is_valid = result
            rows.append(row)
            
            if is_valid:
                collector.mark_completed(code, date)
                success += 1
            else:
                collector.mark_failed(code, date)
                failed += 1
    
    elapsed = time.time() - start_time
    print(f"  [{date}] 완료: {success:,}건 성공, {failed:,}건 실패 ({elapsed:.1f}초)")
    
    # 데이터 저장
    if rows:
        saved_count = collector.save_data(date, rows)
        print(f"  [{date}] 저장 완료: {saved_count:,}행")
    
    # 체크포인트 저장
    collector.save_checkpoint()
    
    return rows, success, failed


def verify_data(out_dir: Path, codes: List[str], dates: List[str]) -> Dict:
    """전체 데이터 검증"""
    print("\n" + "=" * 80)
    print("데이터 검증 시작")
    print("=" * 80)
    
    issues = {
        "missing": [],  # 누락된 데이터
        "invalid": [],  # 잘못된 데이터
        "nan_heavy": []  # NaN이 많은 데이터
    }
    
    for date in dates:
        csv_file = out_dir / f"raw_v52_{date}.csv"
        if not csv_file.exists():
            issues["missing"].extend([(code, date) for code in codes])
            continue
        
        try:
            df = pd.read_csv(csv_file, dtype=str)
            existing_codes = set(df["code"].astype(str).str.zfill(6))
            
            # 누락된 종목 확인
            for code in codes:
                code_str = code.zfill(6)
                if code_str not in existing_codes:
                    issues["missing"].append((code, date))
                    continue
                
                # 해당 종목 데이터 검증
                row_df = df[df["code"] == code_str]
                if row_df.empty:
                    issues["missing"].append((code, date))
                    continue
                
                row_data = row_df.iloc[0].to_dict()
                if not validate_row(row_data, code, date):
                    issues["invalid"].append((code, date))
                
                # NaN 체크
                nan_count = sum(1 for v in row_data.values() 
                               if v is None or 
                               (isinstance(v, str) and str(v).lower() in ["nan", "none", ""]) or
                               (isinstance(v, float) and pd.isna(v)))
                if nan_count > len(row_data) * 0.5:
                    issues["nan_heavy"].append((code, date))
        
        except Exception as e:
            print(f"  [{date}] 검증 오류: {e}")
            issues["missing"].extend([(code, date) for code in codes])
    
    print(f"\n[검증 결과]")
    print(f"  누락: {len(issues['missing']):,}건")
    print(f"  잘못됨: {len(issues['invalid']):,}건")
    print(f"  NaN 많음: {len(issues['nan_heavy']):,}건")
    print(f"  총 문제: {len(issues['missing']) + len(issues['invalid']) + len(issues['nan_heavy']):,}건")
    
    return issues


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


def load_codes(codes_arg: str) -> List[str]:
    """종목코드 로드"""
    if Path(codes_arg).exists():
        with open(codes_arg, "r", encoding="utf-8") as f:
            codes = [line.strip() for line in f if line.strip()]
        print(f"[종목 로드] 파일에서 {len(codes):,}개 종목 로드")
    else:
        codes = [c.strip() for c in codes_arg.split(",") if c.strip()]
        print(f"[종목 로드] {len(codes):,}개 종목")
    return codes


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="raw_v52 스마트 수집기")
    p.add_argument("--codes", type=str, required=True, help="종목코드 파일 또는 콤마 구분 리스트")
    p.add_argument("--date-range", type=str, default=None, help="날짜 범위: START:END (예: 20150102:20150131)")
    p.add_argument("--dates", type=str, default=None, help="특정 날짜들: 콤마 구분 (예: 20150102,20150105,20150106)")
    p.add_argument("--days", type=int, default=None, help="시작일로부터 N영업일 (예: --date-range 20150102:20150102 --days 5)")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "out"), help="결과 저장 폴더")
    p.add_argument("--workers", type=int, default=10, help="워커 프로세스 수 (기본: 10)")
    p.add_argument("--verify-only", action="store_true", help="검증만 수행 (수집 안 함)")
    p.add_argument("--recollect-failed", action="store_true", help="실패한 작업만 재수집")
    p.add_argument("--recollect-invalid", action="store_true", help="검증 실패 데이터만 재수집")
    p.add_argument("--proxy", type=str, default=None, help="프록시 설정 (예: http://proxy.example.com:8080 또는 socks5://127.0.0.1:1080)")
    p.add_argument("--proxy-file", type=str, default=None, help="프록시 설정 파일 경로")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    
    # 프록시 설정
    if args.proxy:
        set_proxy(args.proxy)
        print(f"[프록시 설정] {args.proxy}")
    elif args.proxy_file:
        proxy_path = Path(args.proxy_file)
        if proxy_path.exists():
            proxy = proxy_path.read_text(encoding="utf-8").strip()
            set_proxy(proxy)
            print(f"[프록시 설정] 파일에서 로드: {args.proxy_file}")
        else:
            print(f"[경고] 프록시 파일 없음: {args.proxy_file}")
    
    # 종목 코드 로드
    codes = load_codes(args.codes)
    
    # 날짜 처리
    if args.dates:
        # 특정 날짜들 지정
        dates = [d.strip() for d in args.dates.split(",") if d.strip()]
        print(f"[날짜 지정] {len(dates):,}일: {dates[0]} ~ {dates[-1]}")
    elif args.date_range:
        # 날짜 범위
        start, end = args.date_range.split(":")
        dates = generate_date_range(start.strip(), end.strip())
        
        # --days 옵션이 있으면 영업일 수 제한
        if args.days:
            dates = dates[:args.days]
            print(f"[날짜 범위] {start} ~ {end} (처음 {args.days}영업일)")
        else:
            print(f"[날짜 범위] {start} ~ {end} ({len(dates):,}일)")
    else:
        print("[오류] --date-range 또는 --dates 옵션이 필요합니다.")
        return
    
    # 스마트 수집기 초기화
    collector = SmartCollector(Path(args.out_dir))
    
    # 검증만 수행
    if args.verify_only:
        issues = verify_data(Path(args.out_dir), codes, dates)
        # 문제 데이터 저장
        if any(issues.values()):
            issues_file = Path(args.out_dir) / "issues.json"
            with open(issues_file, "w", encoding="utf-8") as f:
                json.dump({
                    "missing": [f"{code}_{date}" for code, date in issues["missing"]],
                    "invalid": [f"{code}_{date}" for code, date in issues["invalid"]],
                    "nan_heavy": [f"{code}_{date}" for code, date in issues["nan_heavy"]]
                }, f, indent=2)
            print(f"\n[문제 데이터 저장] {issues_file}")
        return
    
    # 실패한 작업만 재수집
    if args.recollect_failed:
        print(f"\n[실패 작업 재수집] {len(collector.failed):,}건")
        failed_tasks = list(collector.failed)
        collector.failed.clear()
        
        # 날짜별로 그룹화
        by_date = {}
        for code, date in failed_tasks:
            by_date.setdefault(date, []).append(code)
        
        for date in sorted(by_date.keys()):
            date_codes = by_date[date]
            collect_by_date(date_codes, date, collector, args.workers)
        return
    
    # 검증 실패 데이터만 재수집
    if args.recollect_invalid:
        print("\n[검증 실패 데이터 재수집]")
        issues = verify_data(Path(args.out_dir), codes, dates)
        
        # invalid와 nan_heavy 재수집
        to_recollect = issues["invalid"] + issues["nan_heavy"]
        if not to_recollect:
            print("재수집할 데이터 없음")
            return
        
        print(f"재수집 대상: {len(to_recollect):,}건")
        
        # 날짜별로 그룹화
        by_date = {}
        for code, date in to_recollect:
            by_date.setdefault(date, []).append(code)
            collector.completed.discard((code, date))  # 완료 표시 제거
        
        for date in sorted(by_date.keys()):
            date_codes = by_date[date]
            collect_by_date(date_codes, date, collector, args.workers)
        return
    
    # 정상 수집: 하루 단위로 차곡차곡
    print(f"\n[수집 시작] {len(codes):,}종목 × {len(dates):,}일 = {len(codes) * len(dates):,}건")
    print(f"[설정] 워커: {args.workers}개")
    
    total_success = 0
    total_failed = 0
    
    for i, date in enumerate(dates, 1):
        print(f"\n[{i}/{len(dates)}] {date} 수집 중...")
        rows, success, failed = collect_by_date(codes, date, collector, args.workers)
        total_success += success
        total_failed += failed
    
    # 최종 체크포인트 저장
    collector.save_checkpoint()
    
    print("\n" + "=" * 80)
    print("[수집 완료]")
    print(f"  성공: {total_success:,}건")
    print(f"  실패: {total_failed:,}건")
    total = total_success + total_failed
    if total > 0:
        print(f"  완료율: {total_success/total*100:.1f}%")
    else:
        print(f"  완료율: 100.0% (모든 작업 이미 완료)")
    print("=" * 80)
    
    # 검증 수행
    print("\n[자동 검증 시작]")
    issues = verify_data(Path(args.out_dir), codes, dates)
    
    if any(issues.values()):
        print("\n[재수집 권장] 다음 명령으로 문제 데이터 재수집:")
        print(f"  python run_raw_v52_smart.py --codes {args.codes} --date-range {args.date_range} --recollect-invalid")


if __name__ == "__main__":
    main()

