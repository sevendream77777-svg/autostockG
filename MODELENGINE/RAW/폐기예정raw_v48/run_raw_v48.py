#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v48 Vectorized 수집기 (대량 히스토리컬 데이터 구축용)
- 종목별 11년치 일괄 수집
- 병렬화 지원
- 체크포인트 지원
"""
from __future__ import annotations

import argparse
import json
import os
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
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v48"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v48_collector import VectorizedCollector, CollectConfig, V48_COLS, set_proxy  # noqa: E402

V48_COLUMNS: List[str] = V48_COLS


class SmartCollector:
    """스마트 수집기: 진행 상황 저장 + 실패 사유 기록"""
    
    def __init__(self, out_dir: Path, checkpoint_file: str = "checkpoint.pkl"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.out_dir / checkpoint_file
        self.completed: Set[str] = set()  # 완료된 종목코드
        self.failed: Set[str] = set()  # 실패한 종목코드
        self.failed_info: Dict[str, dict] = {}  # 실패 사유 메타
        self.load_checkpoint()
    
    def load_checkpoint(self):
        """체크포인트 로드"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "rb") as f:
                    data = pickle.load(f)
                    self.completed = data.get("completed", set())
                    self.failed = data.get("failed", set())
                    self.failed_info = data.get("failed_info", {})
                print(f"[체크포인트 로드] 완료: {len(self.completed):,}개 종목, 실패: {len(self.failed):,}개 종목")
            except Exception as e:
                print(f"[체크포인트 로드 실패] {e}")
    
    def save_checkpoint(self):
        """체크포인트 저장"""
        try:
            data = {
                "completed": self.completed,
                "failed": self.failed,
                "failed_info": self.failed_info,
                "timestamp": datetime.now().isoformat()
            }
            with open(self.checkpoint_file, "wb") as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"[체크포인트 저장 실패] {e}")
    
    def is_completed(self, code: str) -> bool:
        """종목 완료 여부 확인"""
        return code in self.completed
    
    def mark_completed(self, code: str):
        """종목 완료 표시"""
        self.completed.add(code)
        if code in self.failed:
            self.failed.remove(code)
        if code in self.failed_info:
            self.failed_info.pop(code, None)
        # 주기적으로 저장 (10종목마다)
        if len(self.completed) % 10 == 0:
            self.save_checkpoint()
    
    def mark_failed(self, code: str, reason: Optional[dict] = None):
        """종목 실패 표시"""
        self.failed.add(code)
        if reason:
            self.failed_info[code] = reason


ESSENTIAL_COLS = ["date", "code", "close", "volume"]
QUALITY_LOG = "quality_report.jsonl"
STREAM_FILE = "raw_v48_all.csv"
METRIC_LOG = "run_metrics.jsonl"
FAIL_QUEUE_LOG = "fail_queue.jsonl"
# DART 모드: off → 수집 안 함, annual → 사업보고서만, full → 4개 보고서
DART_MODES = {"off", "annual", "full"}
# 스케일/품질 가드 파라미터
USDKRW_MIN, USDKRW_MAX = 900, 1500
VIX_MIN, VIX_MAX = 0, 100
WTI_MIN, WTI_MAX = 0, 200
MACRO_CHECK_COLS = {
    "usdkrw": (USDKRW_MIN, USDKRW_MAX),
    "vix": (VIX_MIN, VIX_MAX),
    "wti": (WTI_MIN, WTI_MAX),
}


def _write_status(out_dir: Path, payload: dict, filename: str = "status_summary.json"):
    """수집 상태를 JSON으로 기록"""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        status_path = out_dir / filename
        status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[상태파일 기록 실패] {e}")


def _log_quality(out_dir: Path, record: dict):
    """품질 검증 로그를 JSONL로 기록"""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / QUALITY_LOG
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[품질로그 실패] {e}")


def _log_metric(out_dir: Path, record: dict):
    """수집 런 메트릭 로그"""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / METRIC_LOG
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _log_fail(out_dir: Path, record: dict):
    """실패/격리 큐 로그"""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        log_path = out_dir / FAIL_QUEUE_LOG
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def validate_stock_df(code: str, df: pd.DataFrame, start_date: str, end_date: str) -> Tuple[bool, dict]:
    """수집 결과 품질 검증"""
    issues: List[str] = []
    warnings: List[str] = []
    
    if df is None or df.empty:
        issues.append("empty_dataframe")
        return False, {"issues": issues, "warnings": warnings, "rows": 0}
    
    missing = [c for c in ESSENTIAL_COLS if c not in df.columns]
    if missing:
        issues.append(f"missing_columns:{','.join(missing)}")
    
    # 전체 스키마 누락 여부 확인
    missing_all = [c for c in V48_COLS if c not in df.columns]
    if missing_all:
        issues.append(f"missing_fields:{len(missing_all)}")
    
    # 메타 필수 값 점검
    for col in ["market_cap", "shares_out"]:
        if col in df.columns:
            if df[col].isna().all():
                issues.append(f"missing_meta_all:{col}")
            elif (df[col] <= 0).any():
                issues.append(f"non_positive_{col}")
        else:
            issues.append(f"missing_meta_col:{col}")

    # 매크로 스케일 점검
    for col, (lo, hi) in MACRO_CHECK_COLS.items():
        if col in df.columns and df[col].notna().any():
            vals = df[col].dropna()
            if ((vals < lo) | (vals > hi)).any():
                issues.append(f"macro_out_of_range:{col}")

    # NaN 비율 점검
    nan_ratios = {}
    for col in ["close", "volume", "market_cap", "shares_out"]:
        if col in df.columns:
            ratio = float(df[col].isna().mean())
            nan_ratios[col] = ratio
            if ratio > 0.5:
                warnings.append(f"high_nan:{col}:{ratio:.2f}")
    
    # 비정상 값 검증: 종가/거래량/시총 음수, 종가 0, 비정상 급등락
    if "close" in df.columns:
        if (df["close"] < 0).any():
            issues.append("negative_close")
        if (df["close"] == 0).any():
            warnings.append("zero_close")
        # 급등락(전일 대비 5배/0.2배 이하) 감지
        close = df["close"].astype(float)
        prev = close.shift(1)
        ratio = (close / prev.replace(0, pd.NA)).replace([pd.NA, pd.NaT], 1)
        if (ratio > 5).any():
            warnings.append("spike_close_gt5x")
        if (ratio < 0.2).any():
            warnings.append("drop_close_lt0.2x")
    
    if "volume" in df.columns:
        if (df["volume"] < 0).any():
            issues.append("negative_volume")
    
    if "market_cap" in df.columns:
        if (df["market_cap"] < 0).any():
            issues.append("negative_market_cap")
    
    # 중복 제거 전 처리
    if "date" in df.columns and "code" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["date", "code"], inplace=True)
        deduped = len(df)
        if deduped < before:
            warnings.append(f"dedup:{before-deduped}")
    
    # 기간 커버리지 대략 점검 (달력일 기준)
    try:
        dt_start = datetime.strptime(start_date, "%Y%m%d")
        dt_end = datetime.strptime(end_date, "%Y%m%d")
        total_days = max((dt_end - dt_start).days + 1, 1)
        coverage_days = df["date"].astype(str).nunique()
        coverage_ratio = coverage_days / total_days
        if coverage_ratio < 0.2:  # 지나친 누락은 실패 처리
            issues.append(f"low_coverage:{coverage_ratio:.2f}")
        elif coverage_ratio < 0.4:
            warnings.append(f"low_coverage_warn:{coverage_ratio:.2f}")
    except Exception:
        coverage_ratio = None
    
    ok = len(issues) == 0
    details = {
        "issues": issues,
        "warnings": warnings,
        "rows": int(len(df)),
        "nan_ratios": nan_ratios,
        "coverage_ratio": coverage_ratio,
    }
    return ok, details


def preflight_checks(codes: List[str], args: argparse.Namespace):
    """실행 전 필수 입력/스키마 점검"""
    problems: List[str] = []
    if len(V48_COLS) != 48:
        problems.append(f"V48_COLS 개수 불일치: {len(V48_COLS)}개 (기대 48)")
    if len(set(V48_COLS)) != len(V48_COLS):
        problems.append("V48_COLS 중복 컬럼 존재")
    if not codes:
        problems.append("codes 입력이 비어있음")
    # 날짜 유효성
    try:
        dt_start = datetime.strptime(args.start_date, "%Y%m%d")
        dt_end = datetime.strptime(args.end_date, "%Y%m%d")
        if dt_start > dt_end:
            problems.append("시작일이 종료일보다 늦음")
    except Exception as e:
        problems.append(f"날짜 형식 오류: {e}")
    # 출력 디렉터리
    out_dir = Path(args.out_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        problems.append(f"출력 디렉터리 생성 실패: {e}")
    if problems:
        print("[사전 점검 실패] 다음 항목을 확인하세요:")
        for p in problems:
            print(f" - {p}")
        sys.exit(1)


def append_streaming(df: pd.DataFrame, out_dir: Path):
    """메모리 누적 없이 즉시 CSV로 append 저장"""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / STREAM_FILE
    header = not out_path.exists()
    # 스키마를 V48_COLS 기준으로 맞춰서 오염 방지
    cols = [c for c in V48_COLS if c in df.columns]
    missing = [c for c in V48_COLS if c not in df.columns]
    # 누락 컬럼은 None으로 채움
    for col in missing:
        df[col] = None
    df = df[[c for c in V48_COLS if c in df.columns]]
    # 컬럼 개수/순서 최종 확인 (디버그 용)
    if len(df.columns) != len(V48_COLS):
        print(f"[경고] 스키마 길이 불일치: {len(df.columns)} vs {len(V48_COLS)}")
    try:
        tmp_path = out_dir / f".tmp_{STREAM_FILE}"
        df.to_csv(tmp_path, mode="w", index=False, encoding="utf-8-sig", header=header)
        with open(out_path, "a", encoding="utf-8-sig", newline="") as fout, open(tmp_path, "r", encoding="utf-8-sig") as fin:
            if not header and not out_path.exists():
                # 안전을 위해 헤더를 한번 더 쓸 상황을 방지
                pass
            for line in fin:
                fout.write(line)
        tmp_path.unlink(missing_ok=True)
    except Exception as e:
        print(f"[스트리밍 저장 실패] {out_path}: {e}")


def collect_one_stock(args: Tuple[str, str, str, bool, Optional[str], str]) -> Optional[Tuple[str, Optional[pd.DataFrame]]]:
    """워커 함수: 단일 종목 11년치 수집 (프록시/재무 옵션 포함)"""
    code, start_date, end_date, use_dart, proxy, dart_mode = args
    
    # 워커 프로세스에서 프록시 설정 재적용 (Windows spawn 대응)
    if proxy:
        try:
            set_proxy(proxy)
        except Exception:
            pass
    
    try:
        cfg = CollectConfig(code=code, start_date=start_date, end_date=end_date, use_dart=use_dart, dart_mode=dart_mode)
        collector_obj = VectorizedCollector(cfg)
        df = collector_obj.collect()
        return code, df
    except Exception as e:
        print(f"  [오류] {code}: {e}")
        return code, None


def collect_parallel(
    codes: List[str],
    start_date: str,
    end_date: str,
    collector: SmartCollector,
    num_workers: int = 16,
    out_dir: Optional[Path] = None,
    use_dart: bool = True,
    stream_save: bool = True,
    proxy: Optional[str] = None,
    dart_mode: str = "annual",
    failed_only: bool = False,
    max_tasks: Optional[int] = None,
    log_interval: int = 30
) -> Tuple[Dict[str, pd.DataFrame], int]:
    """병렬 수집"""
    target_codes = collector.failed if failed_only else codes
    tasks = [(code, start_date, end_date, use_dart, proxy, dart_mode) for code in target_codes 
             if not collector.is_completed(code)]
    if max_tasks is not None:
        tasks = tasks[:max_tasks]
    
    if not tasks:
        print(f"[모든 종목 완료] {len(codes):,}개 종목 모두 완료")
        return {}, 0
    
    print(f"[병렬 수집] {len(tasks):,}개 종목, {num_workers}개 워커")
    
    results: Dict[str, pd.DataFrame] = {}
    start_time = time.time()
    completed = 0
    total_tasks = len(tasks)
    last_metric_flush = time.time()
    
    try:
        with Pool(processes=num_workers) as pool:
            for result in pool.imap(collect_one_stock, tasks, chunksize=1):
                if result is None:
                    continue
                
                code, df = result
                
                if df is None or df.empty:
                    reason = {"issues": ["no_data_returned"], "warnings": []}
                    collector.mark_failed(code, reason=reason)
                    processed = completed + len(collector.failed)
                    status_line = (f"[FAIL] code={code} progress={processed}/{total_tasks} "
                                   f"fail={len(collector.failed)} elapsed={time.time()-start_time:.0f}s")
                    print(status_line)
                    if out_dir:
                        fail_rec = {
                            "code": code,
                            "ok": False,
                            "status": "fail",
                            "details": reason,
                            "timestamp": datetime.now().isoformat(),
                        }
                        _log_quality(out_dir, fail_rec)
                        _log_fail(out_dir, fail_rec)
                    continue
                
                ok, details = validate_stock_df(code, df, start_date, end_date)
                log_record = {
                    "code": code,
                    "ok": ok,
                    "details": details,
                    "timestamp": datetime.now().isoformat(),
                }
                
                if not ok:
                    collector.mark_failed(code, reason=details)
                    processed = completed + len(collector.failed)
                    status_line = (f"[FAIL] code={code} progress={processed}/{total_tasks} "
                                   f"fail={len(collector.failed)} elapsed={time.time()-start_time:.0f}s "
                                   f"issues={';'.join(details.get('issues', []))}")
                    print(status_line)
                    if out_dir:
                        fail_rec = {**log_record, "status": "fail"}
                        _log_quality(out_dir, fail_rec)
                        _log_fail(out_dir, fail_rec)
                    continue
                
                if stream_save and out_dir:
                    append_streaming(df, out_dir)
                else:
                    results[code] = df
                
                collector.mark_completed(code)
                completed += 1
                processed = completed + len(collector.failed)
                status_line = (f"[PASS] code={code} progress={processed}/{total_tasks} "
                               f"ok={completed} fail={len(collector.failed)}")
                print(status_line)
                
                if out_dir:
                    _log_quality(out_dir, {**log_record, "status": "pass"})
                
                if completed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                    print(f"  [진행] {completed:,}/{len(tasks):,} ({completed/len(tasks)*100:.1f}%) - "
                          f"속도: {rate:.2f}종목/초 - 남은시간: {remaining/3600:.1f}시간")
                    collector.save_checkpoint()
                
                now = time.time()
                if out_dir and (now - last_metric_flush) >= log_interval:
                    elapsed = now - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else None
                    status_payload = {
                        "ts": datetime.now().isoformat(),
                        "completed": completed,
                        "total_tasks": total_tasks,
                        "failed": len(collector.failed),
                        "rate_per_sec": rate,
                        "eta_sec": remaining,
                    }
                    _log_metric(out_dir, status_payload)
                    _write_status(out_dir, {**status_payload, "running": True, "processed": processed})
                    last_metric_flush = now
    except KeyboardInterrupt:
        print("\n[중단] 사용자 요청으로 중단. 진행 상황을 저장합니다.")
        collector.save_checkpoint()
    except Exception as e:
        print(f"[오류] 병렬 수집 중단: {e}")
        collector.save_checkpoint()
    
    elapsed = time.time() - start_time
    print(f"\n[완료] {len(results):,}개 종목 수집 완료 - 소요시간: {elapsed/3600:.2f}시간 - 속도: {len(results)/elapsed:.2f}종목/초")
    collector.save_checkpoint()
    processed = completed + len(collector.failed)
    if out_dir:
        final_status = {
            "ts": datetime.now().isoformat(),
            "running": False,
            "completed": completed,
            "failed": len(collector.failed),
            "total_tasks": total_tasks,
            "processed": processed,
            "elapsed_sec": elapsed,
        }
        _write_status(out_dir, final_status)
    return results, completed


def save_results(results: Dict[str, pd.DataFrame], out_dir: Path):
    """결과 저장 (종목별 또는 통합)"""
    if not results:
        return
    
    out_dir.mkdir(parents=True, exist_ok=True)
    all_dfs = []
    for code, df in results.items():
        if not df.empty:
            all_dfs.append(df)
    
    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all = df_all.sort_values(["date", "code"])
        output_file = out_dir / "raw_v48_all.csv"
        df_all.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"[저장] {output_file} ({len(df_all):,}행, {len(results):,}종목)")
        by_date = df_all.groupby("date")
        for date, df_date in by_date:
            date_file = out_dir / f"raw_v48_{date}.csv"
            df_date.to_csv(date_file, index=False, encoding="utf-8-sig")
        print(f"[저장] 날짜별 파일 {len(by_date):,}개 생성")


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
    p = argparse.ArgumentParser(description="raw_v48 Vectorized 수집기")
    p.add_argument("--codes", type=str, required=True, help="종목코드 파일 또는 콤마 구분 리스트")
    p.add_argument("--start-date", type=str, default="20150102", help="시작일 (YYYYMMDD)")
    p.add_argument("--end-date", type=str, default=None, help="종료일 (YYYYMMDD, 기본: 오늘)")
    p.add_argument("--out-dir", type=str, default=str(Path(__file__).resolve().parent / "out"), help="결과 저장 폴더")
    p.add_argument("--workers", type=int, default=None, help="워커 프로세스 수 (기본: CPU*2)")
    p.add_argument("--proxy", type=str, default=None, help="프록시 설정")
    p.add_argument("--proxy-file", type=str, default=None, help="프록시 설정 파일 경로")
    p.add_argument("--disable-dart", action="store_true", help="DART 재무 수집 비활성화")
    p.add_argument("--dart-mode", type=str, default="off", choices=["off", "annual", "full"], help="DART 수집 모드: off(미수집, 기본)/annual(사업보고서만)/full(분기+사업보고서)")
    p.add_argument("--no-stream-save", action="store_true", help="스트리밍 저장 비활성화(메모리 누적 허용)")
    p.add_argument("--failed-only", action="store_true", help="이전 실패 종목만 재수집")
    p.add_argument("--max-tasks", type=int, default=None, help="처리할 최대 종목 수(테스트/부분수집)")
    p.add_argument("--log-interval", type=int, default=30, help="메트릭 로그 주기(초)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.end_date is None:
        args.end_date = datetime.now().strftime("%Y%m%d")
    use_dart = not args.disable_dart and args.dart_mode != "off"
    dart_mode = args.dart_mode if args.dart_mode in DART_MODES else "annual"
    stream_save = not args.no_stream_save
    max_tasks = args.max_tasks
    log_interval = args.log_interval

    proxy_value = None
    if args.proxy:
        proxy_value = args.proxy
        set_proxy(proxy_value)
        print(f"[프록시 설정] {proxy_value}")
    elif args.proxy_file:
        proxy_path = Path(args.proxy_file)
        if proxy_path.exists():
            proxy = proxy_path.read_text(encoding="utf-8").strip()
            proxy_value = proxy
            set_proxy(proxy_value)
            print(f"[프록시 설정] 파일에서 로드: {args.proxy_file}")
    else:
        env_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
        if env_proxy:
            proxy_value = env_proxy
            set_proxy(proxy_value)
            print(f"[프록시 설정] 환경 변수 사용")

    codes = load_codes(args.codes)
    preflight_checks(codes, args)

    num_workers = args.workers
    if num_workers is None:
        num_workers = min(cpu_count() * 2, 50)

    print(f"[설정] 기간: {args.start_date} ~ {args.end_date}")
    print(f"[설정] 워커: {num_workers}개")
    print(f"[설정] 총 {len(codes):,}개 종목")
    print(f"[설정] DART 수집: {'사용' if use_dart else '비활성화'} (mode={dart_mode}, 기본 off)")
    print(f"[설정] 스트리밍 저장: {'사용' if stream_save else '비활성화'} (파일: {STREAM_FILE})")
    if max_tasks:
        print(f"[설정] 최대 처리 종목 수: {max_tasks}")

    collector = SmartCollector(Path(args.out_dir))

    results, completed_count = collect_parallel(
        codes=codes,
        start_date=args.start_date,
        end_date=args.end_date,
        collector=collector,
        num_workers=num_workers,
        out_dir=Path(args.out_dir),
        use_dart=use_dart,
        stream_save=stream_save,
        proxy=proxy_value,
        dart_mode=dart_mode,
        failed_only=args.failed_only,
        max_tasks=max_tasks,
        log_interval=log_interval
    )

    if not stream_save and results:
        print("\n[저장 중...]")
        save_results(results, Path(args.out_dir))

    collector.save_checkpoint()

    total_done = completed_count if stream_save else len(results)
    missing_codes = [c for c in codes if c not in collector.completed and c not in collector.failed]
    summary_payload = {
        "ts": datetime.now().isoformat(),
        "total_codes": len(codes),
        "completed": len(collector.completed),
        "failed": len(collector.failed),
        "missing": len(missing_codes),
        "dart_mode": dart_mode,
        "use_dart": use_dart,
        "stream_save": stream_save,
    }
    print(f"\n[완료] 총 {total_done:,}개 종목 수집 완료 (ok={len(collector.completed):,}, fail={len(collector.failed):,}, missing={len(missing_codes):,})")
    if collector.failed:
        print(f"[실패] {len(collector.failed):,}개 종목 실패: {list(collector.failed)[:10]}")
        if collector.failed_info:
            sample_fail = next(iter(collector.failed_info.items()))
            print(f"[실패 예시] {sample_fail[0]} -> {sample_fail[1].get('issues')}")
    if missing_codes:
        print(f"[누락] {len(missing_codes):,}개 종목 미수집 (예시: {missing_codes[:10]})")
    _write_status(Path(args.out_dir), summary_payload)
    # 스키마/품질 요약 저장
    summary_path = Path(args.out_dir) / "summary_check.json"
    summary_payload["schema_columns"] = V48_COLS
    try:
        import pandas as pd
        sample_path = Path(args.out_dir) / STREAM_FILE
        if sample_path.exists():
            df_head = pd.read_csv(sample_path, nrows=100)
            summary_payload["col_count"] = len(df_head.columns)
            summary_payload["col_unique"] = len(set(df_head.columns))
            summary_payload["meta_nonnull"] = {
                "market_cap": int(df_head["market_cap"].notna().sum()) if "market_cap" in df_head else 0,
                "shares_out": int(df_head["shares_out"].notna().sum()) if "shares_out" in df_head else 0,
            }
            summary_payload["macro_nonnull"] = {
                c: int(df_head[c].notna().sum()) if c in df_head else 0
                for c in ["usdkrw","us10y_yield","kr10y_yield","wti","dxy","cnykrw","gold","vix","earnings_date"]
            }
            summary_payload["price_issues"] = {
                "close_zero": int((df_head["close"]==0).sum()) if "close" in df_head else None,
                "close_neg": int((df_head["close"]<0).sum()) if "close" in df_head else None,
                "volume_zero": int((df_head["volume"]==0).sum()) if "volume" in df_head else None,
                "volume_neg": int((df_head["volume"]<0).sum()) if "volume" in df_head else None,
            }
    except Exception as e:
        print(f"[요약 작성 실패] {e}")
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()




