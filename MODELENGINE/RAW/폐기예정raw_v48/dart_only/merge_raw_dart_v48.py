
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_raw_dart_v48.py
---------------------
RAW(가격/수급/메타/매크로)와 DART(분기/연간 재무)를 안전하게 병합하는 스크립트.

핵심
- ✓ RAW는 "일별" 데이터, DART는 "분기/연간" 데이터 → 일별로 확장 후 forward-fill로 정합
- ✓ (date, code) 키 불변/1:1 보장: RAW의 달력(date,code)을 기준으로만 LEFT JOIN
- ✓ 재시작 가능: 코드 단위 청크 저장, 중간 산출물 유지
- ✓ 진행상황 파일(status.json)로 현재 진행률 확인(동일 폴더)
- ✓ 스키마 강제(V48_COLS), 타입/날짜 정규화

입력
- --raw      : RAW 파일(.parquet/.csv)
- --dart-dir : run_dart_only_v48.py가 생성한 finance/*.parquet 디렉토리

출력
- --out      : 최종 병합 파일(.parquet/.csv)
- 중간 산출물: out_dir/tmp_by_code/{CODE}.parquet

사용 예
PS> python merge_raw_dart_v48.py --raw raw_v48.parquet --dart-dir .\out_dart\finance --out final_v48.parquet
"""

from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import pandas as pd
import numpy as np

V48_COLS = [
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap", "market_cap", "shares_out",
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty",
    "announce_date", "revenue", "op_income", "net_income", "total_equity",
    "total_assets", "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "roe",
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix",
    "earnings_date", "bps", "debt_ratio",
]

FINANCE_COLS = [
    "announce_date","revenue","op_income","net_income","total_equity","total_assets",
    "cash_flow_op","cash_flow_inv","cash_flow_fin","div_amount","eps","roe","bps","debt_ratio"
]

STATUS_FILE = "status.json"

def _read_any(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in [".parquet",".pq"]:
        return pd.read_parquet(path)
    if ext in [".csv",".txt"]:
        return pd.read_csv(path)
    raise ValueError(f"지원하지 않는 확장자: {ext}")

def _write_any(df: pd.DataFrame, path: str):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    ext = out.suffix.lower()
    if ext in [".parquet",".pq"]:
        df.to_parquet(out, index=False)
    elif ext in [".csv",".txt"]:
        df.to_csv(out, index=False, encoding="utf-8-sig")
    else:
        raise ValueError(f"지원하지 않는 확장자: {ext}")

def _flush_status(path: Path, running: bool, processed: int, total: int, extra: dict=None):
    payload = {
        "ts": datetime.now().isoformat(),
        "running": running,
        "processed": int(processed),
        "total": int(total),
    }
    if extra:
        payload.update(extra)
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _normalize_date_col(s: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(s, errors="coerce").dt.strftime("%Y%m%d")
    except Exception:
        return s

def _expand_finance_daily(fin_df: pd.DataFrame, raw_dates: pd.Series) -> pd.DataFrame:
    f = fin_df.copy()
    if "date" in f.columns:
        f["date"] = _normalize_date_col(f["date"])
    if "announce_date" in f.columns:
        f["announce_date"] = _normalize_date_col(f["announce_date"])

    if "date" in f.columns and f["date"].notna().any():
        key = "date"
    elif "announce_date" in f.columns and f["announce_date"].notna().any():
        key = "announce_date"
        f["date"] = f["announce_date"]
    else:
        return pd.DataFrame(columns=["date"] + FINANCE_COLS)

    f = f.sort_values(key).drop_duplicates(subset=[key], keep="last")
    f = f.set_index(key)
    # 중복 date를 한 번 더 제거(announce_date→date로 복사된 후 중복 방지)
    f = f[~f.index.duplicated(keep="last")]

    keep = [c for c in FINANCE_COLS if c in f.columns]
    f = f[keep].copy()

    rdates = pd.Series(pd.unique(raw_dates.astype(str))).dropna()
    rdates = pd.to_datetime(rdates, errors="coerce").dropna().dt.strftime("%Y%m%d")
    rdates = pd.Index(rdates, name="date").sort_values()

    f = f.reindex(rdates, method=None)
    f = f.ffill()
    f = f.reset_index()  # date
    return f

def _enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for c in V48_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[V48_COLS].copy()
    for c in ["date","announce_date","earnings_date"]:
        if c in df.columns:
            df[c] = _normalize_date_col(df[c])
    numeric_cols = [
        "open","high","low","close","volume","amount","adj_factor","vwap","market_cap","shares_out",
        "frgn_net_amt","inst_net_amt","nps_net_amt","tust_net_amt","dealer_net_amt",
        "frgn_net_qty","inst_net_qty","nps_net_qty",
        "revenue","op_income","net_income","total_equity","total_assets",
        "cash_flow_op","cash_flow_inv","cash_flow_fin","div_amount","eps","roe",
        "usdkrw","us10y_yield","kr10y_yield","wti","dxy","cnykrw","gold","vix","bps","debt_ratio"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def merge_main(raw_path: str, dart_dir: str, out_path: str, tmp_dir: Optional[str]=None, codes_file: Optional[str]=None, force_remerge: bool=False):
    raw = _read_any(raw_path)
    if "date" not in raw.columns or "code" not in raw.columns:
        raise ValueError("RAW에 date, code가 필요합니다.")
    raw["date"] = _normalize_date_col(raw["date"])
    raw["code"] = raw["code"].astype(str).str.zfill(6)

    if codes_file and Path(codes_file).exists():
        codes = [line.strip() for line in Path(codes_file).read_text(encoding="utf-8").splitlines() if line.strip()]
        raw = raw[raw["code"].isin(codes)].copy()

    tmp_root = Path(tmp_dir) if tmp_dir else Path(out_path).with_suffix("") / "tmp_by_code"
    tmp_root.mkdir(parents=True, exist_ok=True)

    finance_root = Path(dart_dir)
    if not finance_root.exists():
        raise FileNotFoundError(f"DART 디렉토리를 찾을 수 없습니다: {finance_root}")

    status_file = Path(out_path).with_suffix("") .parent / STATUS_FILE
    codes_uni = raw["code"].dropna().astype(str).str.zfill(6).unique().tolist()
    total = len(codes_uni)
    processed = 0
    _flush_status(status_file, running=True, processed=0, total=total, extra={})

    for code in codes_uni:
        processed += 1
        tmp_file = tmp_root / f"{code}.parquet"
        raw_mtime = Path(raw_path).stat().st_mtime
        if tmp_file.exists() and tmp_file.stat().st_size > 0:
            tmp_mtime = tmp_file.stat().st_mtime
            if not force_remerge and tmp_mtime >= raw_mtime:
                _flush_status(status_file, running=True, processed=processed, total=total,
                              extra={"last": code, "note": "resume_skip"})
                continue

        raw_code = raw[raw["code"] == code].copy()
        # RAW에 이미 존재할 수 있는 재무 컬럼은 병합 전에 제거해 충돌 방지
        raw_code = raw_code.drop(columns=list(set(FINANCE_COLS + ["announce_date", "roe", "bps", "debt_ratio"])), errors="ignore")
        raw_dates = raw_code["date"]

        fin_path = finance_root / f"{code}.parquet"
        if fin_path.exists() and fin_path.stat().st_size > 0:
            fin = pd.read_parquet(fin_path)
        else:
            fin = pd.DataFrame(columns=["date"] + FINANCE_COLS)

        fin_daily = _expand_finance_daily(fin, raw_dates)
        merged = raw_code.merge(fin_daily, on="date", how="left", suffixes=("",""))
        merged["code"] = code  # LEFT JOIN 이후 코드 보장

        merged = _enforce_schema(merged)
        merged = merged.sort_values(["date","code"]).drop_duplicates(subset=["date","code"], keep="last")

        merged.to_parquet(tmp_file, index=False)

        if processed % 10 == 0:
            _flush_status(status_file, running=True, processed=processed, total=total,
                          extra={"last": code})

    parts = []
    for p in sorted(tmp_root.glob("*.parquet")):
        try:
            parts.append(pd.read_parquet(p))
        except Exception:
            pass
    if parts:
        final = pd.concat(parts, ignore_index=True)
    else:
        final = raw.copy()
    final = _enforce_schema(final)
    final = final.sort_values(["date","code"]).drop_duplicates(subset=["date","code"], keep="last")

    _write_any(final, out_path)
    _flush_status(status_file, running=False, processed=total, total=total, extra={"out": out_path})
    print(f"[완료] 병합 결과 저장: {out_path} (행 {len(final):,})")


def main():
    ap = argparse.ArgumentParser(description="RAW + DART 병합기(v48)")
    ap.add_argument("--raw", required=True, help="RAW 파일 경로(.parquet/.csv)")
    ap.add_argument("--dart-dir", required=True, help="DART finance 디렉토리(run_dart_only_v48.py 결과)")
    ap.add_argument("--out", required=True, help="최종 출력 파일(.parquet/.csv)")
    ap.add_argument("--tmp-dir", default=None, help="임시 폴더(생략 시 자동)")
    ap.add_argument("--codes-file", default=None, help="특정 코드만 병합할 경우 코드 리스트 파일")
    ap.add_argument("--force-remerge", action="store_true", help="RAW 갱신/강제 시 기존 tmp 무시하고 재병합")
    args = ap.parse_args()
    merge_main(raw_path=args.raw, dart_dir=args.dart_dir, out_path=args.out, tmp_dir=args.tmp_dir, codes_file=args.codes_file, force_remerge=args.force_remerge)

if __name__ == "__main__":
    main()
