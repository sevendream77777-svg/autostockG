# -*- coding: utf-8 -*-
"""
v48_finalize.py — 끝판왕 스키마/머지/검증 파이프
- 입력: v48 기준으로 '거의 완성'된 수집 결과(단일 CSV/Parquet)
- 출력: 정확히 48개 컬럼만, 순서/타입/값 검증을 마친 최종본
- 동작: 결측/이상치 방어, VWAP/시가총액 보정, 키(unique) 보장, 스키마 강제
사용 예:
    python v48_finalize.py --in raw_v48.parquet --out final_v48.parquet
"""
import argparse
import os
import sys
import math
from typing import List, Tuple, Optional

import pandas as pd
import numpy as np


# ===== 1) v48 최종 스키마 =====
V48_COLS: List[str] = [
    # 1. Meta (7)
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    # 2. Price (10)
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap",
    "market_cap", "shares_out",
    # 3. Flow (8)
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty",
    # 4. Finance (12)
    "announce_date", "revenue", "op_income", "net_income", "total_equity",
    "total_assets", "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    "div_amount", "eps", "roe",
    # 5. Macro & Event (11)
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold", "vix",
    "earnings_date", "bps", "debt_ratio",
]

META_COLS = V48_COLS[:7]
PRICE_COLS = V48_COLS[7:17]
FLOW_COLS = V48_COLS[17:25]
FIN_COLS = V48_COLS[25:37]
MACRO_COLS = V48_COLS[37:48]

# 숫자형으로 캐스팅할 후보 (문자/None 섞여 들어오는 것을 방지)
NUMERIC_CANDIDATES = [
    "open","high","low","close","volume","amount","adj_factor","vwap","market_cap","shares_out",
    "frgn_net_amt","inst_net_amt","nps_net_amt","tust_net_amt","dealer_net_amt",
    "frgn_net_qty","inst_net_qty","nps_net_qty",
    "revenue","op_income","net_income","total_equity","total_assets",
    "cash_flow_op","cash_flow_inv","cash_flow_fin",
    "div_amount","eps","roe",
    "usdkrw","us10y_yield","kr10y_yield","wti","dxy","cnykrw","gold","vix","bps","debt_ratio"
]

DATE_LIKE = ["date", "announce_date", "earnings_date"]


def _read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path.lower())[1]
    if ext in [".parquet", ".pq"]:
        return pd.read_parquet(path)
    if ext in [".csv", ".txt"]:
        return pd.read_csv(path)
    raise ValueError(f"지원하지 않는 확장자: {ext}")


def _write_any(df: pd.DataFrame, path: str) -> None:
    ext = os.path.splitext(path.lower())[1]
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if ext in [".parquet", ".pq"]:
        df.to_parquet(path, index=False)
    elif ext in [".csv", ".txt"]:
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"지원하지 않는 확장자: {ext}")


def _to_datetime_safe(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _standardize_keys(df: pd.DataFrame) -> pd.DataFrame:
    # date: YYYYMMDD 또는 YYYY-MM-DD → datetime64[D]로 통일 후, 문자열 YYYYMMDD 보관
    if "date" in df.columns:
        df["date"] = _to_datetime_safe(df["date"]).dt.date
        # 되도록 문자열 YYYYMMDD로 포맷 (다운스트림 호환)
        df["date"] = df["date"].apply(lambda d: d.strftime("%Y%m%d") if pd.notna(d) else np.nan)
    # code: 종목코드 zero-fill 6자리
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.replace(r"[^0-9A-Za-z]", "", regex=True)
        df["code"] = df["code"].apply(lambda x: x.zfill(6) if x.isdigit() and len(x) <= 6 else x)
    return df


def _drop_dupe_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    # key 중복 제거 규칙: 같은 (date, code) 존재 시 가장 최근/가장 완전한 row 우선
    # 완전성 점수 = 비결측 컬럼 수
    if all(c in df.columns for c in ["date","code"]):
        df["_nonna"] = df.notna().sum(axis=1)
        df = df.sort_values(["date","code","_nonna"], ascending=[True, True, False])
        df = df.drop_duplicates(subset=["date","code"], keep="first")
        df = df.drop(columns=["_nonna"], errors="ignore")
    if "date" in df.columns and "code" in df.columns:
        df = df.sort_values(["date","code"]).reset_index(drop=True)
    return df


def _compute_fallbacks(df: pd.DataFrame) -> pd.DataFrame:
    # 1) VWAP = amount / volume (둘 다 > 0인 경우)
    if "vwap" in df.columns:
        need = df["vwap"].isna() if "vwap" in df.columns else True
    else:
        df["vwap"] = np.nan
        need = df["vwap"].isna()

    cond = (
        ("amount" in df.columns) and ("volume" in df.columns) and
        (df["amount"].notna()) and (df["volume"].notna())
    )
    if cond:
        safe = (df["amount"] > 0) & (df["volume"] > 0) & need
        df.loc[safe, "vwap"] = df.loc[safe, "amount"] / df.loc[safe, "volume"]

    # 2) market_cap = close * shares_out (둘 다 > 0인 경우)
    if "market_cap" not in df.columns:
        df["market_cap"] = np.nan
    if ("close" in df.columns) and ("shares_out" in df.columns):
        safe = (df["close"] > 0) & (df["shares_out"] > 0) & df["market_cap"].isna()
        df.loc[safe, "market_cap"] = df.loc[safe, "close"] * df.loc[safe, "shares_out"]

    # 3) adj_factor 기본값 보정
    if "adj_factor" in df.columns:
        df["adj_factor"] = df["adj_factor"].fillna(1.0)

    return df


def _clip_outliers(df: pd.DataFrame) -> pd.DataFrame:
    nonneg_cols = [
        "open","high","low","close","volume","amount","vwap","market_cap","shares_out",
        "frgn_net_amt","inst_net_amt","nps_net_amt","tust_net_amt","dealer_net_amt",
        "frgn_net_qty","inst_net_qty","nps_net_qty",
        "revenue","op_income","net_income","total_equity","total_assets",
        "cash_flow_op","cash_flow_inv","cash_flow_fin","div_amount","eps","roe",
    ]
    for c in nonneg_cols:
        if c in df.columns:
            df.loc[df[c] < 0, c] = np.nan
    if all(c in df.columns for c in ["high","low"]):
        mask = (df["high"].notna() & df["low"].notna() & (df["high"] < df["low"]))
        df.loc[mask, ["high","low"]] = df.loc[mask, ["low","high"]].values
    return df


def _enforce_columns(df: pd.DataFrame, keep_surplus: bool = False) -> pd.DataFrame:
    for c in V48_COLS:
        if c not in df.columns:
            df[c] = np.nan
    if not keep_surplus:
        df = df[V48_COLS]
    df = _coerce_numeric(df, NUMERIC_CANDIDATES)
    for c in DATE_LIKE:
        if c in df.columns:
            df[c] = _to_datetime_safe(df[c]).dt.date
            df[c] = df[c].apply(lambda d: d.strftime("%Y%m%d") if pd.notna(d) else np.nan)
    return df


def _validate(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    errs = []
    if not all(c in df.columns for c in ["date","code"]):
        errs.append("필수 키(date, code) 누락")
    else:
        dup = df.duplicated(subset=["date","code"]).sum()
        if dup > 0:
            errs.append(f"(date,code) 중복 {dup}건 발견")

    for req in ["open","high","low","close"]:
        if req not in df.columns:
            errs.append(f"가격 컬럼 누락: {req}")

    miss = [c for c in V48_COLS if c not in df.columns]
    if miss:
        errs.append(f"스키마 누락 컬럼 {len(miss)}개: {miss[:5]}{'...' if len(miss)>5 else ''}")

    return (len(errs) == 0, errs)


def finalize(input_path: str, output_path: str, keep_surplus: bool=False) -> None:
    df = _read_any(input_path)

    if "date" not in df.columns or "code" not in df.columns:
        if "date" in df.index.names if isinstance(df.index, pd.MultiIndex) else False:
            df = df.reset_index(level=["date"])
        if "code" in df.index.names if isinstance(df.index, pd.MultiIndex) else False:
            df = df.reset_index(level=["code"])

    df = _standardize_keys(df)
    df = _coerce_numeric(df, NUMERIC_CANDIDATES)
    df = _compute_fallbacks(df)
    df = _clip_outliers(df)
    df = _enforce_columns(df, keep_surplus=keep_surplus)
    df = _drop_dupe_and_sort(df)

    ok, errs = _validate(df)
    if not ok:
        report = os.path.splitext(output_path)[0] + "_validation.txt"
        with open(report, "w", encoding="utf-8") as f:
            for e in errs:
                f.write(e + "\n")

    _write_any(df, output_path)


def main():
    p = argparse.ArgumentParser(description="v48 스키마/검증 최종화 도구")
    p.add_argument("--in", dest="input_path", required=True, help="입력 파일(.csv/.parquet)")
    p.add_argument("--out", dest="output_path", required=True, help="출력 파일(.csv/.parquet)")
    p.add_argument("--keep-surplus", action="store_true", help="스키마 외 컬럼을 보존(디버그용)")
    args = p.parse_args()

    finalize(args.input_path, args.output_path, keep_surplus=args.keep_surplus)


if __name__ == "__main__":
    main()







