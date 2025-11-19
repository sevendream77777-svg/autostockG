# ============================================================
# build_features.py (V31, 15 features)
# RAW(all_stocks_cumulative.parquet) + KOSPI(kospi_index_10y.parquet)
# → FEATURE(features_V31.parquet)
# - 15개 기술 지표 생성
# - 결측/초기구간 정리
# - 저장 + 날짜 버전 백업
# ============================================================

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import os
from datetime import datetime

import numpy as np
import pandas as pd

from config_paths import get_path, versioned_filename


def _compute_features(group: pd.DataFrame) -> pd.DataFrame:
    """개별 종목(Code) 단위로 15개 피처 계산."""
    g = group.sort_values("Date").copy()

    c = g["Close"]
    h = g["High"]
    l = g["Low"]
    v = g["Volume"]
    m = g["KOSPI_종가"]
    r_mkt = g["KOSPI_수익률"]

    # 1~3) SMA_5, SMA_20, SMA_60
    g["SMA_5"] = c.rolling(5).mean()
    g["SMA_20"] = c.rolling(20).mean()
    g["SMA_60"] = c.rolling(60).mean()

    # 4) VOL_SMA_20
    g["VOL_SMA_20"] = v.rolling(20).mean()

    # 5) MOM_10 (10일 모멘텀)
    g["MOM_10"] = c.pct_change(10)

    # 6) ROC_20 (20일 수익률)
    g["ROC_20"] = c.pct_change(20)

    # 7~8) MACD(12,26) + SIGNAL(9)
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    g["MACD_12_26"] = macd
    g["MACD_SIGNAL_9"] = signal

    # 9) BBP_20 (볼린저 밴드 포지션)
    ma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    g["BBP_20"] = (c - lower) / (upper - lower + 1e-9)

    # 10) ATR_14 (평균 실제 변동폭)
    prev_close = c.shift(1)
    tr1 = (h - l).abs()
    tr2 = (h - prev_close).abs()
    tr3 = (l - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    g["ATR_14"] = tr.rolling(14).mean()

    # 11~12) STOCH_K, STOCH_D
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    stoch_k = (c - low14) / (high14 - low14 + 1e-9)
    stoch_d = stoch_k.rolling(3).mean()
    g["STOCH_K"] = stoch_k
    g["STOCH_D"] = stoch_d

    # 13) CCI_20
    tp = (h + l + c) / 3.0
    ma_tp = tp.rolling(20).mean()
    md = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    g["CCI_20"] = (tp - ma_tp) / (0.015 * (md + 1e-9))

    # 14) ALPHA_SMA_20 (종목 - 코스피 수익률)
    r_stock = c.pct_change()
    diff = r_stock - r_mkt
    g["ALPHA_SMA_20"] = diff.rolling(20).mean()

    # 15) KOSPI_수익률은 merge 단계에서 이미 컬럼 존재

    return g


def build_features():
    raw_file = get_path("RAW", "all_stocks_cumulative.parquet")
    kospi_file = get_path("RAW", "kospi_index_10y.parquet")
    feature_file = get_path("FEATURE", "features_V31.parquet")

    print("[FEATURE V31/15] 피처 생성 시작")
    print(f"  RAW 파일:   {raw_file}")
    print(f"  KOSPI 파일: {kospi_file}")
    print(f"  출력 파일:  {feature_file}")

    if not os.path.exists(raw_file):
        raise FileNotFoundError(f"RAW 파일을 찾을 수 없습니다: {raw_file}")
    if not os.path.exists(kospi_file):
        raise FileNotFoundError(f"KOSPI 파일을 찾을 수 없습니다: {kospi_file}")

    # RAW 로드
    df_raw = pd.read_parquet(raw_file)
    if "Date" not in df_raw.columns or "Code" not in df_raw.columns:
        raise KeyError("RAW에는 최소한 Date, Code 컬럼이 있어야 합니다.")

    df_raw["Date"] = pd.to_datetime(df_raw["Date"])
    df_raw = df_raw.sort_values(["Date", "Code"]).reset_index(drop=True)

    raw_min = df_raw["Date"].min().date()
    raw_max = df_raw["Date"].max().date()
    raw_rows = len(df_raw)
    raw_codes = df_raw["Code"].nunique()
    print(f"  [RAW] 기간: {raw_min} ~ {raw_max} / 행: {raw_rows:,} / 종목: {raw_codes:,}")

    # KOSPI 로드
    df_kospi = pd.read_parquet(kospi_file)
    if "Date" not in df_kospi.columns or "KOSPI_종가" not in df_kospi.columns:
        raise KeyError("KOSPI 파일에는 Date, KOSPI_종가 컬럼이 필요합니다.")

    df_kospi["Date"] = pd.to_datetime(df_kospi["Date"])
    df_kospi = df_kospi.sort_values("Date").reset_index(drop=True)

    if "KOSPI_수익률" not in df_kospi.columns:
        df_kospi["KOSPI_수익률"] = df_kospi["KOSPI_종가"].pct_change()

    kospi_min = df_kospi["Date"].min().date()
    kospi_max = df_kospi["Date"].max().date()
    print(f"  [KOSPI] 기간: {kospi_min} ~ {kospi_max} / 행: {len(df_kospi):,}")

    # 날짜 기준 merge (inner join)
    df = pd.merge(
        df_raw,
        df_kospi[["Date", "KOSPI_종가", "KOSPI_수익률"]],
        on="Date",
        how="inner",
    )

    merged_rows = len(df)
    merged_codes = df["Code"].nunique()
    merged_min = df["Date"].min().date()
    merged_max = df["Date"].max().date()

    print(f"  [MERGE] 기간: {merged_min} ~ {merged_max} / 행: {merged_rows:,} / 종목: {merged_codes:,}")

    # 종목별 15개 피처 계산
    df_feat = df.groupby("Code", group_keys=False).apply(_compute_features)

    # 결측 제거 (필수 컬럼 기준)
    essential_cols = [
        "SMA_5", "SMA_20", "SMA_60", "VOL_SMA_20", "MOM_10",
        "ROC_20", "MACD_12_26", "MACD_SIGNAL_9", "BBP_20",
        "ATR_14", "STOCH_K", "STOCH_D", "CCI_20",
        "ALPHA_SMA_20", "KOSPI_수익률",
    ]
    before = len(df_feat)
    df_feat = df_feat.dropna(subset=essential_cols)
    after = len(df_feat)

    print(f"  [CLEAN] 결측 제거: {before:,} → {after:,} 행 남김")

    # 저장
    os.makedirs(os.path.dirname(feature_file), exist_ok=True)
    df_feat.to_parquet(feature_file, index=False)

    # 날짜 버전 백업
    backup_path = versioned_filename(feature_file)
    df_feat.to_parquet(backup_path, index=False)

    feat_min = df_feat["Date"].min().date()
    feat_max = df_feat["Date"].max().date()
    feat_codes = df_feat["Code"].nunique()

    print(f"[FEATURE V31/15] 저장 완료: {feature_file}")
    print(f"[FEATURE V31/15] 버전 저장: {backup_path}")
    print(f"[FEATURE V31/15] 최종 기간: {feat_min} ~ {feat_max} / 행: {after:,} / 종목: {feat_codes:,}")
    print("[FEATURE V31/15] 작업 완료")


def main():
    build_features()


if __name__ == "__main__":
    main()