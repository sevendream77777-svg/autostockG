# ============================================================
# build_features.py (V31/15, patched — correct backup order)
# ============================================================

import sys
import os
from typing import List, Optional
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_paths import get_path, versioned_filename

def get_latest_date_from_parquet(path: str, date_cols: Optional[List[str]] = None):
    if date_cols is None:
        date_cols = ["Date", "날짜", "date"]
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path, columns=date_cols)
    except Exception:
        try:
            df = pd.read_parquet(path)
        except Exception:
            return None
    for col in date_cols:
        if col in df.columns:
            try:
                return pd.to_datetime(df[col]).max().date()
            except Exception:
                continue
    return None

def _compute_features(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("Date").copy()
    c = g["Close"]; h = g["High"]; l = g["Low"]; v = g["Volume"]
    r_mkt = g["KOSPI_수익률"]

    g["SMA_5"] = c.rolling(5).mean()
    g["SMA_20"] = c.rolling(20).mean()
    g["SMA_60"] = c.rolling(60).mean()

    g["VOL_SMA_20"] = v.rolling(20).mean()

    g["MOM_10"] = c.pct_change(10)
    g["ROC_20"] = c.pct_change(20)

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    g["MACD_12_26"] = macd
    g["MACD_SIGNAL_9"] = macd.ewm(span=9, adjust=False).mean()

    ma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = ma20 + 2*std20
    lower = ma20 - 2*std20
    g["BBP_20"] = (c - lower) / (upper - lower + 1e-9)

    prev_close = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h-prev_close).abs(), (l-prev_close).abs()], axis=1).max(axis=1)
    g["ATR_14"] = tr.rolling(14).mean()

    low14 = l.rolling(14).min(); high14 = h.rolling(14).max()
    stoch_k = (c - low14) / (high14 - low14 + 1e-9)
    g["STOCH_K"] = stoch_k; g["STOCH_D"] = stoch_k.rolling(3).mean()

    tp = (h + l + c) / 3.0
    ma_tp = tp.rolling(20).mean()
    md = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    g["CCI_20"] = (tp - ma_tp) / (0.015 * (md + 1e-9))

    r_stock = c.pct_change()
    g["ALPHA_SMA_20"] = (r_stock - r_mkt).rolling(20).mean()

    return g

def normalize_kospi(df_kospi: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in df_kospi.columns:
        for cand in ["날짜", "date"]:
            if cand in df_kospi.columns:
                df_kospi = df_kospi.rename(columns={cand: "Date"})
                break
    if "Date" not in df_kospi.columns:
        raise ValueError("[KOSPI] 'Date' 컬럼을 찾을 수 없습니다.")

    df_kospi["Date"] = pd.to_datetime(df_kospi["Date"], errors="coerce")
    df_kospi = df_kospi.dropna(subset=["Date"])

    if "KOSPI_종가" not in df_kospi.columns:
        for c in ["Close", "close", "종가", "KOSPI_Close", "adj_close"]:
            if c in df_kospi.columns:
                df_kospi = df_kospi.rename(columns={c: "KOSPI_종가"})
                break
        else:
            raise ValueError("[KOSPI] 'KOSPI_종가' 또는 대체 가능한 종가 컬럼이 없습니다.")

    df_kospi["KOSPI_종가"] = pd.to_numeric(df_kospi["KOSPI_종가"], errors="coerce")
    df_kospi = df_kospi.dropna(subset=["KOSPI_종가"])

    if "KOSPI_수익률" not in df_kospi.columns:
        df_kospi = df_kospi.sort_values("Date")
        df_kospi["KOSPI_수익률"] = df_kospi["KOSPI_종가"].pct_change()

    df_kospi = df_kospi.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
    return df_kospi[["Date", "KOSPI_종가", "KOSPI_수익률"]]

def build_features():
    raw_file = get_path("RAW", "stocks", "all_stocks_cumulative.parquet")
    kospi_file = get_path("RAW", "kospi_data", "kospi_data.parquet")
    feature_file = get_path("FEATURE", "features_V31.parquet")

    print("==============================================")
    print("[FEATURE V31/15] 피처 생성 여부 판단 중...")
    print(f"  RAW   경로: {raw_file}")
    print(f"  KOSPI 경로: {kospi_file}")

    # ----------------------------------------------
    # FM RULE: RAW/KOSPI 날짜 기반 데이터 기준일 계산
    # ----------------------------------------------
    raw_latest = get_latest_date_from_parquet(raw_file)
    if raw_latest is None:
        print("❌ RAW 파일을 읽을 수 없습니다."); return
    print(f"  RAW 최신 날짜: {raw_latest}")

    kospi_latest = get_latest_date_from_parquet(kospi_file)
    if kospi_latest is None:
        print("❌ KOSPI 파일을 읽을 수 없습니다."); return
    print(f"  KOSPI 최신 날짜: {kospi_latest}")

    # 데이터 기준일 = RAW/KOSPI 중 더 과거(min)
    data_date = min(raw_latest, kospi_latest)
    print(f"  ➜ 데이터 기준 날짜(DATA_DATE): {data_date}")

    # ----------------------------------------------
    # FEATURE 날짜 확인 및 업데이트 여부 판단
    # ----------------------------------------------
    feat_latest = get_latest_date_from_parquet(feature_file)
    run_generate = False

    if feat_latest is None:
        print("  FEATURE 파일이 없습니다. → 생성 필요")
        run_generate = True

    else:
        print(f"  FEATURE 최신 날짜: {feat_latest}")

        if feat_latest < data_date:
            print("  ➜ FEATURE 날짜가 DATA_DATE보다 과거 → 재생성")
            run_generate = True

        elif feat_latest == data_date:
            print("  ➜ FEATURE 최신 → 생성 스킵")
            return

        else:   # feat_latest > data_date
            print("  ⚠ FEATURE 날짜가 RAW/KOSPI보다 미래입니다. 데이터 오류 → 중단")
            return


    if not run_generate:
        print("⚠ run_generate=False 상태입니다. 작업을 종료합니다."); return

    print("[FEATURE V31/15] 피처 생성 시작")

    if not os.path.exists(kospi_file):
        print(f"❌ [CRITICAL] KOSPI 데이터가 없습니다: {kospi_file}"); return

    try:
        df_raw = pd.read_parquet(raw_file)
        df_kospi = pd.read_parquet(kospi_file)
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}"); return

    df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
    df_raw = df_raw.dropna(subset=["Date"]).sort_values(["Date", "Code"]).reset_index(drop=True)

    required_raw = {"Date", "Open", "High", "Low", "Close", "Volume", "Code"}
    if missing := (required_raw - set(df_raw.columns)):
        print(f"❌ RAW 필수 컬럼 누락: {missing}"); return

    try:
        df_kospi = normalize_kospi(df_kospi)
    except Exception as e:
        print(f"❌ KOSPI 정규화 실패: {e}"); return

    try:
        df = pd.merge(df_raw, df_kospi, on="Date", how="inner")
    except KeyError as e:
        print(f"❌ 병합 실패 (컬럼명 확인 필요): {e}"); return

    before_rows = len(df)
    print("  ... 기술적 지표 계산 중")
    df_feat = df.groupby("Code", group_keys=False).apply(_compute_features)

    essential_cols = ["SMA_5","SMA_20","SMA_60","VOL_SMA_20","MOM_10",
                      "ROC_20","MACD_12_26","MACD_SIGNAL_9","BBP_20",
                      "ATR_14","STOCH_K","STOCH_D","CCI_20",
                      "ALPHA_SMA_20","KOSPI_수익률"]
    df_feat = df_feat.dropna(subset=essential_cols)
    after_rows = len(df_feat)
    print(f"  - 결측 제거: {before_rows:,} → {after_rows:,} 행")
    print("  - 최종 피처 개수: 15개")

    os.makedirs(os.path.dirname(feature_file), exist_ok=True)

    # 올바른 백업 순서: 기존 파일이 있으면 먼저 백업(이동)한 뒤 새 파일 저장
    if os.path.exists(feature_file):
        try:
            backup_path = versioned_filename(feature_file)  # 기존 파일의 날짜로 태그
            os.rename(feature_file, backup_path)            # 기존 파일 이동(백업)
            print(f"[FEATURE V31/15] 기존 파일 백업: {backup_path}")
        except Exception as e:
            print(f"⚠ 기존 파일 백업 실패: {e} (새 파일만 저장됩니다)")

    try:
        df_feat.to_parquet(feature_file, index=False)
        print(f"[FEATURE V31/15] 저장 완료: {feature_file}")
    except Exception as e:
        print(f"❌ 저장 실패: {e}"); return

    print("[FEATURE V31/15] 작업 완료")

def main():
    build_features()

if __name__ == "__main__":
    main()
