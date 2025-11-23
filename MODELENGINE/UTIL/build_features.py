# ============================================================
# build_features.py (V32 - Full Date Range / NaN Allowed)
#   - 앞부분 데이터(SMA_60 등 계산 불가 구간)를 삭제하지 않음
#   - 1월 2일부터의 모든 날짜를 DB에 포함시킴
#  - ALPHA_SMA_20
#  - ATR_14
#  - BBP_20
#  - CCI_20
#  - Change
#  - Close
#  - Code
#  - Date
#  - High
#  - KOSPI_수익률
#  - KOSPI_종가
#  - Low
#  - MACD_12_26
#  - MACD_SIGNAL_9
#  - MOM_10
#  - Market
#  - Name
#  - Open
#  - ROC_20
#  - RSI_14
#  - SMA_120
#  - SMA_20
#  - SMA_40
#  - SMA_5
#  - SMA_60
#  - SMA_90
#  - STOCH_D
#  - STOCH_K
#  - VOL_SMA_20
#  - Volume
# ============================================================

import sys
import os
from typing import List, Optional
import numpy as np
import pandas as pd

# 프로젝트 경로 설정 (기존 유지)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from UTIL.config_paths import get_path, versioned_filename
    from UTIL.version_utils import find_latest_file, save_dataframe_with_date # [수정] 유틸 추가
except ImportError:
    from config_paths import get_path, versioned_filename
    from version_utils import find_latest_file, save_dataframe_with_date # [수정] 유틸 추가

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

    # 1. 이동평균선 (SMA) - [수정] 다양한 기간 추가 (풀 옵션)
    g["SMA_5"] = c.rolling(5).mean()
    g["SMA_20"] = c.rolling(20).mean()
    g["SMA_40"] = c.rolling(40).mean()   # 추가됨
    g["SMA_60"] = c.rolling(60).mean()   # 앞쪽 59일은 NaN이 됨 (삭제 안 함)
    g["SMA_90"] = c.rolling(90).mean()   # 추가됨
    g["SMA_120"] = c.rolling(120).mean() # 추가됨 (장기/경기선)

    g["VOL_SMA_20"] = v.rolling(20).mean()

    # 2. RSI_14 추가 (누락된 핵심 지표 복구)
    delta = c.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    g["RSI_14"] = 100 - (100 / (1 + rs))

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
    # 경로 설정 (기존 유지 -> 최신 파일 탐색 변경)
    # raw_file = get_path("RAW", "stocks", "all_stocks_cumulative.parquet")
    # kospi_file = get_path("RAW", "kospi_data", "kospi_data.parquet")
    # feature_file = get_path("FEATURE", "features_V31.parquet")
    
    # [수정] 최신 파일 탐색 로직 적용
    raw_dir = get_path("RAW", "stocks")
    raw_file = find_latest_file(raw_dir, "all_stocks_cumulative")
    
    kospi_dir = get_path("RAW", "kospi_data")
    kospi_file = find_latest_file(kospi_dir, "kospi_data")
    
    # 저장할 폴더
    feat_dir = get_path("FEATURE")

    print("==============================================")
    print("[FEATURE V32] 피처 생성 (NaN 유지 모드)")
    
    # 날짜 체크 로직은 유지하되, 사용자 요청으로 강제 실행될 수 있음
    
    if not raw_file or not os.path.exists(raw_file):
        print(f"❌ [CRITICAL] RAW 데이터 파일을 찾을 수 없습니다: {raw_dir}")
        return
    print(f"  📥 최신 RAW 로드: {os.path.basename(raw_file)}")

    if not kospi_file or not os.path.exists(kospi_file):
        print(f"❌ [CRITICAL] KOSPI 데이터가 없습니다: {kospi_dir}")
        return
    print(f"  📥 최신 KOSPI 로드: {os.path.basename(kospi_file)}")

    try:
        df_raw = pd.read_parquet(raw_file)
        df_kospi = pd.read_parquet(kospi_file)
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}"); return

    df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
    df_raw = df_raw.dropna(subset=["Date"]).sort_values(["Date", "Code"]).reset_index(drop=True)

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

    # [수정된 부분] dropna를 하지 않음!
    # essential_cols = ["SMA_5", ... ] 리스트는 있지만 사용하지 않음
    # df_feat = df_feat.dropna(subset=essential_cols)  <-- 이 줄 삭제/주석

    after_rows = len(df_feat)
    print(f"  - 생성 결과: {before_rows:,} → {after_rows:,} 행 (삭제 없음, NaN 유지)")
    print("  - 최종 피처 개수: 15개 이상 (확장됨)")

    # os.makedirs(os.path.dirname(feature_file), exist_ok=True) # [수정] save 함수 내부 처리

    # [수정] 기존 파일 덮어쓰기 대신 날짜 태그 저장
    try:
        save_dataframe_with_date(df_feat, feat_dir, "features_V31", date_col="Date")
    except Exception as e:
        print(f"❌ 저장 실패: {e}"); return

    print("[FEATURE] 작업 완료")

def main():
    build_features()

if __name__ == "__main__":
    main()