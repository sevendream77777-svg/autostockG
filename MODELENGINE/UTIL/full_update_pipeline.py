# ============================================================
# full_update_pipeline.py (최종 수정 - V31 출력)
# - RAW 경로: RAW/stocks/all_stocks_cumulative.parquet
# - KOSPI 경로: RAW/kospi_data/kospi_data.parquet
# - 출력 파일: FEATURE/features_V31.parquet (V32 -> V31 변경 완료)
# - 저장 방식: 메인 파일 덮어쓰기 + 날짜별 백업 파일(_YYMMDD) 자동 생성
# ============================================================

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import os
from datetime import datetime

import numpy as np
import pandas as pd

from config_paths import get_path, versioned_filename


# ------------------------------------------------------------
# ⭐ parquet 파일에서 최신 날짜 읽기
# ------------------------------------------------------------
def get_latest_date_from_parquet(path, date_cols=["Date", "날짜", "date"]):
    if not os.path.exists(path):
        return None

    try:
        df = pd.read_parquet(path, columns=date_cols)
    except Exception:
        # 컬럼 로드 실패 시 전체 로드 시도
        try:
            df = pd.read_parquet(path)
        except:
            return None

    for col in date_cols:
        if col in df.columns:
            try:
                return pd.to_datetime(df[col]).max().date()
            except:
                continue

    return None


# ------------------------------------------------------------
# 기존 15개 피처 계산 함수
# ------------------------------------------------------------
def _compute_features(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("Date").copy()

    c = g["Close"]
    h = g["High"]
    l = g["Low"]
    v = g["Volume"]
    r_mkt = g["KOSPI_수익률"]

    # 1~3) SMA
    g["SMA_5"] = c.rolling(5).mean()
    g["SMA_20"] = c.rolling(20).mean()
    g["SMA_60"] = c.rolling(60).mean()

    # 4) VOL SMA
    g["VOL_SMA_20"] = v.rolling(20).mean()

    # 5~6) MOM/ROC
    g["MOM_10"] = c.pct_change(10)
    g["ROC_20"] = c.pct_change(20)

    # 7~8) MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    g["MACD_12_26"] = macd
    g["MACD_SIGNAL_9"] = macd.ewm(span=9, adjust=False).mean()

    # 9) BBP
    ma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    g["BBP_20"] = (c - lower) / (upper - lower + 1e-9)

    # 10) ATR
    prev_close = c.shift(1)
    tr = pd.concat([
        (h - l).abs(),
        (h - prev_close).abs(),
        (l - prev_close).abs()
    ], axis=1).max(axis=1)
    g["ATR_14"] = tr.rolling(14).mean()

    # 11~12) STOCH
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    stoch_k = (c - low14) / (high14 - low14 + 1e-9)
    g["STOCH_K"] = stoch_k
    g["STOCH_D"] = stoch_k.rolling(3).mean()

    # 13) CCI
    tp = (h + l + c) / 3.0
    ma_tp = tp.rolling(20).mean()
    md = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    g["CCI_20"] = (tp - ma_tp) / (0.015 * (md + 1e-9))

    # 14) ALPHA
    r_stock = c.pct_change()
    g["ALPHA_SMA_20"] = (r_stock - r_mkt).rolling(20).mean()

    return g


# ------------------------------------------------------------
# ⭐ 메인 피처 생성 (V31로 수정됨)
# ------------------------------------------------------------
def build_features():
    # RAW 및 KOSPI 경로
    raw_file = get_path("RAW", "stocks", "all_stocks_cumulative.parquet")
    kospi_file = get_path("RAW", "kospi_data", "kospi_data.parquet")
    
    # [수정] V32 -> V31로 변경
    feature_file = get_path("FEATURE", "features_V31.parquet")

    print("==============================================")
    print("[FEATURE V31] 피처 생성 파이프라인 시작")
    print(f"  RAW   경로: {raw_file}")
    print(f"  KOSPI 경로: {kospi_file}")

    # --------------------------------------------------------
    # 1) RAW 파일 존재 여부 확인
    # --------------------------------------------------------
    if not os.path.exists(raw_file):
        print("❌ [CRITICAL] RAW 데이터 파일이 없습니다.")
        print("   → update_raw_data.py를 먼저 실행하여 데이터를 준비해주세요.")
        return

    raw_latest = get_latest_date_from_parquet(raw_file)
    if raw_latest is None:
        print("❌ RAW 파일은 존재하나, 날짜를 읽을 수 없습니다 (파일 손상 가능성).")
        return

    print(f"  RAW 최신 날짜: {raw_latest}")

    # --------------------------------------------------------
    # 2) FEATURE 최신 날짜 읽기
    # --------------------------------------------------------
    run_generate = False
    feat_latest = get_latest_date_from_parquet(feature_file)

    if feat_latest is None:
        print("  FEATURE 없음 → 처음 실행. 생성 필요.")
        run_generate = True
    else:
        print(f"  FEATURE 최신 날짜: {feat_latest}")

    # --------------------------------------------------------
    # 3) 날짜 비교 후 SKIP/생성 결정
    # --------------------------------------------------------
    if not run_generate:
        if raw_latest > feat_latest:
            print(f"  ➜ 업데이트 감지: RAW({raw_latest}) > FEAT({feat_latest})")
            
            # 사용자 확인
            answer = input("피처 데이터를 업데이트 하시겠습니까? (예/아니오/y/n): ").strip().lower()
            if answer not in ["예", "y", "yes"]:
                print("🚫 사용자가 업데이트를 취소했습니다. 종료합니다.")
                return
            run_generate = True
        else:
            print("  ➜ FEATURE가 이미 최신입니다. 작업을 건너뜁니다.")
            return

    # --------------------------------------------------------
    # 피처 생성 실행
    # --------------------------------------------------------
    print("\n[FEATURE V31] 피처 생성 연산 시작...")

    if not os.path.exists(kospi_file):
        print(f"❌ [CRITICAL] KOSPI 인덱스 파일이 없습니다: {kospi_file}")
        print("   경로 설정을 확인해주세요.")
        return

    try:
        df_raw = pd.read_parquet(raw_file)
    except Exception as e:
        print(f"❌ RAW 파일 읽기 실패: {e}")
        return

    try:
        df_kospi = pd.read_parquet(kospi_file)
    except Exception as e:
        print(f"❌ KOSPI 파일 읽기 실패: {e}")
        return

    # 전처리
    df_raw["Date"] = pd.to_datetime(df_raw["Date"])
    df_raw = df_raw.sort_values(["Date", "Code"]).reset_index(drop=True)

    df_kospi["Date"] = pd.to_datetime(df_kospi["Date"])
    df_kospi = df_kospi.sort_values("Date").reset_index(drop=True)

    if "KOSPI_수익률" not in df_kospi.columns:
        if "KOSPI_종가" in df_kospi.columns:
            df_kospi["KOSPI_수익률"] = df_kospi["KOSPI_종가"].pct_change()
        else:
            print("⚠️ KOSPI 파일에 'KOSPI_종가' 컬럼이 없어 수익률을 계산할 수 없습니다.")

    # Merge
    try:
        df = pd.merge(
            df_raw,
            df_kospi[["Date", "KOSPI_종가", "KOSPI_수익률"]],
            on="Date",
            how="inner",
        )
    except KeyError as e:
        print(f"❌ 병합 실패: 컬럼 이름을 확인하세요. ({e})")
        print(f"   RAW 컬럼: {list(df_raw.columns)[:5]}")
        print(f"   KOSPI 컬럼: {list(df_kospi.columns)}")
        return

    # 피처 계산
    print("  ... 기술적 지표 계산 중 (시간이 걸릴 수 있습니다)")
    df_feat = df.groupby("Code", group_keys=False).apply(_compute_features)

    essential_cols = [
        "SMA_5", "SMA_20", "SMA_60", "VOL_SMA_20", "MOM_10",
        "ROC_20", "MACD_12_26", "MACD_SIGNAL_9", "BBP_20",
        "ATR_14", "STOCH_K", "STOCH_D", "CCI_20",
        "ALPHA_SMA_20", "KOSPI_수익률",
    ]
    
    # 필수 컬럼 확인
    missing_cols = [c for c in essential_cols if c not in df_feat.columns]
    if missing_cols:
        print(f"⚠️ 일부 피처 생성 실패: {missing_cols}")
    else:
        df_feat = df_feat.dropna(subset=essential_cols)

    # 저장 (V31)
    os.makedirs(os.path.dirname(feature_file), exist_ok=True)
    df_feat.to_parquet(feature_file, index=False)

    # 버전 백업 (이 부분에서 날짜 규칙이 적용됩니다!)
    backup_path = versioned_filename(feature_file)
    df_feat.to_parquet(backup_path, index=False)

    print("[FEATURE V31] 모든 작업 완료")
    print(f"  저장됨: {feature_file}")
    print(f"  백업됨: {os.path.basename(backup_path)}")


def main():
    build_features()


if __name__ == "__main__":
    main()