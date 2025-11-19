# ============================================================
# build_features.py (V31/15)
# RAW(stocks/all_stocks_cumulative.parquet) + KOSPI(kospi_data)
# → FEATURE(features_V31.parquet)
#
# [데이터 읽기 원칙]
# 1. 메인 파일명(all_stocks_cumulative.parquet 등)만 읽습니다.
# 2. 날짜가 붙은 백업 파일(_251118 등)은 자동으로 무시됩니다.
# 3. 항상 최신 데이터가 메인 파일명으로 존재해야 합니다.
# ============================================================

import sys
import os

import numpy as np
import pandas as pd

# 상위 폴더(config_paths.py 위치) 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_paths import get_path, versioned_filename


# ------------------------------------------------------------
# parquet 파일에서 최신 날짜 읽기
# ------------------------------------------------------------
def get_latest_date_from_parquet(path, date_cols=None):
    """
    지정된 parquet 파일에서 가장 최근 날짜를 date 객체로 반환.
    date_cols 리스트 중 실제로 존재하는 첫 번째 컬럼을 사용.
    """
    if date_cols is None:
        date_cols = ["Date", "날짜", "date"]

    if not os.path.exists(path):
        return None

    try:
        # 날짜 컬럼만 부분 로드 시도
        df = pd.read_parquet(path, columns=date_cols)
    except Exception:
        # 실패하면 전체 로드
        try:
            df = pd.read_parquet(path)
        except Exception:
            return None

    # 우선순위에 따라 컬럼 선택
    for col in date_cols:
        if col in df.columns:
            try:
                return pd.to_datetime(df[col]).max().date()
            except Exception:
                continue

    return None


# ------------------------------------------------------------
# 15개 기술적 지표 피처 계산
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
    tr = pd.concat(
        [
            (h - l).abs(),
            (h - prev_close).abs(),
            (l - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
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
    md = tp.rolling(20).apply(
        lambda x: np.mean(np.abs(x - x.mean())), raw=True
    )
    g["CCI_20"] = (tp - ma_tp) / (0.015 * (md + 1e-9))

    # 14) ALPHA (시장 대비 초과수익)
    r_stock = c.pct_change()
    g["ALPHA_SMA_20"] = (r_stock - r_mkt).rolling(20).mean()

    # 15) KOSPI_수익률은 이미 포함

    return g


# ------------------------------------------------------------
# 메인 피처 생성 (V31/15)
# ------------------------------------------------------------
def build_features():
    # [경로 설정] 백업파일(_날짜)이 아닌 '메인 파일명'을 지정합니다.
    raw_file = get_path("RAW", "stocks", "all_stocks_cumulative.parquet")
    kospi_file = get_path("RAW", "kospi_data", "kospi_data.parquet")
    
    feature_file = get_path("FEATURE", "features_V31.parquet")

    print("==============================================")
    print("[FEATURE V31/15] 피처 생성 여부 판단 중...")
    print(f"  RAW   경로: {raw_file}")
    print(f"  KOSPI 경로: {kospi_file}")

    # 1) RAW 최신 날짜 확인
    raw_latest = get_latest_date_from_parquet(raw_file)
    if raw_latest is None:
        print("❌ RAW 파일을 읽을 수 없습니다.")
        print(f"   경로: {raw_file}")
        print("   (해당 경로에 'all_stocks_cumulative.parquet' 이름의 파일이 있는지 확인하세요)")
        return
    print(f"  RAW 최신 날짜: {raw_latest}")

    # 2) FEATURE 최신 날짜 확인
    feat_latest = get_latest_date_from_parquet(feature_file)
    run_generate = False

    if feat_latest is None:
        print("  FEATURE 파일이 없습니다. (처음 실행으로 간주)")
        run_generate = True
    else:
        print(f"  FEATURE 최신 날짜: {feat_latest}")

        # 3) 날짜 비교
        if raw_latest > feat_latest:
            print("  ➜ RAW가 FEATURE보다 최신입니다.")
            answer = input(
                "원본데이터가 업데이트 됐습니다.\n"
                "피처데이터 업데이트 하시겠습니까? (예/아니오): "
            ).strip()
            if answer not in ["예", "y", "Y", "yes", "YES"]:
                print("❌ 사용자가 피처 업데이트를 취소했습니다. 작업을 종료합니다.")
                return
            run_generate = True
        elif raw_latest == feat_latest:
            print("  ➜ RAW와 FEATURE 날짜가 동일합니다. 피처 생성을 건너뜁니다 (SKIP).")
            return
        else:
            print(
                "  ⚠ 경고: RAW 최신 날짜가 FEATURE 최신 날짜보다 과거입니다.\n"
                "     (DB 정합성 확인 필요) 기본적으로 피처 생성을 건너뜁니다."
            )
            return

    if not run_generate:
        print("⚠ run_generate=False 상태입니다. 작업을 종료합니다.")
        return

    # --------------------------------------------------------
    # 피처 생성 실행
    # --------------------------------------------------------
    print("[FEATURE V31/15] 피처 생성 시작")
    
    if not os.path.exists(kospi_file):
        print(f"❌ [CRITICAL] KOSPI 데이터가 없습니다: {kospi_file}")
        print("   (kospi_data.parquet 파일이 해당 폴더에 있는지 확인해주세요)")
        return

    # --- RAW / KOSPI 로드 ---
    try:
        df_raw = pd.read_parquet(raw_file)
        df_kospi = pd.read_parquet(kospi_file)
    except Exception as e:
        print(f"❌ 파일 로드 실패: {e}")
        return

    df_raw["Date"] = pd.to_datetime(df_raw["Date"])
    df_raw = df_raw.sort_values(["Date", "Code"]).reset_index(drop=True)

    df_kospi["Date"] = pd.to_datetime(df_kospi["Date"])
    df_kospi = df_kospi.sort_values("Date").reset_index(drop=True)

    # KOSPI 수익률 없으면 생성 (컬럼명 호환성 체크)
    if "KOSPI_수익률" not in df_kospi.columns:
        if "KOSPI_종가" in df_kospi.columns:
            df_kospi["KOSPI_수익률"] = df_kospi["KOSPI_종가"].pct_change()
        else:
            print("⚠️ KOSPI 파일에 'KOSPI_종가' 컬럼이 없습니다.")

    # --- RAW + KOSPI 병합 ---
    try:
        df = pd.merge(
            df_raw,
            df_kospi[["Date", "KOSPI_종가", "KOSPI_수익률"]],
            on="Date",
            how="inner",
        )
    except KeyError as e:
        print(f"❌ 병합 실패 (컬럼명 확인 필요): {e}")
        return

    # --- 피처 계산 ---
    before_rows = len(df)
    print("  ... 기술적 지표 계산 중")
    df_feat = df.groupby("Code", group_keys=False).apply(_compute_features)

    essential_cols = [
        "SMA_5", "SMA_20", "SMA_60", "VOL_SMA_20", "MOM_10",
        "ROC_20", "MACD_12_26", "MACD_SIGNAL_9", "BBP_20",
        "ATR_14", "STOCH_K", "STOCH_D", "CCI_20",
        "ALPHA_SMA_20", "KOSPI_수익률",
    ]
    df_feat = df_feat.dropna(subset=essential_cols)
    after_rows = len(df_feat)

    print(
        f"  - 결측 제거: {before_rows:,} → {after_rows:,} 행"
    )
    print("  - 최종 피처 개수: 15개")

    # --- 저장 ---
    os.makedirs(os.path.dirname(feature_file), exist_ok=True)
    df_feat.to_parquet(feature_file, index=False)

    # 버전 백업 (타임스탬프/날짜 기반 파일명)
    backup_path = versioned_filename(feature_file)
    df_feat.to_parquet(backup_path, index=False)

    print(f"[FEATURE V31/15] 저장 완료: {feature_file}")
    print(f"[FEATURE V31/15] 버전 저장: {backup_path}")
    print("[FEATURE V31/15] 작업 완료")


def main():
    build_features()


if __name__ == "__main__":
    main()