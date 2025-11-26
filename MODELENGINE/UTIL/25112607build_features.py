
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# === 필수 추가 (UTIL 경로 인식) ===
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from UTIL.version_utils import find_latest_file, load_raw_data, load_kospi_index

# ============================================================
#  BUILD FEATURES  —  Version V31 (Smart Skip & Fast)
# ============================================================

def build_features(raw_dir, kospi_dir, feat_dir):
    print("------------------------------------------------------------")
    print("[FEATURE] 피처 생성 시작 (V31 - 스마트 스킵 적용)")
    print("------------------------------------------------------------")

    # ------------------------------------------------------------
    # 1) RAW 로드
    # ------------------------------------------------------------
    raw_path = find_latest_file(raw_dir, "all_stocks_cumulative")
    if raw_path is None:
        print(f"❌ RAW 파일을 찾을 수 없습니다. (경로: {raw_dir})")
        return

    print(f"  ✓ RAW 로딩: {raw_path.name}")
    df = load_raw_data(raw_path)

    # ------------------------------------------------------------
    # 2) KOSPI 로드 및 전처리
    # ------------------------------------------------------------
    kospi_path = find_latest_file(kospi_dir, "kospi_data")
    if kospi_path is None:
        print(f"❌ KOSPI 파일을 찾을 수 없습니다. (경로: {kospi_dir})")
        return

    print(f"  ✓ KOSPI 로딩: {kospi_path.name}")
    df_kospi = load_kospi_index(kospi_path)

    # [안전장치] 수익률 계산 및 컬럼명 변경
    if "Close" in df_kospi.columns:
        df_kospi = df_kospi.sort_values("Date")
        df_kospi["Change"] = df_kospi["Close"].pct_change() 

    rename_map = {"Close": "KOSPI_Close", "Change": "KOSPI_Change"}
    df_kospi = df_kospi.rename(columns=rename_map)

    cols_to_use = ["Date"]
    if "KOSPI_Close" in df_kospi.columns: cols_to_use.append("KOSPI_Close")
    if "KOSPI_Change" in df_kospi.columns: cols_to_use.append("KOSPI_Change")
    df_kospi = df_kospi[cols_to_use]

    # ------------------------------------------------------------
    # 3) 병합 및 날짜 확인 (★여기서 바로 SKIP 판단★)
    # ------------------------------------------------------------
    print("  ✓ RAW + KOSPI 병합")
    df = df.merge(df_kospi, on="Date", how="left")

    print(">>> DEBUG: 병합 후 DF:", df.shape)
    print(">>> DEBUG: Code unique:", df["Code"].nunique(), "  NaN:", df["Code"].isna().sum())
    print(">>> DEBUG: Date range:", df["Date"].min(), " ~ ", df["Date"].max())
    print(df.head(10))
    
    if "KOSPI_Close" in df.columns: df["KOSPI_Close"] = df["KOSPI_Close"].ffill()
    if "KOSPI_Change" in df.columns: df["KOSPI_Change"] = df["KOSPI_Change"].fillna(0)

    # 병합된 데이터 기준 최신 날짜 확인
    feat_dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if len(feat_dates) == 0:
        print("❌ 데이터에 Date가 없습니다.")
        return

    new_date = feat_dates.max().date()
    new_tag = new_date.strftime("%y%m%d")
    print(f"  → 데이터 최신 날짜: {new_date}")

    # === [핵심 수정] 기존 파일 확인 및 입구 컷 ===
    prefix = "features_V31"
    feat_dir = Path(feat_dir)
    if not feat_dir.exists():
        feat_dir.mkdir(parents=True)

    existing_dates = []
    for fn in os.listdir(feat_dir):
        if fn.startswith(prefix) and fn.endswith(".parquet"):
            try:
                # features_V31_251126.parquet 파싱
                parts = fn.split("_")
                if len(parts) >= 3:
                    dtag = parts[2].split(".")[0]
                    d = pd.to_datetime(dtag, format="%y%m%d").date()
                    existing_dates.append(d)
            except:
                pass
    
    # 이미 최신 날짜 이상의 파일이 있으면 여기서 즉시 종료!
    if existing_dates and max(existing_dates) >= new_date:
        print(f"  ✓ [SKIP] 최신 파일이 이미 존재합니다. ({max(existing_dates)} >= {new_date})")
        print("       (지표 생성을 건너뜁니다.)")
        print("------------------------------------------------------------")
        return  # <--- ★ 무거운 계산 하기 전에 탈출! ★

    # ------------------------------------------------------------
    # 4) 기술적 지표 생성 (SKIP 통과한 경우만 실행)
    # ------------------------------------------------------------
    print("  ✓ 신규 데이터 감지 -> 기술적 지표 생성 시작 (고속 연산)...")

    # ================= RSI TEST (임시) =================
    print(">>> TEST: RSI 시작")
    import time
    _t = time.time()

    delta = df.groupby("Code")["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    _ = gain.groupby(df["Code"]).rolling(14).mean()

    print(">>> TEST: RSI 끝 =", round(time.time() - _t, 2), "초")
    # ====================================================
    # ================= ATR TEST (임시) =================
    print(">>> TEST: ATR 시작")
    import time
    _t = time.time()

    prev_close = df.groupby("Code")["Close"].shift(1)

    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs()
    ], axis=1).max(axis=1)

    _ = tr.groupby(df["Code"]).rolling(14).mean().reset_index(0, drop=True)

    print(">>> TEST: ATR 끝 =", round(time.time() - _t, 2), "초")
    # ====================================================


    # 속도 최적화 (정렬)
    df.sort_values(["Code", "Date"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # groupby 객체 미리 생성
    g = df.groupby("Code")
    
    # (1) 이동평균 (SMA)
    for w in [5, 20, 40, 60, 90, 120]:
        df[f"SMA_{w}"] = g["Close"].transform(lambda x: x.rolling(w).mean())

    # (2) 거래량 평균
    df["VOL_SMA_20"] = g["Volume"].transform(lambda x: x.rolling(20).mean())

    # (3) RSI
    delta = g["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_gain = gain.groupby(df['Code']).rolling(14).mean().reset_index(0, drop=True)
    roll_loss = loss.groupby(df['Code']).rolling(14).mean().reset_index(0, drop=True)
    rs = roll_gain / roll_loss.replace(0, 1e-6)
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # (4) STOCHASTIC (clip 포함 + 분모 보정)
    high14 = g["High"].transform(lambda x: x.rolling(14).max())
    low14 = g["Low"].transform(lambda x: x.rolling(14).min())

    # 분모 보정: 고가<저가 역전/0/음수 모두 차단
    denom = (high14 - low14).clip(lower=1e-6)

    df["STOCH_K"] = ((df["Close"] - low14) / denom).clip(0, 1)
    df["STOCH_D"] = df.groupby("Code")["STOCH_K"].transform(lambda x: x.rolling(3).mean())


    # (5) MOM / ROC
    df["MOM_10"] = g["Close"].diff(10)
    df["ROC_20"] = g["Close"].pct_change(20)

    # (6) MACD
    ema12 = g["Close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["Close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["MACD_12_26"] = ema12 - ema26
    df["MACD_SIGNAL_9"] = df.groupby("Code")["MACD_12_26"].transform(lambda x: x.ewm(span=9, adjust=False).mean())

    # (7) BBP
    mband = df["SMA_20"]
    std20 = g["Close"].transform(lambda x: x.rolling(20).std())
    ub = mband + 2 * std20
    lb = mband - 2 * std20
    df["BBP_20"] = (df["Close"] - lb) / (ub - lb).replace(0, 1e-6)

    # (8) ATR
    prev_close = g["Close"].shift(1)
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - prev_close).abs()
    low_close = (df["Low"] - prev_close).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR_14"] = tr.groupby(df["Code"]).rolling(14).mean().reset_index(0, drop=True)

    # (9) CCI — 벡터 최적화 버전 (산식 동일)
    tp = (df["High"] + df["Low"] + df["Close"]) / 3

    # 20일 단순 평균
    sma_tp = tp.groupby(df["Code"]).transform(lambda x: x.rolling(20).mean())

    # MAD: |TP - SMA| 의 20일 평균 (apply 제거)
    abs_dev = (tp - sma_tp).abs()
    mad = abs_dev.groupby(df["Code"]).transform(lambda x: x.rolling(20).mean())
    mad = mad.replace(0, 1e-6)

    df["CCI_20"] = (tp - sma_tp) / (0.015 * mad)



    # (10) ALPHA
    df["ALPHA_SMA_20"] = df["Close"] / df["SMA_20"]

    # ------------------------------------------------------------
    # 5) 저장
    # ------------------------------------------------------------
    base = feat_dir / f"{prefix}_{new_tag}.parquet"
    out = base
    i = 1
    while out.exists():
        out = feat_dir / f"{prefix}_{new_tag}_{i}.parquet"
        i += 1

    print(f"  ✓ 저장 경로: {out}")
        # === KOSPI 컬럼명 표준화 ===
    df.rename(columns={
        "KOSPI_Close": "KOSPI_종가",
        "KOSPI_Change": "KOSPI_수익률",
    }, inplace=True)

    df.to_parquet(out, index=False)

    print(f"  🎉 FEATURE 저장 완료: {out.name}")
    print("------------------------------------------------------------")
    print("[FEATURE] 작업 완료")
    print("------------------------------------------------------------")

if __name__ == "__main__":
    ROOT = Path(__file__).resolve().parents[1]
    RAW_DIR = ROOT / "RAW" / "stocks" 
    KOSPI_DIR = ROOT / "RAW" / "kospi_data"
    FEAT_DIR = ROOT / "FEATURE"

    build_features(RAW_DIR, KOSPI_DIR, FEAT_DIR)