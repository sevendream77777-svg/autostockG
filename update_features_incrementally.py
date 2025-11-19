# ============================================================
# [update_features_incrementally.py] V21C-Final (15피처 완전판)
# ============================================================
import pandas as pd
import numpy as np
import traceback

print("=================================================")
print("[update_features_incrementally.py] ▶️ 실행 시작...")
print("=================================================")

try:
    # 1) 입력 로드
    px = pd.read_parquet("all_stocks_cumulative.parquet")
    kospi = pd.read_parquet("kospi_index_10y.parquet")
    print(f"✅ 시세 데이터 로드 완료 ({len(px):,}행) | KOSPI ({len(kospi):,}행)")

    # 2) 컬럼 확인
    need_cols = {"Date","Code","Open","High","Low","Close","Volume"}
    missing = need_cols - set(px.columns)
    if missing:
        raise KeyError(f"❌ 시세 컬럼 누락: {missing}")

    # 3) 날짜 정규화 & KOSPI 수익률/20일수익률
    px["Date"] = pd.to_datetime(px["Date"]).dt.tz_localize(None)
    kospi["Date"] = pd.to_datetime(kospi["Date"]).dt.tz_localize(None)
    kospi = kospi.sort_values("Date").reset_index(drop=True)
    kospi["KOSPI_Return"] = kospi["KOSPI_Close"].pct_change(1)
    kospi["KOSPI_Return_20"] = kospi["KOSPI_Close"].pct_change(20)

    # 4) 피처 계산 (종목별)
    def make_features(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("Date").reset_index(drop=True)

        close = g["Close"]
        high  = g["High"]
        low   = g["Low"]
        vol   = g["Volume"]

        # SMA들
        g["SMA_20"] = close.rolling(20, min_periods=1).mean()
        g["SMA_40"] = close.rolling(40, min_periods=1).mean()
        g["SMA_60"] = close.rolling(60, min_periods=1).mean()
        g["SMA_90"] = close.rolling(90, min_periods=1).mean()

        # RSI_14
        r = close.pct_change().fillna(0)
        up = r.clip(lower=0).rolling(14, min_periods=1).mean()
        dn = (-r.clip(upper=0)).rolling(14, min_periods=1).mean()
        rs = up / (dn.replace(0, np.nan))
        g["RSI_14"] = 100 - (100/(1+rs.replace([np.inf,-np.inf], np.nan)))

        # 거래량 평균
        g["VOL_SMA_20"] = vol.rolling(20, min_periods=1).mean()

        # MACD / Signal (EMA12, EMA26, Signal9)
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        g["MACD"] = macd
        g["MACD_Sig"] = macd.ewm(span=9, adjust=False).mean()

        # BBP_20
        roll_max = close.rolling(20, min_periods=1).max()
        roll_min = close.rolling(20, min_periods=1).min()
        denom = (roll_max - roll_min).replace(0, np.nan)
        g["BBP_20"] = (close - roll_min) / denom

        # ATR_14
        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        g["ATR_14"] = tr.rolling(14, min_periods=1).mean()

        # STOCH_K, D
        ll14 = low.rolling(14, min_periods=1).min()
        hh14 = high.rolling(14, min_periods=1).max()
        denom2 = (hh14 - ll14).replace(0, np.nan)
        k = (close - ll14) / denom2 * 100
        g["STOCH_K"] = k
        g["STOCH_D"] = k.rolling(3, min_periods=1).mean()

        # CCI_20
        tp = (high + low + close) / 3
        sma_tp = tp.rolling(20, min_periods=1).mean()
        md = (tp - sma_tp).abs().rolling(20, min_periods=1).mean()
        g["CCI_20"] = (tp - sma_tp) / (0.015 * md.replace(0, np.nan))

        # 20일 수익률 / ALPHA_SMA_20
        g["RET_20"] = close.pct_change(20)

        return g

    feat = px.groupby("Code", group_keys=False).apply(make_features)

    # 5) KOSPI 병합 + KOSPI_수익률/ALPHA_SMA_20
    feat = feat.merge(kospi[["Date","KOSPI_Close","KOSPI_Return","KOSPI_Return_20"]],
                      on="Date", how="left")
    feat["KOSPI_수익률"] = feat["KOSPI_Return"]
    # ALPHA_SMA_20: 종목 20일 수익률 - KOSPI 20일 수익률
    feat["ALPHA_SMA_20"] = feat["RET_20"] - feat["KOSPI_Return_20"]

    # 마무리 정리
    feat = feat.drop(columns=["KOSPI_Return","RET_20"], errors="ignore")
    feat = feat.loc[:, ~feat.columns.duplicated()]
    feat = feat.dropna(subset=["Close"])  # 기본 유효성
    feat = feat.sort_values(["Code","Date"]).reset_index(drop=True)

    # 생성된 피처 목록 확인용
    gen_cols = ["SMA_20","SMA_40","SMA_60","SMA_90","RSI_14","VOL_SMA_20",
                "MACD","MACD_Sig","BBP_20","ATR_14","STOCH_K","STOCH_D",
                "CCI_20","KOSPI_수익률","ALPHA_SMA_20"]
    print("📊 생성 피처 개수:", len(gen_cols))
    print("🧩 피처 샘플:", ", ".join(gen_cols[:8]) + ", ...")

    # 저장
    out = "all_features_cumulative_V21_Hoj.parquet"  # 파일명 유지(규격)
    feat.to_parquet(out)
    print(f"✅ [저장 완료] {out} ({len(feat):,}행)")
    print("=================================================")
    print("[update_features_incrementally.py] ✅ 성공 (V21C-Final)")
    print("=================================================")

except Exception as e:
    print("❌ [치명적 오류 발생] 자동 종료합니다.")
    print("오류 내용:", str(e))
    traceback.print_exc()
    print("=================================================")
