# --- test_runner_hoj_gridsearch.py ---
import pandas as pd
import numpy as np
from time import perf_counter
import itertools
import os

# =========================
# 고정 경로/폴더
# =========================
ENGINE_NAME = "hoj"
BASE_DIR = "F:/autostock"
ENGINE_DIR = os.path.join(BASE_DIR, ENGINE_NAME)
os.makedirs(ENGINE_DIR, exist_ok=True)

DATA_FILE = os.path.join(BASE_DIR, "all_stocks_cumulative.parquet")
OUTPUT_BASE = os.path.join(ENGINE_DIR, f"{ENGINE_NAME}_grid_result.xlsx")

# =========================
# 실험 세팅
# =========================
TOP_N = 20
DURATIONS = [20, 40, 60, 80]
TARGET_DAYS = [1, 5, 10, 20]

# 현실성/체결 보정
EXCLUDE_LIMIT_UP = True          # 당일 상한가(≈+29.5%)는 선정 제외
LIMIT_UP_TH = 0.295
MIN_PRICE = 1000                 # 최소 가격 필터(원)
VOL_WIN = 20                     # 거래량 필터 윈도(평균 거래량)
VOL_Q = 0.25                     # 하위 25% 유동성 제외 (0~1)

# 피처 윈도
MA_S = 5
MA_M = 20
RSI_N = 14
ATR_N = 14
CCI_N = 20
STO_N = 14
BB_N = 20
Z_N = 252                        # 롤링 표준화 윈도 (약 1년)

ENTRY_SHIFT = 1                  # D일 선정 → D+1부터 보유
RET_CLIP = None                  # 예: 0.30 지정 시 ±30% 클립

# =========================
# 유틸
# =========================
def make_unique_filename(base_path):
    if not os.path.exists(base_path):
        return base_path
    base, ext = os.path.splitext(base_path)
    k = 1
    while True:
        cand = f"{base}_{k}{ext}"
        if not os.path.exists(cand):
            return cand
        k += 1

def _group_roll_mean(g, win):  # 과거만 보도록 shift(1)
    return g.shift(1).rolling(win, min_periods=win).mean()

def _group_roll_std(g, win):
    return g.shift(1).rolling(win, min_periods=win).std()

def _ema_safe(s, span):
    # EWM도 과거만 쓰도록 전체를 1칸 시프트한 뒤 계산
    return s.shift(1).ewm(span=span, adjust=False, min_periods=span).mean()

def add_features(df):
    """
    12개급 피처를 '과거 정보만'으로 생성 + 롤링 표준화(z-score).
    모든 롤링/지수평균은 shift(1)로 미래 정보 차단.
    """
    g = df.groupby("code")
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["volume"]

    # 1) 이동평균/모멘텀 류
    ma5  = _group_roll_mean(close, MA_S)
    ma20 = _group_roll_mean(close, MA_M)
    df["mom_5"]  = close.shift(1) / ma5 - 1.0
    df["mom_20"] = close.shift(1) / ma20 - 1.0

    # 2) RSI (Wilder 근사)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs_up = g.apply(lambda x: _ema_safe(gain.loc[x.index], RSI_N)).reset_index(level=0, drop=True)
    rs_dn = g.apply(lambda x: _ema_safe(loss.loc[x.index], RSI_N)).reset_index(level=0, drop=True)
    rs = rs_up / (rs_dn + 1e-12)
    rsi = 100 - (100 / (1 + rs))
    df["rsi_14"] = rsi

    # 3) MACD (12-26, signal 9)
    ema12 = g.apply(lambda x: _ema_safe(close.loc[x.index], 12)).reset_index(level=0, drop=True)
    ema26 = g.apply(lambda x: _ema_safe(close.loc[x.index], 26)).reset_index(level=0, drop=True)
    macd = ema12 - ema26
    signal = macd.shift(1).ewm(span=9, adjust=False, min_periods=9).mean()
    df["macd"] = macd
    df["macd_sig"] = signal
    df["macd_hist"] = macd - signal

    # 4) 볼린저 밴드 위치
    bb_mid = ma20
    bb_std = _group_roll_std(close, BB_N)
    df["bb_pos"] = (close.shift(1) - bb_mid) / (bb_std + 1e-12)  # 표준편차 대비 위치

    # 5) ATR(평균진폭)
    tr1 = (high - low).abs()
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = g.apply(lambda x: _ema_safe(tr.loc[x.index], ATR_N)).reset_index(level=0, drop=True)
    df["atr_n"] = atr / (close.shift(1) + 1e-12)

    # 6) Stochastic %K/%D
    ll = g.apply(lambda x: low.shift(1).rolling(STO_N, min_periods=STO_N).min()).reset_index(level=0, drop=True)
    hh = g.apply(lambda x: high.shift(1).rolling(STO_N, min_periods=STO_N).max()).reset_index(level=0, drop=True)
    k = (close.shift(1) - ll) / (hh - ll + 1e-12)
    d = k.shift(1).rolling(3, min_periods=3).mean()
    df["stoch_k"] = k
    df["stoch_d"] = d

    # 7) CCI
    tp = (high + low + close) / 3
    sma_tp = _group_roll_mean(tp, CCI_N)
    md = (tp.shift(1) - sma_tp).abs().rolling(CCI_N, min_periods=CCI_N).mean()
    df["cci"] = (tp.shift(1) - sma_tp) / (0.015 * md + 1e-12)

    # 8) 거래량 모멘텀
    vol_ma = _group_roll_mean(vol, VOL_WIN)
    df["vol_mom"] = (vol.shift(1) / (vol_ma + 1e-12)) - 1.0

    # ---- 롤링 표준화 (Z_N) : 종목별 과거 분포에서 표준화 ----
    feats = ["mom_5","mom_20","rsi_14","macd","macd_sig","macd_hist",
             "bb_pos","atr_n","stoch_k","stoch_d","cci","vol_mom"]

    for f in feats:
        mean = g[f].apply(lambda s: s.shift(1).rolling(Z_N, min_periods=Z_N).mean())
        std  = g[f].apply(lambda s: s.shift(1).rolling(Z_N, min_periods=Z_N).std())
        df[f"{f}_z"] = (df[f] - mean.values) / (std.values + 1e-12)

    # 최종 스코어(동가중 합산; 추후 가중치/모델로 대체 가능)
    z_feats = [f"{f}_z" for f in feats]
    df["hoj_score"] = df[z_feats].sum(axis=1)

def add_returns(df):
    # 과거 1일 변화율(당일 상한가 필터용)
    df["ret_1d_past"] = df.groupby("code")["close"].pct_change(1)

    # 미래 수익률: D → D+X (라벨 누출 방지)
    for tgt in TARGET_DAYS:
        col = f"fwd_{tgt}d_return"
        df[col] = df.groupby("code")["close"].shift(-tgt) / df["close"] - 1.0
        if RET_CLIP is not None:
            df[col] = df[col].clip(-RET_CLIP, RET_CLIP)

def select_universe(day_df):
    # 유동성/가격 필터
    cond_price = day_df["close"].shift(0) >= MIN_PRICE
    # 최근 VOL_WIN의 평균 거래량이 하위 25%면 제외
    # (add_features에서 vol_ma를 만들었음: shift(1) rolling) → 당일에는 vol_ma가 이미 들어있음
    vol_ma = day_df.groupby("code")["volume"].transform(
        lambda x: x.shift(1).rolling(VOL_WIN, min_periods=VOL_WIN).mean()
    )
    q = vol_ma.groupby(day_df["date"]).transform(lambda s: s.rank(pct=True))
    cond_liq = q >= VOL_Q

    filt = cond_price & cond_liq
    out = day_df[filt].copy()

    # 상한가 제외
    if EXCLUDE_LIMIT_UP and "ret_1d_past" in out.columns:
        out = out[~(out["ret_1d_past"] >= LIMIT_UP_TH)].copy()
    return out

def backtest_with_log(df, day_list, tgt, top_n, dur):
    target_col = f"fwd_{tgt}d_return"
    rets, logs = [], []

    for d in day_list:
        day = df[df["date"] == d].copy()
        if day.empty:
            continue

        # 유니버스 필터
        day = select_universe(day)
        if day.empty:
            continue

        # 점수 상위 TOP_N
        top = day.nlargest(top_n, "hoj_score").copy()
        if top.empty:
            continue

        # D→D+X 미래 수익률 평균
        port_ret = top[target_col].mean()
        rets.append(port_ret)

        # 로그
        top["rank"] = np.arange(1, len(top)+1)
        top["기간일수"] = dur
        top["목표수익일"] = tgt
        top["테스트날짜"] = d
        top["same_day_ret(%)"] = top["ret_1d_past"] * 100
        top["수익률(%)"] = top[target_col] * 100
        logs.append(top[["테스트날짜","기간일수","목표수익일","rank","code","close","hoj_score",
                         "same_day_ret(%)", target_col, "수익률(%)"]])

    mean_ret = float(np.nanmean(rets)) if len(rets) else np.nan
    detail = pd.concat(logs, ignore_index=True) if len(logs) else pd.DataFrame()
    return mean_ret, detail

def main():
    t0 = perf_counter()
    print(f"[INFO] {ENGINE_NAME.upper()} 엔진(누출차단·다중피처) 테스트 시작...")

    # 1) 로드/정렬
    df = pd.read_parquet(DATA_FILE)
    rename_map = {
        "날짜":"date","종목코드":"code","종가":"close","시가":"open",
        "고가":"high","저가":"low","거래량":"volume"
    }
    df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["code","date"]).reset_index(drop=True)

    # 2) 피처 + 수익률
    add_features(df)
    add_returns(df)

    # 3) 날짜 리스트(미래 수익 가능 구간만)
    dates_all = sorted(df["date"].unique())
    results, detail_logs = [], []

    for dur, tgt in itertools.product(DURATIONS, TARGET_DAYS):
        if len(dates_all) < (dur + tgt + ENTRY_SHIFT):
            continue
        # ENTRY_SHIFT만큼도 끝에서 제외 (D 선정 후 D+1 진입 대비 여유)
        day_list = dates_all[-(dur + tgt + ENTRY_SHIFT):-(tgt + ENTRY_SHIFT)]

        mean_ret, log_df = backtest_with_log(df, day_list, tgt, TOP_N, dur)

        results.append({
            "기간일수": dur,
            "목표수익일": tgt,
            "평균수익률(%)": None if pd.isna(mean_ret) else round(mean_ret*100, 3),
            "테스트일수": len(day_list)
        })
        if not log_df.empty:
            detail_logs.append(log_df)

        print(f"[OK] {dur}일 / {tgt}일 → {(mean_ret*100 if not pd.isna(mean_ret) else np.nan):.3f}%")

    # 4) 저장
    df_result = pd.DataFrame(results).sort_values(["기간일수","목표수익일"]).reset_index(drop=True)
    df_detail = pd.concat(detail_logs, ignore_index=True) if detail_logs else pd.DataFrame()

    out_path = make_unique_filename(OUTPUT_BASE)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_result.to_excel(w, index=False, sheet_name="요약결과")
        if not df_detail.empty:
            df_detail.to_excel(w, index=False, sheet_name="상세기록")

    print("\n[완료] 호엔진 그리드 테스트(누출 차단·다중피처) ✅")
    print(f"총 조합: {len(df_result)}개 / 엑셀 저장: {out_path}")
    print(df_result)
    print(f"\n총 소요시간: {perf_counter() - t0:.2f}초")

if __name__ == "__main__":
    main()
