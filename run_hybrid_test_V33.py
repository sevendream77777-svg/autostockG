import pandas as pd
import joblib
from config_paths import (
    HOJ_DB_REAL,
    # [REMOVED: SLE BLOCK]
    HOJ_ENGINE_REAL,
    # [REMOVED: SLE BLOCK]
)
print("=== Hybrid Test V33 시작 ===")
# --- DB 로드 ---
df_hoj = pd.read_parquet(HOJ_DB_REAL)
df_sle = pd.read_parquet(# [REMOVED: SLE BLOCK]
# 날짜 컬럼 정규화
if "date" not in df_hoj.columns:
    if "Date" in df_hoj.columns:
        df_hoj.rename(columns={"Date": "date"}, inplace=True)
    else:
        raise KeyError("HOJ DB에 날짜(Date/date) 컬럼이 없습니다.")
if "date" not in df_sle.columns:
    if "Date" in df_sle.columns:
    else:
        raise KeyError("SLE DB에 날짜(Date/date) 컬럼이 없습니다.")
df_hoj["date"] = pd.to_datetime(df_hoj["date"])
df_sle["date"] = pd.to_datetime(df_sle["date"])
# --- 엔진 로드 ---
hoj_engine = joblib.load(HOJ_ENGINE_REAL)
sle_engine = joblib.load(# [REMOVED: SLE BLOCK]
# --- 최신 날짜 ---
latest_date = df_hoj["date"].max()
print(f"[INFO] 최신 테스트 날짜: {latest_date}")
X_hoj = df_hoj[df_hoj["date"] == latest_date].copy()
# 특성 컬럼 자동 감지
f_hoj = [c for c in X_hoj.columns if c.startswith(("SMA", "RSI", "MACD", "BBP", "ATR", "STOCH", "CCI", "VOL", "ALPHA", "KOSPI"))]
f_sle = [c for c in  hoj_engine['reg_model'].predict(X_hoj[f_hoj])
 sle_engine['reg_model'].predict({"Code": "code"}, inplace=True)
df_sle.rename(columns={"Code": "code"}, inplace=True)
# --- 머지 ---
merged = pd.merge(
    X_hoj[["code", "pred_hoj"]],
    "code",
    how="inner"
)
merged["hybrid_score"] = merged["pred_hoj"] * 0.6 + merged[" merged.sort_values("hybrid_score", ascending=False).head(10)
print("\n=== Hybrid Top10 결과 ===")
print(top10.to_string(index=False))
print("\n[완료] Hybrid Test V33 종료!")
