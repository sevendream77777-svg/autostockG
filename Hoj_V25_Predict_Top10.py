import pandas as pd
import pickle

# ==========================================================
#  Hoj V25 엔진 - 오늘 날짜 Top10 추천 종목 추출 스크립트
#  파일명: Hoj_V25_Predict_Top10.py
# ==========================================================

DB = "new_Hoj_DB_V25_FULL.parquet"          # 일일 수익률 포함 FULL DB
MODEL = "new_Hoj_MODELENGINE_V25.pkl"       # 연구 엔진 V25 모델

print("[1] 데이터 로드 중...")
df = pd.read_parquet(DB)
df_latest = df[df["Date"] == df["Date"].max()].copy()
print(" > 최신 날짜:", df_latest["Date"].iloc[0])

print("\n[2] 모델 로드 중...")
with open(MODEL, "rb") as f:
    bundle = pickle.load(f)

reg = bundle["reg"]
print(" > 회귀 모델 로드 완료!")

FEATURES = [
    "SMA_20","SMA_40","SMA_60","SMA_90",
    "RSI_14","VOL_SMA_20","MACD","MACD_Sig",
    "BBP_20","ATR_14","STOCH_K","STOCH_D",
    "CCI_20","KOSPI_수익률","ALPHA_SMA_20"
]

print("\n[3] 예측값 생성 중...")
df_latest["Pred"] = reg.predict(df_latest[FEATURES])

top10 = df_latest.sort_values("Pred", ascending=False).head(10)

print("\n=== 📌 Hoj V25 엔진 Top10 추천 종목 ===")
print(top10[["Code","Name","Pred"]])
print("=====================================")
