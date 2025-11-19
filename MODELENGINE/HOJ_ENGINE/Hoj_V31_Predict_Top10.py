# -*- coding: utf-8 -*-
"""
Hoj V31 엔진 - 오늘 날짜 Top10 추천 종목 추출
(V25 구조 유지 + CSV 저장 + UI 100% 호환 포맷)
"""

import pandas as pd
import lightgbm as lgb
import datetime
import os

# ==========================================================
#  경로 (V31 기준)
# ==========================================================
DB = r"F:\autostockG\MODELENGINE\HOJ_DB\REAL\HOJ_DB_REAL_V31.parquet"
MODEL = r"F:\autostockG\MODELENGINE\HOJ_ENGINE\REAL\HOJ_ENGINE_REAL_V31.pkl"
SAVE_DIR = r"F:\autostockG\MODELENGINE\TOP10"

os.makedirs(SAVE_DIR, exist_ok=True)

print("[1] 데이터 로드 중...")
df = pd.read_parquet(DB)
df_today = df[df["Date"] == df["Date"].max()].copy()

today_date = df_today["Date"].iloc[0]
print(f" > 최신 날짜: {today_date}")

print("\n[2] V31 엔진 로드 중...")
reg = lgb.Booster(model_file=MODEL)
print(" > 모델 로드 완료!")

# ==========================================================
# 15개 피처 (V31 공식)
# ==========================================================
FEATURES = [
    "SMA_20","SMA_40","SMA_60","SMA_90",
    "RSI_14","VOL_SMA_20",
    "MACD","MACD_Sig",
    "BBP_20","ATR_14",
    "STOCH_K","STOCH_D",
    "CCI_20","KOSPI_수익률",
    "ALPHA_SMA_20",
]

print("\n[3] 예측값 생성...")
df_today["ExpectedReturn"] = reg.predict(df_today[FEATURES])
df_today["ExpectedReturnPct"] = df_today["ExpectedReturn"] * 100

# ==========================================================
#  Top10 선정
# ==========================================================
top10 = df_today.sort_values("ExpectedReturn", ascending=False).head(10)

print("\n=== 📌 HoJ V31 엔진 Top10 추천 종목 ===")
out_cols = ["Date", "Code"]

# Name 컬럼 있을 경우 포함
if "Name" in df_today.columns:
    out_cols.append("Name")

out_cols += ["Close", "ExpectedReturn", "ExpectedReturnPct"]

print(top10[out_cols])

# ==========================================================
#  CSV 저장 (UI 호환)
# ==========================================================
csv_name = f"recommendation_HOJ_{today_date}.csv"
csv_path = os.path.join(SAVE_DIR, csv_name)

top10[out_cols].to_csv(csv_path, index=False, encoding="utf-8-sig")

print("\n💾 CSV 저장 완료!")
print(f"   → {csv_path}")
print("=== 추천 완료 ===")
