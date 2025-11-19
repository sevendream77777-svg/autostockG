import pandas as pd
import joblib
from pathlib import Path

print("=== Hybrid Test V34 (HOJ 전용) 시작 ===")

BASE = Path("F:/autostockG")

DB_PATH = BASE / "HOJ_DB" / "REAL" / "HOJ_DB_REAL_V25.parquet"
ENGINE_PATH = BASE / "HOJ_ENGINE" / "REAL" / "HOJ_ENGINE_REAL_V25.pkl"

# DB 로드
df = pd.read_parquet(DB_PATH)
df["Date"] = pd.to_datetime(df["Date"])

latest_date = df["Date"].max()
print(f"[HOJ] 최신 날짜 = {latest_date}")

df_latest = df[df["Date"] == latest_date].copy()

# 엔진 로드
engine = joblib.load(ENGINE_PATH)
reg = engine["reg_model"]
clf = engine["clf_model"]
feature_cols = engine["feature_cols"]

X = df_latest[feature_cols]

reg_pred = reg.predict(X)
clf_pred = clf.predict_proba(X)[:, 1]

df_latest["reg_pred"] = reg_pred
df_latest["clf_pred"] = clf_pred

df_sorted = df_latest.sort_values("reg_pred", ascending=False)

print("\n=== 상위 10개 (Regression 기준) ===")
print(df_sorted[["Code", "Name", "reg_pred", "clf_pred"]].head(10))

print("\n=== Hybrid Test V34 완료 (HOJ only) ===")