import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
import os

# =============================================================
# 설정
# =============================================================
DB_FILE = "new_Hoj_DB_V25_FULL.parquet"
MODEL_FILE = "new_Hoj_MODELENGINE_V25.pkl"
SAVE_FOLDER = "Hoj_MODELREPORT"
os.makedirs(SAVE_FOLDER, exist_ok=True)

# =============================================================
# 1) 데이터 로드
# =============================================================
print("=== [1] DB 로드 ===")
df = pd.read_parquet(DB_FILE)
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values(["Date", "Code"])
print(f"DB 로드 완료 ({len(df):,}행)")

# =============================================================
# 2) 검증 구간 선택
# =============================================================
valid_start = "2024-11-05"
df_valid = df[df["Date"] >= valid_start].copy()
print(f"검증 구간: {df_valid['Date'].min().date()} ~ {df_valid['Date'].max().date()}")

# =============================================================
# 3) 모델 로드
# =============================================================
print("\n=== [2] 모델 로드 ===")
with open(MODEL_FILE, "rb") as f:
    bundle = pickle.load(f)
reg_model = bundle["reg"]
print("회귀 모델 로드 완료!")

FEATURE_COLS = [
    "SMA_20","SMA_40","SMA_60","SMA_90","RSI_14","VOL_SMA_20","MACD","MACD_Sig",
    "BBP_20","ATR_14","STOCH_K","STOCH_D","CCI_20","KOSPI_수익률","ALPHA_SMA_20"
]

# =============================================================
# 4) 예측값 생성
# =============================================================
X_valid = df_valid[FEATURE_COLS]
df_valid["Predicted"] = reg_model.predict(X_valid)

# =============================================================
# 5) 리밸런싱 날짜 선정 (5일 간격)
# =============================================================
all_dates = sorted(df_valid["Date"].unique())
rebalance_dates = all_dates[::5]
print(f"\n리밸런싱 횟수: {len(rebalance_dates)}회")

# =============================================================
# 6) 리밸런싱별 Top10 종목 선택
# =============================================================
top10_list = []   # 각 리밸런싱마다 Top10 코드 리스트
rebalance_info = []

for d in rebalance_dates:
    day_df = df_valid[df_valid["Date"] == d].copy()
    day_df = day_df.sort_values("Predicted", ascending=False)
    top10 = day_df.head(10)

    if len(top10) < 10:
        continue

    codes = list(top10["Code"].values)
    top10_list.append((d, codes))

print("\nTop10 선정 완료.")

# =============================================================
# 7) 일일 자산곡선 생성 (Daily Equity Curve)
# =============================================================
print("\n=== [3] 일일 자산곡선 생성 ===")

equity = 1.0
equity_curve = []

for d, codes in top10_list:
    start_date = d
    # 종료일 = 다음 리밸런싱 5일 뒤
    idx = rebalance_dates.index(d)
    if idx + 1 < len(rebalance_dates):
        end_date = rebalance_dates[idx + 1]
    else:
        end_date = df_valid["Date"].max()

    # 구간 종가 데이터
    slice_df = df_valid[(df_valid["Date"] >= start_date) & 
                        (df_valid["Date"] < end_date)]

    for dt in sorted(slice_df["Date"].unique()):
        day_prices = slice_df[slice_df["Date"] == dt]

        day_ret_list = []

        for code in codes:
            row = day_prices[day_prices["Code"] == code]
            if len(row) == 1:
                daily_ret = row["Return_1d"].values[0]
                day_ret_list.append(daily_ret)

        if len(day_ret_list) == 0:
            continue

        portfolio_ret = np.mean(day_ret_list)
        equity *= (1 + portfolio_ret)
        equity_curve.append(equity)

equity_curve = np.array(equity_curve)

print(f"일일 데이터 길이: {len(equity_curve)}")

# =============================================================
# 8) 실전형 MDD 계산
# =============================================================
peak = np.maximum.accumulate(equity_curve)
dd = equity_curve / peak - 1.0
daily_mdd = dd.min()

# =============================================================
# 9) 실제 CAGR 계산
# =============================================================
total_years = len(equity_curve) / 252
cagr = (equity_curve[-1]) ** (1/total_years) - 1

# =============================================================
# 10) 변동성 / Sharpe Ratio
# =============================================================
daily_returns = pd.Series(equity_curve).pct_change().dropna()
volatility = daily_returns.std() * np.sqrt(252)
sharpe = (daily_returns.mean() * 252) / volatility

# =============================================================
# 11) 그래프 저장
# =============================================================
plt.figure(figsize=(12,6))
plt.plot(equity_curve)
plt.title("Equity Curve (Daily)")
plt.grid(True)
plt.savefig(f"{SAVE_FOLDER}/EquityCurve_V25.png")

# =============================================================
# 12) 리포트 저장
# =============================================================
now = datetime.now().strftime("%y%m%d_%H%M%S")
report_file = f"{SAVE_FOLDER}/REPORT_DailyMDD_V25_{now}.txt"

with open(report_file, "w", encoding="utf-8") as f:
    f.write("=== V25 Daily Backtest Report ===\n")
    f.write(f"일단위 MDD: {daily_mdd*100:.2f}%\n")
    f.write(f"CAGR: {cagr*100:.2f}%\n")
    f.write(f"연변동성: {volatility*100:.2f}%\n")
    f.write(f"Sharpe Ratio: {sharpe:.3f}\n")
    f.write(f"데이터 길이: {len(equity_curve)}일\n")

print("\n=== 백테스트 완료 ===")
print(f"일단위 MDD: {daily_mdd*100:.2f}%")
print(f"CAGR: {cagr*100:.2f}%")
print(f"Sharpe: {sharpe:.3f}")
print(f"리포트 저장 완료 → {report_file}")
