import pandas as pd
import lightgbm as lgb
import numpy as np
import os
import pickle
from datetime import datetime

# ==========================================================
#  설정
# ==========================================================
DB_FILE = "new_Hoj_DB_V25.parquet"
MODEL_FILE = "new_Hoj_MODELENGINE_V25.pkl"
REPORT_FOLDER = "Hoj_MODELREPORT"

os.makedirs(REPORT_FOLDER, exist_ok=True)

# ==========================================================
#  1. 데이터 로드
# ==========================================================
print("=== [1] DB 로드 ===")
df = pd.read_parquet(DB_FILE)
df = df.sort_values(["Date", "Code"])
print(f"DB 로드 완료 ({len(df):,}행)")

# ==========================================================
#  2. 학습/검증 구간 분리
# ==========================================================
train_end = "2024-11-04"
valid_start = "2024-11-05"

df["Date"] = pd.to_datetime(df["Date"])

df_train = df[df["Date"] <= train_end]
df_valid = df[df["Date"] >= valid_start]

print(f"학습 구간: {df_train['Date'].min().date()} ~ {df_train['Date'].max().date()}  ({len(df_train):,}행)")
print(f"검증 구간: {df_valid['Date'].min().date()} ~ {df_valid['Date'].max().date()}  ({len(df_valid):,}행)")

# ==========================================================
#  3. 피처 목록
# ==========================================================
FEATURE_COLS = [
    "SMA_20","SMA_40","SMA_60","SMA_90","RSI_14","VOL_SMA_20","MACD","MACD_Sig",
    "BBP_20","ATR_14","STOCH_K","STOCH_D","CCI_20","KOSPI_수익률","ALPHA_SMA_20"
]

TARGET_REG = "Expected_Return_5d"
TARGET_CLS = "Label_5d"

# ==========================================================
#  4. 검증용 데이터 준비
# ==========================================================
X_valid = df_valid[FEATURE_COLS]
y_valid_reg = df_valid[TARGET_REG]
y_valid_cls = df_valid[TARGET_CLS]

# ==========================================================
#  5. 모델 로드 (Pickle)
# ==========================================================
print("\n=== [2] 모델 로드 ===")
with open(MODEL_FILE, "rb") as f:
    bundle = pickle.load(f)

reg_model = bundle["reg"]
cls_model = bundle["cls"]

print("회귀 모델, 분류 모델 로드 완료!")

# ==========================================================
#  6. 회귀 검증 (RMSE)
# ==========================================================
print("\n=== [3] 회귀 엔진 검증 (RMSE) ===")
pred_valid_reg = reg_model.predict(X_valid)

rmse = np.sqrt(np.mean((pred_valid_reg - y_valid_reg) ** 2))
print(f"RMSE: {rmse:.6f}")

# ==========================================================
#  7. 분류 검증 (정확도)
# ==========================================================
print("\n=== [4] 분류 엔진 검증 (정확도) ===")
pred_valid_cls = cls_model.predict(X_valid)
pred_valid_cls = (pred_valid_cls > 0.5).astype(int)

accuracy = (pred_valid_cls == y_valid_cls).mean()
print(f"정확도: {accuracy:.4f}")

# ==========================================================
#  유틸 함수: CAGR, MDD
# ==========================================================
def calc_cagr(returns, period_days=5):
    returns = np.array(returns, dtype=float)
    if returns.size == 0:
        return 0.0
    total_ret = np.prod(1.0 + returns)
    years = (len(returns) * period_days) / 252.0
    if years <= 0:
        return 0.0
    return total_ret ** (1.0 / years) - 1.0

def calc_mdd(returns):
    returns = np.array(returns, dtype=float)
    if returns.size == 0:
        return 0.0
    equity_curve = np.cumprod(1.0 + returns)
    peak = np.maximum.accumulate(equity_curve)
    drawdown = equity_curve / peak - 1.0
    return drawdown.min()

# ==========================================================
#  8. Top10 전략 1년 백테스트 (5일 보유 / 5일 리밸런싱)
# ==========================================================
print("\n=== [5] Top10 전략 1년 백테스트 (5일 보유 / 5일 리밸런싱) ===")

df_valid_bt = df_valid.copy()
df_valid_bt.loc[:, "Predicted"] = pred_valid_reg

all_dates = sorted(df_valid_bt["Date"].unique())
if len(all_dates) == 0:
    print("검증 구간에 거래일이 없습니다.")
    top10_returns = []
    kospi_returns = []
else:
    rebalance_indices = list(range(0, len(all_dates), 5))
    rebalance_dates = [all_dates[i] for i in rebalance_indices]

    top10_returns = []
    kospi_returns = []

    for d in rebalance_dates:
        day_df = df_valid_bt[df_valid_bt["Date"] == d].copy()
        if day_df.empty:
            continue

        day_df = day_df.sort_values("Predicted", ascending=False)
        top10 = day_df.head(10)

        if len(top10) < 10:
            continue

        valid_top10 = top10.dropna(subset=["Return_5d"])
        if valid_top10.empty:
            continue

        strat_ret = valid_top10["Return_5d"].mean()
        top10_returns.append(strat_ret)

        kospi_slice = day_df["KOSPI_수익률"].dropna()
        if not kospi_slice.empty:
            kospi_returns.append(kospi_slice.mean())

    if len(top10_returns) == 0:
        print("유효한 리밸런싱 구간이 없어 수익률을 계산할 수 없습니다.")
    else:
        top10_cum = np.prod(1.0 + np.array(top10_returns)) - 1.0
        top10_cagr = calc_cagr(top10_returns)
        top10_mdd = calc_mdd(top10_returns)

        print(f"Top10 전략 누적 수익률: {top10_cum * 100:.2f}%")
        print(f"Top10 전략 CAGR: {top10_cagr * 100:.2f}%")
        print(f"Top10 전략 MDD: {top10_mdd * 100:.2f}%")

# ==========================================================
#  9. KOSPI 비교 (동일 리밸런싱 구간 기준)
# ==========================================================
print("\n=== [6] KOSPI 대비 ===")

if len(kospi_returns) == 0:
    print("KOSPI 기준 수익률을 계산할 수 없습니다.")
    kospi_cum = 0.0
    kospi_cagr = 0.0
else:
    kospi_array = np.array(kospi_returns, dtype=float)
    kospi_cum = np.prod(1.0 + kospi_array) - 1.0
    kospi_cagr = calc_cagr(kospi_array)
    print(f"KOSPI 누적 수익률: {kospi_cum * 100:.2f}%")
    print(f"KOSPI CAGR: {kospi_cagr * 100:.2f}%")

ai_edge = (top10_cum - kospi_cum) if len(top10_returns) > 0 and len(kospi_returns) > 0 else 0.0
print(f"AI 우위 (누적): {ai_edge * 100:.2f}%p")

# ==========================================================
#  ★ 추가 기능 — Top10 Return_5d 분포 분석
# ==========================================================
if len(top10_returns) > 0:
    arr = np.array(top10_returns)

    print("\n=== [추가 분석] Top10 Return_5d 분포 ===")
    print(f"개수: {len(arr)}")
    print(f"평균: {arr.mean():.4f}")
    print(f"중앙값: {np.median(arr):.4f}")
    print(f"표준편차: {arr.std():.4f}")
    print(f"최소값: {arr.min():.4f}")
    print(f"최대값: {arr.max():.4f}")

    for p in [1, 5, 95, 99]:
        print(f"{p}퍼센타일: {np.percentile(arr, p):.4f}")

# ==========================================================
# 10. 리포트 저장
# ==========================================================
print("\n=== [7] 리포트 저장 ===")

now = datetime.now().strftime("%y%m%d_%H%M%S")
report_file = f"{REPORT_FOLDER}/REPORT_V25_{now}.txt"

with open(report_file, "w", encoding="utf-8") as f:
    f.write("=== V25 엔진 검증 리포트 ===\n")
    f.write(f"RMSE: {rmse:.6f}\n")
    f.write(f"정확도: {accuracy:.4f}\n")

    if len(top10_returns) > 0:
        f.write(f"Top10 누적 수익률: {top10_cum*100:.2f}%\n")
        f.write(f"Top10 CAGR: {top10_cagr*100:.2f}%\n")
        f.write(f"Top10 MDD: {top10_mdd*100:.2f}%\n")
    else:
        f.write("Top10 수익률: 계산 불가(유효 리밸런싱 구간 없음)\n")

    f.write(f"KOSPI 누적 수익률: {kospi_cum*100:.2f}%\n")
    f.write(f"KOSPI CAGR: {kospi_cagr*100:.2f}%\n")
    f.write(f"AI 우위(누적): {ai_edge*100:.2f}%p\n")

print(f"리포트 저장 완료 → {report_file}")
print("\n=== 검증 완료 ===")

# ==========================================================
#  [추가 기능 #1] Top10 Return_5d 시각화
# ==========================================================
import matplotlib.pyplot as plt

if len(top10_returns) > 0:
    arr = np.array(top10_returns)

    plt.figure(figsize=(10,6))
    plt.hist(arr, bins=50)
    plt.title("Top10 5-Day Return Distribution")
    plt.xlabel("Return_5d")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()

    # Boxplot
    plt.figure(figsize=(8,4))
    plt.boxplot(arr, vert=False)
    plt.title("Top10 5-Day Return Boxplot")
    plt.xlabel("Return_5d")
    plt.grid(True)
    plt.show()

else:
    print("Top10 수익률 데이터가 없어 시각화를 건너뜁니다.")

# ==========================================================
#  [추가 기능 #2] 일 단위 MDD 재계산
# ==========================================================

def calc_daily_mdd(df_valid_bt, rebalance_dates, top10_returns):
    """
    일 단위로 자산曲線을 만들고 MDD 재계산
    """
    daily_equity = []
    equity = 1.0

    for i, d in enumerate(rebalance_dates):
        # 리밸런싱 날짜 가져오기
        if i >= len(top10_returns):
            break

        ret_5d = top10_returns[i]

        # 5일간의 실제 데이터를 가져와서 일단위로 변환 필요
        # d ~ d+4일까지 Return_1d 로 확장
        slice_df = df_valid_bt[df_valid_bt["Date"] == d]

        if slice_df.empty:
            continue

        # Return_5d → Return_1d 로 균등 분배(근사)
        # ※ 실제 안정적으로 하려면 daily close 데이터 필요
        daily_rate = (1 + ret_5d) ** (1/5) - 1

        # 5일 동안 누적 적용
        for _ in range(5):
            equity *= (1 + daily_rate)
            daily_equity.append(equity)

    daily_equity = np.array(daily_equity)
    if len(daily_equity) == 0:
        print("일 단위 MDD 계산 불가")
        return 0.0

    peak = np.maximum.accumulate(daily_equity)
    dd = daily_equity / peak - 1.0
    return dd.min()

