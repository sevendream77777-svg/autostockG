import pandas as pd
import numpy as np
import pickle

# ============================================
#  Hoj V25 연구엔진 최종 성능 검증 스크립트
#  파일명: Hoj_V25_FinalValidation.py
#
#  - 입력 DB: new_Hoj_DB_V25_FULL.parquet
#  - 입력 모델: new_Hoj_MODELENGINE_V25.pkl
#
#  주요 지표:
#   1) RMSE, 분류 정확도 (기본)
#   2) 전체 상관계수(IC)
#   3) 예측값 Decile 별 실제 수익률
#   4) Top 1% / 5% / 10% 수익률 및 양수 비율
#   5) 검증기간 KOSPI 평균 수익률 비교
# ============================================

DB_FILE = "new_Hoj_DB_V25_FULL.parquet"
MODEL_FILE = "new_Hoj_MODELENGINE_V25.pkl"

# 학습/검증 구간 기준 (evaluate_engine_V25.py와 동일하게)
TRAIN_END = "2024-11-04"
VALID_START = "2024-11-05"

FEATURE_COLS = [
    "SMA_20","SMA_40","SMA_60","SMA_90",
    "RSI_14","VOL_SMA_20","MACD","MACD_Sig",
    "BBP_20","ATR_14","STOCH_K","STOCH_D",
    "CCI_20","KOSPI_수익률","ALPHA_SMA_20"
]

TARGET_REG = "Expected_Return_5d"
TARGET_CLS = "Label_5d"
REAL_RET_COL = "Return_5d"  # 실제 5일 수익률

print("=== [1] DB 로드 ===")
df = pd.read_parquet(DB_FILE)
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values(["Date", "Code"])

print(f"전체 행수: {len(df):,}행")
print(f"날짜 범위: {df['Date'].min().date()} ~ {df['Date'].max().date()}")

# --------------------------------------------
# 1-1) 학습/검증 구간 분리
# --------------------------------------------
df_train = df[df["Date"] <= TRAIN_END].copy()
df_valid = df[df["Date"] >= VALID_START].copy()

print("\n=== [2] 학습/검증 구간 ===")
print(f"학습 구간: {df_train['Date'].min().date()} ~ {df_train['Date'].max().date()}  ({len(df_train):,}행)")
print(f"검증 구간: {df_valid['Date'].min().date()} ~ {df_valid['Date'].max().date()}  ({len(df_valid):,}행)")

# --------------------------------------------
# 2) 모델 로드
# --------------------------------------------
print("\n=== [3] 모델 로드 ===")
with open(MODEL_FILE, "rb") as f:
    bundle = pickle.load(f)

reg_model = bundle["reg"]
cls_model = bundle["cls"]

print("회귀 모델, 분류 모델 로드 완료.")

# --------------------------------------------
# 3) 검증 구간에 대한 예측 생성
# --------------------------------------------
print("\n=== [4] 검증 구간 예측 생성 ===")
X_valid = df_valid[FEATURE_COLS]
y_valid_reg = df_valid[TARGET_REG]
y_valid_cls = df_valid[TARGET_CLS]

pred_reg = reg_model.predict(X_valid)
pred_cls = cls_model.predict(X_valid)
pred_cls_bin = (pred_cls > 0.5).astype(int)

# 기본 RMSE, 분류 정확도
rmse = np.sqrt(np.mean((pred_reg - y_valid_reg) ** 2))
acc = (pred_cls_bin == y_valid_cls).mean()

print(f"RMSE (Expected_Return_5d): {rmse:.6f}")
print(f"분류 정확도(Label_5d): {acc:.4f}")

# --------------------------------------------
# 4) 예측값 vs 실제 수익률 상관관계 (IC)
# --------------------------------------------
print("\n=== [5] 상관관계(IC) 분석 ===")

# 실제 5일 수익률이 있는 행만 사용
valid_mask = df_valid[REAL_RET_COL].notna()
df_ic = df_valid.loc[valid_mask].copy()
df_ic["Pred"] = pred_reg[valid_mask.values]

if len(df_ic) == 0:
    print("검증 구간에 실제 5일 수익률(Return_5d)이 없습니다.")
else:
    corr_pearson = df_ic["Pred"].corr(df_ic[REAL_RET_COL], method="pearson")
    corr_spearman = df_ic["Pred"].corr(df_ic[REAL_RET_COL], method="spearman")

    print(f"Pearson 상관계수(IC): {corr_pearson:.4f}")
    print(f"Spearman 상관계수(IC): {corr_spearman:.4f}")

    # 일자별 IC도 확인 (평균, 중앙값 등)
    daily_ic_list = []
    for d, g in df_ic.groupby("Date"):
        if g[REAL_RET_COL].nunique() > 1:
            ic_d = g["Pred"].corr(g[REAL_RET_COL], method="spearman")
            if pd.notna(ic_d):
                daily_ic_list.append(ic_d)

    if len(daily_ic_list) > 0:
        daily_ic_arr = np.array(daily_ic_list)
        print(f"일별 IC 개수: {len(daily_ic_arr)}")
        print(f"일별 IC 평균: {daily_ic_arr.mean():.4f}")
        print(f"일별 IC 중앙값: {np.median(daily_ic_arr):.4f}")
        print(f"일별 IC 최소값: {daily_ic_arr.min():.4f}")
        print(f"일별 IC 최대값: {daily_ic_arr.max():.4f}")

# --------------------------------------------
# 5) 예측값 Decile별 수익률
# --------------------------------------------
print("\n=== [6] 예측값 Decile 분석 ===")

df_dec = df_ic.copy()
# 예측값 기준 전체를 10개 구간으로 나눔
try:
    df_dec["Decile"] = pd.qcut(df_dec["Pred"], 10, labels=False, duplicates="drop") + 1  # 1~10
except ValueError as e:
    print("Decile 계산 중 오류 발생:", e)
    df_dec["Decile"] = np.nan

if df_dec["Decile"].notna().any():
    decile_stats = (
        df_dec
        .groupby("Decile")[REAL_RET_COL]
        .agg(["count", "mean", "median", "min", "max"])
        .reset_index()
        .sort_values("Decile")
    )

    print("Decile 별 실제 5일 수익률 통계:")
    print(decile_stats.to_string(index=False))
else:
    print("Decile 정보를 계산할 수 없습니다.")

# --------------------------------------------
# 6) Top 1% / 5% / 10% 구간 성능
# --------------------------------------------
print("\n=== [7] Top 구간 성능 분석 ===")

df_rank = df_ic.copy()
df_rank = df_rank.sort_values("Pred", ascending=False)
n = len(df_rank)

def top_slice_info(pct):
    k = int(n * pct / 100)
    if k <= 0:
        return None
    sub = df_rank.head(k)
    avg_ret = sub[REAL_RET_COL].mean()
    hit_ratio = (sub[REAL_RET_COL] > 0).mean()
    return k, avg_ret, hit_ratio

for pct in [1, 5, 10]:
    info = top_slice_info(pct)
    if info is None:
        print(f"Top {pct}%: 표본 부족")
    else:
        k, avg_ret, hit_ratio = info
        print(f"Top {pct}% ({k}개 종목 기준): 평균수익 {avg_ret*100:.2f}%, 양수비율 {hit_ratio*100:.2f}%")

# --------------------------------------------
# 7) 검증기간 KOSPI 평균 수익률 비교
# --------------------------------------------
print("\n=== [8] 검증기간 KOSPI 평균 수익률 ===")

if "KOSPI_수익률" in df_valid.columns:
    kospi_valid = df_valid["KOSPI_수익률"].dropna()
    if len(kospi_valid) > 0:
        kospi_mean = kospi_valid.mean()
        print(f"KOSPI 평균 5일 수익률(검증기간): {kospi_mean*100:.2f}%")
    else:
        print("검증 구간에 KOSPI_수익률 데이터가 없습니다.")
else:
    print("DB에 KOSPI_수익률 컬럼이 없습니다.")

print("\n=== 최종 검증 완료 ===")
