# ===============================================
#  check_nulls_in_V25_DB.py
#  Hoj 엔진 학습 전 전체 결측률 자동 분석 스크립트
#  작성: 호봉이 (GPT-5), 2025-11-13
# ===============================================

import pandas as pd
from datetime import timedelta

print("=================================================")
print("[check_nulls_in_V25_DB.py] ▶️ 실행 시작...")
print("=================================================")

# 1) DB 로드
try:
    df = pd.read_parquet("new_Hoj_DB_V25.parquet")
    print(f"✅ DB 로드 완료: {len(df):,} 행")
except Exception as e:
    print(f"❌ DB 로드 실패: {e}")
    exit()

# 2) 날짜 정렬 및 기본 정보
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date")

start_date = df["Date"].min().date()
end_date = df["Date"].max().date()

print(f"📅 데이터 기간: {start_date} ~ {end_date}")

# 3) 주요 피처 목록
feature_cols = [
    "SMA_20","SMA_40","SMA_60","SMA_90","RSI_14",
    "VOL_SMA_20","MACD","MACD_Sig","BBP_20","ATR_14",
    "STOCH_K","STOCH_D","CCI_20","KOSPI_수익률","ALPHA_SMA_20"
]

target_cols = ["Expected_Return_5d", "Return_5d", "Label_5d"]

# 4) 전체 결측률 계산
print("\n📊 전체 결측률:")
print("-------------------------------------------")
null_report = {}

for col in feature_cols + target_cols:
    if col in df.columns:
        null_ratio = df[col].isna().mean() * 100
        null_report[col] = null_ratio
        print(f"{col:<18}: {null_ratio:6.2f}%")
    else:
        print(f"{col:<18}: ❌ 존재하지 않음")

print("-------------------------------------------")

# 5) 최근 1년(검증구간) 결측률 분석
cutoff = df["Date"].max() - timedelta(days=365)
df_valid = df[df["Date"] >= cutoff]

print(f"\n📅 검증 구간: {df_valid['Date'].min().date()} ~ {df_valid['Date'].max().date()}")
print(f"📌 검증 구간 행 수: {len(df_valid):,}")

print("\n📊 검증 구간 결측률:")
print("-------------------------------------------")
valid_null_report = {}
for col in feature_cols + target_cols:
    if col in df_valid.columns:
        null_ratio = df_valid[col].isna().mean() * 100
        valid_null_report[col] = null_ratio
        print(f"{col:<18}: {null_ratio:6.2f}%")
print("-------------------------------------------")

# 6) 결측으로 인해 제거될 행 수 계산 (학습 기준)
df_reg = df.dropna(subset=["Expected_Return_5d"] if "Expected_Return_5d" in df.columns else ["Return_5d"])
removed_rows = len(df) - len(df_reg)

print(f"\n🧹 학습 시 결측으로 제거될 행 수: {removed_rows:,} 행")

# 7) 결측이 없는 첫 날짜 감지 (검증구간 시작 검증)
first_valid_date = None
req_cols = feature_cols + ["Expected_Return_5d"] if "Expected_Return_5d" in df.columns else feature_cols + ["Return_5d"]

for date in sorted(df_valid["Date"].unique()):
    tmp = df_valid[df_valid["Date"] == date]
    if not tmp[req_cols].isna().any().any():
        first_valid_date = date
        break

if first_valid_date:
    print(f"📌 결측이 없는 검증 첫 날짜: {first_valid_date.date()}")
else:
    print("⚠️ 검증 구간에서 결측이 없는 날짜를 찾지 못했습니다.")

# 8) 종목별 결측률 Top 30
print("\n🔍 종목별 결측률 TOP 30 (전체 기준):")
code_nulls = df.groupby("Code")[feature_cols + target_cols].apply(lambda x: x.isna().mean().mean() * 100)
code_nulls = code_nulls.sort_values(ascending=False).head(30)

print(code_nulls.to_string())

# 9) 요약 출력
print("\n=================================================")
print("[check_nulls_in_V25_DB.py] ▶️ 분석 완료")
print("=================================================")
