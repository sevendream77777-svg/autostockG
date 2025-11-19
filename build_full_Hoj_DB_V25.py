import pandas as pd

# -------------------------------------------
# 1) 원본 시세 데이터 로드
# -------------------------------------------
print("[1] 시세 파일 로드 중...")

stocks = pd.read_parquet("all_stocks_cumulative.parquet")
stocks["Date"] = pd.to_datetime(stocks["Date"])
stocks = stocks.sort_values(["Code", "Date"])

print(f"시세 데이터: {len(stocks):,}행")
print("컬럼 확인:", list(stocks.columns))


# -------------------------------------------
# 2) Return_1d 생성
# -------------------------------------------
print("\n[2] Return_1d 생성 중...")

# 종가 컬럼명 자동 감지
price_col_candidates = ["Close", "close", "종가"]
price_col = None

for c in price_col_candidates:
    if c in stocks.columns:
        price_col = c
        break

if price_col is None:
    raise ValueError("❌ 종가(가격) 컬럼을 찾을 수 없습니다. Close 또는 종가 컬럼 필요.")

# Return_1d = 오늘종가 / 전일종가 - 1
stocks["Return_1d"] = stocks.groupby("Code")[price_col].pct_change()

print("Return_1d 생성 완료.")


# -------------------------------------------
# 3) 기존 V25 DB 로드
# -------------------------------------------
print("\n[3] V25 DB 로드 중...")

db = pd.read_parquet("new_Hoj_DB_V25.parquet")
db["Date"] = pd.to_datetime(db["Date"])

print(f"DB 로드: {len(db):,}행")
print("기존 DB 컬럼:", list(db.columns))


# -------------------------------------------
# 4) Merge 수행 (Date + Code 기준)
# -------------------------------------------
print("\n[4] Merge 시작 (Date + Code 기준)...")

merge_cols = ["Date", "Code"]

merged = pd.merge(
    db,
    stocks[merge_cols + ["Return_1d"]],
    how="left",
    on=merge_cols
)

print(f"병합 후: {len(merged):,}행")
print("병합된 컬럼:", list(merged.columns))


# -------------------------------------------
# 5) 결측치 처리
# -------------------------------------------
print("\n[5] 결측치 처리 중...")

before = len(merged)
merged = merged.dropna(subset=["Return_1d"])   # Return_1d 없는 경우 제거
after = len(merged)

print(f"결측치 제거: {before - after:,}행 제거")
print(f"최종 DB 크기: {after:,}행")


# -------------------------------------------
# 6) 최종 저장
# -------------------------------------------
output_file = "new_Hoj_DB_V25_FULL.parquet"
merged.to_parquet(output_file, index=False)

print(f"\n🎉 [완료] FULL DB 생성됨 → {output_file}")
