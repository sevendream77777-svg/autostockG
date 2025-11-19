# raw_checker.py
import pandas as pd
import numpy as np
import os

RAW_PATH = r"F:\autostockG\MODELENGINE\RAW\all_stocks_cumulative.parquet"

print("\n========= RAW 오염 진단 시작 =========")

df = pd.read_parquet(RAW_PATH)

# 기본 정규화
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date", "Code"])

# 날짜별 종목수
cnt = df.groupby("Date")["Code"].nunique()

print("\n[1] 날짜별 종목수 체크 (정상=3200~3800)")
print(cnt.tail(10))

# 완전 오염 날짜
bad_full = cnt[cnt < 1000]
print("\n[❌ 완전 오염 날짜]")
print(bad_full)

# 부분 오염 날짜(1000~3500)
bad_partial = cnt[(cnt >= 1000) & (cnt < 3500)]
print("\n[⚠ 부분 오염 날짜]")
print(bad_partial)

# 각 날짜에서 종목 단위 결측/이상값 체크
print("\n[2] 종목 단위 결측/이상값 검사]")

bad_rows = df[
    (df["Close"] <= 0) |
    (df["High"] < df["Low"]) |
    (df["Volume"] <= 0) |
    (df["Open"].isna()) |
    (df["High"].isna()) |
    (df["Low"].isna()) |
    (df["Close"].isna()) |
    (df["Volume"].isna())
]

print(f"총 결측/이상 데이터 개수: {len(bad_rows)}")
print(bad_rows.head(20))

# 날짜 기준으로 정리
bad_by_date = bad_rows.groupby("Date")["Code"].count()
print("\n[날짜별 결측 종목 개수]")
print(bad_by_date.sort_index())

print("\n========= RAW 오염 진단 종료 =========")
