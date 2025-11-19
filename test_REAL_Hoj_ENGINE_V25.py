import pandas as pd
import joblib
from config_paths import (
    HOJ_DB_REAL,
    HOJ_ENGINE_REAL
)

print("[TEST] HOJ 실전 엔진 테스트 시작")

# --- 1) DB 로드 ---
if HOJ_DB_REAL is None:
    raise FileNotFoundError("HOJ_DB_REAL 경로를 찾을 수 없습니다.")

print(f"[DB] 로드 중: {HOJ_DB_REAL}")
df = pd.read_parquet(HOJ_DB_REAL)

# --- 날짜 컬럼 정규화 ---
if "date" not in df.columns:
    if "Date" in df.columns:
        df.rename(columns={"Date": "date"}, inplace=True)
    else:
        raise KeyError("날짜 컬럼(Date/date)이 존재하지 않습니다.")

print(f"[DB] 전체 행수: {len(df)}")
print(f"[DB] 날짜 범위: {df['Date'].min()} ~ {df['Date'].max()}")

# --- 2) 엔진 로드 ---
if HOJ_ENGINE_REAL is None:
    raise FileNotFoundError("HOJ_ENGINE_REAL 경로를 찾을 수 없습니다.")

print(f"[ENGINE] 로드 중: {HOJ_ENGINE_REAL}")
hoj_engine = joblib.load(engine_path)

# --- 3) 최신 날짜 데이터 테스트 ---
latest_date = df["date"].max()
test_df = df[df["date"] == latest_date].copy()

if len(test_df) == 0:
    raise RuntimeError(f"테스트할 데이터가 없습니다. ({latest_date})")

print(f"[TEST] 최신 날짜 테스트: {latest_date}, 종목 {len(test_df)}개")

# feature 컬럼 자동 리스트
feature_cols = [col for col in test_df.columns if col.startswith(("SMA", "RSI", "MACD", "BBP", "ATR", "STOCH", "CCI", "VOL", "ALPHA", "KOSPI"))]

X = test_df[feature_cols]
preds = hoj_engine['reg_model'].predict(X)

test_df["pred"] = preds
top10 = test_df.sort_values("pred", ascending=False).head(10)

print("\n[RESULT] Top10 예측 결과:")
print(top10[["Code", "pred"]].to_string(index=False))

print("\n[완료] HOJ 실전 엔진 테스트 완료!")
