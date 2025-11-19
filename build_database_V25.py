# --- build_database_V25.py (V25 Hoj DB 생성 스크립트 - 풀버전) ---
# V25_Hoj_DB.parquet / new_Hoj_DB_V25.parquet 생성
# - 입력: all_features_cumulative_V21_Hoj.parquet
# - 출력: V25용 피처 + 라벨(Return_5d, Expected_Return_5d, Label_5d)

import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np

# ---------------------------------------
# 1. 기본 설정
# ---------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FEATURE_FILE = os.path.join(BASE_DIR, "all_features_cumulative_V21_Hoj.parquet")

OUT_V25_DB = os.path.join(BASE_DIR, "V25_Hoj_DB.parquet")
OUT_NEW_DB = os.path.join(BASE_DIR, "new_Hoj_DB_V25.parquet")

# 콘솔 인코딩 문제 방지 (UTF-8 강제)
try:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass


def find_price_column(df: pd.DataFrame) -> str:
    """
    시세(종가) 컬럼 자동 탐색
    """
    candidates = ["Close", "close", "종가", "현재가", "Price"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"가격 컬럼을 찾을 수 없습니다. (후보: {candidates})")


def find_code_column(df: pd.DataFrame) -> str:
    """
    종목코드 컬럼 자동 탐색
    """
    candidates = ["code", "Code", "티커"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"종목코드 컬럼을 찾을 수 없습니다. (후보: {candidates})")


def find_date_column(df: pd.DataFrame) -> str:
    """
    날짜 컬럼 자동 탐색
    """
    candidates = ["date", "Date", "날짜"]
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"날짜 컬럼을 찾을 수 없습니다. (후보: {candidates})")


def add_labels(df: pd.DataFrame,
               code_col: str,
               date_col: str,
               price_col: str,
               horizon: int = 5) -> pd.DataFrame:
    """
    코드/날짜 기준으로 정렬 후
    - Return_5d: 5일 후 수익률
    - Expected_Return_5d: 모델이 예측할 타깃 (현재는 Return_5d와 동일하게 설정)
    - Label_5d: 5일 수익률이 0보다 크면 1, 아니면 0
    """

    df = df.sort_values([code_col, date_col]).copy()

    # 5일 후 종가
    df["_future_price"] = (
        df.groupby(code_col)[price_col]
        .shift(-horizon)
    )

    # 수익률 계산: (미래가격 / 현재가격 - 1)
    df["Return_5d"] = (df["_future_price"] / df[price_col]) - 1.0

    # Expected_Return_5d: 회귀 타깃 (지금은 동일하게 사용)
    df["Expected_Return_5d"] = df["Return_5d"]

    # Label_5d: 분류 타깃 (양수 수익률이면 1, 아니면 0)
    df["Label_5d"] = (df["Return_5d"] > 0).astype(int)

    # 미래 가격이 없는(마지막 4일 등) 행은 학습에 사용 불가 → 제거
    df = df.dropna(subset=["Return_5d", "Expected_Return_5d"]).copy()

    # 보조 컬럼 제거
    df = df.drop(columns=["_future_price"])

    return df


def main():
    print("=" * 80)
    print("[build_database_V25.py] ▶️ 실행 시작... (V25 FULL DB 빌드)")
    print("=" * 80)

    if not os.path.exists(INPUT_FEATURE_FILE):
        raise FileNotFoundError(
            f"입력 피처 파일을 찾을 수 없습니다: {INPUT_FEATURE_FILE}"
        )

    # ---------------------------------------
    # 2. V21 피처 로드
    # ---------------------------------------
    print(f"📥 V21 피처 로드 시도: {INPUT_FEATURE_FILE}")
    df = pd.read_parquet(INPUT_FEATURE_FILE)
    print(f"✅ V21 피처 로드 완료: {len(df):,} 행")

    # ---------------------------------------
    # 3. 기본 컬럼 파악 (code/date/price)
    # ---------------------------------------
    code_col = find_code_column(df)
    date_col = find_date_column(df)
    price_col = find_price_column(df)

    print(f"🔍 코드 컬럼: {code_col}, 날짜 컬럼: {date_col}, 가격 컬럼: {price_col}")

    # 형식 정리
    df[code_col] = df[code_col].astype(str)
    df[date_col] = pd.to_datetime(df[date_col])

    # ---------------------------------------
    # 4. 피처 결측치/이상치 정리 (필요 최소 수준)
    #    - 너무 과도하게 드랍하지 않고, 기본적인 NaN만 제거
    # ---------------------------------------
    # 피처 컬럼(타깃/메타 제외) 대략 추출
    exclude_cols = {
        code_col,
        date_col,
        price_col,
    }
    # 이미 있으면 같이 제외
    exclude_cols.update({"Return_5d", "Expected_Return_5d", "Label_5d"})

    feature_cols = [c for c in df.columns if c not in exclude_cols]

    # 피처만 기준으로 NaN 제거
    before = len(df)
    df = df.dropna(subset=feature_cols)
    after = len(df)
    print(f"🧹 피처 NaN 제거: {before:,} → {after:,} 행")

    # ---------------------------------------
    # 5. 라벨 생성 (Return_5d, Expected_Return_5d, Label_5d)
    # ---------------------------------------
    print("🎯 5일 수익률 라벨 생성 중 (Return_5d / Expected_Return_5d / Label_5d)...")
    df = add_labels(df, code_col=code_col, date_col=date_col, price_col=price_col, horizon=5)
    print(f"✅ 라벨 생성 완료: {len(df):,} 행")

    # ---------------------------------------
    # 6. 정렬 및 최종 컬럼 정리
    # ---------------------------------------
    df = df.sort_values([date_col, code_col]).reset_index(drop=True)

    # 컬럼 순서: [기본 메타] + [피처] + [라벨]
    ordered_cols = [date_col, code_col, price_col]
    # 피처
    ordered_cols += [c for c in feature_cols if c not in ordered_cols]
    # 라벨
    for c in ["Return_5d", "Expected_Return_5d", "Label_5d"]:
        if c in df.columns:
            ordered_cols.append(c)

    df = df[ordered_cols]

    # ---------------------------------------
    # 7. 저장
    # ---------------------------------------
    print(f"💾 [저장] V25_Hoj_DB.parquet → {OUT_V25_DB}")
    df.to_parquet(OUT_V25_DB, index=False)

    print(f"💾 [저장] new_Hoj_DB_V25.parquet → {OUT_NEW_DB}")
    df.to_parquet(OUT_NEW_DB, index=False)

    print("=" * 80)
    print(f"[build_database_V25.py] ✅ 성공 | 최종 행 수: {len(df):,}")
    print("   - V25_Hoj_DB.parquet")
    print("   - new_Hoj_DB_V25.parquet")
    print("=" * 80)


if __name__ == "__main__":
    main()
