from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# test_REAL_HOJ_V25.py
# -------------------------------------------------------------
# REAL_HOJ_ENGINE_V25 실전 엔진 동작 테스트 스크립트
# - HOJ_DB_REAL_V25.parquet 기준 최신 날짜 Top10 점검
# -------------------------------------------------------------

import os
import sys
import pandas as pd
import joblib

# ==============================
# [0] 경로/상수 정의
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_REAL = os.path.join(BASE_DIR, HOJ_DB_REAL)
ENGINE_REAL = os.path.join(BASE_DIR, HOJ_ENGINE_REAL)

TOP_N = 10  # Top N 종목 출력


def load_real_db(db_path: str):
    """실전용 DB 로드 + 날짜 컬럼 자동 인식"""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"실전용 DB 파일을 찾을 수 없습니다: {db_path}")

    print(f"[DB] 로드 중: {db_path}")
    df = pd.read_parquet(db_path)

    # 날짜 컬럼 자동 탐지
    date_col = None
    for cand in ["date", "Date", "날짜", "DATE"]:
        if cand in df.columns:
            date_col = cand
            break

    if date_col is None:
        # dtype 기준 fallback
        dt_cols = df.select_dtypes(include=["datetime64[ns]", "datetime64"]).columns
        if len(dt_cols) > 0:
            date_col = dt_cols[0]
        else:
            raise KeyError("날짜 컬럼(date/Date/날짜/DATE)을 찾을 수 없습니다.")

    df[date_col] = pd.to_datetime(df[date_col])

    print(f"[DB] 전체 행수: {len(df):,}행")
    print(f"[DB] 날짜 범위: {df[date_col].min()} ~ {df[date_col].max()}")

    return df, date_col


def load_real_engine(model_path: str):
    """REAL_HOJ 엔진 로드"""
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"실전 엔진 파일을 찾을 수 없습니다: {model_path}")

    print(f"[MODEL] 실전 엔진 로드 중: {model_path}")
    engine = joblib.load(model_path)

    if not isinstance(engine, dict):
        raise TypeError("엔진 파일 구조가 dict가 아닙니다. (reg_model, clf_model, feature_cols 필요)")

    reg_model = engine.get("reg_model") or engine.get("regressor") or engine.get("reg")
    clf_model = engine.get("clf_model") or engine.get("classifier") or engine.get("clf")
    feature_cols = engine.get("feature_cols") or engine.get("feature_columns") or engine.get("features")

    if reg_model is None or clf_model is None:
        raise ValueError("엔진에 reg_model 또는 clf_model이 없습니다.")

    if feature_cols is None:
        # 최후의 방어: V25 표준 피처 강제
        feature_cols = [
            "SMA_20", "SMA_40", "SMA_60", "SMA_90",
            "RSI_14", "VOL_SMA_20",
            "MACD", "MACD_Sig",
            "BBP_20", "ATR_14",
            "STOCH_K", "STOCH_D",
            "CCI_20", "KOSPI_수익률", "ALPHA_SMA_20",
        ]
        print("[WARN] feature_cols 메타 없음 → V25 표준 15개 피처 사용")

    print(f"[MODEL] 회귀/분류 모델 로드 완료")
    print(f"[MODEL] 피처 개수: {len(feature_cols)}개")
    print(f"[MODEL] 피처 목록: {feature_cols}")

    return reg_model, clf_model, feature_cols


def pick_latest_slice(df: pd.DataFrame, date_col: str):
    """가장 최신 날짜 슬라이스 추출"""
    latest_date = df[date_col].max()
    today_df = df[df[date_col] == latest_date].copy()
    print(f"\n[DATA] 최신 날짜: {latest_date.date()} | 행수: {len(today_df):,}행")

    if len(today_df) == 0:
        raise ValueError("최신 날짜 데이터가 0행입니다.")
    return today_df, latest_date


def detect_code_col(df: pd.DataFrame):
    """종목코드 컬럼 자동 탐지"""
    for cand in ["code", "Code", "종목코드", "티커"]:
        if cand in df.columns:
            return cand
    return None


def detect_name_col(df: pd.DataFrame):
    """종목명 컬럼 자동 탐지"""
    for cand in ["name", "Name", "종목명"]:
        if cand in df.columns:
            return cand
    return None


def main():
    try:
        # 1) 실전용 DB 로드
        df, date_col = load_real_db(DB_REAL)

        # 2) 실전 엔진 로드
        reg_model, clf_model, feature_cols = load_real_engine(ENGINE_REAL)

        # 3) 최신 날짜 데이터
        today_df, latest_date = pick_latest_slice(df, date_col)

        # 4) 피처 존재 여부 확인
        missing = [c for c in feature_cols if c not in today_df.columns]
        if missing:
            print("\n[ERROR] DB에 없는 피처 컬럼이 있습니다:")
            for c in missing:
                print(" -", c)
            sys.exit(1)

        # 5) 결측 제거
        X = today_df[feature_cols].copy()
        mask_valid = X.notnull().all(axis=1)
        X = X[mask_valid]
        valid_df = today_df.loc[mask_valid].copy()

        print(f"[DATA] 결측 제거 후 유효 행수: {len(valid_df):,}행")

        # 6) 예측 수행
        print("\n[PREDICT] 회귀/분류 예측 실행 중...")
        valid_df["Expected_Return_5d_pred"] = reg_model.predict(X)

        if hasattr(clf_model, "predict_proba"):
            proba = clf_model.predict_proba(X)
            if proba.shape[1] == 2:
                valid_df["Prob_Up"] = proba[:, 1]
            else:
                valid_df["Prob_Up"] = proba.max(axis=1)
        else:
            valid_df["Prob_Up"] = clf_model.predict(X)

        # 7) Top N 정렬
        valid_df_sorted = valid_df.sort_values("Expected_Return_5d_pred", ascending=False)
        top_df = valid_df_sorted.head(TOP_N).copy()

        code_col = detect_code_col(top_df)
        name_col = detect_name_col(top_df)

        print("\n" + "=" * 60)
        print(f" REAL_HOJ_ENGINE_V25 실전 엔진 Top {TOP_N}")
        print(f" 기준일: {latest_date.date()}")
        print("=" * 60)

        for i, (_, row) in enumerate(top_df.iterrows(), start=1):
            code_str = row[code_col] if code_col else "N/A"
            name_str = row[name_col] if name_col else ""
            pred_ret = row["Expected_Return_5d_pred"]
            prob_up = row["Prob_Up"]

            label_info = ""
            for cand in ["Label_5d", "label_5d", "label"]:
                if cand in top_df.columns:
                    label_info = f", 실제 Label: {row[cand]}"
                    break

            print(
                f"[{i:02d}] {code_str} {name_str} | "
                f"예측수익률(5d): {pred_ret:.4f} | "
                f"상승확률: {prob_up:.3f}{label_info}"
            )

        print("\n[OK] 실전 엔진 예측 테스트 완료.")

    except Exception as e:
        print("\n[EXCEPTION] 테스트 중 오류 발생:")
        print(f"  -> {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
