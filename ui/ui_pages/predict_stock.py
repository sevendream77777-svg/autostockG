import streamlit as st
import pandas as pd
import joblib
import matplotlib.pyplot as plt

from config_paths import HOJ_DB_REAL
from engine_helper import get_current_engine


@st.cache_resource
def load_engine():
    engine_path = get_current_engine()
    if not engine_path:
        raise FileNotFoundError("사용할 엔진 파일을 찾을 수 없습니다.")
    model = joblib.load(engine_path)
    reg = model.get("reg_model")
    cls = model.get("clf_model")
    feature_cols = model.get("feature_cols")
    if reg is None or feature_cols is None:
        raise RuntimeError("엔진 파일에 reg_model 또는 feature_cols 정보가 없습니다.")
    return reg, cls, feature_cols, engine_path


@st.cache_data
def load_db():
    if not HOJ_DB_REAL:
        raise FileNotFoundError("HOJ_DB_REAL 경로가 설정되지 않았습니다.")
    df = pd.read_parquet(HOJ_DB_REAL)

    code_col = None
    for cand in ["code", "Code", "티커"]:
        if cand in df.columns:
            code_col = cand
            break
    if not code_col:
        raise KeyError("종목코드 컬럼(code/Code/티커)을 찾을 수 없습니다.")
    df["code"] = df[code_col].astype(str)

    date_col = None
    for cand in ["date", "Date", "날짜"]:
        if cand in df.columns:
            date_col = cand
            break
    if not date_col:
        raise KeyError("날짜 컬럼(date/Date/날짜)을 찾을 수 없습니다.")
    df["date"] = pd.to_datetime(df[date_col])

    return df


def render():
    st.title("🔍 개별 종목 예측 (HOJ 실전 엔진)")

    ticker = st.text_input("종목코드 (예: 005930)", max_chars=6)

    if st.button("예측하기"):
        if not ticker:
            st.warning("종목코드를 입력하세요.")
            return

        with st.spinner("예측 중..."):
            try:
                df = load_db()
                reg, cls, feature_cols, eng_path = load_engine()
            except Exception as e:
                st.error(f"로딩 오류: {e}")
                return

            sub = df[df["code"] == str(ticker).strip()].copy()
            if sub.empty:
                st.error("해당 종목을 DB에서 찾을 수 없습니다.")
                return

            latest_date = sub["date"].max()
            row = sub[sub["date"] == latest_date]
            X = row[feature_cols]

            pred = float(reg.predict(X)[0])
            prob = None
            if cls is not None:
                try:
                    proba = cls.predict_proba(X)[0]
                    prob = float(proba[1])
                except Exception:
                    prob = None

        st.success(f"📅 기준일: {latest_date.strftime('%Y-%m-%d')}")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("5일 예상수익률", f"{pred * 100:.2f}%")
        with col2:
            if prob is not None:
                st.metric("상승 확률", f"{prob * 100:.1f}%")
            else:
                st.metric("상승 확률", "N/A")

        st.markdown("---")
        st.subheader("📉 최근 60일 가격 차트")

        hist = sub.sort_values("date").copy()
        hist = hist[hist["date"] <= latest_date].tail(60)

        price_col = None
        for cand in ["Close", "close", "종가", "현재가", "Price"]:
            if cand in hist.columns:
                price_col = cand
                break

        if price_col:
            fig, ax = plt.subplots(figsize=(8, 3))
            ax.plot(hist["date"], hist[price_col])
            ax.set_xlabel("날짜")
            ax.set_ylabel(price_col)
            ax.grid(True)
            st.pyplot(fig)
        else:
            st.info("가격 컬럼(Close/종가/현재가)을 찾을 수 없습니다.")

        st.markdown("---")
        st.subheader("🧬 사용된 피처 값 (최신 1일)")

        feat_row = row[feature_cols].T.reset_index()
        feat_row.columns = ["Feature", "Value"]
        st.dataframe(feat_row, width="stretch")

        st.caption(f"사용 엔진: {eng_path}")
