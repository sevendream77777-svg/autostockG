import streamlit as st
import pandas as pd
import glob
import os

from config_paths import TOP10_DIR


def render():
    st.title("🔥 오늘의 TOP10 추천")

    if not TOP10_DIR or not os.path.exists(TOP10_DIR):
        st.error(f"TOP10_DIR 폴더가 없습니다: {TOP10_DIR}")
        return

    files = sorted(
        glob.glob(os.path.join(TOP10_DIR, "recommendation_HOJ_*.csv")),
        reverse=True,
    )
    if not files:
        st.error("추천 결과 파일(recommendation_HOJ_*.csv)이 없습니다.")
        return

    latest = files[0]
    st.info(f"최신 추천 파일: {latest}")

    try:
        df = pd.read_csv(latest)
    except Exception as e:
        st.error(f"CSV 로드 오류: {e}")
        return

    rename_map = {
        "종목명": "Name",
        "Code": "Code",
        "현재가": "Price",
        "예상수익률(%)": "ExpectedReturnPct",
        "예상수익률": "ExpectedReturn",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 최신 streamlit 권장 방식
    st.dataframe(df, width="stretch")

    if "Code" in df.columns:
        codes = df["Code"].astype(str).tolist()
        selected = st.multiselect("매수 후보 종목코드 선택", options=codes, default=codes[:3])
        st.session_state["buy_candidates"] = selected
        st.write("📌 현재 매수 후보:", selected)
    else:
        st.info("Code 컬럼이 없어 매수 후보 선택 기능은 비활성화됩니다.")
