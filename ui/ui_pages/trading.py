

import streamlit as st
import subprocess
import os

ROOT = r"F:\autostockG"


def render():
    st.title("💹 매매 프로그램 (기본 세팅)")

    st.info("현재는 main_trading.py 직접 실행 기반입니다.")

    if st.button("매매 실행 (main_trading.py)"):
        st.write("실행 중…")
        try:
            target = os.path.join(ROOT, "main_trading.py")
            out = subprocess.check_output(["python", target], text=True)
            st.text(out)
        except Exception as e:
            st.error(f"실행 오류: {e}")

    st.markdown("---")
    st.subheader("매수 후보 (오늘의 TOP10에서 선택된 종목)")

    candidates = st.session_state.get("buy_candidates", [])
    if candidates:
        st.write(candidates)
    else:
        st.write("선택된 매수 후보가 없습니다. '오늘의 TOP10' 페이지에서 선택하세요.")
