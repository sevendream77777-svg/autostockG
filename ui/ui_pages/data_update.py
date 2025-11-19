
import streamlit as st
import subprocess
import os

ROOT = r"F:\autostockG"


def render():
    st.title("🔄 데이터 업데이트 (기본 세팅)")

    st.info("현재 구성은 run_weekly_update.py 단일 실행 기반입니다.")

    if st.button("전체 업데이트 실행 (run_weekly_update.py)"):
        st.write("실행 중…")
        try:
            target = os.path.join(ROOT, "run_weekly_update.py")
            out = subprocess.check_output(["python", target], text=True)
            st.text(out)
        except Exception as e:
            st.error(f"실행 오류: {e}")

    st.markdown("---")
    st.write("📌 추후: 단계별 업데이트(시세/수급/PBRPER/병합) 버튼 추가 예정.")
