
import streamlit as st
import subprocess

def render():
    st.title("🔄 데이터 업데이트")

    if st.button("run_weekly_update.py 실행"):
        st.write("업데이트 실행...")
        try:
            out=subprocess.check_output(["python","run_weekly_update.py"], text=True)
            st.text(out)
        except Exception as e:
            st.error(str(e))
