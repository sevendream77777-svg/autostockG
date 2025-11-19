import streamlit as st
import os
from datetime import datetime

from config_paths import HOJ_DB_REAL, TOP10_DIR
from engine_helper import get_current_engine


def _fmt_time(path):
    if not path or not os.path.exists(path):
        return "파일 없음"
    t = datetime.fromtimestamp(os.path.getmtime(path))
    return t.strftime("%Y-%m-%d %H:%M:%S")


def render():
    st.title("📊 대시보드")

    st.subheader("데이터 / 엔진 상태")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📁 HOJ_DB_REAL**")
        st.write(HOJ_DB_REAL or "None")
        st.write("갱신:", _fmt_time(HOJ_DB_REAL))
    with col2:
        current_engine = get_current_engine()
        st.markdown("**🧠 현재 사용 엔진**")
        st.write(current_engine or "None")
        st.write("갱신:", _fmt_time(current_engine) if current_engine else "파일 없음")

    st.markdown("---")
    st.subheader("🔥 최신 TOP10 파일")

    latest_top10 = None
    if TOP10_DIR and os.path.exists(TOP10_DIR):
        import glob
        files = glob.glob(os.path.join(TOP10_DIR, "recommendation_HOJ_*.csv"))
        if files:
            latest_top10 = max(files, key=os.path.getmtime)

    if latest_top10:
        st.write(latest_top10)
    else:
        st.write("TOP10 파일 없음")

    st.markdown("---")
    st.subheader("🔗 시스템 플로우 (요약)")

    st.markdown(
        """
        1. **데이터 업데이트**: run_weekly_update.py  
        2. **DB 생성**: HOJ_DB_REAL_*.parquet  
        3. **엔진 학습**: HOJ_ENGINE_REAL_*.pkl  
        4. **추천 생성**: recommendation_HOJ_*.csv  
        5. **UI**: 오늘의 TOP10 / 개별 예측 / 매매 프로그램
        """
    )
