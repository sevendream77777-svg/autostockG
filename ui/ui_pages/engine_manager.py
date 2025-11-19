import streamlit as st
import os
import json

from engine_helper import ENGINE_DIR, get_engine_list, get_current_engine, OVERRIDE_FILE


def save_current_engine(path: str):
    os.makedirs(ENGINE_DIR, exist_ok=True)
    data = {"engine": path}
    with open(OVERRIDE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def render():
    st.title("🧠 엔진 관리 (HOJ ENGINE REAL)")

    st.write(f"엔진 폴더: `{ENGINE_DIR}`")

    engines = get_engine_list()
    if not engines:
        st.error("엔진 파일(HOJ_ENGINE_REAL_*.pkl)을 찾을 수 없습니다.")
        return

    current = get_current_engine()
    if current:
        st.success(f"현재 사용 중인 엔진:\n{current}")
    else:
        st.warning("현재 사용 중인 엔진을 결정할 수 없습니다.")

    st.markdown("---")
    st.subheader("엔진 선택")

    labels = [os.path.basename(p) for p in engines]

    index_default = 0
    if current and current in engines:
        index_default = engines.index(current)

    selected_label = st.radio(
        "사용할 엔진을 선택하세요:",
        labels,
        index=index_default if engines else 0,
    )

    selected_path = engines[labels.index(selected_label)]

    if st.button("✅ 이 엔진 사용하기"):
        save_current_engine(selected_path)
        st.success(f"선택된 엔진으로 변경되었습니다:\n{selected_path}")
        st.info("predict_stock / TOP10 / 대시보드 등에서 이 엔진을 기준으로 동작합니다.")
