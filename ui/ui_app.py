import streamlit as st

st.set_page_config(page_title="HOJ PRO PLATFORM", layout="wide")

menu = st.sidebar.selectbox(
    "📌 메뉴",
    [
        "대시보드",
        "오늘의 TOP10",
        "개별 종목 예측",
        "매매 프로그램",
        "데이터 업데이트",
        "엔진 관리",
        "SLE 엔진(준비중)",
        "설정",
    ],
)

if menu == "대시보드":
    import ui_pages.dashboard as p; p.render()
elif menu == "오늘의 TOP10":
    import ui_pages.today_top10 as p; p.render()
elif menu == "개별 종목 예측":
    import ui_pages.predict_stock as p; p.render()
elif menu == "매매 프로그램":
    import ui_pages.trading as p; p.render()
elif menu == "데이터 업데이트":
    import ui_pages.data_update as p; p.render()
elif menu == "엔진 관리":
    import ui_pages.engine_manager as p; p.render()
elif menu == "SLE 엔진(준비중)":
    import ui_pages.sle_pending as p; p.render()
elif menu == "설정":
    import ui_pages.settings as p; p.render()
