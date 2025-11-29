
import json
import os
from Send.kakao_send import send_kakao_message
from Send.telegram_send import send_telegram_message
from Send.sms_send import send_sms_message

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def make_box_table_message(data):
    meta = data.get("engine_meta", {})
    top10 = data.get("top10", [])
    ai = data.get("ai_report", "")

    header = (
        "┌─────────────────────────────────────────────┐\n"
        f"│   HOJ {meta.get('version')} 실전엔진 / {meta.get('horizon')}일 예측            │\n"
        f"│   예측기간: {meta.get('prediction_date')} ~ {meta.get('prediction_end_date','')}     │\n"
        "└─────────────────────────────────────────────┘\n"
    )

    body = (
        "\n📊 오늘의 추천 종목 (TOP 10)\n\n"
        "┌────┬──────────────┬────────┬────────┬────────┬────────┬────────┐\n"
        "│순위│종목명         │코드     │현재가   │확률(%) │수익률(%)│기대값(%)│\n"
        "├────┼──────────────┼────────┼────────┼────────┼────────┼────────┤\n"
    )

    rows = ""
    for r in top10:
        rows += (
            f"│{str(r.get('rank')).ljust(4)}│{r.get('종목명','')[:12].ljust(12)}│"
            f"{r.get('종목코드','').ljust(8)}│{str(r.get('종가','')).ljust(8)}│"
            f"{str(r.get('상승확률(%)','')).rjust(7)}│{str(r.get('예측수익률(%)','')).rjust(9)}│"
            f"{str(r.get('동시적용 기대수익(%)','')).rjust(9)}│\n"
        )

    table_end = "└────┴──────────────┴────────┴────────┴────────┴────────┴────────┘"

    ai_part = f"\n\n🤖 AI 분석 요약\n{ai}"

    return header + body + rows + table_end + ai_part

def run(json_path, target="kakao"):
    data = load_json(json_path)
    msg = make_box_table_message(data)

    if target == "kakao":
        return send_kakao_message(msg)
    elif target == "telegram":
        return send_telegram_message(msg)
    elif target == "sms":
        return send_sms_message(msg)
    else:
        print("Unknown target.")
        return False
