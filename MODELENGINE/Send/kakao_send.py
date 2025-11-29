def make_msg(data):
    meta = data.get("engine_meta", {})
    top = data.get("top10", [])
    ai = data.get("ai_report", "")

    version = meta.get("version", "")
    horizon = meta.get("horizon", "")
    pdate = meta.get("prediction_date", "")
    pend = meta.get("prediction_end_date", "")

    # 상단 헤더
    header = (
        f"📊 [HOJ 실전엔진 {version}] {horizon}일 예측\n"
        f"예측기간: {pdate} ~ {pend}\n"
        "━━━━━━━━━━━━━━━━\n"
        "   🔝 오늘의 TOP 10 추천주\n"
        "━━━━━━━━━━━━━━━━\n"
    )

    # TOP10 리스트
    rows = ""
    for r in top:
        rows += (
            f"{r.get('rank')}) {r.get('종목명')} ({r.get('종목코드')})\n"
            f"   - 확률: {r.get('상승확률(%)')}%\n"
            f"   - 수익률: {r.get('예측수익률(%)')}%\n"
            f"   - 기대값: {r.get('동시적용 기대수익(%)')}%\n\n"
        )

    # AI 분석
    ai_block = (
        "━━━━━━━━━━━━━━━━\n"
        "🤖 AI 분석 요약\n"
        f"{ai}\n"
        "━━━━━━━━━━━━━━━━"
    )

    return header + rows + ai_block
