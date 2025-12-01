# -*- coding: utf-8 -*-
"""
alert_notify.py (완성본)
- AI 분석 파트 전체 출력
- 항목 파싱 및 한국어 줄바꿈 개선
- 이미지 겹침/누락/깨짐 문제 해결
"""

import sys, os, glob, json, argparse
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# ==== PATH BOOTSTRAP ====
sys.path.append(r"F:\autostockG\MODELENGINE")
sys.path.append(r"F:\autostockG\MODELENGINE\Send")
sys.path.append(r"F:\autostockG\MODELENGINE")

# ====== 카카오 전송 ======
try:
    from kakao_notifier import KakaoNotifier
    _kakao = KakaoNotifier()
    def send_kakao_message(text=None, image_path=None):
        if text:
            return _kakao.send_message(text)
        return False
except Exception as _e:
    def send_kakao_message(text=None, image_path=None):
        print(f"[KAKAO] 모듈 오류: {_e}")
        return False

# ====== 텔레그램 전송 ======
try:
    from Send import telegram_send as _tg
except ModuleNotFoundError:
    import telegram_send as _tg

send_telegram_message = getattr(
    _tg,
    "send_telegram_message",
    getattr(_tg, "send_telegram", None)
)
if not callable(send_telegram_message):
    raise ImportError("telegram_send: send_telegram_message 없음")

# ====== SMS 전송 ======
try:
    from Send import sms_send as _sm
except ModuleNotFoundError:
    import sms_send as _sm

send_sms_message = getattr(
    _sm,
    "send_sms_message",
    getattr(_sm, "send_sms", None)
)
if not callable(send_sms_message):
    raise ImportError("sms_send: send_sms_message 없음")

# ====== PIL ======
from PIL import Image, ImageDraw, ImageFont

DEF_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.otf",
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.ttf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.otf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.ttf",
    r"C:\Windows\Fonts\malgun.ttf",
]

BEST_TOP_DIR = r"F:\autostockG\MODELENGINE\INFO\best_top"
INFO_DIR     = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def load_font(size: int) -> ImageFont.FreeTypeFont:
    for p in DEF_FONT_CANDIDATES:
        if os.path.exists(p):
            try: return ImageFont.truetype(p, size)
            except: continue
    return ImageFont.load_default()

def weekday_kr(dt: datetime) -> str:
    return ["월","화","수","목","금","토","일"][dt.weekday()]

def to_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")

def from_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def next_business_days(start_next_day: datetime, n: int) -> List[datetime]:
    days = []
    cur = start_next_day
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days

def infer_engine_basename(json_path: str) -> str:
    return os.path.splitext(os.path.basename(json_path))[0]

def pct_str(v) -> str:
    try: return f"{float(v):.2f}%"
    except: return "-"

def price_str(v):
    try:
        iv = int(round(float(v)))
        return f"{iv:,}원"
    except:
        return str(v)

# =============================================================
# TEXT MESSAGE
# =============================================================
def make_header_and_period(meta: dict):
    version = meta.get("version","")
    horizon = int(meta.get("horizon",0) or 0)
    start_base = meta.get("prediction_date","")

    if not start_base or horizon <= 0:
        return ("","")

    d0 = to_dt(start_base) + timedelta(days=1)
    biz = next_business_days(d0, horizon)
    d_start, d_end = biz[0], biz[-1]
    human = f"{from_dt(d_start)}({weekday_kr(d_start)}) ~ {from_dt(d_end)}({weekday_kr(d_end)})"

    header = (
        "┌─────────────────────────────────────────────┐\n"
        f"│   HOJ {version} 실전엔진 / {horizon}일 예측            │\n"
        f"│   예측기간: {human}               │\n"
        "└─────────────────────────────────────────────┘\n"
    )
    return header, human

def make_text_message(data: dict, human_period: str) -> str:
    meta = data.get("engine_meta",{})
    items = data.get("top10",[])
    ai = data.get("ai_report","")

    header, _ = make_header_and_period(meta)

    lines = [f"\n📊 오늘의 추천 종목 (TOP {len(items)})"]

    for i,r in enumerate(items, start=1):
        lines.append(
            f" {i}) {r.get('종목명','')} ({r.get('종목코드','')} / {price_str(r.get('현재가',''))})"
            f"  확률 {pct_str(r.get('상승확률(%)'))} · 수익률 {pct_str(r.get('예측수익률(%)'))}"
            f" · 기대값 {pct_str(r.get('동시적용 기대수익(%)'))}"
        )

    if ai:
        lines.append("\n🤖 AI 분석 요약")
        lines.append(ai)

    return header + "\n".join(lines)
# =============================================================
# IMAGE RENDER
# =============================================================
def render_card_image(data: dict, out_path: str, human_period: str, dark: bool=False):
    # ---- Layout ----
    W = 540
    pad = 24
    head_h = 160
    chip_h = 56
    chip_gap = 18
    row_h = 96
    sep_h = 2
    ai_title_gap = 12
    ai_line_gap = 28

    # ---- Palette ----
    if not dark:
        bg        = (245,247,250)
        card_bg   = (255,255,255)
        head_bg   = (18,38,64)
        head_fg   = (255,255,255)
        chip_bg   = (36,128,220)
        text_main = (20,24,35)
        text_sub  = (90,100,120)
        sep       = (230,234,240)
        pos       = (33,158,90)
    else:
        bg        = (12,24,39)
        card_bg   = (22,36,56)
        head_bg   = (11,22,36)
        head_fg   = (235,242,255)
        chip_bg   = (58,129,245)
        text_main = (232,238,250)
        text_sub  = (165,178,196)
        sep       = (48,63,86)
        pos       = (85,200,130)

    f_h1    = load_font(34)
    f_h2    = load_font(24)
    f_chip  = load_font(22)
    f_name  = load_font(26)
    f_small = load_font(20)
    f_kv    = load_font(22)

    # ---------- Pre-calc image height ----------
    items = data.get("top10",[])
    n = len(items)

    # 워드랩 측정용 이미지
    test_img = Image.new("RGB",(W,200), bg)
    test_draw = ImageDraw.Draw(test_img)

    col_l = pad + 20
    col_r = W - pad - 20
    wrap_w = col_r - col_l

    # ======================================================
    # 🔥 AI 분석 항목별 전체 추출 + 워드랩 (패치된 핵심 부분)
    # ======================================================
    ai_text = str(data.get("ai_report","") or "").strip()
    ai_lines = []

    import re
    # "1. 디와이디: 문장…" 형태 파싱
    pattern = r"(\d+)\.\s*([^\n]+)"
    matches = re.findall(pattern, ai_text)

    for num, txt in matches:
        merged = f"{num}. {txt.strip()}"
        words = merged.split(" ")
        line = ""
        wrapped = []

        for w in words:
            test = (line + " " + w).strip()
            if test_draw.textlength(test, font=f_small) <= wrap_w:
                line = test
            else:
                wrapped.append(line)
                line = w
        if line:
            wrapped.append(line)

        ai_lines.extend(wrapped)

    # BODY HEIGHT
    body_h = head_h + chip_h + chip_gap + n*row_h + (sep_h if n>0 else 0)
    ai_h = 0
    if ai_lines:
        ai_h = ai_title_gap + 30 + len(ai_lines) * ai_line_gap

    H = pad + body_h + ai_h + pad

    # ---------- Draw ----------
    img = Image.new("RGB",(W,H), bg)
    draw = ImageDraw.Draw(img)

    card = [pad,pad, W-pad, H-pad]
    draw.rounded_rectangle(card, radius=24, fill=card_bg)

    # HEADER
    meta = data.get("engine_meta",{})
    head = [card[0], card[1], card[2], card[1]+head_h]
    draw.rounded_rectangle(head, radius=24, fill=head_bg)

    draw.text((card[0]+18, card[1]+16),
              f"G2G GARAGE : HOJ 실전엔진 {meta.get('version','')}",
              fill=head_fg, font=f_h1)

    draw.text((card[0]+20, card[1]+64),
              f"{meta.get('horizon','')}일 예측",
              fill=head_fg, font=f_h2)

    draw.text((card[0]+20, card[1]+96),
              f"예측기간  {human_period}",
              fill=head_fg, font=f_h2)

    # TOP CHIP
    chip_top = head[3] + 12
    chip = [card[0]+14, chip_top, card[2]-14, chip_top+chip_h]
    draw.rounded_rectangle(chip, radius=18, fill=chip_bg)
    draw.text((chip[0]+16, chip[1]+10),
              f"오늘의 추천 종목 (TOP {n})",
              fill=(255,255,255), font=f_chip)

    # LIST
    start_y = chip[3] + chip_gap
    col_l = card[0] + 16
    col_r = card[2] - 16
    right_x = col_r - 180
    # ==============================
    # LIST RENDER
    # ==============================
    items = data.get("top10",[])
    for i, r in enumerate(items, start=1):
        y = start_y + (i-1)*row_h

        # 번호
        draw.text((col_l, y+6),
                  f"{i}.",
                  fill=text_main, font=f_name)

        name_x = col_l + 36

        # 종목명
        draw.text((name_x, y+6),
                  r.get("종목명",""),
                  fill=text_main, font=f_name)

        # 종목코드 / 현재가
        price_str_form = price_str(r.get("현재가",""))
        code_str = f"{r.get('종목코드','')} / {price_str_form}"
        draw.text((name_x, y+38),
                  code_str,
                  fill=text_sub, font=f_small)

        # 확률/수익률/기대값
        prob = pct_str(r.get("상승확률(%)"))
        ret  = pct_str(r.get("예측수익률(%)"))
        exp  = pct_str(r.get("동시적용 기대수익(%)"))

        val = f"확률 {prob} · 수익률 {ret} · 기대값 {exp}"
        draw.text((right_x, y+18),
                  val,
                  fill=text_sub, font=f_kv)

        # separator
        yy = y + row_h - sep_h
        draw.line([(card[0]+14, yy),
                   (card[2]-14, yy)], fill=sep, width=sep_h)

    # ==============================
    # AI REPORT (패치포인트)
    # ==============================
    if ai_lines:
        y0 = start_y + len(items)*row_h + 12
        draw.line([(card[0]+14, y0),
                   (card[2]-14, y0)], fill=sep, width=sep_h)

        draw.text((card[0]+16, y0+ai_title_gap),
                  "🤖 AI 분석 요약", fill=text_main, font=f_chip)

        y2 = y0 + ai_title_gap + 30
        for ln in ai_lines:
            draw.text((card[0]+16, y2),
                      ln,
                      fill=text_sub, font=f_small)
            y2 += ai_line_gap

    # SAVE
    ensure_dir(os.path.dirname(out_path))
    img.save(out_path, format="PNG")


# =============================================================
# CORE EXECUTION
# =============================================================
def process(json_path: str,
            out_img: str,
            text_only: bool=False,
            to_kakao: bool=False,
            to_telegram: bool=False,
            to_sms: bool=False):

    data = load_json(json_path)
    meta = data.get("engine_meta",{})

    header, human_period = make_header_and_period(meta)

    text_msg = make_text_message(data, human_period)

    if text_only:
        print(text_msg)
        return

    render_card_image(data, out_img, human_period, dark=False)

    if to_kakao:
        send_kakao_message(text=text_msg)

    if to_telegram:
        send_telegram_message(text_msg, image_path=out_img)

    if to_sms:
        send_sms_message(text_msg)


# =============================================================
# MAIN
# =============================================================
def main(**kwargs):
    json_path = kwargs.get("json")
    out_img = kwargs.get("image")
    text_only = kwargs.get("text_only", False)
    to_kakao = kwargs.get("to_kakao", False)
    to_telegram = kwargs.get("to_telegram", False)
    to_sms = kwargs.get("to_sms", False)

    if not json_path:
        print("[ERROR] --json 파일 경로 필수")
        return

    if not out_img:
        basename = infer_engine_basename(json_path)
        out_img = os.path.join(BEST_TOP_DIR, f"{basename}.png")

    process(
        json_path=json_path,
        out_img=out_img,
        text_only=text_only,
        to_kakao=to_kakao,
        to_telegram=to_telegram,
        to_sms=to_sms
    )


# =============================================================
# CLI
# =============================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True)
    ap.add_argument("--image", default=None)
    ap.add_argument("--text-only", action="store_true")
    ap.add_argument("--to-kakao", action="store_true")
    ap.add_argument("--to-telegram", action="store_true")
    ap.add_argument("--to-sms", action="store_true")
    args = ap.parse_args()

    main(
        json=args.json,
        image=args.image,
        text_only=args.text_only,
        to_kakao=args.to_kakao,
        to_telegram=args.to_telegram,
        to_sms=args.to_sms
    )
