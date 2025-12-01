# -*- coding: utf-8 -*-
"""
alert_notify.py — 600x1200 카카오 기준 템플릿(완성본)
- 캔버스 고정 600x1200
- TOP10 리스트 항상 이미지 포함
- AI '종합해설'은 자동 줄바꿈/자동 폰트축소로 시도, 공간에 안 들어가면 이미지에서 제외(텍스트만 전송)
- 종목별 개별 해설은 이미지에 넣지 않음(텍스트 전용)
- --text-only 지원(표시 텍스트 stdout)
- --to-kakao / --to-telegram / --to-sms 지원
기존 경로/옵션과 100% 호환
"""

import sys, os, json, argparse
from datetime import datetime, timedelta
from typing import List, Tuple

# ==== PATH BOOTSTRAP ====
sys.path.append(r"F:\autostockG\MODELENGINE")
sys.path.append(r"F:\autostockG\MODELENGINE\Send")

# ====== 카카오 전송 ======
def _load_kakao():
    try:
        from api.kakao_api.kakao_notifier import KakaoNotifier  # 권장 경로
    except Exception:
        try:
            from kakao_notifier import KakaoNotifier  # 구 경로 호환
        except Exception as e:
            return None, e
    try:
        return KakaoNotifier(), None
    except Exception as e:
        return None, e

_KAKAO, _KAKAO_ERR = _load_kakao()

def send_kakao_message(text=None, image_path=None):
    if _KAKAO is None:
        if _KAKAO_ERR:
            print(f"[KAKAO] 모듈 오류: {_KAKAO_ERR}")
        return False
    # 이미지 업로드 로직이 별도일 수 있어, 우선 텍스트만 보냄(요구사항상 텍스트 중심)
    try:
        return _KAKAO.send_message(text or "")
    except Exception as e:
        print(f"[KAKAO] 전송 실패: {e}")
        return False

# ====== 텔레그램 전송 ======
def _load_telegram():
    try:
        from Send import telegram_send as _tg
    except Exception:
        try:
            import telegram_send as _tg
        except Exception as e:
            return None, e
    fn = getattr(_tg, "send_telegram_message", getattr(_tg, "send_telegram", None))
    return (fn if callable(fn) else None), (None if callable(fn) else "send_telegram_message 없음")

_SEND_TG, _TG_ERR = _load_telegram()

def send_telegram_message(text, image_path=None):
    if _SEND_TG is None:
        if _TG_ERR:
            print(f"[TG] 모듈 오류: {_TG_ERR}")
        return False
    try:
        return _SEND_TG(text, image_path=image_path)
    except TypeError:
        # 구형 시그니처 호환
        try:
            return _SEND_TG(text)
        except Exception as e:
            print(f"[TG] 전송 실패: {e}")
            return False
    except Exception as e:
        print(f"[TG] 전송 실패: {e}")
        return False

# ====== SMS 전송 ======
def _load_sms():
    try:
        from Send import sms_send as _sm
    except Exception:
        try:
            import sms_send as _sm
        except Exception as e:
            return None, e
    fn = getattr(_sm, "send_sms_message", getattr(_sm, "send_sms", None))
    return (fn if callable(fn) else None), (None if callable(fn) else "send_sms_message 없음")

_SEND_SMS, _SMS_ERR = _load_sms()

def send_sms_message(text):
    if _SEND_SMS is None:
        if _SMS_ERR:
            print(f"[SMS] 모듈 오류: {_SMS_ERR}")
        return False
    try:
        return _SEND_SMS(text)
    except Exception as e:
        print(f"[SMS] 전송 실패: {e}")
        return False

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

# ----------------- UTIL -----------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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
    days, cur = [], start_next_day
    while len(days) < max(1, n):
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

# ----------------- TEXT GEN -----------------
def make_header_and_period(meta: dict):
    version = meta.get("version","");
    horizon = int(meta.get("horizon",0) or 0)
    start_base = meta.get("prediction_date","");

    if not start_base or horizon <= 0:
        return ("","");

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
    ai_all = str(data.get("ai_report","") or "").strip()

    header, _ = make_header_and_period(meta)

    lines = [f"\n📊 오늘의 추천 종목 (TOP {len(items)})"]
    for i,r in enumerate(items, start=1):
        lines.append(
            f" {i}) {r.get('종목명','')} ({r.get('종목코드','')} / {price_str(r.get('현재가',''))})"
            f"  확률 {pct_str(r.get('상승확률(%)'))} · 수익률 {pct_str(r.get('예측수익률(%)'))} · 기대값 {pct_str(r.get('동시적용 기대수익(%)'))}"
        )

    if ai_all:
        lines.append("\n🤖 AI 종합 해설")
        lines.append(ai_all)

    return header + "\n".join(lines)

# ----------------- IMAGE RENDER (600x1200) -----------------
class Canvas600x1200:
    W, H = 600, 1200

    def __init__(self, dark=False):
        self.dark = dark
        if not dark:
            self.bg        = (245,247,250)
            self.card_bg   = (255,255,255)
            self.head_bg   = (18,38,64)
            self.head_fg   = (255,255,255)
            self.chip_bg   = (36,128,220)
            self.text_main = (20,24,35)
            self.text_sub  = (90,100,120)
            self.sep       = (230,234,240)
        else:
            self.bg        = (12,24,39)
            self.card_bg   = (22,36,56)
            self.head_bg   = (11,22,36)
            self.head_fg   = (235,242,255)
            self.chip_bg   = (58,129,245)
            self.text_main = (232,238,250)
            self.text_sub  = (165,178,196)
            self.sep       = (48,63,86)

        self.pad = 18
        self.head_h = 140
        self.chip_h = 48
        self.row_h  = 90
        self.gap    = 12

        # 폰트
        self.f_h1    = load_font(30)
        self.f_h2    = load_font(22)
        self.f_chip  = load_font(20)
        self.f_name  = load_font(24)
        self.f_small = load_font(18)
        self.f_kv    = load_font(20)

    def _wrap(self, draw, text, font, max_w):
        words = str(text).split(" ")
        line = ""
        out = []
        for w in words:
            t = (line + " " + w).strip()
            if draw.textlength(t, font=font) <= max_w:
                line = t
            else:
                if line: out.append(line)
                line = w
        if line: out.append(line)
        return out

    def render(self, data: dict, out_path: str, human_period: str) -> bool:
        from PIL import ImageDraw

        W, H = self.W, self.H
        img = Image.new("RGB", (W, H), self.bg)
        draw = ImageDraw.Draw(img)

        card = [self.pad, self.pad, W - self.pad, H - self.pad]
        # 카드 배경
        try:
            draw.rounded_rectangle(card, radius=20, fill=self.card_bg)
        except Exception:
            draw.rectangle(card, fill=self.card_bg)

        # 헤더
        head = [card[0], card[1], card[2], card[1] + self.head_h]
        try:
            draw.rounded_rectangle(head, radius=20, fill=self.head_bg)
        except Exception:
            draw.rectangle(head, fill=self.head_bg)

        meta = data.get("engine_meta",{})
        items = data.get("top10",[])

        draw.text((head[0]+14, head[1]+12),
                  f"G2G GARAGE : HOJ 실전엔진 {meta.get('version','')}",
                  fill=self.head_fg, font=self.f_h1)
        draw.text((head[0]+14, head[1]+56),
                  f"{meta.get('horizon','')}일 예측",
                  fill=self.head_fg, font=self.f_h2)
        draw.text((head[0]+14, head[1]+86),
                  f"예측기간  {human_period}",
                  fill=self.head_fg, font=self.f_h2)

        # TOP 영역 타이틀 칩
        chip_top = head[3] + 10
        chip = [card[0]+10, chip_top, card[2]-10, chip_top + self.chip_h]
        try:
            draw.rounded_rectangle(chip, radius=14, fill=self.chip_bg)
        except Exception:
            draw.rectangle(chip, fill=self.chip_bg)
        draw.text((chip[0]+12, chip[1]+10),
                  f"오늘의 추천 종목 (TOP {len(items)})",
                  fill=(255,255,255), font=self.f_chip)

        # 리스트 렌더링
        list_top = chip[3] + self.gap
        available_h_for_list = (card[3] - list_top) - (self.gap + 270)  # 하단 여유(종합해설 예상 최대)
        # 항목 줄수 계산: row_h 기준으로 가능한 만큼만 그림
        max_rows = max(1, min(len(items), available_h_for_list // self.row_h))
        for i in range(max_rows):
            r = items[i]
            y = list_top + i * self.row_h
            # 번호
            draw.text((card[0]+14, y+4), f"{i+1}.", fill=self.text_main, font=self.f_name)
            name_x = card[0]+14+34
            draw.text((name_x, y+4), r.get("종목명",""), fill=self.text_main, font=self.f_name)

            code_price = f"{r.get('종목코드','')} / {price_str(r.get('현재가',''))}"
            draw.text((name_x, y+36), code_price, fill=self.text_sub, font=self.f_small)

            prob = pct_str(r.get("상승확률(%)")); ret = pct_str(r.get("예측수익률(%)")); exp = pct_str(r.get("동시적용 기대수익(%)"))
            kv = f"확률 {prob} · 수익률 {ret} · 기대값 {exp}"
            draw.text((card[2]-10- draw.textlength(kv, font=self.f_kv), y+20),
                      kv, fill=self.text_sub, font=self.f_kv)

            # 구분선
            yy = y + self.row_h - 2
            draw.line([(card[0]+10, yy), (card[2]-10, yy)], fill=self.sep, width=2)

        # 종합해설 블록(자동 축소/줄수 제한)
        ai_full = str(data.get("ai_report","") or "").strip()
        if ai_full:
            # 제목
            ai_title_y = list_top + max_rows * self.row_h + 10
            draw.line([(card[0]+10, ai_title_y),(card[2]-10, ai_title_y)], fill=self.sep, width=2)
            draw.text((card[0]+14, ai_title_y + 10), "🤖 AI 종합 해설", fill=self.text_main, font=self.f_chip)

            # 본문 사각형
            area_top = ai_title_y + 10 + 28
            area_bottom = card[3] - 14
            area_left, area_right = card[0]+14, card[2]-14
            area_w = area_right - area_left
            area_h = area_bottom - area_top

            # 폰트 크기 자동 조정 + 워드랩
            min_size, max_size = 14, 20
            chosen_font = None
            wrapped_lines = None

            for size in range(max_size, min_size-1, -1):
                f = load_font(size)
                # 문장 단위 줄바꿈 (한글 공백기준 기본)
                lines = []
                # 기준: 문단을 공백 기준으로 wrap
                words = ai_full.split(" ")
                line = ""
                for w in words:
                    t = (line + " " + w).strip()
                    if ImageDraw.Draw(Image.new("RGB", (10,10))).textlength(t, font=f) <= area_w:
                        line = t
                    else:
                        if line: lines.append(line)
                        line = w
                if line: lines.append(line)

                # 높이 계산
                line_h = int(size * 1.45)
                need_h = len(lines) * line_h
                if need_h <= area_h and len(lines) <= max(3, area_h // line_h):
                    chosen_font = f
                    wrapped_lines = lines
                    break

            include_ai = chosen_font is not None
            if include_ai:
                y = area_top
                line_h = int(chosen_font.size * 1.45)
                for ln in wrapped_lines:
                    ImageDraw.Draw(img).text((area_left, y), ln, fill=self.text_sub, font=chosen_font)
                    y += line_h
            # include_ai == False 이면 이미지에 종합해설 미포함(텍스트 전용)

        ensure_dir(os.path.dirname(out_path))
        img.save(out_path, format="PNG")
        return True

# ----------------- CORE -----------------
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

    canvas = Canvas600x1200(dark=False)
    canvas.render(data, out_img, human_period)

    if to_kakao:
        send_kakao_message(text=text_msg)

    if to_telegram:
        send_telegram_message(text_msg, image_path=out_img)

    if to_sms:
        send_sms_message(text_msg)

# ----------------- MAIN/CLI -----------------
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
