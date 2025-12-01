# -*- coding: utf-8 -*-
"""
alert_notify.py
- HOJ 엔진 JSON을 읽어 전송용 텍스트와 카드 이미지를 생성하고 저장/반환
- UI 또는 배치에서 이 스크립트를 호출해 카카오/텔레그램/SMS 전송에 활용
- 사용 예:
    python alert_notify.py --json "F:\\autostockG\\MODELENGINE\\INFO\\hoj_engine_info\\HOJ_ENGINE_REAL_*.json" --make-only
    python alert_notify.py --json "F:\\...\\251128.json" --channels kakao,telegram --mode both
옵션:
  --json         : 단일 JSON 파일 경로 또는 glob 패턴
  --channels     : kakao,telegram,sms 중 콤마구분 (기본: none = 생성만)
  --mode         : text | image | both  (기본: both)
  --dark         : 다크 테마 이미지 생성 (기본: 라이트)
  --dry-run      : 실제 전송하지 않고 콘솔 출력
  --make-only    : 파일 생성만(전송 없음)
"""
# ==== import bootstrap (PUT THIS AT TOP) ====
import sys, os
sys.path.append(r"F:\autostockG\MODELENGINE")
sys.path.append(r"F:\autostockG\MODELENGINE\Send")
sys.path.append(r"F:\autostockG\MODELENGINE")        # 패키지 루트
sys.path.append(r"F:\autostockG\MODELENGINE\Send")   # 모듈 직접

# 카카오 (kakao_notifier로 교체)
try:
    from kakao_notifier import KakaoNotifier
    _kakao = KakaoNotifier()
    def send_kakao_message(text=None, image_path=None):
        # 카카오는 현재 TEXT ONLY (나에게 보내기 기준)
        if not text:
            print("[KAKAO] 이미지 전송은 지원하지 않습니다.")
            return False
        return _kakao.send_message(text)
except Exception as _e:
    # 최후방어: 카카오 모듈 불가 시 더미 함수
    def send_kakao_message(text=None, image_path=None):
        print(f"[KAKAO] 모듈 오류: {_e}")
        return False

# 텔레그램 (기존 파일은 send_telegram 이라는 이름일 수 있음)
try:
    from Send import telegram_send as _tg
except ModuleNotFoundError:
    import telegram_send as _tg
send_telegram_message = getattr(_tg, "send_telegram_message", getattr(_tg, "send_telegram", None))
if not callable(send_telegram_message):
    raise ImportError("telegram_send: send_telegram_message 함수가 없습니다.")

# SMS (기존 파일은 send_sms 라는 이름일 수 있음)
try:
    from Send import sms_send as _sm
except ModuleNotFoundError:
    import sms_send as _sm
send_sms_message = getattr(_sm, "send_sms_message", getattr(_sm, "send_sms", None))
if not callable(send_sms_message):
    raise ImportError("sms_send: send_sms_message 함수가 없습니다.")
# ============================================

import os, glob, json, argparse
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# === 전송 모듈(프로젝트 내 Send/...) ===
# send_* 시그니처는 다음을 가정: send_xxx_message(text:Optional[str]=None, image_path:Optional[str]=None) -> bool
#from Send.telegram_send import send_telegram_message
#from Send.sms_send import send_sms_message

# === 이미지 렌더링 ===
from PIL import Image, ImageDraw, ImageFont

# 폰트 후보 (Noto Sans CJK JP 우선)
DEF_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.otf",
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.ttf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.otf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.ttf",
    r"C:\Windows\Fonts\malgun.ttf",  # fallback
]

BEST_TOP_DIR = r"F:\autostockG\MODELENGINE\INFO\best_top"
INFO_DIR     = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

# --------------------------------------------------------------------------------------
# 유틸
# --------------------------------------------------------------------------------------
def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def load_font(size: int) -> ImageFont.FreeTypeFont:
    for p in DEF_FONT_CANDIDATES:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()

def weekday_kr(dt: datetime) -> str:
    # 월화수목금토일
    return ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]

def to_dt(date_str: str) -> datetime:
    # "YYYY-MM-DD"
    return datetime.strptime(date_str, "%Y-%m-%d")

def from_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def next_business_days(start_next_day: datetime, n: int) -> List[datetime]:
    """토/일 제외 영업일 n일 목록 생성"""
    days = []
    cur = start_next_day
    while len(days) < n:
        if cur.weekday() < 5:  # 0~4만 영업일
            days.append(cur)
        cur += timedelta(days=1)
    return days

def infer_engine_basename(json_path: str) -> str:
    # 예: ...\HOJ_ENGINE_REAL_V31_h5_w60_n1000_251128.json -> 같은 이름 .png
    base = os.path.splitext(os.path.basename(json_path))[0]
    return base

def pct_str(v) -> str:
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):.2f}%"
    except Exception:
        return str(v)

def price_str(v) -> str:
    try:
        iv = int(round(float(v)))
        return f"{iv:,}원"
    except Exception:
        return str(v)

# --------------------------------------------------------------------------------------
# 텍스트 생성
# --------------------------------------------------------------------------------------
def make_header_and_period(meta: dict, use_holidays: bool = False) -> Tuple[str, str]:
    """
    meta.prediction_date(YYYY-MM-DD), meta.horizon(int) 기준으로
    영업일 1일차~마지막 날짜 계산(토/일 제외).
    반환:
      - header(박스헤더 문자열)
      - human_period("YYYY-MM-DD(요일) ~ YYYY-MM-DD(요일)")
    """
    version = meta.get("version", "")
    horizon = int(meta.get("horizon", 0) or 0)
    start_base = meta.get("prediction_date", "")  # 예: "2025-11-28"
    if not start_base or horizon <= 0:
        return ("", "")

    d0 = to_dt(start_base) + timedelta(days=1)  # 다음날부터 시작
    biz_days = next_business_days(d0, horizon)
    d_start, d_end = biz_days[0], biz_days[-1]
    human = f"{from_dt(d_start)}({weekday_kr(d_start)}) ~ {from_dt(d_end)}({weekday_kr(d_end)})"

    header = (
        "┌─────────────────────────────────────────────┐\n"
        f"│   HOJ {version} 실전엔진 / {horizon}일 예측            │\n"
        f"│   예측기간: {human}               │\n"
        "└─────────────────────────────────────────────┘\n"
    )
    return header, human

def make_text_message(data: dict, human_period: str) -> str:
    meta = data.get("engine_meta", {})
    items = data.get("top10", [])
    ai    = data.get("ai_report", "")

    header, _ = make_header_and_period(meta)

    # TOPN 제목
    n = len(items)
    lines = [f"\n📊 오늘의 추천 종목 (TOP {n})"]

    # 각 종목: 현재가 포함(A안) + 확률/수익률/기대값 표시
    for i, r in enumerate(items, start=1):
        name = str(r.get("종목명",""))
        code = str(r.get("종목코드",""))
        price = price_str(r.get("현재가", ""))
        prob = pct_str(r.get("상승확률(%)"))
        ret  = pct_str(r.get("예측수익률(%)"))
        ev   = pct_str(r.get("동시적용 기대수익(%)"))
        lines.append(f" {i}) {name} ({code} / {price})  확률 {prob} · 수익률 {ret} · 기대값 {ev}")

    if ai:
        lines.append("\n🤖 AI 분석 요약")
        lines.append(ai)

    return header + "\n".join(lines)

# --------------------------------------------------------------------------------------
# 이미지 렌더링 (가로 540 고정, 세로는 내용에 따라 동적)
# --------------------------------------------------------------------------------------
def render_card_image(data: dict, out_path: str, human_period: str, dark: bool=False) -> str:
    # ---- 레이아웃 파라미터(540 기준) ----
    W = 540                      # 가로 고정
    pad = 24
    head_h = 160
    chip_h = 56
    chip_gap = 18
    row_h = 96                   # 항목 1행 높이
    sep_h = 2
    ai_title_gap = 12
    ai_line_gap = 28

    # 팔레트/폰트
    if not dark:
        bg = (245,247,250); card_bg=(255,255,255); head_bg=(18,38,64); head_fg=(255,255,255)
        chip_bg=(36,128,220); text_main=(20,24,35); text_sub=(90,100,120); sep=(230,234,240); pos=(33,158,90)
    else:
        bg = (12,24,39); card_bg=(22,36,56); head_bg=(11,22,36); head_fg=(235,242,255)
        chip_bg=(58,129,245); text_main=(232,238,250); text_sub=(165,178,196); sep=(48,63,86); pos=(85,200,130)

    f_h1 = load_font(34)
    f_h2 = load_font(24)
    f_chip = load_font(22)
    f_name = load_font(26)
    f_small = load_font(20)
    f_kv = load_font(22)

    # ---------- 1차: 높이 계산 ----------
    items = data.get("top10", [])
    n = len(items)
    # AI 요약 줄수 계산(워드랩)
    ai = str(data.get("ai_report","") or "")
    test_img = Image.new("RGB", (W, 200), bg)
    test_draw = ImageDraw.Draw(test_img)
    card_w = W - pad*2
    col_l = pad + 20
    col_r = W - pad - 20
    wrap_w = col_r - col_l

    ai_lines = []
    if ai:
        words = ai.split()
        line = ""
        for w in words:
            t = (line + " " + w).strip()
            if test_draw.textlength(t, font=f_small) <= wrap_w:
                line = t
            else:
                ai_lines.append(line); line = w
        if line:
            ai_lines.append(line)

    body_h = head_h + chip_h + chip_gap + n*row_h + (sep_h if n>0 else 0)
    ai_h = 0
    if ai:
        ai_h = ai_title_gap + 30 + len(ai_lines) * ai_line_gap

    H = pad + body_h + ai_h + pad

    # ---------- 2차: 실제 렌더 ----------
    img = Image.new("RGB", (W, H), bg)
    draw = ImageDraw.Draw(img)
    card = [pad, pad, W-pad, H-pad]
    draw.rounded_rectangle(card, radius=24, fill=card_bg)

    meta = data.get("engine_meta", {})
    # 헤더
    head = [card[0], card[1], card[2], card[1]+head_h]
    draw.rounded_rectangle(head, radius=24, fill=head_bg)
    draw.text((card[0]+18, card[1]+16), f"G2G GARAGE : HOJ 실전엔진 {meta.get('version','')}", fill=head_fg, font=f_h1)
    draw.text((card[0]+20, card[1]+64), f"{meta.get('horizon','')}일 예측", fill=head_fg, font=f_h2)
    draw.text((card[0]+20, card[1]+96), f"예측기간  {human_period}", fill=head_fg, font=f_h2)

    # 칩
    chip_top = head[3] + 12
    chip = [card[0]+14, chip_top, card[2]-14, chip_top+chip_h]
    draw.rounded_rectangle(chip, radius=18, fill=chip_bg)
    draw.text((chip[0]+16, chip[1]+10), f"오늘의 추천 종목 (TOP {n})", fill=(255,255,255), font=f_chip)

    # 리스트
    start_y = chip[3] + chip_gap
    col_l = card[0]+16
    col_r = card[2]-16
    right_x = col_r - 180  # 우측 KV 시작 x (540폭에 맞게 축소)

    for i, r in enumerate(items, start=1):
        y1 = start_y + (i-1)*row_h
        if i > 1:
            draw.line([(col_l, y1-8), (col_r, y1-8)], fill=sep, width=sep_h)

        name = str(r.get("종목명",""))
        code = str(r.get("종목코드",""))
        price = price_str(r.get("현재가",""))
        prob = pct_str(r.get("상승확률(%)"))
        ret  = pct_str(r.get("예측수익률(%)"))
        ev   = pct_str(r.get("동시적용 기대수익(%)"))

        # 좌측: "1) 종목명 (코드 / 200원)"
        draw.text((col_l, y1+4), f"{i}) {name}", fill=text_main, font=f_name)
        draw.text((col_l, y1+40), f"({code} / {price})", fill=text_sub, font=f_small)

        # 우측: 확률/수익률/기대값
        draw.text((right_x,    y1+2),  "확률",   fill=text_sub, font=f_small); draw.text((right_x+72,  y1+2),  prob, fill=text_main, font=f_kv)
        draw.text((right_x,    y1+30), "수익률", fill=text_sub, font=f_small); draw.text((right_x+72,  y1+30), ret,  fill=text_main, font=f_kv)
        draw.text((right_x,    y1+58), "기대값", fill=text_sub, font=f_small); draw.text((right_x+72,  y1+58), ev,   fill=pos,       font=f_kv)

    # AI 요약
    if ai:
        y0 = start_y + n*row_h + 8
        draw.line([(col_l, y0), (col_r, y0)], fill=sep, width=sep_h)
        draw.text((col_l, y0+ai_title_gap), "🤖 AI 분석 요약", fill=text_main, font=f_chip)
        y = y0 + ai_title_gap + 30
        for ln in ai_lines:
            draw.text((col_l, y), ln, fill=text_sub, font=f_small)
            y += ai_line_gap

    ensure_dir(os.path.dirname(out_path))
    img.save(out_path, "PNG")
    return out_path

# --------------------------------------------------------------------------------------
# 오케스트레이션
# --------------------------------------------------------------------------------------
def build_payload(json_path: str, dark: bool=False):
    """JSON 하나를 받아 텍스트/이미지 생성 후 (text, image_path, human_period) 반환"""
    data = load_json(json_path)
    meta = data.get("engine_meta", {})
    header, human_period = make_header_and_period(meta)
    if not human_period:
        raise ValueError("prediction_date/horizon 정보가 부족합니다.")

    # 텍스트
    text = make_text_message(data, human_period)

    # 이미지 경로 (JSON과 동일 파일명)
    base = infer_engine_basename(json_path)
    out_path = os.path.join(BEST_TOP_DIR, base + ".png")
    render_card_image(data, out_path, human_period, dark=dark)

    return text, out_path, human_period

def send_channels(text: Optional[str], image_path: Optional[str], channels: List[str], mode: str="both", dry_run: bool=False):
    results = {}
    for ch in channels:
        ok = True
        if dry_run:
            print(f"[DRY] {ch}: mode={mode}, text_len={len(text or '')}, image={image_path}")
            results[ch] = True
            continue

        if ch == "kakao":
            # 카카오는 텍스트/이미지 동시 지원. 내부 API 형식에 맞춰 send_kakao_message에서 처리.
            if mode == "text":
                ok = send_kakao_message(text=text, image_path=None)
            elif mode == "image":
                ok = send_kakao_message(text=None, image_path=image_path)
            else:  # both
                ok = send_kakao_message(text=text, image_path=image_path)

        elif ch == "telegram":
            if mode == "text":
                ok = send_telegram_message(text=text, image_path=None)
            elif mode == "image":
                ok = send_telegram_message(text=None, image_path=image_path)
            else:
                ok = send_telegram_message(text=text, image_path=image_path)

        elif ch == "sms":
            # SMS는 이미지 미지원
            if mode == "image":
                ok = False
            else:
                ok = send_sms_message(text=text)
        else:
            ok = False

        results[ch] = ok
    return results

# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def find_latest_json() -> str:
    files = glob.glob(os.path.join(INFO_DIR, "HOJ_ENGINE_REAL_*.json"))
    if not files:
        raise FileNotFoundError("엔진 JSON이 없습니다.")
    files_sorted = sorted(files, key=lambda x: os.path.getmtime(x))
    return files_sorted[-1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=False, default="")
    ap.add_argument("--channels", default="", help="예: kakao,telegram,sms")
    ap.add_argument("--mode", choices=["text","image","both"], default="both")
    ap.add_argument("--dark", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--make-only", action="store_true")
    args = ap.parse_args()

    json_path = args.json.strip()
    if not json_path:
        json_path = find_latest_json()
    # glob 패턴 지원
    matches = glob.glob(json_path)
    if not matches:
        raise FileNotFoundError(f"JSON 경로가 올바르지 않습니다: {json_path}")

    # 여러 개면 최신 1개만 처리(필요시 확장 가능)
    json_path = sorted(matches, key=lambda x: os.path.getmtime(x))[-1]

    text, image_path, human_period = build_payload(json_path, dark=args.dark)

    # --dump-text(호출측 옵션) 대응: stdout에 텍스트를 그대로 출력
    #  p5_send.py에서 capture_output으로 가운데 패널에 그대로 표기함.
    if "--dump-text" in sys.argv:
        print(text)

    print(f"[OK] 생성 완료\n- JSON: {json_path}\n- PERIOD: {human_period}\n- IMAGE: {image_path}\n- TEXT_LEN: {len(text)}")

    if args.make_only:
        return

    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    if channels:
        results = send_channels(text, image_path, channels, mode=args.mode, dry_run=args.dry_run)
        print("[RESULT]", results)

if __name__ == "__main__":
    main()
