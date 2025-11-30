# -*- coding: utf-8 -*-
"""
card_renderer.py
- HOJ 실전엔진 TOP10을 카드뉴스 PNG로 렌더링
- 사용법(단독 실행):
    python card_renderer.py --json "F:\\autostockG\\MODELENGINE\\INFO\\hoj_engine_info\\HOJ_ENGINE_REAL_*.json" --out "F:\\autostockG\\MODELENGINE\\INFO\\best_top\\card.png" --mode classic
- mode: classic | dark
"""
import os, glob, json, math, argparse
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

DEF_FONT_CANDIDATES = [
    # 사용자 선호: Noto Sans CJK JP 우선
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.otf",
    r"C:\Windows\Fonts\NotoSansCJKjp-Regular.ttf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.otf",
    r"C:\Windows\Fonts\NotoSansKR-Regular.ttf",
    r"C:\Windows\Fonts\malgun.ttf",  # fallback
]

def load_font(size: int):
    for p in DEF_FONT_CANDIDATES:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
    return ImageFont.load_default()

def find_latest_json(pattern: str):
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError("No JSON files matched.")
    files_sorted = sorted(files, key=lambda x: os.path.getmtime(x))
    return files_sorted[-1]

def short_pct(v):
    if v is None or v == "":
        return "-"
    try:
        return f"{float(v):.2f}%"
    except Exception:
        return f"{v}%"

def render_card(data: dict, out_path: str, mode: str = "classic"):
    W, H = 1080, 1920  # 모바일 세로
    img = Image.new("RGB", (W, H), (245, 247, 250) if mode=="classic" else (12, 24, 39))
    draw = ImageDraw.Draw(img)

    # 팔레트
    if mode == "classic":
        card_bg = (255, 255, 255)
        head_bg = (18, 38, 64)
        head_fg = (255, 255, 255)
        title_chip = (36, 128, 220)
        text_main = (20, 24, 35)
        text_sub = (90, 100, 120)
        sep = (230, 234, 240)
        pos = (33, 158, 90)
    else:
        card_bg = (22, 36, 56)
        head_bg = (11, 22, 36)
        head_fg = (235, 242, 255)
        title_chip = (58, 129, 245)
        text_main = (232, 238, 250)
        text_sub = (165, 178, 196)
        sep = (48, 63, 86)
        pos = (85, 200, 130)

    # 폰트
    f_h1 = load_font(56)
    f_h2 = load_font(36)
    f_chip = load_font(34)
    f_name = load_font(40)
    f_small = load_font(30)
    f_kv = load_font(34)

    # 카드 박스
    pad = 40
    card = [pad, pad, W-pad, H-pad]
    draw.rounded_rectangle(card, radius=40, fill=card_bg)

    # 헤더
    head_h = 220
    head = [card[0], card[1], card[2], card[1]+head_h]
    draw.rounded_rectangle(head, radius=40, fill=head_bg)
    meta = data.get("engine_meta", {})
    version = meta.get("version", "")
    horizon = meta.get("horizon", "")
    pdate = meta.get("prediction_date", "")
    pend = meta.get("prediction_end_date", "")

    draw.text((card[0]+40, card[1]+40), f"G2G GARAGE : HOJ 실전엔진 {version}", fill=head_fg, font=f_h1)
    draw.text((card[0]+42, card[1]+120), f"{horizon}일 예측", fill=head_fg, font=f_h2)
    draw.text((card[0]+42, card[1]+168), f"예측기간  {pdate} ~ {pend}", fill=head_fg, font=f_h2)

    # 섹션 타이틀칩
    chip_y = head[3] + 30
    chip = [card[0]+30, chip_y, card[0]+30+500, chip_y+64]
    draw.rounded_rectangle(chip, radius=24, fill=title_chip)
    draw.text((chip[0]+22, chip[1]+12), "오늘의 추천 종목 (TOP 10)", fill=(255,255,255), font=f_chip)

    # 리스트 영역
    top = data.get("top10", [])[:10]
    row_h = 132
    start_y = chip[3] + 20
    col_l = card[0]+40
    col_r = card[2]-40

    for i, r in enumerate(top, start=1):
        row_y1 = start_y + (i-1)*row_h
        row_y2 = row_y1 + row_h - 12
        # 구분선
        if i > 1:
            draw.line([(col_l, row_y1-10), (col_r, row_y1-10)], fill=sep, width=2)

        name = str(r.get("종목명",""))
        code = str(r.get("종목코드",""))
        prob = short_pct(r.get("상승확률(%)"))
        ret  = short_pct(r.get("예측수익률(%)"))
        ev   = short_pct(r.get("동시적용 기대수익(%)"))

        # 좌측: 순번·이름·코드
        draw.text((col_l, row_y1+8), f"{i}) {name}", fill=text_main, font=f_name)
        draw.text((col_l, row_y1+70), f"({code})", fill=text_sub, font=f_small)

        # 우측: 지표 3개
        right_x = col_r - 360
        draw.text((right_x, row_y1+6),  "확률", fill=text_sub, font=f_small)
        draw.text((right_x+140, row_y1+6), prob, fill=text_main, font=f_kv)

        draw.text((right_x, row_y1+46), "수익률", fill=text_sub, font=f_small)
        draw.text((right_x+140, row_y1+46), ret, fill=text_main, font=f_kv)

        draw.text((right_x, row_y1+86), "기대값", fill=text_sub, font=f_small)
        draw.text((right_x+140, row_y1+86), ev, fill=pos, font=f_kv)

    # AI 요약
    ai = data.get("ai_report","")
    if ai:
        y0 = start_y + len(top)*row_h + 14
        draw.line([(col_l, y0), (col_r, y0)], fill=sep, width=2)
        draw.text((col_l, y0+18), "🤖 AI 분석 요약", fill=text_main, font=f_chip)
        # 줄바꿈 처리
        wrap_w = col_r - col_l
        lines = []
        words = str(ai).split()
        line = ""
        test_font = f_small
        for w in words:
            t = (line+" "+w).strip()
            if draw.textlength(t, font=test_font) <= wrap_w:
                line = t
            else:
                lines.append(line); line = w
        if line: lines.append(line)
        y = y0 + 84
        for ln in lines[:8]:
            draw.text((col_l, y), ln, fill=text_sub, font=f_small)
            y += 38

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    img.save(out_path, "PNG")
    return out_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=False, default=r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info\HOJ_ENGINE_REAL_*.json")
    ap.add_argument("--out", required=False, default=r"F:\autostockG\MODELENGINE\INFO\best_top\card.png")
    ap.add_argument("--mode", choices=["classic","dark"], default="classic")
    args = ap.parse_args()

    latest = find_latest_json(args.json)
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)
    out = render_card(data, args.out, mode=args.mode)
    print(f"[OK] Card saved: {out}")

if __name__ == "__main__":
    main()
