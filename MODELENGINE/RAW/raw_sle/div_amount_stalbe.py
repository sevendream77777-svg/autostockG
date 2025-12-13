# -*- coding: utf-8 -*-
"""
div_amount_fsc_2015_2025.py

금융위원회_주식배당정보(getDiviInfo) 기반
2015~2025년 보통주 주당배당금(DPS) 수집 스크립트.

- 엔드포인트: http://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo
- DPS 필드: stckGenrDvdnAmt (1주당 현금 배당 금액)
"""

import requests
import pandas as pd
from pathlib import Path

# -----------------------------------------
# 경로 설정
# -----------------------------------------
KEY_PATH = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\datagokr_apikey.txt"

OUT_DIR = r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\div_amount_data"
OUT_FILE = "div_amount_fsc_2015_2025.csv"

API_URL = "http://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo"

ROWS_PER_PAGE = 10000
START_YEAR = 2015
END_YEAR = 2025


def load_key() -> str:
    """data.go.kr 일반인증키 로드"""
    with open(KEY_PATH, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().strip()


def fetch_all_items(key: str):
    """
    getDiviInfo 전체 페이지를 순회하면서 모든 item을 가져옴.
    basDt, crno, stckIssuCmpyNm 필터는 쓰지 않고 풀테이블 받아서
    로컬에서 연도 조건(2015~2025) 거를 것.
    """
    all_items = []
    page = 1

    while True:
        params = {
            # 공공데이터 예제들에서 통상 'serviceKey' 사용 :contentReference[oaicite:2]{index=2}
            "serviceKey": key,
            "pageNo": page,
            "numOfRows": ROWS_PER_PAGE,
            "resultType": "json",
        }
        r = requests.get(API_URL, params=params, timeout=20)

        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} 응답 (page={page})")

        j = r.json().get("response", {})
        header = j.get("header", {})
        body = j.get("body", {})

        result_code = str(header.get("resultCode", ""))
        if result_code != "00":
            msg = header.get("resultMsg", "")
            raise RuntimeError(f"API 오류 resultCode={result_code}, msg={msg}")

        total_count = int(body.get("totalCount", 0))
        items = body.get("items", {}).get("item", [])

        # item이 dict 한 개로 올 수도 있음
        if isinstance(items, dict):
            items = [items]
        if not items:
            break

        all_items.extend(items)

        # 마지막 페이지까지 도달했으면 종료
        if page * ROWS_PER_PAGE >= total_count:
            break

        page += 1

    return all_items


def main():
    key = load_key()

    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[INFO] 금융위 주식배당정보 전체 수집 중...")
    items = fetch_all_items(key)

    rows = []

    for r in items:
        # 배당기준일자 (dvdnBasDt) 기준으로 연도 필터링
        baseline_date = str(r.get("dvdnBasDt", "")).strip()
        if len(baseline_date) != 8:
            continue

        try:
            year = int(baseline_date[:4])
        except ValueError:
            continue

        if year < START_YEAR or year > END_YEAR:
            continue

        # 보통주만 사용 (scrsItmsKcdNm == '보통주' 등)
        kind_name = (r.get("scrsItmsKcdNm") or "").strip()
        if kind_name and "보통주" not in kind_name:
            continue

        # DPS = 주식일반배당금액(1주당 현금 배당 금액) :contentReference[oaicite:3]{index=3}
        dps_raw = r.get("stckGenrDvdnAmt")
        if dps_raw in (None, "", "NULL"):
            continue

        dps_str = str(dps_raw).replace(",", "").strip()
        try:
            dps_val = float(dps_str)
        except ValueError:
            dps_val = None

        rows.append(
            {
                "year": year,
                "corp_name": r.get("stckIssuCmpyNm"),
                "baseline_date": baseline_date,
                "dps": dps_val,
                "dps_raw": dps_raw,
                "pay_date": str(r.get("cashDvdnPayDt") or "").strip(),
                "scrsItmsKcd": r.get("scrsItmsKcd"),
                "scrsItmsKcdNm": kind_name,
                "basDt": str(r.get("basDt") or "").strip(),
                "crno": r.get("crno"),
            }
        )

    df = pd.DataFrame(rows)
    out_path = out_dir / OUT_FILE
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[완료] {len(df)}행 저장: {out_path}")


if __name__ == "__main__":
    main()
