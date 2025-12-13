# -*- coding: utf-8 -*-
"""
div_amount_test_FINAL_KEY.py
- 금융위원회_주식배당정보(div_amount) 테스트용 완전체
- 요청한 대로 APIKEY를 코드 내부에 직접 박아서 제공
"""

import requests
import xml.etree.ElementTree as ET
import json

API_KEY = "9c3cf7dd64c3f256bc2533ea8698751579ccbd7df0bf5489c5493abce4a99f7b"
URL = "https://apis.data.go.kr/1160100/service/GetStocDiviInfoService/getDiviInfo"

def get_div(company, bas_dt):
    params = {
        "serviceKey": API_KEY,
        "numOfRows": "100",
        "pageNo": "1",
        "resultType": "xml",     # 원문 먼저 확인
        "stckIssuCmpyNm": company,
        "dvdnBasDt": bas_dt,
    }

    r = requests.get(URL, params=params, timeout=10)
    print("STATUS =", r.status_code)
    print("URL =", r.url)
    print("----- RAW XML -----")
    print(r.text)

    # XML → 파싱
    root = ET.fromstring(r.text)
    items = root.find(".//items")

    result = []
    if items is None:
        return result

    for item in items.findall("item"):
        result.append({
            "bas_dt": item.findtext("dvdnBasDt"),
            "pay_dt": item.findtext("cashDvdnPayDt"),
            "div_amount": item.findtext("stckGenrDvdnAmt"),
            "par": item.findtext("stckParPrc"),
        })

    return result


if __name__ == "__main__":
    print("=== 삼성전자 배당금 테스트 (2024년 1분기 기준일) ===")
    rows = get_div("삼성전자", "20240331")

    print("\n----- PARSED RESULT -----")
    print(json.dumps(rows, indent=2, ensure_ascii=False))
