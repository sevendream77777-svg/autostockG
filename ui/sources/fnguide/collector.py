# -*- coding: utf-8 -*-
"""
FnGuide 섹터/테마 스냅샷
"""
import requests
from typing import Any, Dict


def fetch_fnguide(code: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    try:
        url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=6)
        if resp.status_code == 200:
            # 단순 문자열 파싱 (bs4 없는 환경 호환)
            txt = resp.text
            # 회사명
            if "corp_group1" in txt:
                # name
                import re

                m = re.search(r"<h2[^>]*>([^<]+)</h2>", txt)
                if m:
                    res["name"] = m.group(1).strip()
                # sector name (첫 번째 stxt2)
                m2 = re.search(r'<span class="stxt stxt2">([^<]+)</span>', txt)
                if m2:
                    res["sector_name"] = m2.group(1).strip()
    except Exception:
        pass
    return res

