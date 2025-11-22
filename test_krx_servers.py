"""
Quick probe script: hit multiple KRX candidate hosts for daily all-ticker OHLCV JSON.
Prints status code, response length, and first few hundred chars so we can see
which servers respond with real data vs. block/HTML.
"""
import requests

TARGET_DATE = "20240105"  # known trading day
PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
    "mktId": "ALL",
    "trdDd": TARGET_DATE,
    "share": 1,
    "money": 1,
    "csvxls_isNo": False,
}

# Browser-like headers to reduce basic bot blocking.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ),
    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader.jsp",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Candidate hosts (http/https variants and common subdomains).
BASE_URLS = [
    "https://data.krx.co.kr",
    "http://data.krx.co.kr",
    "https://kind.krx.co.kr",
    "http://kind.krx.co.kr",
    "https://file.krx.co.kr",
    "http://file.krx.co.kr",
    "https://new.krx.co.kr",
    "http://new.krx.co.kr",
    # Add more if you know other mirrors.
]


def probe():
    for base in BASE_URLS:
        url = f"{base}/comm/bldAttendant/getJsonData.cmd"
        try:
            r = requests.post(url, headers=HEADERS, data=PAYLOAD, timeout=8)
            snippet = r.text[:400].replace("\n", " ")
            print(f"[{base}] status={r.status_code}, len={len(r.text)}")
            print(snippet)
            print("-" * 80)
        except Exception as e:
            print(f"[{base}] error: {e}")
            print("-" * 80)


if __name__ == "__main__":
    probe()
