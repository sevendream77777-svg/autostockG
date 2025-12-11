# ================================================
#  DART 2015 FULL TESTER (Unified Test Engine)
#  모든 가능한 방법을 한 번에 테스트
#  - reprt_code 조합 테스트
#  - fs_div 조합 테스트
#  - SinglAcnt / SinglAcntAll
#  - undocumented crp_cd / rept_no
#  - XBRL 다운로드 테스트 (선택)
# ================================================

import requests
import itertools
import time
import os
from pathlib import Path

API_KEY = "d1c780f274e4a88da0804ece578e5d040d78098d"  # <<< 여기에 키 넣기
CORP_CODE = "00126380"  # 삼성전자
BSNS_YEAR = "2015"
RCP_NO = "20150515001379"   # 2015년 1분기 보고서 접수번호

BASE_ALL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
BASE_ONE = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
BASE_UNDOC = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

# 공식 reprt_code
OFFICIAL = ["11011", "11012", "11013", "11014"]

# 과거 실제로 잡히던 비공식 내부코드(추정)
NON_OFFICIAL = [
    "11015", "11016", "11010", "11017"
]

# 테스트할 모든 reprt_code 조합
REPRT_CODES = list(dict.fromkeys(OFFICIAL + NON_OFFICIAL))

# fs_div 조합
FS_DIV = ["CFS", "OFS", None]

def req(url, params):
    try:
        r = requests.get(url, params=params, timeout=7)
        return r.json()
    except Exception as e:
        return {"status": "ERR", "message": str(e)}


def test_api():
    results = []
    print("\n===============================================")
    print("     DART 2015 FULL TESTER START")
    print("===============================================")

    for reprt_code, fs_div in itertools.product(REPRT_CODES, FS_DIV):
        for mode in ["ALL", "ONE", "UNDOC"]:
            if mode == "ALL":
                url = BASE_ALL
                name = "fnlttSinglAcntAll"
                params = {
                    "crtfc_key": API_KEY,
                    "corp_code": CORP_CODE,
                    "bsns_year": BSNS_YEAR,
                    "reprt_code": reprt_code,
                    "rcp_no": RCP_NO
                }
                if fs_div:
                    params["fs_div"] = fs_div

            elif mode == "ONE":
                url = BASE_ONE
                name = "fnlttSinglAcnt"
                params = {
                    "crtfc_key": API_KEY,
                    "corp_code": CORP_CODE,
                    "bsns_year": BSNS_YEAR,
                    "reprt_code": reprt_code,
                    "rcp_no": RCP_NO
                }
                if fs_div:
                    params["fs_div"] = fs_div

            else:  # undocumented 방식
                url = BASE_UNDOC
                name = "UNDOCUMENTED"
                params = {
                    "crtfc_key": API_KEY,
                    "crp_cd": CORP_CODE,
                    "rept_no": RCP_NO,
                    "reprt_code": reprt_code
                }

            res = req(url, params)
            status = res.get("status")
            msg = res.get("message")
            length = len(res.get("list", [])) if isinstance(res, dict) else 0

            line = f"[{name}] reprt={reprt_code} fs_div={fs_div} -> status={status}, msg={msg}, list_len={length}"
            print(line)
            results.append(line)

            time.sleep(0.15)

    return results


# =============== XBRL TEST =======================
def test_xbrl():
    try:
        import dart_fss as dart
    except:
        print("\n[WARNING] dart-fss 미설치 → XBRL 테스트 생략됨")
        return

    print("\n===============================================")
    print("    XBRL DOWNLOAD TEST")
    print("===============================================")

    try:
        import dart_fss as dart
        dart.set_api_key(API_KEY)
        out = dart.api.finance.download_xbrl(
            path="xbrl_2015",
            rcept_no=RCP_NO,
            reprt_code=None
        )
        print("[XBRL OK] 다운로드 완료 →", out)
    except Exception as e:
        print("[XBRL FAIL]", e)


# =============== MAIN ============================
if __name__ == "__main__":
    log = test_api()

    print("\n===============================================")
    print("    SUMMARY: SUCCESS / FAIL 정리")
    print("===============================================")
    for l in log:
        if "status=000" in l:
            print("[SUCCESS] ", l)
        elif "list_len=" in l and not "0" in l.split("list_len=")[1].split(",")[0]:
            print("[SUCCESS] ", l)

    print("\n===============================================")
    print("이제 XBRL 테스트 시작 (선택)")
    print("===============================================")
    test_xbrl()
