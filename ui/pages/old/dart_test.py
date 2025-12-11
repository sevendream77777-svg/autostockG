import requests
import zipfile
import xml.etree.ElementTree as ET
import io
import os
import json
import time # Time needed for path checks
from collections import defaultdict

# ==============================================================================
# 🔥 [참고] 여기에 DART API 키를 직접 붙여넣으세요! (이전 테스트에 사용된 키)
# ==============================================================================
MY_API_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" 
# ==============================================================================

# DART 재무제표 계정 매핑 (핵심 컬럼) - [과거 계정명 최대 확장]
ACCOUNT_MAPPING = {
    "revenue": ["매출액", "영업수익", "수익(매출액)", "Revenue", "매출", "수익"], 
    "op_income": ["영업이익", "OperatingIncomeLoss", "영업손익", "영업손실", "이익(손실)", "사업이익", "매출총이익"], 
    "net_income": ["당기순이익", "ProfitLoss", "순이익", "단기순이익", "분기순이익", "반기순이익"], 
    "assets": ["자산총계", "자산", "TotalAssets", "총자산"],
    "liabilities": ["부채총계", "부채", "TotalLiabilities", "총부채"],
    "equity": ["자본총계", "자본", "TotalEquity", "총자본"],
    "eps": ["주당순이익", "기본주당이익", "EarningsPerShare"], 
}

# 보고서 코드 우선순위 (가장 완전한 데이터 순)
REPORT_CODES_PRIORITY = {
    "11011": "사업보고서",   # Annual Report
    "11012": "반기보고서",   # Semi-Annual Report
    "11014": "3분기보고서",  # 3rd Quarter Report
    "11013": "1분기보고서",  # 1st Quarter Report
}

# 재무제표 구분 순위 (CFS=연결, OFS=개별)
FS_DIV_PRIORITY = ["CFS", "OFS"] 

def _get_corp_code(api_key, stock_code):
    """종목코드 -> DART 고유번호 매핑 (1회성 다운로드)"""
    map_file = "dart_corp_map.json"
    if os.path.exists(map_file):
        with open(map_file, "r", encoding="utf-8") as f:
            mapping = json.load(f)
            return mapping.get(stock_code)
    
    print(">>> DART 고유번호 다운로드 중... (최초 1회)")
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    try:
        resp = requests.get(url, timeout=30)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xml_data = zf.read(zf.namelist()[0])
        
        root = ET.fromstring(xml_data)
        mapping = {}
        for child in root.findall("list"):
            sc = child.findtext("stock_code").strip()
            cc = child.findtext("corp_code").strip()
            if sc: mapping[sc] = cc
        
        with open(map_file, "w", encoding="utf-8") as f:
            json.dump(mapping, f)
        return mapping.get(stock_code)
    except Exception as e:
        print(f"❌ DART 매핑 다운로드 실패: {e}")
        return None


def fetch_dart_financial(api_key, corp_code, year, report_code, fs_div):
    """특정 연도, 보고서 코드, 재무제표 타입으로 데이터를 조회"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": report_code,
        "fs_div": fs_div 
    }
    
    try:
        resp = requests.get(url, params=params, timeout=5)
        data = resp.json()
        
        if data.get("status") != "000":
            return None, data.get("message", "데이터 없음")
            
        result = defaultdict(float)
        
        # 데이터 파싱 및 매핑
        for item in data.get("list", []):
            acct_nm = item.get("account_nm", "")
            val_str = item.get("thstrm_amount", "").replace(",", "")
            
            if not val_str or val_str == "-": continue
            
            try:
                val = float(val_str)
                # ★★★ 핵심: 여러 계정명 시도 (과거 데이터 확보용) ★★★
                for en_name, kr_names in ACCOUNT_MAPPING.items():
                    # 어떤 한국어 계정명이든 acct_nm에 포함되어 있으면 매칭
                    if any(kr_name in acct_nm for kr_name in kr_names):
                        # 발견된 값으로 업데이트 (가장 마지막에 나오는 항목이 최종)
                        result[en_name] = val
                        break
                
                # 현금흐름도 확인
                if "영업활동현금흐름" in acct_nm: result["cash_flow_op"] = val
                if "투자활동현금흐름" in acct_nm: result["cash_flow_inv"] = val
                if "재무활동현금흐름" in acct_nm: result["cash_flow_fin"] = val

            except ValueError:
                continue

        # DART 데이터 기반 파생 지표 계산
        if result["equity"] != 0:
            if result["net_income"] != 0:
                result["roe"] = (result["net_income"] / result["equity"]) * 100
            if result["liabilities"] != 0:
                result["debt_ratio"] = (result["liabilities"] / result["equity"]) * 100
        
        # EPS/BPS 초기화
        if 'eps' not in result: result['eps'] = 0.0
        if 'bps' not in result: result['bps'] = 0.0

        result["status"] = "SUCCESS"
        result["fs_div"] = fs_div
        return dict(result), None
        
    except Exception as e:
        return None, str(e)

def fetch_dart_financial_robust(api_key, corp_code, year):
    """재무제표 타입(CFS/OFS)과 보고서 코드(11011/11012/...)를 순회하며 데이터를 찾는 로직"""
    
    for fs_div in FS_DIV_PRIORITY:
        for report_code, report_name in REPORT_CODES_PRIORITY.items():
            result, error = fetch_dart_financial(api_key, corp_code, year, report_code, fs_div)
            
            # 로그 출력
            print(f"   -> {year}년 {report_name} ({report_code}, {fs_div}) 시도: {'✅ 성공' if result and result.get('status') == "SUCCESS" else '❌ 실패'} (메시지: {error if error else '성공'})")
            
            if result and result.get("status") == "SUCCESS":
                result['report_type'] = f"{report_name} ({fs_div})"
                return result
            
    return {"status": "FAIL", "message": "모든 보고서 타입 및 코드를 시도했지만 찾지 못했습니다."}

def run_full_test():
    stock_code = "005930"
    api_key = MY_API_KEY
    
    # ★★★ [FIXED] 유저 지정 키 경로 (경로 고정) ★★★
    key_path_user = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"
    
    if "xxxx" in api_key: # 코드에 키가 없을 경우
        if os.path.exists(key_path_user):
            print(f"✅ 유저 지정 경로에서 키 로드 시도: {key_path_user}")
            try:
                with open(key_path_user, "r", encoding="utf-8") as f:
                    api_key = f.read().strip()
                print("✅ 키 로드 성공.")
            except Exception as e:
                print(f"❌ 유저 지정 경로 키 파일 읽기 실패: {e}")
                
        if "xxxx" in api_key:
            print("❌ [종료] API 키를 찾을 수 없습니다. 키를 입력하거나, 파일을 경로에 두세요.")
            return

    corp_code = _get_corp_code(api_key, stock_code)
    if not corp_code: return

    # ★★★ [업데이트] 테스트할 연도 목록 (2015년~2024년 연속) ★★★
    test_years = list(range(2015, 2025))
    
    print("\n" + "="*70)
    print(f"💰 [DART 연도별 수집 가능 경계 테스트] 종목: {stock_code}, 고유번호: {corp_code}")
    print("="*70)
    print(f"⚙️ 적용된 계정명 매칭 옵션: {len(ACCOUNT_MAPPING)}개 항목에 대해 과거 계정명 다수 시도.")

    
    final_result = {}
    for year in test_years:
        print(f"\n--- 조회 연도: {year} ---")
        
        # 로버스트 로직 실행 (CFS/OFS + 보고서 우선순위 순회)
        result = fetch_dart_financial_robust(api_key, corp_code, year)
        
        if result.get("status") == "SUCCESS":
            print(f"✅ [최종 성공] 확보 보고서: {result.get('report_type')}")
            final_result[year] = "✅ 성공"
        else:
            print(f"❌ [최종 실패] {result.get('message')}")
            final_result[year] = "❌ 실패"
            
    
    # 최종 결과 요약 출력
    print("\n" + "="*70)
    print("🚀 [최종 연도별 수집 경계 요약]")
    print("="*70)
    for year, status in final_result.items():
        print(f"   {year}년 데이터: {status}")
        
    print("\n테스트 완료. 이제 DART를 통한 재무 데이터 수집 가능 연도를 정확히 알 수 있습니다.")


if __name__ == "__main__":
    run_full_test()
    input("\n[확인] 엔터 키를 누르면 종료합니다...")