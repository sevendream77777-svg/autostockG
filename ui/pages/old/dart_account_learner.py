# -*- coding: utf-8 -*-
"""
dart_account_learner.py
-----------------------
연도별 DART 계정명( account_nm ) 변형을 자동 학습하여
표준 재무키( revenue, op_income, net_income, assets, liabilities, equity, eps, cash_flow_* )로
매핑하는 보조 모듈.

용도
- 독립 실행: 특정 종목(또는 여러 종목)의 연도별 재무제표를 긁어와서, 계정명 변형을 자동 수집/학습
- 통합 사용: p0_index.py 의 정규화(mapping_rules)에 학습 결과를 주입하여 2014년 등 레거시 명칭을 자동 매칭

출력
- JSON 파일 (기본: dart_account_map_learned.json)
  {
    "global": { "revenue": [...], "op_income": [...], ... },         # 전체 빈도 상위 후보
    "by_year": {
      "2014": { "revenue": [...], ... },
      "2015": { ... }
    },
    "ifrs_id_hits": { "ifrs-full_Assets": ["자산총계", ...], ... }    # IFRS 태그와 계정명 관찰치(참고용)
  }

통합 포인트
- merge_mapping_into_rules(base_rules:dict, learned_json_path:str) -> dict
  : 기존 p0의 mapping_rules(dict)에 학습한 별칭을 덧붙여 반환
"""
from __future__ import annotations

import os, io, json, zipfile, time
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, Counter

import requests

# --------------------------- 기본 설정 ---------------------------
DEFAULT_TIMEOUT = 8
LEARNED_JSON_DEFAULT = "dart_account_map_learned.json"

# DART 보고서/연결재무 우선순위
REPORT_CODES_PRIORITY: List[Tuple[str, str]] = [
    ("11011", "사업보고서"),
    ("11012", "반기보고서"),
    ("11014", "3분기보고서"),
    ("11013", "1분기보고서"),
]
FS_DIV_PRIORITY = ["CFS", "OFS"]

# 표준키 ↔ IFRS 태그 힌트(부분 포함 매칭)
IFRS_HINTS = {
    "assets": ["Assets", "ifrs-full_Assets"],
    "liabilities": ["Liabilities", "ifrs-full_Liabilities"],
    "equity": ["Equity", "ifrs-full_Equity"],
    "net_income": ["ProfitLoss", "ifrs-full_ProfitLoss"],
    "revenue": ["Revenue", "ifrs-full_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"],
    "op_income": ["OperatingIncome", "OperatingIncomeLoss", "ifrs-full_ProfitLossFromOperatingActivities"],
    "eps": ["EarningsPerShare", "ifrs-full_BasicEarningsLossPerShare"],
}

# 표준키 ↔ 기본 한글 별칭(현행 + 일부 레거시 포함)
BASE_KR_ALIASES = {
    "revenue": ["매출액", "영업수익", "수익(매출액)", "매출", "수익",
                "Ⅰ.매출액", "매출액(수익)", "매출액(매출)", "영업수익(매출)"],
    "op_income": ["영업이익", "영업손익", "영업손실", "이익(손실)", "사업이익", "매출총이익",
                  "Ⅱ.영업손익", "영업손익(손실)", "영업이익(손실)", "영업손익(이익)"],
    "net_income": ["당기순이익", "순이익", "단기순이익", "분기순이익", "반기순이익",
                   "당기순이익(손실)", "분기순손익", "반기순손익"],
    "assets": ["자산총계", "자산", "총자산", "자산총계(원)"],
    "liabilities": ["부채총계", "부채", "총부채", "부채총계(원)"],
    "equity": ["자본총계", "자본", "총자본", "자본총계(원)"],
    "eps": ["주당순이익", "기본주당이익", "기본이익(손실)주당액", "주당순이익(손실)"],
}

CF_KR_ALIASES = {
    "cash_flow_op": ["영업활동현금흐름", "영업으로부터의현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름"],
}

# --------------------------- 유틸 ---------------------------
def _read_api_key(inline_key: str = "", user_key_path: Optional[str] = None) -> str:
    key = (inline_key or "").strip()
    if key and "xxxx" not in key:
        return key
    # 유저 경로
    if user_key_path and os.path.exists(user_key_path):
        try:
            return open(user_key_path, "r", encoding="utf-8").read().strip()
        except Exception:
            pass
    # 환경변수
    env = os.environ.get("DART_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError("DART API 키를 찾을 수 없습니다. (inline/user_path/env 모두 실패)")

def _get_corp_code(api_key: str, stock_code: str, cache_path: str = "dart_corp_map.json") -> Optional[str]:
    if os.path.exists(cache_path):
        try:
            m = json.load(open(cache_path, "r", encoding="utf-8"))
            if stock_code in m:
                return m[stock_code]
        except Exception:
            pass
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        xml_data = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml_data)
    mapping = {}
    for child in root.findall("list"):
        sc = (child.findtext("stock_code") or "").strip()
        cc = (child.findtext("corp_code") or "").strip()
        if sc:
            mapping[sc] = cc
    json.dump(mapping, open(cache_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return mapping.get(stock_code)

def _fetch_fnltt(api_key: str, corp_code: str, year: int, report_code: str, fs_div: str) -> Dict[str, Any]:
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = dict(crtfc_key=api_key, corp_code=corp_code, bsns_year=str(year),
                  reprt_code=report_code, fs_div=fs_div)
    r = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data

def _iter_candidates(api_key: str, corp_code: str, year: int):
    for fs in FS_DIV_PRIORITY:
        for rc, _name in REPORT_CODES_PRIORITY:
            yield (year, rc, fs)

# --------------------------- 학습 로직 ---------------------------
def _score_standard_key(account_nm: str, account_id: str) -> List[str]:
    """
    account_nm / account_id(=ifrs tag) 를 보고 해당될 가능성이 있는 표준키 후보를 리턴.
    - 부분문자열 기준의 다중 후보 허용 (빈도 누적 후 최종 상위 채택)
    """
    candidates = set()
    nm = (account_nm or "").replace(" ", "")
    aid = (account_id or "").strip()

    # 한글 별칭 스캔
    for std, aliases in BASE_KR_ALIASES.items():
        for a in aliases:
            if a.replace(" ", "") in nm:
                candidates.add(std); break

    for std, aliases in CF_KR_ALIASES.items():
        for a in aliases:
            if a.replace(" ", "") in nm:
                candidates.add(std); break

    # IFRS 힌트 스캔
    if aid:
        for std, hints in IFRS_HINTS.items():
            for h in hints:
                if h.lower() in aid.lower():
                    candidates.add(std); break

    return list(candidates)

def learn_account_aliases(api_key: str,
                          stock_code: str,
                          years: List[int],
                          user_key_path: Optional[str] = None,
                          min_count: int = 1) -> Dict[str, Any]:
    """
    지정 연도(들)에 대해 보고서 순회를 하며 account_nm/ifrs id를 관찰,
    표준키별로 가능성이 높은 별칭들을 빈도 기반으로 학습한다.
    """
    key = _read_api_key(api_key, user_key_path)
    corp_code = _get_corp_code(key, stock_code)
    if not corp_code:
        raise RuntimeError(f"종목코드 매핑 실패: {stock_code}")

    by_year: Dict[str, Dict[str, Counter]] = {}
    ifrs_hits: Dict[str, List[str]] = defaultdict(list)

    for y in years:
        yearly_counts: Dict[str, Counter] = defaultdict(Counter)
        found_any = False
        # 연도 백오프: y, y-1, ... y-4
        for back in range(0, 5):
            yr = y - back
            for _, rc, fs in ((yr, rc, fs) for yr, rc, fs in _iter_candidates(key, corp_code, yr)):
                try:
                    data = _fetch_fnltt(key, corp_code, yr, rc, fs)
                except Exception:
                    continue
                if data.get("status") != "000":
                    continue
                lst = data.get("list") or []
                if not lst:
                    continue
                found_any = True
                for it in lst:
                    nm = it.get("account_nm") or ""
                    aid = it.get("account_id") or ""
                    cands = _score_standard_key(nm, aid)
                    if aid:
                        ifrs_hits[aid].append(nm)
                    for std in cands:
                        yearly_counts[std][nm] += 1
            if found_any:  # 해당 연도에 대해 최소 하나라도 수집되면 백오프 중단
                break
        # Counter -> 리스트 정제
        by_year[str(y)] = {}
        for std, cnt in yearly_counts.items():
            aliases = [a for a, c in cnt.most_common() if c >= min_count]
            by_year[str(y)][std] = aliases

    # 전역(글로벌) 상위 후보 만들기
    global_counts: Dict[str, Counter] = defaultdict(Counter)
    for y, d in by_year.items():
        for std, aliases in d.items():
            for a in aliases:
                global_counts[std][a] += 1
    global_top = {std: [a for a, _ in c.most_common(50)] for std, c in global_counts.items()}

    # IFRS 관찰치 압축
    ifrs_compact = {k: sorted(list(set(v)))[:50] for k, v in ifrs_hits.items()}

    learned = {
        "global": global_top,
        "by_year": by_year,
        "ifrs_id_hits": ifrs_compact,
        "meta": {
            "stock_code": stock_code,
            "years": years,
            "fs_div": FS_DIV_PRIORITY,
            "reprt_codes": [x[0] for x in REPORT_CODES_PRIORITY],
        },
    }
    return learned

# --------------------------- 통합 보조 ---------------------------
def merge_mapping_into_rules(base_rules: Dict[str, List[str]], learned_json_path: str) -> Dict[str, List[str]]:
    """
    p0_index.py 의 mapping_rules(예: {"revenue":[...], "op_income":[...], ...}) 에
    학습한 별칭을 주입.
    - 같은 key 에 대해 리스트를 합치되, 기존 순서 보존 + 중복 제거.
    """
    if not os.path.exists(learned_json_path):
        raise FileNotFoundError(f"learned json not found: {learned_json_path}")

    learned = json.load(open(learned_json_path, "r", encoding="utf-8"))
    merged = {k: list(v) for k, v in base_rules.items()}

    def _merge_list(dst: List[str], src: List[str]):
        seen = set(x.strip() for x in dst)
        for s in src:
            s2 = s.strip()
            if s2 and s2 not in seen:
                dst.append(s2); seen.add(s2)

    # 글로벌 우선 주입
    for std, aliases in (learned.get("global") or {}).items():
        if std in merged:
            _merge_list(merged[std], aliases)
        else:
            merged[std] = list(aliases)

    # 연도별은 참고용: 필요 시 외부에서 연도 필터링하여 선택적으로 주입
    merged["_by_year"] = learned.get("by_year", {})  # 참고로 유지(사용처에서 선택 적용)

    return merged

# --------------------------- CLI ---------------------------
def save_json(obj: Any, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    import argparse
    p = argparse.ArgumentParser(description="DART 계정명 연도별 자동학습기")
    p.add_argument("--api-key", default="", help="DART API Key (미지정 시 user-path/env 검색)")
    p.add_argument("--user-key-path", default=r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt")
    p.add_argument("--stock", default="005930", help="종목코드 (기본: 삼성전자)")
    p.add_argument("--years", default="2014-2024", help="예: 2012,2013,2014 또는 2014-2024")
    p.add_argument("--out", default=LEARNED_JSON_DEFAULT, help="학습 결과 JSON 경로")
    args = p.parse_args()

    # years 파싱
    yrs: List[int] = []
    s = args.years.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        yrs = list(range(int(a), int(b) + 1))
    else:
        yrs = [int(x) for x in s.split(",") if x.strip()]

    learned = learn_account_aliases(api_key=args.api_key,
                                    stock_code=args.stock,
                                    years=yrs,
                                    user_key_path=args.user_key_path,
                                    min_count=1)
    save_json(learned, args.out)
    print(f"[OK] Learned mapping saved to: {args.out}")

if __name__ == "__main__":
    main()
