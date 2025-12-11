# -*- coding: utf-8 -*-
"""
p53_full_collector_v3.py  (완성본)
--------------------------------
- v58에서 비원천 5개(bps/roe/roa/debt_ratio/earnings_surprise) 제외한 **원천 53개** 수집/병합/로그/덤프.
- v3 주요 수정(요청 사항 반영):
  A) "Kiwoom 0개" 문제: import 경로/HTTP 직접 호출 지원, 영업일 백오프 + 키/값 정규화 보강
  B) PyKRX amount=None 문제: 3단계 대체 루트(단일/티커별/추가백오프)로 보강
  C) 섹터/테마 4개 누락: 네이버 종목/업종/테마 페이지 파서 추가(sector_code/theme_code/theme_name/sector_index_close)
  D) kr10y_yield 누락: FDR 실패 시 한국은행(BOK)·네이버 지표 페이지 크롤러로 대체
  E) v53(원천 53) 확보율 향상: code/date 명시·로그 강화·결측 사유 기록·성공률≥40~50개 목표

사용 예:
  python p53_full_collector_v3.py --code 005930 --date 20251205
"""

from __future__ import annotations
import sys, os, json, math, time, re
import datetime as dt

# ---------- Safe optional imports ----------
def _safe_import(module, alias=None):
    try:
        m = __import__(module)
        return m if alias is None else __import__(module, fromlist=[alias])
    except Exception:
        return None

requests = _safe_import("requests")
np = _safe_import("numpy")
pd = _safe_import("pandas")
# bs4
BeautifulSoup = None
try:
    from bs4 import Beautifulsoup as _BS  # typo-proof
except Exception:
    try:
        from bs4 import BeautifulSoup as _BS
        BeautifulSoup = _BS
    except Exception:
        BeautifulSoup = None

# ---------- CLI ----------
def _arg(key, default=None):
    for i,a in enumerate(sys.argv[1:]):
        if a.strip().lower() == f"--{key}":
            if key in ("help",): return True
            if i+2 <= len(sys.argv[1:]): return sys.argv[1:][i+1]
            return True
        if a.lower().startswith(f"--{key}="):
            return a.split("=",1)[1]
    return default

CODE = (_arg("code") or "005930").strip()
DATE = (_arg("date") or dt.date.today().strftime("%Y%m%d")).replace("-","").replace(".","")

OUTDIR = os.path.join(os.getcwd(), "logs")
os.makedirs(OUTDIR, exist_ok=True)

# ---------- v58 minus 5 non-source = v53 ----------
V58 = [
  # Price (12)
  "date","code","name","market","open","high","low","close","volume","amount","adj_factor","vwap",
  # Flow (12)
  "inst_net_qty","inst_net_amt","frgn_net_qty","frgn_net_amt","nps_net_qty","nps_net_amt",
  "dealer_net_qty","dealer_net_amt","short_sell_qty","short_sell_amt","loan_balance_qty","loan_balance_amt",
  # Finance (11)
  "revenue","op_income","net_income","eps","bps","roe","roa","debt_ratio","cash_flow_op","cash_flow_inv","cash_flow_fin",
  # Sector/Theme (5)
  "sector_code","sector_name","theme_code","theme_name","sector_index_close",
  # Macro (8)
  "usdkrw","cnykrw","dxy","us10y_yield","kr10y_yield","wti","gold","vix",
  # Event (10)
  "earnings_announce_date","earnings_surprise","earnings_effective_date",
  "ex_div_date","div_amount","split_announce_date","split_effective_date",
  "rights_issue_announce_date","rights_issue_effective_date","mna_announce_date"
]
NON_SOURCE_5 = {"bps","roe","roa","debt_ratio","earnings_surprise"}
V53 = [c for c in V58 if c not in NON_SOURCE_5]

# ---------- Utils ----------
def log(msg): print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}")

def _is_valid(val):
    if val is None: return False
    if isinstance(val, str) and not val.strip(): return False
    try:
        if isinstance(val, (int, float)) and (math.isnan(val) or math.isinf(val)): return False
    except Exception: pass
    try:
        import pandas as _pd
        if _pd.isna(val): return False
    except Exception: pass
    return True

def _short(d:dict, n=10):
    if not isinstance(d, dict): return ""
    out = []
    for i,(k,v) in enumerate(sorted(d.items())):
        if i>=n: break
        out.append(f"{k}={v}")
    return "; ".join(out)

def _bd_backoff_dates(base_dt, k=3):
    """base_dt(YYYYMMDD)에서 앞쪽으로 영업일 추정 백오프 문자열 리스트"""
    try:
        from pandas.tseries.offsets import BDay
        b = pd.to_datetime(base_dt)
        dates = [base_dt]
        for i in range(1, k+1):
            dates.append((b - BDay(i)).strftime("%Y%m%d"))
        return dates
    except Exception:
        b = dt.datetime.strptime(base_dt, "%Y%m%d")
        return [(b - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(0, k+1)]

def _json_safe(o):
    try:
        import numpy as _np
        if isinstance(o, (_np.integer,)):  return int(o)
        if isinstance(o, (_np.floating,)): return float(o)
        if isinstance(o, (_np.ndarray,)):  return o.tolist()
    except Exception: pass
    try:
        import pandas as _pd
        if isinstance(o, (_pd.Timestamp,)): return o.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(o, (_pd.Series,)):    return o.to_dict()
        if isinstance(o, (_pd.DataFrame,)): return o.to_dict(orient="records")
    except Exception: pass
    if isinstance(o, (dt.date, dt.datetime)): return o.isoformat()
    if isinstance(o, set): return list(o)
    return str(o)

# ---------- Kiwoom (REST) ----------
def _resolve_kiwoom_module_paths():
    """
    - 환경변수 KIWOOM_REST_ROOT, 또는 기본 경로 F:\\autostockG\\api\\kiwoom_rest
    - 현재 작업 경로 내 'api/kiwoom_rest' 폴더
    """
    cands = []
    env_root = os.environ.get("KIWOOM_REST_ROOT", "").strip()
    if env_root:
        cands.append(env_root)
        cands.append(os.path.join(env_root, "api", "kiwoom_rest"))
    cands.append(r"F:\autostockG\api\kiwoom_rest")
    cands.append(os.path.join(os.getcwd(), "api", "kiwoom_rest"))
    # sys.path 주입
    for p in cands:
        if p and os.path.isdir(p) and (p not in sys.path):
            sys.path.insert(0, p)

def _kiwoom_http_call(base_url, api_id, path, body):
    if not requests: return {"_error": "requests missing"}
    try:
        url = base_url.rstrip("/") + path
        r = requests.post(url, json={"api_id": api_id, "body": body}, timeout=8)
        if r.status_code != 200:
            return {"_error": f"http {r.status_code}"}
        return r.json()
    except Exception as e:
        return {"_error": f"http err: {e}"}

def fetch_kiwoom(code, base_dt):
    """
    Kiwoom REST: 가격/기본 일부/정보. 
    개선점:
      - import 경로 복수 시도(_resolve_kiwoom_module_paths)
      - 로컬 HTTP 프록시 사용(환경변수 KIWOOM_REST_BASE)도 지원
      - 영업일 백오프, 정규화 보강
    """
    out = {}
    reasons = []
    try:
        _resolve_kiwoom_module_paths()
        api = None

        # 1) 파이썬 모듈 직접 import
        try:
            # 두 형태를 모두 시도
            try:
                from kiwoom_api import KiwoomRestApi as _KRA  # repo 루트에 설치된 경우
            except Exception:
                try:
                    from api.kiwoom_rest.kiwoom_api import KiwoomRestApi as _KRA  # 패키지 하위
                except Exception as e2:
                    raise ImportError(str(e2))
            api = _KRA()
            def call(api_id, path, body): 
                try: 
                    return api._call_api(api_id, path, body=body)  # type: ignore
                except Exception as e: 
                    return {"_error": str(e)}
        except Exception as e:
            reasons.append(f"import fail: {e}")
            api = None

        # 2) HTTP 프록시(선택): 환경변수 KIWOOM_REST_BASE = "http://127.0.0.1:8000"
        http_base = os.environ.get("KIWOOM_REST_BASE", "").strip()
        if (api is None) and not http_base:
            # 모듈도 없고 HTTP도 없으면 종료
            return out, reasons
        if api is None and http_base:
            def call(api_id, path, body): return _kiwoom_http_call(http_base, api_id, path, body)

        def ext(res):
            if not isinstance(res, dict): return {}
            if res.get("_error"): 
                reasons.append(f"API error: {res['_error']}")
                return {}
            for k in ("output","chart","data","result","stk_dt_pole_chart_qry"):
                if k in res:
                    blk = res[k]
                    if isinstance(blk, list) and blk and isinstance(blk[0], dict): return blk[0]
                    if isinstance(blk, dict): return blk
            # 마지막 수단: 평면 dict
            return {k:v for k,v in res.items() if k not in ("return_code","return_msg")}

        # 핵심 호출(영업일 백오프 2)
        for dt_str in _bd_backoff_dates(base_dt, k=2):
            merged = {}
            c = call("ka10081","/api/dostk/chart",{"stk_cd": code, "base_dt": dt_str, "upd_stkpc_tp":"D", "term_cnt":"1"})
            i = call("ka10014","/api/dostk/shsa", {"stk_cd": code, "tm_tp":"0", "strt_dt": dt_str, "end_dt": dt_str})
            d = call("ka10001","/api/dostk/stkinfo",{"stk_cd": code})
            merged.update(ext(c)); merged.update(ext(i)); merged.update(ext(d))

            m = {}
            norm = {
                "open":   ["open","stck_oprc","시가"],
                "high":   ["high","stck_hgpr","고가"],
                "low":    ["low","stck_lwpr","저가"],
                "close":  ["close","stck_clpr","종가","cur_prc"],
                "volume": ["volume","acml_vol","거래량"],
                "amount": ["amount","acc_trde_prica","거래대금","trd_amt","tot_tr_pbmn"],
                "name":   ["name","stk_nm","itm_nm","hname"],
                "date":   ["date","base_dt","stck_bsop_date"],
                "code":   ["code","stk_cd","stock_code","shcode"],
            }
            for std, cand in norm.items():
                for ckey in cand:
                    if ckey in merged and _is_valid(merged[ckey]):
                        m[std] = merged[ckey]; break

            if m:
                if "date" not in m: m["date"] = dt_str
                if "code" not in m: m["code"] = code
                out.update(m)
                break
        if not out and not reasons:
            reasons.append("no data after backoff")
    except Exception as e:
        reasons.append(f"fatal: {e}")
    return out, reasons

# ---------- PyKRX ----------
def fetch_pykrx(code, base_dt):
    res = {}
    reasons = []
    try:
        from pykrx import stock  # type: ignore
    except Exception as e:
        reasons.append(f"import pykrx fail: {e}")
        return res, reasons

    # 1) 단일 종목 OHLCV (영업일 백오프 최대 3)
    got_ohlcv = False
    last_err = None
    for d in _bd_backoff_dates(base_dt, k=3):
        try:
            df = stock.get_market_ohlcv(d, d, code)
            if df is not None and not df.empty:
                row = df.iloc[0].to_dict()
                res.update({
                  "open": row.get('시가'), "high": row.get('고가'), "low": row.get('저가'),
                  "close": row.get('종가'), "volume": row.get('거래량'), "amount": row.get('거래대금'),
                })
                got_ohlcv = True
                break
        except Exception as e:
            last_err = f"ohlcv err({d}): {e}"
            reasons.append(last_err)

    # 2) 거래대금 누락 시: 티커별 스냅샷에서 대체
    if (("amount" not in res) or (res.get("amount") in (None, float('nan')))) and got_ohlcv:
        for d in _bd_backoff_dates(base_dt, k=3):
            try:
                # 시장 추정 (숫자형 6자리면 KOSPI로 우선)
                market = "KOSPI" if (code.isdigit() and int(code) < 100000) else "KOSDAQ"
                df2 = stock.get_market_ohlcv_by_ticker(d, market=market)
                if df2 is not None and not df2.empty and code in df2.index:
                    row = df2.loc[code].to_dict()
                    amt = row.get("거래대금")
                    if _is_valid(amt):
                        res["amount"] = amt
                        break
            except Exception as e:
                reasons.append(f"ohlcv_by_ticker err({d}): {e}")

    if "amount" not in res:
        reasons.append("amount None (after backoff/fallback)")

    # 수급
    try:
        df_vol = stock.get_market_trading_volume_by_date(base_dt, base_dt, code)
        df_val = stock.get_market_trading_value_by_date(base_dt, base_dt, code)
        if df_vol is not None and not df_vol.empty and df_val is not None and not df_val.empty:
            rv = df_vol.iloc[0]; ra = df_val.iloc[0]
            mp = {"기관합계":"inst","외국인합계":"frgn","외국인":"frgn","금융투자":"dealer","연기금":"nps","연기금등":"nps"}
            for kr,en in mp.items():
                if kr in rv.index:
                    res[f"{en}_net_qty"] = rv[kr]
                    res[f"{en}_net_amt"] = ra.get(kr)
    except Exception as e:
        reasons.append(f"flow err: {e}")

    # 공매도/대차
    try:
        df_short = stock.get_shorting_status_by_date(base_dt, base_dt, code)
        if df_short is not None and not df_short.empty:
            row = df_short.iloc[0].to_dict()
            # 일부 버전에서 컬럼명이 다를 수 있어 보강
            res["short_sell_qty"] = row.get("거래량") or row.get("공매도거래량")
            res["short_sell_amt"] = row.get("거래대금") or row.get("공매도거래대금")
            res["loan_balance_qty"] = row.get("잔고수량")
            res["loan_balance_amt"] = row.get("잔고금액")
    except Exception as e:
        reasons.append(f"shorting err: {e}")

    # 이름
    try:
        name = stock.get_market_ticker_name(code)
        if _is_valid(name): res.setdefault("name", name)
    except Exception: pass

    # code/date 보정
    res.setdefault("code", code)
    res.setdefault("date", base_dt)

    return res, reasons

# ---------- Macro (FDR / Naver / BOK) ----------
def fetch_fdr_macro(base_dt):
    out = {}; reasons = []
    try:
        import FinanceDataReader as fdr  # type: ignore
    except Exception as e:
        reasons.append(f"import FDR fail: {e}")
        return out, reasons

    mapping_primary = {
        "usdkrw":"USD/KRW","cnykrw":"CNY/KRW","dxy":"DX-Y.NYB","us10y_yield":"US10YT",
        "kr10y_yield":"KR10YT","wti":"CL=F","gold":"GC=F","vix":"VIX"
    }
    mapping_alt = {
        # 대체 심볼 후보 (환경에 따라 변경될 수 있음)
        "kr10y_yield": ["KR10YT=RR", "^KTB10Y"],  # 일부 환경에서 동작
    }

    start = (dt.datetime.strptime(base_dt, "%Y%m%d") - dt.timedelta(days=10)).strftime("%Y-%m-%d")
    for k,sym in mapping_primary.items():
        try:
            df = fdr.DataReader(sym, start, base_dt)
            if df is not None and not df.empty:
                df = df.fillna(method='ffill')
                val = df.iloc[-1].get("Close")
                if _is_valid(val):
                    out[k] = float(val)
                    continue
        except Exception as e:
            reasons.append(f"{k}:{sym} err {e}")

        # 대체 심볼 재시도
        if k in mapping_alt:
            for alt in mapping_alt[k]:
                try:
                    df = fdr.DataReader(alt, start, base_dt)
                    if df is not None and not df.empty:
                        df = df.fillna(method='ffill')
                        val = df.iloc[-1].get("Close")
                        if _is_valid(val):
                            out[k] = float(val); break
                except Exception as e:
                    reasons.append(f"{k}:{alt} alt err {e}")
    return out, reasons

def fetch_naver_macro():
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        url = "https://finance.naver.com/marketindex/"
        resp = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if resp.status_code != 200:
            reasons.append(f"http {resp.status_code}"); return out, reasons
        soup = BeautifulSoup(resp.text, "html.parser")
        # 환율
        ex = soup.select("#exchangeList > li")
        if len(ex)>=4:
            usd = ex[0].select_one("span.value").text.replace(",","");
            cny = ex[3].select_one("span.value").text.replace(",","");
            out["usdkrw"] = float(usd); out["cnykrw"] = float(cny)
        # WTI/Gold
        oil = soup.select("#oilList > li")
        for li in oil:
            label = li.select_one("span.blind"); val = li.select_one("span.value")
            if not (label and val): continue
            t = label.text.strip(); v = float(val.text.replace(",",""))
            if "WTI" in t: out["wti"] = v
            if ("국제 금" in t) or ("Gold" in t): out["gold"] = v
    except Exception as e:
        reasons.append(f"crawl err: {e}")
    return out, reasons

def fetch_bok_kr10y(base_dt):
    """
    한국은행 또는 네이버 지표 페이지에서 '국고채(10년)' 수익률 파싱 (보완 경로)
    """
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        # 네이버 채권 지표 페이지(간편): finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT_KR_10Y
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT_KR_10Y"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            v = soup.select_one(".tbl_exchange tbody tr td")
            if v:
                val = float(v.text.strip().replace(",",""))
                out["kr10y_yield"] = val
                return out, reasons
        reasons.append(f"naver-10y http {r.status_code}")
    except Exception as e:
        reasons.append(f"naver-10y err: {e}")
    return out, reasons

# ---------- Sector/Theme (Naver/FnGuide 보완) ----------
def fetch_naver_sector_theme(code):
    """
    - 종목 메인: https://finance.naver.com/item/main.nhn?code=005930
      업종/테마 링크 파싱 → sector_code, sector_name, theme_code, theme_name
    - 업종 지수 페이지: https://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no=<sector_code>
      → sector_index_close
    """
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={code}"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if r.status_code != 200:
            reasons.append(f"item.main http {r.status_code}"); return out, reasons
        soup = BeautifulSoup(r.text, "html.parser")

        # 업종 링크
        upjong_a = soup.select_one("a[href*='sise_group_detail.nhn?type=upjong']")
        if upjong_a:
            out["sector_name"] = upjong_a.text.strip()
            m = re.search(r"no=(\\d+)", upjong_a.get("href",""))
            if m: out["sector_code"] = m.group(1)

        # 테마 링크들 중 첫 번째를 채택
        theme_a = soup.select_one("a[href*='sise_group_detail.nhn?type=theme']")
        if theme_a:
            out["theme_name"] = theme_a.text.strip()
            m2 = re.search(r"no=(\\d+)", theme_a.get("href",""))
            if m2: out["theme_code"] = m2.group(1)

        # 업종 지수 종가
        if "sector_code" in out:
            url2 = f"https://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no={out['sector_code']}"
            r2 = requests.get(url2, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
            if r2.status_code == 200:
                s2 = BeautifulSoup(r2.text, "html.parser")
                # 상단 현재지수
                val = s2.select_one(".subtop_sise_graph2 em#now_value")
                if val:
                    out["sector_index_close"] = float(val.text.replace(",",""))
            else:
                reasons.append(f"upjong http {r2.status_code}")
    except Exception as e:
        reasons.append(f"naver sector/theme err: {e}")
    return out, reasons

def fetch_fnguide_sector(code):
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&NewMenuID=101&stkGb=701"
        resp = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if resp.status_code != 200:
            reasons.append(f"http {resp.status_code}"); return out, reasons
        soup = BeautifulSoup(resp.text, "html.parser")
        grp = soup.find("div", class_="corp_group1")
        if grp:
            sp = grp.find("span", class_="stxt stxt2")
            if sp: out["sector_name"] = sp.text.strip()
    except Exception as e:
        reasons.append(f"crawl err: {e}")
    return out, reasons

# --- DART (요약 매핑) ---
def _dart_key():
    paths = [
        r"C:\\공유주방\\!개인폴더\\!이호정이사\\각종key_appkey_decret\\opendart_apikey.txt",
        "opendart_apikey.txt"
    ]
    for p in paths:
        if os.path.exists(p):
            try: return open(p,"r",encoding="utf-8").read().strip()
            except Exception: pass
    return os.environ.get("DART_API_KEY","" ).strip()

def fetch_dart_finance(code, base_year):
    res = {}; reasons = []
    key = _dart_key()
    if not (key and requests):
        reasons.append("no key or requests missing"); return res, reasons

    # corpCode 캐시
    cache = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
    mapping = {}
    try:
        if os.path.exists(cache):
            with open(cache,"r",encoding="utf-8") as f: mapping = json.load(f)
        else:
            url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={key}"
            rr = requests.get(url, timeout=30)
            if rr.status_code == 200:
                import zipfile, xml.etree.ElementTree as ET, io
                zf = zipfile.ZipFile(io.BytesIO(rr.content))
                xml_data = zf.read(zf.namelist()[0])
                root = ET.fromstring(xml_data)
                for node in root.iter("list"):
                    sc = (node.findtext("stock_code") or "").strip()
                    cc = (node.findtext("corp_code") or "").strip()
                    if sc and cc: mapping[sc]=cc
                with open(cache,"w",encoding="utf-8") as f:
                    json.dump(mapping,f,ensure_ascii=False)
    except Exception as e:
        reasons.append(f"corpmap err: {e}"); return res, reasons

    corp = mapping.get(code)
    if not corp:
        reasons.append("no corp_code"); return res, reasons

    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    years = [base_year - i for i in range(5)]
    report = ["11011","11012","11014","11013"]
    fsdiv  = ["CFS","OFS"]
    strong = {
        "revenue": {"aid":{"ifrs-full_Revenue"}, "nm":{"매출액"}},
        "op_income": {"aid":{"ifrs-full_ProfitLossFromOperatingActivities","OperatingIncomeLoss"}, "nm":{"영업이익"}},
        "net_income": {"aid":{"ifrs-full_ProfitLoss"}, "nm":{"당기순이익"}},
        "eps": {"aid":{"ifrs-full_BasicEarningsLossPerShare"}, "nm":{"기본주당순이익","주당순이익"}},
        "assets": {"aid":{"ifrs-full_Assets"}, "nm":{"자산총계"}},
        "liabilities": {"aid":{"ifrs-full_Liabilities"}, "nm":{"부채총계"}},
        "equity": {"aid":{"ifrs-full_Equity","ifrs-full_EquityAttributableToOwnersOfParent"}, "nm":{"자본총계","지배주주지분"}},
        "cash_flow_op": {"aid":{"ifrs-full_CashFlowsFromUsedInOperatingActivities"}, "nm":{"영업활동현금흐름"}},
        "cash_flow_inv": {"aid":{"ifrs-full_CashFlowsFromUsedInInvestingActivities"}, "nm":{"투자활동현금흐름"}},
        "cash_flow_fin": {"aid":{"ifrs-full_CashFlowsFromUsedInFinancingActivities"}, "nm":{"재무활동현금흐름"}},
    }
    for y in years:
        for fs in fsdiv:
            for rc in report:
                try:
                    rr = requests.get(url, params={
                        "crtfc_key": key, "corp_code": corp, "bsns_year": str(y),
                        "reprt_code": rc, "fs_div": fs
                    }, timeout=8)
                    js = rr.json()
                    if js.get("status")!="000": continue
                    lst = js.get("list") or []
                    tmp = {}
                    for it in lst:
                        nm = (it.get("account_nm") or "").strip().replace(" ","")
                        aid = (it.get("account_id") or "").strip()
                        sj  = (it.get("sj_div") or "").strip().upper()
                        v   = (it.get("thstrm_amount") or "").replace(",","");
                        if not v or v == "-": continue
                        try: val = float(v)
                        except: continue
                        # 강매핑 (sj gate)
                        for k,rule in strong.items():
                            sj_need = "IS" if k in ("revenue","op_income","net_income","eps") else \
                                      "BS" if k in ("assets","liabilities","equity") else "CF"
                            if sj != sj_need: continue
                            if (aid in rule["aid"]) or any(x in aid for x in rule["aid"]) or any(nx in nm for nx in rule["nm"]):
                                tmp[k] = val
                    if tmp:
                        res = tmp; res["_dart_year"]=y; res["_dart_fs"]=fs; res["_dart_rc"]=rc
                        return res, reasons
                except Exception as e:
                    reasons.append(f"map err: {e}"); continue
    if not res: reasons.append("no match after backoff")
    return res, reasons

# ---------- Merge & Helpers ----------
PRIORITY = ["Kiwoom","PyKRX","NaverSectorTheme","FnGuide","NaverMacro","FDR","BOK","DART"]

def merge_payloads(payloads: dict):
    final = {}
    for src in PRIORITY:
        data = payloads.get(src) or {}
        for k,v in data.items():
            if k in V53 and _is_valid(v):
                final[k] = (v, src)
    # 필수 메타 보정
    vals = {k:v for k,(v,_) in final.items()}
    final.setdefault("code", (CODE, "Fixed"))
    final.setdefault("date", (DATE, "Fixed"))
    if "market" not in final:
        mkt = "KOSPI" if (CODE.isdigit() and int(CODE)<100000) else "KOSDAQ"
        final["market"] = (mkt, "Inferred")
    # vwap (amount & volume 있어야만)
    if "vwap" not in final and ("amount" in vals) and ("volume" in vals):
        try:
            vol = float(vals["volume"]); amt = float(vals["amount"]); close = float(vals.get("close",0.0))
            if vol>0:
                vwap = amt/vol
                if close>0 and vwap < close*0.01: vwap *= 1_000_000  # 스케일 보정
                final["vwap"] = (vwap, "Calc")
        except Exception: pass
    return final

# ---------- Main ----------
def main():
    print(f"=== p53 Collector(v3) Start: code={CODE}, date={DATE} ===")
    raw = {}; norm = {}; notes = {}

    # Kiwoom
    print(">>> Kiwoom")
    k, k_reason = fetch_kiwoom(CODE, DATE); raw["Kiwoom"]=k; norm["Kiwoom"]=k; notes["Kiwoom"]=k_reason
    print(f"    keys: {len(k)} | sample: {_short(k)}")
    if k_reason: print("    note:", "; ".join(k_reason))

    # PyKRX
    print(">>> PyKRX")
    p, p_reason = fetch_pykrx(CODE, DATE); raw["PyKRX"]=p; norm["PyKRX"]=p; notes["PyKRX"]=p_reason
    print(f"    keys: {len(p)} | sample: {_short(p)}")
    if p_reason: print("    note:", "; ".join(p_reason))

    # Sector/Theme: Naver
    print(">>> Naver Sector/Theme")
    nvst, nvst_reason = fetch_naver_sector_theme(CODE); raw["NaverSectorTheme"]=nvst; norm["NaverSectorTheme"]=nvst; notes["NaverSectorTheme"]=nvst_reason
    print(f"    keys: {len(nvst)} | sample: {_short(nvst)}")
    if nvst_reason: print("    note:", "; ".join(nvst_reason))

    # FnGuide Sector (보조)
    print(">>> FnGuide")
    fn, fn_reason = fetch_fnguide_sector(CODE); raw["FnGuide"]=fn; norm["FnGuide"]=fn; notes["FnGuide"]=fn_reason
    print(f"    keys: {len(fn)} | sample: {_short(fn)}")
    if fn_reason: print("    note:", "; ".join(fn_reason))

    # Macro (Naver)
    print(">>> Naver Macro")
    nm, nm_reason = fetch_naver_macro(); raw["NaverMacro"]=nm; norm["NaverMacro"]=nm; notes["NaverMacro"]=nm_reason
    print(f"    keys: {len(nm)} | sample: {_short(nm)}")
    if nm_reason: print("    note:", "; ".join(nm_reason))

    # Macro (FDR)
    print(">>> FDR Macro")
    fdr, fdr_reason = fetch_fdr_macro(DATE); raw["FDR"]=fdr; norm["FDR"]=fdr; notes["FDR"]=fdr_reason
    print(f"    keys: {len(fdr)} | sample: {_short(fdr)}")
    if fdr_reason: print("    note:", "; ".join(fdr_reason))

    # Macro (BOK/Naver 10Y 보완)
    print(">>> BOK 10Y")
    bok, bok_reason = fetch_bok_kr10y(DATE); raw["BOK"]=bok; norm["BOK"]=bok; notes["BOK"]=bok_reason
    print(f"    keys: {len(bok)} | sample: {_short(bok)}")
    if bok_reason: print("    note:", "; ".join(bok_reason))

    # DART Finance
    print(">>> DART Finance")
    byear = int(DATE[:4])
    d, d_reason = fetch_dart_finance(CODE, byear); raw["DART"]=d; norm["DART"]=d; notes["DART"]=d_reason
    print(f"    keys: {len(d)} | sample: {_short(d)}")
    if d_reason: print("    note:", "; ".join(d_reason))

    # Merge
    final = merge_payloads(norm)
    got = {k:v for k,(v,_) in final.items()}
    cnt = len(got)
    missing = [c for c in V53 if c not in got]

    print("")
    print(f"[RESULT] V53 확보: {cnt}/{len(V53)} ({cnt/len(V53)*100:.1f}%)")
    print(f"[HAVE]   {', '.join(sorted(got.keys()))}")
    print(f"[MISS]   {', '.join(missing) if missing else '(없음)'}")

    # Save JSON
    ts = int(time.time())
    dump = {
        "code": CODE, "date": DATE, "v53_count": cnt,
        "have": sorted(got.keys()), "missing": missing,
        "result": {k: v for k,(v,src) in final.items()},
        "source": norm, "raw": raw, "notes": notes
    }
    out = os.path.join(OUTDIR, f"p53_dump_{CODE}_{DATE}_{ts}.json")
    try:
        with open(out,"w",encoding="utf-8") as f:
            json.dump(dump, f, ensure_ascii=False, indent=2, default=_json_safe)
        print(f"[SAVE] {out}")
    except Exception as e:
        print(f"[ERROR] save failed: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}")
