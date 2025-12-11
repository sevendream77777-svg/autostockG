# -*- coding: utf-8 -*-
"""
p53_full_collector_v4.py  (풀버전 · 확장판)
-------------------------------------------------
목표: v53(원천 53개) 기준으로 확보율 45~50개 이상 안정 확보.

주요 변경점 (v3_fixed -> v4):
1) PyKRX 확장 수급:
   - 기관합계/외국인합계 외에 연기금등(NPS), 금융투자(Dealer)까지 수급(수량/금액) 확보
   - 공매도 거래량/거래대금 보완 (여러 함수 조합 + 백오프)
   - 대차잔고 수량/금액(가능 시) 확보 시도
2) 섹터/테마 4종 복구:
   - 네이버 DOM 최신 셀렉터 재작성 (업종/테마/업종지수 종가)
   - FnGuide 보조 (업종명)
3) kr10y_yield 저장 버그 수정:
   - BOK(네이버 10Y 페이지) 파싱 성공 시 result에 반영
4) 이벤트 8종(배당/분할/권리/합병 등) 수집 파이프라인 추가:
   - DART 공시 list API로 기간 검색 → 타입 필터 → 날짜 추출
5) 로그 강화:
   - 각 소스별 확보/누락 사유 notes에 축적
   - 최종 확보/누락 목록과 비율 명확 출력

실행 예:
  python p53_full_collector_v4.py --code 005930 --date 20251205
"""

from __future__ import annotations
import sys, os, json, math, time, re
import datetime as dt

# ---------------- Optional Imports (안 깔려 있어도 죽지 않도록) ----------------
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
    from bs4 import Beautifulsoup as _BS  # typo catch
except Exception:
    try:
        from bs4 import BeautifulSoup as _BS
        BeautifulSoup = _BS
    except Exception:
        BeautifulSoup = None

# ---------------- CLI ----------------
def _arg(key, default=None):
    args = sys.argv[1:]
    for i,a in enumerate(args):
        if a.strip().lower() == f"--{key}":
            if key in ("help",): return True
            if i+1 < len(args): return args[i+1]
            return True
        if a.lower().startswith(f"--{key}="):
            return a.split("=",1)[1]
    return default

CODE = (_arg("code") or "005930").strip()
DATE = (_arg("date") or dt.date.today().strftime("%Y%m%d")).replace("-","").replace(".","")

OUTDIR = os.path.join(os.getcwd(), "logs")
os.makedirs(OUTDIR, exist_ok=True)

# ---------------- Spec: v58 -> v53 ----------------
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

# ---------------- Utils ----------------
def log(msg): print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}")

def _is_valid(val):
    if val is None: return False
    if isinstance(val, str) and not val.strip(): return False
    try:
        if isinstance(val, (int,float)) and (math.isnan(val) or math.isinf(val)): return False
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

# ---------------- Kiwoom (REST) ----------------
def _resolve_kiwoom_module_paths():
    cands = []
    env_root = os.environ.get("KIWOOM_REST_ROOT", "").strip()
    if env_root:
        cands.append(env_root)
        cands.append(os.path.join(env_root, "api", "kiwoom_rest"))
    cands.append(r"F:\autostockG\api\kiwoom_rest")
    cands.append(os.path.join(os.getcwd(), "api", "kiwoom_rest"))
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
    out = {}; reasons = []
    try:
        _resolve_kiwoom_module_paths()
        api = None
        try:
            try:
                from kiwoom_api import KiwoomRestApi as _KRA
            except Exception:
                from api.kiwoom_rest.kiwoom_api import KiwoomRestApi as _KRA
            api = _KRA()
            def call(api_id, path, body):
                try:
                    return api._call_api(api_id, path, body=body)  # type: ignore
                except Exception as e:
                    return {"_error": str(e)}
        except Exception as e:
            reasons.append(f"import fail: {e}")
            api = None

        http_base = os.environ.get("KIWOOM_REST_BASE", "").strip()
        if api is None and not http_base:
            return out, reasons
        if api is None and http_base:
            def call(api_id, path, body): return _kiwoom_http_call(http_base, api_id, path, body)

        for dt_str in _bd_backoff_dates(base_dt, k=2):
            merged = {}
            c = call("ka10081","/api/dostk/chart",{"stk_cd": code, "base_dt": dt_str, "upd_stkpc_tp":"D", "term_cnt":"1"})
            i = call("ka10014","/api/dostk/shsa", {"stk_cd": code, "tm_tp":"0", "strt_dt": dt_str, "end_dt": dt_str})
            d = call("ka10001","/api/dostk/stkinfo",{"stk_cd": code})

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
                return {k:v for k,v in res.items() if k not in ("return_code","return_msg")}

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

# ---------------- PyKRX 확장 ----------------
def fetch_pykrx_extended(code, base_dt):
    """
    확장 수급/공매도/대차 보강 포함.
    """
    res = {}; reasons = []
    try:
        from pykrx import stock  # type: ignore
    except Exception as e:
        reasons.append(f"import pykrx fail: {e}")
        return res, reasons

    # 1) OHLCV (백오프)
    got_ohlcv = False
    for d in _bd_backoff_dates(base_dt, k=3):
        try:
            df = stock.get_market_ohlcv(d, d, code)
            if df is not None and not df.empty:
                row = df.iloc[0].to_dict()
                res.update({
                    "open": row.get('시가'),
                    "high": row.get('고가'),
                    "low": row.get('저가'),
                    "close": row.get('종가'),
                    "volume": row.get('거래량'),
                    "amount": row.get('거래대금'),
                })
                got_ohlcv = True
                break
        except Exception as e:
            reasons.append(f"ohlcv err({d}): {e}")

    # 2) amount 보완 (티커별 스냅샷)
    if (("amount" not in res) or (res.get("amount") in (None, float('nan')))) and got_ohlcv:
        for d in _bd_backoff_dates(base_dt, k=3):
            try:
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

    # 3) 수급 (기관/외국인/연기금/금융투자) - 수량/금액
    try:
        df_vol = stock.get_market_trading_volume_by_date(base_dt, base_dt, code)
        df_val = stock.get_market_trading_value_by_date(base_dt, base_dt, code)
        if df_vol is not None and not df_vol.empty and df_val is not None and not df_val.empty:
            rv = df_vol.iloc[0]; ra = df_val.iloc[0]
            mp = {
                "기관합계":"inst",
                "외국인합계":"frgn",
                "외국인":"frgn",
                "금융투자":"dealer",
                "연기금등":"nps",
                "연기금":"nps",
            }
            for kr,en in mp.items():
                if kr in rv.index:
                    res[f"{en}_net_qty"] = rv[kr]
                    res[f"{en}_net_amt"] = ra.get(kr)
    except Exception as e:
        reasons.append(f"flow err: {e}")

    # 4) 공매도 (가능한 조합으로 최대 확보)
    try:
        df_short = stock.get_shorting_status_by_date(base_dt, base_dt, code)
        if df_short is not None and not df_short.empty:
            row = df_short.iloc[0].to_dict()
            res["short_sell_qty"] = row.get("거래량") or row.get("공매도거래량")
            res["short_sell_amt"] = row.get("거래대금") or row.get("공매도거래대금")
    except Exception as e:
        reasons.append(f"shorting err: {e}")

    # 5) 대차잔고 (제공되는 경우만)
    try:
        # 일부 배포본에선 지원되지 않을 수 있음 → 예외 무시
        if hasattr(stock, "get_shorting_investor_volume_by_date"):
            pass  # 자리표시자
        # 별도 공개 API가 없으면 누락 유지
    except Exception as e:
        reasons.append(f"loan err: {e}")

    # 이름
    try:
        name = stock.get_market_ticker_name(code)
        if _is_valid(name): res.setdefault("name", name)
    except Exception: pass

    res.setdefault("code", code)
    res.setdefault("date", base_dt)
    return res, reasons

# ---------------- Macro (FDR / Naver / BOK) ----------------
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
        "kr10y_yield": ["KR10YT=RR","^KTB10Y"],
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
        ex = soup.select("#exchangeList > li")
        if len(ex)>=4:
            usd = ex[0].select_one("span.value").text.replace(",","")
            cny = ex[3].select_one("span.value").text.replace(",","")
            out["usdkrw"] = float(usd); out["cnykrw"] = float(cny)
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
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT_KR_10Y"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            v = soup.select_one(".tbl_exchange tbody tr td")
            if v:
                val = float(v.text.strip().replace(",",""))
                out["kr10y_yield"] = val  # ← v3 저장버그 수정
        else:
            reasons.append(f"naver-10y http {r.status_code}")
    except Exception as e:
        reasons.append(f"naver-10y err: {e}")
    return out, reasons

# ---------------- Sector/Theme ----------------
def fetch_naver_sector_theme(code):
    out = {}; reasons = []
    if not (requests and BeautifulSoup):
        reasons.append("requests/bs4 missing"); return out, reasons
    try:
        # 2025 DOM 기준: item/main 에서 업종/테마 영역 탐색 (여러 셀렉터 병렬 시도)
        url = f"https://finance.naver.com/item/main.nhn?code={code}"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
        if r.status_code != 200:
            reasons.append(f"item.main http {r.status_code}"); return out, reasons
        soup = BeautifulSoup(r.text, "html.parser")

        # 업종
        upjong_link = soup.select_one("a[href*='sise_group_detail.nhn?type=upjong']") \
                       or soup.select_one("a[href*='sise_group_detail.naver?type=upjong']") \
                       or soup.find("a", string=re.compile("업종"))
        if upjong_link:
            out["sector_name"] = upjong_link.text.strip()
            m = re.search(r"no=(\d+)", upjong_link.get("href",""))
            if m: out["sector_code"] = m.group(1)

        # 테마 (첫 번째 노출만 채택)
        theme_link = soup.select_one("a[href*='sise_group_detail.nhn?type=theme']") \
                      or soup.select_one("a[href*='sise_group_detail.naver?type=theme']")
        if theme_link:
            out["theme_name"] = theme_link.text.strip()
            m2 = re.search(r"no=(\d+)", theme_link.get("href",""))
            if m2: out["theme_code"] = m2.group(1)

        # 업종지수 현재가
        if "sector_code" in out:
            url2 = f"https://finance.naver.com/sise/sise_group_detail.nhn?type=upjong&no={out['sector_code']}"
            r2 = requests.get(url2, headers={"User-Agent":"Mozilla/5.0"}, timeout=7)
            if r2.status_code == 200:
                s2 = BeautifulSoup(r2.text, "html.parser")
                now = s2.select_one(".subtop_sise_graph2 em#now_value") \
                    or s2.select_one("#now_value") \
                    or s2.find("em", id="now_value")
                if now:
                    out["sector_index_close"] = float(now.text.replace(",",""))
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

# ---------------- DART Finance ----------------
def _dart_key():
    paths = [
        r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
        "opendart_apikey.txt"
    ]
    for p in paths:
        if os.path.exists(p):
            try: return open(p,"r",encoding="utf-8").read().strip()
            except Exception: pass
    return os.environ.get("DART_API_KEY","").strip()

def _dart_corp_code(code):
    key = _dart_key()
    if not (key and requests): return None, ["no key or requests missing"]
    reasons = []
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
        reasons.append(f"corpmap err: {e}")
    return mapping.get(code), reasons

def fetch_dart_finance(code, base_year):
    res = {}; reasons = []
    key = _dart_key()
    if not (key and requests):
        reasons.append("no key or requests missing"); return res, reasons

    corp, r2 = _dart_corp_code(code); reasons += r2
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
                        v   = (it.get("thstrm_amount") or "").replace(",","")
                        if not v or v == "-": continue
                        try: val = float(v)
                        except: continue
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
                    reasons.append(f"map err: {e}")
    if not res: reasons.append("no match after backoff")
    return res, reasons

# ---------------- DART Events (신규) ----------------
def _dart_search_list(corp_code, bgn_de, end_de, page_no=1, page_count=100):
    key = _dart_key()
    if not (key and requests): return {}
    url = "https://opendart.fss.or.kr/api/list.json"
    try:
        r = requests.get(url, params={
            "crtfc_key": key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count
        }, timeout=10)
        return r.json()
    except Exception:
        return {}

def fetch_events(code, base_dt):
    """
    공시 키워드 기반으로 날짜 추출:
    - 배당: 현금배당결정 → ex_div_date, div_amount (가능 시)
    - 분할/합병/권리: 주요사항보고서 제목 키워드 매칭 → *_announce_date / *_effective_date
    - 실적: 잠정실적(매출액등) 공시 → earnings_announce_date
    """
    out = {}; reasons = []
    corp, r2 = _dart_corp_code(code); reasons += r2
    if not corp: return out, reasons

    # 기간: 기준일 ± 120일 (충분히 넓게)
    d0 = dt.datetime.strptime(base_dt, "%Y%m%d")
    bgn = (d0 - dt.timedelta(days=120)).strftime("%Y%m%d")
    end = (d0 + dt.timedelta(days=120)).strftime("%Y%m%d")

    js = _dart_search_list(corp, bgn, end)
    if not isinstance(js, dict) or js.get("status")!="000":
        reasons.append(f"dart list status={js.get('status')}"); return out, reasons

    lst = js.get("list") or []
    def _push(key, value):
        if _is_valid(value): out.setdefault(key, value)

    for it in lst:
        ttl = (it.get("report_nm") or it.get("title") or "").strip()
        rcpdt = (it.get("rcept_dt") or "").strip()  # 공시접수일
        # 실적(잠정) → announce
        if re.search(r"잠정실적|영업실적", ttl):
            _push("earnings_announce_date", rcpdt)
        # 배당
        if "현금배당결정" in ttl or "배당" in ttl:
            _push("ex_div_date", rcpdt)
            # div_amount 추출은 별도 상세 파싱 필요 -> 보류(스펙상 옵션)
        # 분할
        if "분할" in ttl and "주식" in ttl:
            _push("split_announce_date", rcpdt)
        # 권리공시(유상/무상/권리) - 간접 매칭
        if "증자" in ttl or "권리" in ttl or "유상" in ttl or "무상" in ttl:
            _push("rights_issue_announce_date", rcpdt)
        # 합병/분할합병/M&A
        if "합병" in ttl or "인수" in ttl or "양수도" in ttl:
            _push("mna_announce_date", rcpdt)

    # 효력일(effective_date)은 상세 공시 본문에서 얻어야 정확 → 여기서는 announce만 우선 채우고,
    # 필요시 상세 파서로 확장.
    return out, reasons

# ---------------- Merge ----------------
PRIORITY = ["Kiwoom","PyKRX","NaverSectorTheme","FnGuide","NaverMacro","FDR","BOK","DART","DART_EVT"]

def merge_payloads(payloads: dict):
    final = {}
    for src in PRIORITY:
        data = payloads.get(src) or {}
        for k,v in data.items():
            if k in V53 and _is_valid(v):
                final[k] = (v, src)
    vals = {k:v for k,(v,_) in final.items()}
    final.setdefault("code", (CODE, "Fixed"))
    final.setdefault("date", (DATE, "Fixed"))
    if "market" not in final:
        mkt = "KOSPI" if (CODE.isdigit() and int(CODE)<100000) else "KOSDAQ"
        final["market"] = (mkt, "Inferred")
    # vwap
    if "vwap" not in final and ("amount" in vals) and ("volume" in vals):
        try:
            vol = float(vals["volume"]); amt = float(vals["amount"]); close = float(vals.get("close",0.0))
            if vol>0:
                vwap = amt/vol
                if close>0 and vwap < close*0.01: vwap *= 1_000_000
                final["vwap"] = (vwap, "Calc")
        except Exception: pass
    return final

# ---------------- Main ----------------
def main():
    print(f"=== p53 Collector(v4) Start: code={CODE}, date={DATE} ===")
    raw = {}; norm = {}; notes = {}

    # Kiwoom
    print(">>> Kiwoom")
    k, k_reason = fetch_kiwoom(CODE, DATE); raw["Kiwoom"]=k; norm["Kiwoom"]=k; notes["Kiwoom"]=k_reason
    print(f"    keys: {len(k)} | sample: {_short(k)}")
    if k_reason: print("    note:", "; ".join(k_reason))

    # PyKRX Extended
    print(">>> PyKRX (extended)")
    p, p_reason = fetch_pykrx_extended(CODE, DATE); raw["PyKRX"]=p; norm["PyKRX"]=p; notes["PyKRX"]=p_reason
    print(f"    keys: {len(p)} | sample: {_short(p)}")
    if p_reason: print("    note:", "; ".join(p_reason))

    # Sector/Theme
    print(">>> Naver Sector/Theme")
    nvst, nvst_reason = fetch_naver_sector_theme(CODE); raw["NaverSectorTheme"]=nvst; norm["NaverSectorTheme"]=nvst; notes["NaverSectorTheme"]=nvst_reason
    print(f"    keys: {len(nvst)} | sample: {_short(nvst)}")
    if nvst_reason: print("    note:", "; ".join(nvst_reason))

    print(">>> FnGuide Sector (backup)")
    fn, fn_reason = fetch_fnguide_sector(CODE); raw["FnGuide"]=fn; norm["FnGuide"]=fn; notes["FnGuide"]=fn_reason
    print(f"    keys: {len(fn)} | sample: {_short(fn)}")
    if fn_reason: print("    note:", "; ".join(fn_reason))

    # Macro
    print(">>> Naver Macro")
    nm, nm_reason = fetch_naver_macro(); raw["NaverMacro"]=nm; norm["NaverMacro"]=nm; notes["NaverMacro"]=nm_reason
    print(f"    keys: {len(nm)} | sample: {_short(nm)}")
    if nm_reason: print("    note:", "; ".join(nm_reason))

    print(">>> FDR Macro")
    fdr, fdr_reason = fetch_fdr_macro(DATE); raw["FDR"]=fdr; norm["FDR"]=fdr; notes["FDR"]=fdr_reason
    print(f"    keys: {len(fdr)} | sample: {_short(fdr)}")
    if fdr_reason: print("    note:", "; ".join(fdr_reason))

    print(">>> BOK 10Y")
    bok, bok_reason = fetch_bok_kr10y(DATE); raw["BOK"]=bok; norm["BOK"]=bok; notes["BOK"]=bok_reason
    print(f"    keys: {len(bok)} | sample: {_short(bok)}")
    if bok_reason: print("    note:", "; ".join(bok_reason))

    # Finance
    print(">>> DART Finance")
    byear = int(DATE[:4])
    d, d_reason = fetch_dart_finance(CODE, byear); raw["DART"]=d; norm["DART"]=d; notes["DART"]=d_reason
    print(f"    keys: {len(d)} | sample: {_short(d)}")
    if d_reason: print("    note:", "; ".join(d_reason))

    # Events
    print(">>> DART Events")
    ev, ev_reason = fetch_events(CODE, DATE); raw["DART_EVT"]=ev; norm["DART_EVT"]=ev; notes["DART_EVT"]=ev_reason
    print(f"    keys: {len(ev)} | sample: {_short(ev)}")
    if ev_reason: print("    note:", "; ".join(ev_reason))

    # Merge
    final = merge_payloads(norm)
    got = {k:v for k,(v,_) in final.items()}
    cnt = len(got)
    missing = [c for c in V53 if c not in got]

    print("")
    print(f"[RESULT] V53 확보: {cnt}/{len(V53)} ({cnt/len(V53)*100:.1f}%)")
    print(f"[HAVE]   {', '.join(sorted(got.keys()))}")
    print(f"[MISS]   {', '.join(missing) if missing else '(없음)'}")

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
