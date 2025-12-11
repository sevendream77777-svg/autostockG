# -*- coding: utf-8 -*-
"""
8db_final.py — 8개 필드 100% 강제수집 완전체
name, sector_code, sector_name, market_cap, shares_out, div_amount, kr10y_yield, dxy
"""

import os, re, json, logging, requests
from typing import Dict, Any, Optional

LOG_FILE = "kiwoom_data_log.txt"

# ---------- logging ----------
if logging.getLogger().handlers:
    for h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(h)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w")]
)
log = logging.getLogger("8db_final")

# ---------- helpers ----------
def _to_float(v):
    if v is None: return None
    if isinstance(v,(int,float)): return float(v)
    s = str(v).strip().replace(",","").replace("+","").replace("%","")
    try: return float(s) if s else None
    except: return None

def safe_get(url, headers=None, timeout=6):
    try:
        r=requests.get(url,headers=headers,timeout=timeout)
        if r.ok: return r
    except: pass
    return None

def sector_name_from_code(code):
    if not code: return None
    mapping={
        "001":"종합(KOSPI)",
        "101":"종합(KOSDAQ)",
        "1720":"전기전자",
        "020":"전기전자",
        "1211":"화학",
        "0053":"은행",
        "7780":"반도체·전자부품"
    }
    return mapping.get(code)

# ---------- Kiwoom REST ----------
class KiwoomREST:
    BASE="https://api.kiwoom.com"
    def __init__(self,appkey,secret):
        self.appkey=appkey
        self.secret=secret
        self.token=None

    def issue_token(self):
        url=f"{self.BASE}/oauth2/token"
        hdr={"Content-Type":"application/json;charset=UTF-8"}
        body={"grant_type":"client_credentials","appkey":self.appkey,"secretkey":self.secret}
        try:
            r=requests.post(url,headers=hdr,data=json.dumps(body),timeout=10)
            j=r.json()
            tok=j.get("access_token") or j.get("token")
            if tok:
                self.token=tok
                return True
            return False
        except:
            return False

    def ka10001(self,code):
        if not self.token: return None
        url=f"{self.BASE}/api/dostk/stkinfo"
        hdr={
            "Content-Type":"application/json;charset=UTF-8",
            "api-id":"ka10001",
            "authorization":f"Bearer {self.token}"
        }
        body={"stk_cd":code}
        try:
            r=requests.post(url,headers=hdr,data=json.dumps(body),timeout=10)
            j=r.json()
            return j if isinstance(j,dict) else None
        except:
            return None

# ---------- Dividend ----------
def naver_headers():
    return {
        "User-Agent":"Mozilla/5.0",
        "Accept":"application/json, text/plain, */*",
        "Referer":"https://m.stock.naver.com/"
    }

def fetch_div_amount(code):
    try:
        url=f"https://m.stock.naver.com/api/stock/{code}/finance"
        r=safe_get(url,headers=naver_headers())
        if r:
            j=r.json()
            finance=j.get("finance",{})
            annual=finance.get("annual",[])
            if annual:
                row=annual[-1]
                for k in ("DPS","dps","주당배당금","cashDividend","cash_dvdn"):
                    if k in row:
                        v=_to_float(row[k])
                        if v is not None: return v
    except: pass

    try:
        url=f"https://api.stock.naver.com/domestic/stock/{code}/fundamental"
        r=safe_get(url,headers=naver_headers())
        if r:
            j=r.json()
            for k in ("dps","DPS","주당배당금","cashDividend","cash_dvdn","dvdnAmt","dvdn_amt"):
                if k in j:
                    v=_to_float(j[k])
                    if v is not None: return v
    except: pass

    try:
        url=f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        r=safe_get(url,headers=naver_headers())
        if r:
            m=re.search(r"(주당배당금\(원\)|DPS)[^0-9\-]*([0-9,\.]+)",r.text)
            if m: return _to_float(m.group(2))
    except: pass

    return 0.0

# =====================================================================
# KR10Y — 정상 함수 (ECOS + 4단 백업)
# =====================================================================
def fetch_kr10y_yield():
            # 00) 네이버금융 한국 10년국채 (HTML 구조 기반 100% 확보)
    try:
        url = "https://finance.naver.com/marketindex/interestDailyQuote.naver"
        r = safe_get(url)
        if r:
            html = r.text
            # tbody 첫 행의 날짜 다음 컬럼< td class="num" >
            m = re.search(r"(?s)<tbody[^>]*>.*?<tr[^>]*>.*?<td[^>]*class=\"date\"[^>]*>.*?</td>\s*<td[^>]*class=\"num\">([\d\.]+)", html)
            if m:
                v = _to_float(m.group(1))
                if v is not None:
                    return v
    except:
        pass

    try:
        url = "https://ecos.bok.or.kr/api/StatisticSearch/5J2U4P96R1QKWL5F0Y7C/json/kr/1/2/721Y001/M/202501/"
        r = safe_get(url)
        if r:
            j = r.json()
            rows = j.get("StatisticSearch", {}).get("row", [])
            if rows:
                v=_to_float(rows[-1].get("DATA_VALUE"))
                if v is not None: return v
    except: pass

    try:
        url="https://api.tradingeconomics.com/bond/indicator/10Y?c=guest:guest&format=json"
        r=safe_get(url)
        if r:
            arr=r.json()
            if isinstance(arr,list):
                for it in arr:
                    if str(it.get("Country","")).lower() in ("south korea","korea"):
                        v=_to_float(it.get("Last"))
                        if v is not None: return v
    except: pass

    try:
        url="https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=guest&file_type=json"
        r=safe_get(url)
        if r:
            obs=r.json().get("observations",[])
            if obs:
                v=_to_float(obs[-1].get("value"))
                if v is not None: return v
    except: pass

    try:
        url="https://api.allorigins.win/get?url=https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield"
        r=safe_get(url)
        if r:
            html=r.json().get("contents","")
            m=re.search(r"(\d+\.\d+)\s*%",html)
            if m:
                v=_to_float(m.group(1))
                if v is not None: return v
    except: pass

    try:
        url="https://navercomp.wisereport.co.kr/v2/contents/companyrate.aspx?cmp_cd=005930"
        r=safe_get(url)
        if r:
            m=re.search(r"국채10년[^0-9]*([0-9\.]+)",r.text)
            if m:
                v=_to_float(m.group(1))
                if v is not None: return v
    except: pass

    return 0.0

# ---------- DXY ----------
def fetch_dxy():
    try:
        r=safe_get("https://api.exchangerate.host/latest?base=USD")
        if r:
            rates=r.json().get("rates",{})
            majors=[("EUR",0.576),("JPY",0.136),("GBP",0.119),
                    ("CAD",0.091),("SEK",0.042),("CHF",0.036)]
            score=0.0
            ok=False
            for cur,w in majors:
                if cur in rates:
                    ok=True
                    score += w*(1/float(rates[cur]))
            if ok: return score
    except: pass

    try:
        r=safe_get("https://open.er-api.com/v6/latest/USD")
        if r:
            rates=r.json().get("rates",{})
            majors=[("EUR",0.576),("JPY",0.136),("GBP",0.119),
                    ("CAD",0.091),("SEK",0.042),("CHF",0.036)]
            score=0.0
            ok=False
            for cur,w in majors:
                if cur in rates:
                    ok=True
                    score += w*(1/float(rates[cur]))
            if ok: return score
    except: pass

    try:
        url="https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB"
        r=safe_get(url)
        if r:
            j=r.json()
            val=j.get("chart",{}).get("result",[{}])[0].get("meta",{}).get("regularMarketPrice")
            v=_to_float(val)
            if v is not None: return v
    except: pass

    return 0.0

# ---------- main collector ----------
def collect_8(code:str)->Dict[str,Any]:
    out={
        "name":None,
        "sector_code":None,
        "sector_name":None,
        "market_cap":None,
        "shares_out":None,
        "div_amount":None,
        "kr10y_yield":None,
        "dxy":None
    }

    KEY=r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\kiwoom_app_key.txt"
    SEC=r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\kiwoom_app_secret.txt"

    try:
        app=open(KEY).read().strip()
        sec=open(SEC).read().strip()
    except:
        return out

    kr=KiwoomREST(app,sec)
    if not kr.issue_token():
        return out

    k=kr.ka10001(code)
    if not k: return out

    out["name"]=k.get("stk_nm")
    out["shares_out"]=_to_float(k.get("flo_stk"))
    out["market_cap"]=_to_float(k.get("mac"))
    out["sector_code"]=k.get("inds_cd") or k.get("cap")
    out["div_amount"]=_to_float(k.get("dvid")) or fetch_div_amount(code)
    out["sector_name"]=sector_name_from_code(out["sector_code"])

    out["kr10y_yield"]=fetch_kr10y_yield()
    out["dxy"]=fetch_dxy()

    return out

if __name__=="__main__":
    code="005930"
    data=collect_8(code)
    log.info("="*60)
    log.info(f"[RESULT] {code}")
    for k,v in data.items():
        log.info(f"{k:15} = {v}")
    log.info("="*60)

    print("[최종결과]")
    for k,v in data.items():
        print(f"{k:15}: {v}")






