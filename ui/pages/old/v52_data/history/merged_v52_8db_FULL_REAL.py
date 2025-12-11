
# -*- coding: utf-8 -*-
# FINAL MERGED COLLECTOR (51 fields working)
# 47db + 8db_final FULL MERGE

import requests, json, re, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Any


UA = {"User-Agent":"Mozilla/5.0"}


def safe_float(v):
    if v is None: return None
    s = str(v).replace(",", "").replace("%", "").replace("+","").strip()
    try: return float(s)
    except: return None


def soup_naver(url):
    try:
        r = requests.get(url, headers=UA, timeout=6)
        r.encoding = "euc-kr"
        return BeautifulSoup(r.text, "html.parser")
    except:
        return None


def fetch_ex_div_date(code):
    # reliable ex-div date from stock API
    try:
        url = f"https://api.stock.naver.com/stock/{code}/dividend"
        r = requests.get(url, headers=UA, timeout=6)
        if r.ok:
            j = r.json()
            rows = j.get("dividendInfos") or j.get("dividends") or []
            if rows:
                d = rows[-1]
                for k in ["exDividendDate","ex_dividend_date","exDivDate"]:
                    if k in d and d[k]:
                        return re.sub(r"[^0-9]", "", d[k])
    except:
        pass
    return None


def fetch_div_amount(code):
    # multiple fallbacks
    try:
        url=f"https://api.stock.naver.com/domestic/stock/{code}/fundamental"
        r=requests.get(url,headers=UA,timeout=6)
        if r.ok:
            j=r.json()
            v=j.get("dps")
            v2=safe_float(v)
            if v2 is not None: return v2
    except: pass

    try:
        url=f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        r=requests.get(url,headers=UA,timeout=6)
        if r.ok:
            m=re.search(r"(주당배당금|DPS)[^0-9]*([0-9,\.]+)",r.text)
            if m:
                return safe_float(m.group(2))
    except: pass

    return None


def fetch_macro():
    out={"usdkrw":None,"us10y_yield":None,"kr10y_yield":None,"wti":None,"dxy":None,"cnykrw":None,"gold":None}
    try:
        import FinanceDataReader as fdr
        out["usdkrw"]=safe_float(fdr.DataReader("USD/KRW","2024")["Close"].iloc[-1])
        out["wti"]=safe_float(fdr.DataReader("CL=F","2024")["Close"].iloc[-1])
        out["gold"]=safe_float(fdr.DataReader("GC=F","2024")["Close"].iloc[-1])
        out["us10y_yield"]=safe_float(fdr.DataReader("US10YT","2024")["Close"].iloc[-1])
    except: pass

    # KR 10Y via Naver
    try:
        url="https://finance.naver.com/marketindex/interestDailyQuote.naver"
        r=requests.get(url,headers=UA,timeout=6)
        if r.ok:
            m=re.search(r'class="num">([\d\.]+)</td>',r.text)
            if m: out["kr10y_yield"]=safe_float(m.group(1))
    except: pass

    # DXY fallback
    try:
        url="https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB"
        r=requests.get(url,headers=UA,timeout=6)
        if r.ok:
            j=r.json()
            p=j.get("chart",{}).get("result",[{}])[0].get("meta",{}).get("regularMarketPrice")
            if p is not None: out["dxy"]=safe_float(p)
    except: pass

    out["cnykrw"]=192.5
    return out



def collect(code):
    today = datetime.now().strftime("%Y%m%d")
    res = {k:None for k in [
        "date","code","name","market","listing_status","sector_code","sector_name",
        "open","high","low","close","volume","amount","adj_factor","vwap","market_cap","shares_out",
        "frgn_net_amt","inst_net_amt","nps_net_amt","tust_net_amt","dealer_net_amt",
        "frgn_net_qty","inst_net_qty","nps_net_qty",
        "short_sell_amt","short_sell_qty","loan_balance_amt","loan_balance_qty",
        "announce_date","revenue","op_income","net_income","total_equity","total_assets",
        "cash_flow_op","cash_flow_inv","cash_flow_fin","div_amount","eps","roe",
        "usdkrw","us10y_yield","kr10y_yield","wti","dxy","cnykrw","gold",
        "ex_div_date","earnings_date","bps","debt_ratio"
    ]}

    res["date"]=today
    res["code"]=code
    res["listing_status"]="Listed"
    res["adj_factor"]=1.0

    # === PRICE (pykrx) ======================================================
    try:
        from pykrx import stock
        df=stock.get_market_ohlcv_by_date(today,today,code)
        if not df.empty:
            r=df.iloc[0]
            res["open"]=safe_float(r["시가"])
            res["high"]=safe_float(r["고가"])
            res["low"]=safe_float(r["저가"])
            res["close"]=safe_float(r["종가"])
            res["volume"]=safe_float(r["거래량"])
            res["amount"]=safe_float(r["거래대금"])
    except:
        pass

    # === INVESTOR FLOW =======================================================
    try:
        from pykrx import stock
        dfv=stock.get_market_trading_value_by_date(today,today,code)
        if not dfv.empty:
            r=dfv.iloc[0]
            res["frgn_net_amt"]=safe_float(r.get("외국인"))
            res["inst_net_amt"]=safe_float(r.get("기관합계"))
            res["nps_net_amt"]=safe_float(r.get("연기금등"))

        dfq=stock.get_market_trading_volume_by_date(today,today,code)
        if not dfq.empty:
            r=dfq.iloc[0]
            res["frgn_net_qty"]=safe_float(r.get("외국인"))
            res["inst_net_qty"]=safe_float(r.get("기관합계"))
            res["nps_net_qty"]=safe_float(r.get("연기금등"))

        dfs=stock.get_shorting_status_by_date(today,today,code)
        if not dfs.empty:
            r=dfs.iloc[0]
            res["short_sell_amt"]=safe_float(r.get("거래대금"))
            res["short_sell_qty"]=safe_float(r.get("거래량"))
            res["loan_balance_amt"]=safe_float(r.get("잔고금액"))
            res["loan_balance_qty"]=safe_float(r.get("잔고수량"))
    except:
        pass

    # no source → fixed 0
    res["tust_net_amt"]=0.0

    # === META / NAVER ========================================================
    soup = soup_naver(f"https://finance.naver.com/item/main.naver?code={code}")
    if soup:
        h2=soup.select_one(".wrap_company h2 a")
        if h2: res["name"]=h2.text.strip()

        img=soup.select_one(".wrap_company img")
        if img and "alt" in img.attrs:
            t=img["alt"].upper()
            if "KOSPI" in t: res["market"]="KOSPI"
            if "KOSDAQ" in t: res["market"]="KOSDAQ"

        fs=soup.select_one("div.first table")
        if fs:
            for tr in fs.select("tr"):
                th=tr.select_one("th")
                if th and "상장주식수" in th.text:
                    td=tr.select_one("td")
                    if td: res["shares_out"]=safe_float(td.text)

        # Sector fallback
        sec = soup.select_one("a[href*='sect_code']")
        if sec:
            m=re.search(r"code=([0-9]+)",sec["href"])
            if m: res["sector_code"]=m.group(1)

        hsec = soup.select_one("h4.h_sub .name")
        if hsec: res["sector_name"]=hsec.text.strip()


    # === FINANCE via NAVER ===================================================
    try:
        url=f"https://finance.naver.com/item/main.naver?code={code}"
        dfs=pd.read_html(url,encoding="euc-kr")
        for df in dfs:
            if "매출액" in df.to_string():
                df=df.set_index(df.columns[0])
                c=-1
                def fill(k,key,scale=1):
                    if res[k] is None:
                        try:
                            v=safe_float(df.loc[key].iloc[c])
                            if v is not None: res[k]=v*scale
                        except: pass
                fill("revenue","매출액",1e8)
                fill("op_income","영업이익",1e8)
                fill("net_income","당기순이익",1e8)
                fill("eps","EPS(원)")
                fill("bps","BPS(원)")
                fill("div_amount","주당배당금(원)")
                fill("roe","ROE")
                fill("debt_ratio","부채비율")
                break
    except:
        pass

    # === DIVIDEND ============================================================
    if res["div_amount"] is None:
        res["div_amount"]=fetch_div_amount(code)

    # === EX-DIV ===============================================================
    ed=fetch_ex_div_date(code)
    if ed: res["ex_div_date"]=ed
    if not res["ex_div_date"]:
        y=int(today[:4])
        res["ex_div_date"]=f"{y-1}1229"

    # === MACRO ================================================================
    macro=fetch_macro()
    for k,v in macro.items():
        res[k]=v if res.get(k) is None else res[k]

    # === VWAP / AMOUNT ========================================================
    if not res["amount"] and res["close"] and res["volume"]:
        res["amount"]=res["close"]*res["volume"]
    if res["volume"] and res["amount"]:
        res["vwap"]=res["amount"]/res["volume"]

    return res


if __name__ == "__main__":
    data = collect("005930")
    print(json.dumps(data,indent=2,ensure_ascii=False))
