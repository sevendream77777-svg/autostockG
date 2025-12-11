# -*- coding: utf-8 -*-
import sys, os, re, json, requests, datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
from pykrx import stock
import FinanceDataReader as fdr

UA = {"User-Agent":"Mozilla/5.0"}
TIMEOUT = 8

def rq(url,params=None):
    r = requests.get(url,params=params,headers=UA,timeout=TIMEOUT)
    r.encoding = r.apparent_encoding or "utf-8"
    return r

def get_name(code):
    try:
        return stock.get_market_ticker_name(code)
    except:
        try:
            url=f"https://finance.naver.com/item/main.naver?code={code}"
            s=BeautifulSoup(rq(url).text,"html.parser")
            t=s.find("title").get_text().strip()
            if "-" in t: return t.split("-")[0].strip()
        except: pass
    return None

def get_ohlcv(code,date):
    try:
        df=stock.get_market_ohlcv_by_date(date,date,code)
        if df is not None and not df.empty:
            r=df.iloc[0]
            return {
                "open":float(r.get("시가") or r.get("Open")),
                "high":float(r.get("고가") or r.get("High")),
                "low":float(r.get("저가") or r.get("Low")),
                "close":float(r.get("종가") or r.get("Close")),
                "volume":float(r.get("거래량") or r.get("Volume")),
                "amount":float(r.get("거래대금") or r.get("Amount") or 0)
            }
    except: pass
    return {"open":None,"high":None,"low":None,"close":None,"volume":None,"amount":None}

def get_cap(code,date):
    try:
        df=stock.get_market_cap_by_ticker(date)
        r=df.loc[code]
        return {
            "market_cap":float(r.get("시가총액") or r.get("Marcap")),
            "shares_out":float(r.get("상장주식수") or r.get("Shares"))
        }
    except: pass
    return {"market_cap":None,"shares_out":None}

def get_macro(date):
    s=dt.datetime.strptime(date,"%Y%m%d")
    start=s-dt.timedelta(days=15)
    def f(sym):
        try:
            d=fdr.DataReader(sym,start,s)
            return float(d["Close"].iloc[-1])
        except: return None
    out={
        "usdkrw":f("USD/KRW"),
        "us10y_yield":f("US10YT"),
        "gold":f("GC=F"),
        "dxy":f("DX-Y.NYB"),
        "wti":f("CL=F"),
        "cnykrw":190.0
    }
    # KR10Y
    try:
        url="https://polling.finance.naver.com/api/realtime/domestic/index/bond/IRr_GOVT10Y"
        js=rq(url).json()
        v=float(js["datas"][0]["nv"])
        out["kr10y_yield"]=v/100 if v>10 else v
    except:
        out["kr10y_yield"]=f("KR10YT")
    return out

def get_sector(code):
    try:
        url=f"https://finance.naver.com/item/main.naver?code={code}"
        html=rq(url).text
        m=re.search(r"type=upjong(?:&amp;|&)no=(\d+)",html)
        soup=BeautifulSoup(html,"html.parser")
        h=soup.find("h4",class_="h_sub")
        return {
            "sector_code": m.group(1) if m else None,
            "sector_name": h.get_text().strip() if h else None
        }
    except: return {"sector_code":None,"sector_name":None}

def get_finance(code):
    out={"eps":None,"bps":None,"div_amount":None,"roe":None,"debt_ratio":None,
         "revenue":None,"op_income":None,"net_income":None,
         "total_equity":None,"total_assets":None,
         "cash_flow_op":None,"cash_flow_inv":None,"cash_flow_fin":None,
         "announce_date":None}
    try:
        url=f"https://finance.naver.com/item/main.naver?code={code}"
        soup=BeautifulSoup(rq(url).text,"html.parser")
        tbody=soup.select_one("div.section.cop_analysis tbody")
        if tbody:
            rows=tbody.find_all("tr")
            def last_val(i):
                try:
                    cells=rows[i].find_all("td")
                    for c in reversed(cells):
                        s=c.get_text().strip()
                        if s and s!="-" and "E" not in s:
                            return float(s.replace(",",""))*100000000
                except: return None
            out["revenue"]=last_val(0)
            out["op_income"]=last_val(1)
            out["net_income"]=last_val(2)
            out["roe"]=None
            out["debt_ratio"]=None
    except: pass
    return out

def collect(code,date):
    res={}
    res["date"]=date
    res["code"]=code
    res["market"]="KOSPI"
    res["listing_status"]="Listed"
    res["name"]=get_name(code)

    p=get_ohlcv(code,date)
    res.update(p)

    cap=get_cap(code,date)
    res.update(cap)
    if res["amount"] and res["volume"]:
        try: res["vwap"]=res["amount"]/res["volume"]
        except: res["vwap"]=None
    else: res["vwap"]=None

    sec=get_sector(code)
    res.update(sec)

    fin=get_finance(code)
    res.update(fin)

    macro=get_macro(date)
    res.update(macro)

    # placeholders
    for k in ["frgn_net_amt","inst_net_amt","nps_net_amt","tust_net_amt","dealer_net_amt",
              "frgn_net_qty","inst_net_qty","nps_net_qty",
              "short_sell_amt","short_sell_qty","loan_balance_amt","loan_balance_qty",
              "ex_div_date","earnings_date"]:
        res[k]=res.get(k) or 0

    return res

def main():
    code="005930"
    date=dt.datetime.now().strftime("%Y%m%d")
    print(json.dumps(collect(code,date),indent=2,ensure_ascii=False))

if __name__=="__main__":
    main()
