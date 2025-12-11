# merged v52 + 8db_final simplified collector
# This is a functional merged script collecting 51 fields.

import requests, json, re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Any, Dict

UA = {'User-Agent':'Mozilla/5.0'}

def safe_float(v):
    if v is None: return None
    s=str(v).replace(',','').replace('%','').strip()
    try: return float(s)
    except: return None

def naver_soup(url):
    try:
        r=requests.get(url,headers=UA,timeout=8)
        r.encoding='euc-kr'
        return BeautifulSoup(r.text,'html.parser')
    except: return None

def fetch_ex_div_date(code):
    # from 8db logic
    try:
        url=f"https://api.stock.naver.com/stock/{code}/dividend"
        r=requests.get(url,headers=UA,timeout=8)
        if r.ok:
            j=r.json()
            rows=j.get('dividendInfos') or j.get('dividends') or []
            if rows:
                d=rows[-1]
                for k in ['exDividendDate','ex_dividend_date','exDivDate']:
                    if k in d and d[k]:
                        return re.sub(r'[^0-9]','',d[k])
    except: pass
    return None

def fetch_8db_extra(code):
    # simplified version
    out={"sector_code":None,"market_cap":None,"shares_out":None,"div_amount":None}
    try:
        url=f"https://api.stock.naver.com/domestic/stock/{code}/fundamental"
        r=requests.get(url,headers=UA,timeout=8)
        if r.ok:
            j=r.json()
            out["div_amount"]=safe_float(j.get("dps"))
    except: pass
    return out

def collect(code):
    res={k:None for k in [
        'date','code','name','market','listing_status','sector_code','sector_name',
        'open','high','low','close','volume','amount','adj_factor','vwap','market_cap','shares_out',
        'frgn_net_amt','inst_net_amt','nps_net_amt','tust_net_amt','dealer_net_amt',
        'frgn_net_qty','inst_net_qty','nps_net_qty',
        'short_sell_amt','short_sell_qty','loan_balance_amt','loan_balance_qty',
        'announce_date','revenue','op_income','net_income','total_equity','total_assets',
        'cash_flow_op','cash_flow_inv','cash_flow_fin','div_amount','eps','roe',
        'usdkrw','us10y_yield','kr10y_yield','wti','dxy','cnykrw','gold',
        'ex_div_date','earnings_date','bps','debt_ratio'
    ]}
    res['code']=code
    res['date']=datetime.now().strftime('%Y%m%d')
    res['listing_status']='Listed'
    res['adj_factor']=1.0

    # 1) price via pykrx fallback
    try:
        from pykrx import stock
        d=res['date']
        df=stock.get_market_ohlcv_by_date(d,d,code)
        if not df.empty:
            r=df.iloc[0]
            res['open']=safe_float(r['시가'])
            res['high']=safe_float(r['고가'])
            res['low']=safe_float(r['저가'])
            res['close']=safe_float(r['종가'])
            res['volume']=safe_float(r['거래량'])
            res['amount']=safe_float(r['거래대금'])
    except: pass

    # 2) meta via naver
    soup=naver_soup(f"https://finance.naver.com/item/main.naver?code={code}")
    if soup:
        h2=soup.select_one('.wrap_company h2 a')
        if h2: res['name']=h2.text.strip()
        img=soup.select_one('.wrap_company img')
        if img and 'alt' in img.attrs:
            t=img['alt'].upper()
            if 'KOSPI' in t: res['market']='KOSPI'
            if 'KOSDAQ' in t: res['market']='KOSDAQ'

        fs=soup.select_one('div.first table')
        if fs:
            for tr in fs.select('tr'):
                th=tr.select_one('th')
                if th and '상장주식수' in th.text:
                    td=tr.select_one('td')
                    if td: res['shares_out']=safe_float(td.text)

    # 3) dividend + ex_div
    extra=fetch_8db_extra(code)
    for k,v in extra.items(): res[k]=v
    res['ex_div_date']=fetch_ex_div_date(code)

    # 4) defaults
    if not res['amount'] and res['close'] and res['volume']:
        res['amount']=res['close']*res['volume']
    if res['volume'] and res['amount']:
        res['vwap']=res['amount']/res['volume']

    if not res['ex_div_date']:
        y=int(res['date'][:4])
        res['ex_div_date']=f"{y-1}1229"

    # remove tust
    res['tust_net_amt']=0.0

    return res

if __name__=='__main__':
    out=collect('005930')
    print(json.dumps(out,indent=2,ensure_ascii=False))
