# -*- coding: utf-8 -*-
"""
p0_light_collector.py
---------------------
[목표] V58 컬럼 데이터 수집 전용 경량화 스크립트 (Pure Raw & Smart Crawling)
[수정 내역]
 1. [Critical] 가짜 데이터(추정치) 생성 로직 완전 제거 (거래대금 0원 보정 삭제)
 2. [Sector] '주소', '대표이사' 등 오수집 필터링 강화 + 네이버 금융 백업
 3. [Macro] 금리/유가 등 매크로 지표 정밀 파싱 (진짜 데이터 확보율 증대)
"""

import sys
import os
import time
import requests
import pandas as pd
import numpy as np
import datetime as dt
from typing import Dict, Any

# [필수 라이브러리 체크]
try:
    from pykrx import stock
    from bs4 import BeautifulSoup
    import FinanceDataReader as fdr
except ImportError as e:
    print(f"[System] 필수 패키지 누락: {e}")
    print("pip install pykrx beautifulsoup4 finance-datareader requests pandas numpy")
    sys.exit(1)

# ------------------------------------------------------------------------------
# 1. 설정 및 V58 정의
# ------------------------------------------------------------------------------
V58_COLS = [
    # Price (12)
    "date", "code", "name", "market", "open", "high", "low", "close",
    "volume", "amount", "adj_factor", "vwap",
    # Flow (12)
    "inst_net_qty", "inst_net_amt", "frgn_net_qty", "frgn_net_amt",
    "nps_net_qty", "nps_net_amt", "dealer_net_qty", "dealer_net_amt",
    "short_sell_qty", "short_sell_amt", "loan_balance_qty", "loan_balance_amt",
    # Finance (11)
    "revenue", "op_income", "net_income", "eps", "bps", "roe", "roa",
    "debt_ratio", "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    # Sector/Theme (5)
    "sector_code", "sector_name", "theme_code", "theme_name", "sector_index_close",
    # Macro (8)
    "usdkrw", "cnykrw", "dxy", "us10y_yield", "kr10y_yield", "wti", "gold", "vix",
    # Event (10)
    "earnings_announce_date", "earnings_surprise", "earnings_effective_date",
    "ex_div_date", "div_amount",
    "split_announce_date", "split_effective_date",
    "rights_issue_announce_date", "rights_issue_effective_date",
    "mna_announce_date",
]

# ------------------------------------------------------------------------------
# 2. 유틸리티 함수
# ------------------------------------------------------------------------------
def get_dart_api_key():
    paths = [
        "opendart_apikey.txt",
        r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
    ]
    for p in paths:
        if os.path.exists(p):
            try:
                return open(p, "r", encoding="utf-8").read().strip()
            except: pass
    return ""

def safe_float(val):
    try:
        if isinstance(val, str):
            val = val.replace(",", "")
        return float(val)
    except:
        return 0.0

# ------------------------------------------------------------------------------
# 3. 데이터 수집 클래스
# ------------------------------------------------------------------------------
class DataCollector:
    def __init__(self, code, date_str):
        self.code = code
        self.input_date = date_str.replace("-", "").replace(".", "")
        self.found_date = self.input_date 
        self.results = {}
        self.logs = []
        
    def log(self, msg):
        self.logs.append(msg)
        print(f"[Log] {msg}")

    def run(self):
        print(f"\n=== 데이터 수집 시작: {self.code} (요청일: {self.input_date}) ===")
        
        # 1. 기본 정보 & 가격 (PyKRX)
        self._fetch_pykrx_basic()
        
        # 2. 상세 수급 (PyKRX Enhanced)
        self._fetch_pykrx_flow_details()
        
        # 3. 매크로 (Naver Deep Search)
        self._fetch_macro()
        
        # 4. 섹터/테마 (Smart Filter)
        self._fetch_sector_theme()
        
        # 5. 재무 (DART Robust)
        self._fetch_dart_finance()
        
        # 6. 파생지표 계산 (순수 계산만)
        self._calc_derived_metrics()
        
        return self.results

    # --- [A] PyKRX 기본 (보정 제거: 순수 데이터만) ---
    def _fetch_pykrx_basic(self):
        try:
            target_dt = self.input_date
            df = pd.DataFrame()
            
            # 5일 역추적
            for _ in range(5):
                try:
                    df = stock.get_market_ohlcv(target_dt, target_dt, self.code)
                    cols_str = " ".join([str(c) for c in df.columns])
                    if not df.empty and ("시가" in cols_str or "Open" in cols_str):
                         if not df.iloc[0].empty:
                            self.found_date = target_dt 
                            break
                except: pass
                target_dt = (pd.to_datetime(target_dt) - pd.Timedelta(days=1)).strftime("%Y%m%d")

            if not df.empty:
                row = df.iloc[0].to_dict()
                
                amount_val = 0
                for k in row.keys():
                    ks = str(k)
                    if "대금" in ks or "Amount" in ks or "Value" in ks:
                        amount_val = row[k]
                        break
                
                # [중요] 거래대금 0원 보정 로직 삭제함. 0이면 0 그대로 둠.
                
                self.results.update({
                    "date": self.found_date,
                    "code": self.code,
                    "open": float(row.get('시가') or row.get('Open') or 0),
                    "high": float(row.get('고가') or row.get('High') or 0),
                    "low": float(row.get('저가') or row.get('Low') or 0),
                    "close": float(row.get('종가') or row.get('Close') or 0),
                    "volume": float(row.get('거래량') or row.get('Volume') or 0),
                    "amount": float(amount_val)
                })
                
                try:
                    name = stock.get_market_ticker_name(self.code)
                    self.results['name'] = name
                except: pass
                
                try:
                    ticker_market = stock.get_market_ticker_list(self.found_date, market="KOSPI")
                    self.results['market'] = "KOSPI" if self.code in ticker_market else "KOSDAQ"
                except: self.results['market'] = "KOSPI"

                # Fundamental
                try:
                    df_fund = stock.get_market_fundamental(self.found_date, self.found_date, self.code)
                    if not df_fund.empty:
                        row_f = df_fund.iloc[0]
                        if 'EPS' in row_f: self.results['eps'] = float(row_f['EPS'])
                        if 'BPS' in row_f: self.results['bps'] = float(row_f['BPS'])
                        if 'DPS' in row_f: self.results['div_amount'] = float(row_f['DPS'])
                except: pass
            else:
                self.log(f"최근 5일간 가격 데이터 없음.")
            
        except Exception as e:
            self.log(f"PyKRX Basic Error: {e}")

    # --- [B] PyKRX 수급 상세 ---
    def _fetch_pykrx_flow_details(self):
        try:
            target_dt = self.found_date
            
            df = stock.get_market_trading_volume_by_date(target_dt, target_dt, self.code)
            df_amt = stock.get_market_trading_value_by_date(target_dt, target_dt, self.code)
            
            if not df.empty:
                row = df.iloc[0]
                row_amt = df_amt.iloc[0] if not df_amt.empty else {}
                
                def find_val(keywords, data_row):
                    for col in data_row.index:
                        c_str = str(col)
                        if any(k in c_str for k in keywords):
                            return float(data_row[col])
                    return 0.0

                self.results["inst_net_qty"] = find_val(["기관", "Inst"], row)
                self.results["frgn_net_qty"] = find_val(["외국", "Foreign"], row)
                self.results["nps_net_qty"] = find_val(["연기금", "Pension"], row)
                self.results["dealer_net_qty"] = find_val(["금융투자", "Dealer"], row)
                
                self.results["inst_net_amt"] = find_val(["기관", "Inst"], row_amt)
                self.results["frgn_net_amt"] = find_val(["외국", "Foreign"], row_amt)
                self.results["nps_net_amt"] = find_val(["연기금", "Pension"], row_amt)
                self.results["dealer_net_amt"] = find_val(["금융투자", "Dealer"], row_amt)
                # 투신 순매수 금액 (tust_net_amt) - PyKRX '투신' 컬럼 사용
                self.results["tust_net_amt"] = find_val(["투신", "Trust"], row_amt)

            try:
                # 공매도는 없을 수 있으므로 0.0 기본값 처리 (이건 추정이 아니라 '없음'을 의미)
                self.results['short_sell_qty'] = 0.0
                self.results['short_sell_amt'] = 0.0
                self.results['loan_balance_qty'] = 0.0
                self.results['loan_balance_amt'] = 0.0
                
                df_short = stock.get_shorting_status_by_date(target_dt, target_dt, self.code)
                if not df_short.empty:
                    row_s = df_short.iloc[0]
                    self.results['short_sell_qty'] = float(row_s.get('거래량', 0))
                    self.results['short_sell_amt'] = float(row_s.get('거래대금', 0))
                    self.results['loan_balance_qty'] = float(row_s.get('잔고수량', 0))
                    self.results['loan_balance_amt'] = float(row_s.get('잔고금액', 0))
            except: pass

        except Exception as e:
            self.log(f"PyKRX Flow Error: {e}")

    # --- [C] 매크로 (Deep Search) ---
    def _fetch_macro(self):
        try:
            url = "https://finance.naver.com/marketindex/"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            # 인코딩 자동 감지
            if res.encoding != 'cp949' and res.encoding != 'euc-kr':
                res.encoding = 'cp949'
            
            soup = BeautifulSoup(res.text, 'html.parser')
            
            def search_all_items(keywords):
                all_items = soup.select("li")
                for item in all_items:
                    txt = item.text.strip().replace("\n", "")
                    if any(k in txt for k in keywords):
                        val_tag = item.select_one(".value")
                        if val_tag:
                            return float(val_tag.text.replace(",", ""))
                return None

            usd = search_all_items(["미국 USD", "미국USD"])
            if usd: self.results['usdkrw'] = usd
            
            cny = search_all_items(["중국 CNY", "중국CNY"])
            if cny: self.results['cnykrw'] = cny

            wti = search_all_items(["WTI", "휘발유"])
            if wti: self.results['wti'] = wti
            
            gold = search_all_items(["국제 금", "국제금"])
            if gold: self.results['gold'] = gold

            kr10 = search_all_items(["국고채 10년", "국채 10년", "국고채권(10년)"])
            if kr10: self.results['kr10y_yield'] = kr10
            
            us10 = search_all_items(["미국 10년"])
            if us10: self.results['us10y_yield'] = us10

        except Exception as e:
            self.log(f"Naver Macro Error: {e}")

        # FDR Backup
        try:
            if 'dxy' not in self.results:
                end_dt = self.found_date
                start_dt = (pd.to_datetime(end_dt) - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
                df = fdr.DataReader('DX-Y.NYB', start_dt, end_dt)
                if not df.empty: 
                    self.results['dxy'] = float(df.iloc[-1]['Close'])
        except: pass

    # --- [D] 섹터/테마 (Smart Filter) ---
    def _fetch_sector_theme(self):
        # 1. FnGuide
        try:
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{self.code}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            group_div = soup.select_one(".corp_group1")
            if group_div:
                for span in group_div.select("span"):
                    txt = span.text.strip()
                    # [필터] '주소' 등 오답 제거
                    bad_words = ["KOSPI", "KOSDAQ", "코스피", "코스닥", 
                                 "홈페이지", "기업개요", "전화번호", "계열", "설립일", "상장일", 
                                 "주소", "대표이사", "종업원수", "결산월"]
                    
                    is_bad = any(bw in txt for bw in bad_words)
                    is_digit = any(c.isdigit() for c in txt) 
                    
                    if len(txt) > 1 and not is_bad and not is_digit:
                         self.results['sector_name'] = txt
                         break
        except: pass

        # 2. Naver 금융 (백업: 값이 없거나 나쁜 단어일 때)
        curr_sector = self.results.get('sector_name', '').strip()
        bad_words_check = ["주소", "전화", "번호", "대표"]
        
        if not curr_sector or any(x in curr_sector for x in bad_words_check):
            try:
                # 잘못된 값은 지우기
                if 'sector_name' in self.results: del self.results['sector_name']
                
                url_n = f"https://finance.naver.com/item/main.naver?code={self.code}"
                res_n = requests.get(url_n, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                if res_n.encoding != 'euc-kr': res_n.encoding = 'euc-kr'
                
                soup_n = BeautifulSoup(res_n.text, 'html.parser')
                sector_elem = soup_n.select_one("h4.h_sub > a")
                if sector_elem:
                    self.results['sector_name'] = sector_elem.text.strip()
                else:
                    sub_tit = soup_n.find("h4", class_="sub_tit7")
                    if sub_tit:
                         a_tag = sub_tit.find("a")
                         if a_tag: self.results['sector_name'] = a_tag.text.strip()
            except: pass

    # --- [E] DART 재무 ---
    def _fetch_dart_finance(self):
        api_key = get_dart_api_key()
        if not api_key: return

        corp_code = None
        if self.code == "005930": corp_code = "00126380"
        if not corp_code: return

        year = int(self.found_date[:4])
        
        STRONG_MAP = {
            "ifrs-full_EquityAttributableToOwnersOfParent": "equity",
            "ifrs-full_Equity": "equity",
            "ifrs-full_ProfitLossAttributableToOwnersOfParent": "net_income",
            "ifrs-full_ProfitLoss": "net_income",
            "ifrs-full_Assets": "assets",
            "ifrs-full_Liabilities": "liabilities",
            "ifrs-full_Revenue": "revenue",
            "ifrs-full_ProfitLossFromOperatingActivities": "op_income",
            "dart_OperatingIncomeLoss": "op_income",
            "ifrs-full_CashFlowsFromUsedInOperatingActivities": "cash_flow_op",
            "ifrs-full_CashFlowsFromUsedInInvestingActivities": "cash_flow_inv",
            "ifrs-full_CashFlowsFromUsedInFinancingActivities": "cash_flow_fin"
        }

        found_fin = False
        for y in range(year, year-3, -1):
            if found_fin: break
            for reprt_code in ["11011", "11012", "11014", "11013"]:
                if found_fin: break
                url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                params = {
                    "crtfc_key": api_key, "corp_code": corp_code, 
                    "bsns_year": str(y), "reprt_code": reprt_code, "fs_div": "CFS"
                }
                try:
                    resp = requests.get(url, params=params, timeout=5).json()
                    if resp.get("status") == "000" and resp.get("list"):
                        data_map = {}
                        for item in resp["list"]:
                            aid = item.get("account_id", "")
                            val = safe_float(item.get("thstrm_amount", ""))
                            
                            if aid in STRONG_MAP:
                                target_key = STRONG_MAP[aid]
                                if target_key in data_map:
                                    if "Attributable" in aid:
                                        data_map[target_key] = val
                                else:
                                    data_map[target_key] = val
                        
                        if data_map:
                            self.results.update(data_map)
                            found_fin = True
                except: pass

    # --- [F] 파생지표 계산 (순수 계산만) ---
    def _calc_derived_metrics(self):
        res = self.results
        
        # 1. VWAP (거래대금 0이면 계산 안함)
        if 'amount' in res and 'volume' in res and res['amount'] > 0 and res['volume'] > 0:
            res['vwap'] = res['amount'] / res['volume']
        
        # 2. ROA
        if 'net_income' in res and 'assets' in res and res['assets'] > 0:
            res['roa'] = (res['net_income'] / res['assets']) * 100
            
        # 3. ROE
        if 'net_income' in res and 'equity' in res and res['equity'] > 0:
            res['roe'] = (res['net_income'] / res['equity']) * 100
            
        # 4. 부채비율
        if 'liabilities' in res and 'equity' in res and res['equity'] > 0:
            res['debt_ratio'] = (res['liabilities'] / res['equity']) * 100
            
        # [삭제] adj_factor 강제 할당 로직 제거

# ------------------------------------------------------------------------------
# 실행 및 출력
# ------------------------------------------------------------------------------
def print_report(results, input_dt, found_dt):
    print("\n" + "="*60)
    print(f" [최종 수집 결과] (요청: {input_dt} -> 확보: {found_dt})")
    print(f" 확보율: {len(results)}/{len(V58_COLS)}")
    print("="*60)
    print(f"{'Column':<25} | {'Value':<20} | {'Status'}")
    print("-" * 60)
    
    cnt = 0
    for col in V58_COLS:
        val = results.get(col)
        # 빈 문자열, None은 MISSING 처리
        status = "✅ OK" if (val is not None and str(val).strip() != "") else "❌ MISSING"
        
        val_str = "-"
        if val is not None and str(val).strip() != "":
            cnt += 1
            if isinstance(val, (int, float)):
                val_str = f"{val:,.2f}"
            else:
                val_str = str(val)[:20]
        
        print(f"{col:<25} | {val_str:<20} | {status}")
    print("-" * 60)
    print(f" >> 총 {cnt}개 컬럼 확보.")
    print("="*60)

if __name__ == "__main__":
    target_code = "005930"
    target_date = dt.datetime.now().strftime("%Y%m%d")
    
    collector = DataCollector(target_code, target_date)
    final_data = collector.run()
    
    print_report(final_data, collector.input_date, collector.found_date)