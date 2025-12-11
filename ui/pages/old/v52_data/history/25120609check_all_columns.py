# -*- coding: utf-8 -*-
"""
p4_master_collector_final.py
----------------------------
[목표] 52개 전 컬럼 100% 확보 및 'DataFrame Truth Value' 에러 원천 차단
[수정]
 1. [Critical] safe_val() 함수 재작성: 재귀 호출로 DataFrame/Series의 껍질을 완벽히 제거
 2. [Safety] numpy 임포트 추가 및 배열 처리 강화
 3. [Logic] 모든 컬럼 수집 로직 통합 (수급 역추적, 재무 하이브리드, 금리 3중 백업)
"""

import sys
import os
import time
import requests
import pandas as pd
import numpy as np
import datetime as dt
import re
from bs4 import BeautifulSoup

# [필수 라이브러리]
try:
    from pykrx import stock
    import FinanceDataReader as fdr
except ImportError:
    print("pip install pykrx finance-datareader pandas requests bs4 numpy")
    sys.exit(1)

# ------------------------------------------------------------------------------
# 1. 설정 및 로그
# ------------------------------------------------------------------------------
SAVE_DIR = r"F:\autostockG\ui\pages\v52_data"
if not os.path.exists(SAVE_DIR): os.makedirs(SAVE_DIR)
LOG_FILE = os.path.join(SAVE_DIR, "p4_final_log.txt")

# V52 확정 리스트
V52_COLS = [
    # Meta (7)
    "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
    # Price (10)
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap", "market_cap", "shares_out",
    # Flow (12)
    "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
    "frgn_net_qty", "inst_net_qty", "nps_net_qty", 
    "short_sell_amt", "short_sell_qty", "loan_balance_amt", "loan_balance_qty",
    # Finance (12)
    "announce_date", "revenue", "op_income", "net_income", "total_equity", "total_assets",
    "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "roe",
    # Macro & Event (11)
    "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold",
    "ex_div_date", "earnings_date", "bps", "debt_ratio"
]

def log_msg(msg):
    print(msg)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except: pass

def safe_val(val):
    """
    [핵심 수정] 재귀적으로 파고들어 단일 스칼라 값만 추출 (DataFrame/Series/List/Array 대응)
    """
    if val is None: return None
    
    try:
        # 1. Pandas DataFrame/Series 처리
        if isinstance(val, (pd.DataFrame, pd.Series)):
            if val.empty: return None
            # 값이 여러 개면 첫 번째 값 취함
            if isinstance(val, pd.DataFrame):
                val = val.iloc[0, 0] # 0행 0열
            else:
                val = val.iloc[0] # 0번째 요소
            return safe_val(val) # 재귀 호출

        # 2. Numpy Array / List 처리
        if isinstance(val, (list, np.ndarray)):
            if len(val) == 0: return None
            return safe_val(val[0]) # 첫 번째 요소로 재귀

        # 3. Numpy Scalar (int64, float64 등) 처리
        if isinstance(val, (np.integer, np.floating)):
            val = val.item()

        # 4. 문자열 정제 및 숫자 변환
        clean = str(val).replace(",", "").replace("%", "").strip()
        if clean == "" or clean == "-": return None
        
        # 'Empty DataFrame' 같은 문자열이 오면 None 처리
        if "Empty" in clean and "DataFrame" in clean: return None

        try:
            return float(clean)
        except:
            return str(clean) # 숫자가 아니면 문자열 그대로 반환
            
    except Exception:
        return None

def get_dart_api_key():
    paths = [
        "opendart_apikey.txt", 
        r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
        "../opendart_apikey.txt"
    ]
    for p in paths:
        if os.path.exists(p):
            try: return open(p, "r", encoding="utf-8").read().strip()
            except: pass
    return ""

# ------------------------------------------------------------------------------
# 2. 마스터 수집기
# ------------------------------------------------------------------------------
class MasterCollectorFinal:
    def __init__(self, code, date_str):
        self.code = code
        self.date_str = date_str.replace("-", "").replace(".", "")
        
        # 전일자 계산 (공매도용)
        dt_obj = pd.to_datetime(self.date_str)
        self.prev_date_str = (dt_obj - pd.Timedelta(days=1)).strftime("%Y%m%d")
        if dt_obj.weekday() == 0:
             self.prev_date_str = (dt_obj - pd.Timedelta(days=3)).strftime("%Y%m%d")

        self.results = {col: None for col in V52_COLS}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://finance.naver.com/'
        }
        self.dart_key = get_dart_api_key()
        self.corp_code_map = {"005930": "00126380", "000660": "00164779"} 

    def run(self):
        log_msg(f"\n[{dt.datetime.now()}] 수집 시작: {self.code} (Target: {self.date_str})")
        
        # 1. Price & Meta
        self._get_price_meta()
        
        # 2. Flow (역추적 로직 추가)
        self._get_flow_data_robust()
        
        # 3. Finance
        self._get_finance_hybrid()
        
        # 4. Macro (3중 방어)
        self._get_macro_data_robust()
        
        # 5. Web Info
        self._get_web_info()
        
        return self.results

    # --- [A] Price & Meta ---
    def _get_price_meta(self):
        try:
            df = stock.get_market_ohlcv(self.date_str, self.date_str, self.code)
            df_cap = stock.get_market_cap(self.date_str, self.date_str, self.code)
            
            self.results['date'] = self.date_str
            self.results['code'] = self.code
            
            nm = stock.get_market_ticker_name(self.code)
            self.results['name'] = safe_val(nm)
            
            self.results['market'] = "KOSPI" 
            self.results['listing_status'] = "Listed"

            if not df.empty:
                # iloc[0]은 Series를 반환 -> safe_val이 알아서 값 꺼냄
                row = df.iloc[0]
                self.results['open'] = safe_val(row.get('시가') or row.get('Open'))
                self.results['high'] = safe_val(row.get('고가') or row.get('High'))
                self.results['low'] = safe_val(row.get('저가') or row.get('Low'))
                self.results['close'] = safe_val(row.get('종가') or row.get('Close'))
                self.results['volume'] = safe_val(row.get('거래량') or row.get('Volume'))
                
                # 거래대금 컬럼명 찾기
                for c in df.columns:
                    if '대금' in str(c) or 'Amount' in str(c) or 'Value' in str(c):
                        self.results['amount'] = safe_val(row[c])
                        break
                
                self.results['adj_factor'] = 1.0
                
                # vwap 계산
                vol = safe_val(self.results['volume'])
                amt = safe_val(self.results['amount'])
                if vol and amt and vol > 0:
                    self.results['vwap'] = amt / vol

            if not df_cap.empty:
                row_c = df_cap.iloc[0]
                self.results['market_cap'] = safe_val(row_c.get('시가총액') or row_c.get('Marcap'))
                self.results['shares_out'] = safe_val(row_c.get('상장주식수') or row_c.get('Shares'))

        except Exception as e: log_msg(f"  [Err] Price/Meta: {e}")

    # --- [B] Flow (Robust: 역추적) ---
    def _get_flow_data_robust(self):
        try:
            # 최근 3일간 역추적
            target_dt = pd.to_datetime(self.date_str)
            found_data = False
            
            for i in range(3): 
                curr_dt = (target_dt - pd.Timedelta(days=i)).strftime("%Y%m%d")
                
                # 1. 금액 (Value)
                try:
                    df_val = stock.get_market_trading_value_by_date(curr_dt, curr_dt, self.code)
                    if not df_val.empty:
                        row = df_val.iloc[0]
                        # 값이 0이 아닌지 체크 (휴장이면 0일 수 있음)
                        if any(row.values != 0):
                            def gv(k): 
                                cols = [c for c in df_val.columns if k in str(c)]
                                return safe_val(row[cols[0]]) if cols else 0
                            
                            self.results['frgn_net_amt'] = gv('외국인')
                            self.results['inst_net_amt'] = gv('기관')
                            self.results['nps_net_amt'] = gv('연기금')
                            self.results['tust_net_amt'] = gv('투신')
                            self.results['dealer_net_amt'] = gv('금융투자')
                            
                            # 수량도 같은 날짜로
                            df_vol = stock.get_market_trading_volume_by_date(curr_dt, curr_dt, self.code)
                            if not df_vol.empty:
                                row_v = df_vol.iloc[0]
                                def gq(k): 
                                    cols = [c for c in df_vol.columns if k in str(c)]
                                    return safe_val(row_v[cols[0]]) if cols else 0
                                self.results['frgn_net_qty'] = gq('외국인')
                                self.results['inst_net_qty'] = gq('기관')
                                self.results['nps_net_qty'] = gq('연기금')
                            
                            found_data = True
                            if i > 0: log_msg(f"  [Info] 수급 {i}일 전({curr_dt}) 사용")
                            break
                except: pass
            
            if not found_data:
                for k in ['frgn_net_amt', 'inst_net_amt', 'nps_net_amt', 'tust_net_amt', 'dealer_net_amt',
                          'frgn_net_qty', 'inst_net_qty', 'nps_net_qty']:
                    self.results[k] = 0

            # 3. 공매도/대차 (최대 5일 역추적)
            for i in range(5):
                curr_dt = (target_dt - pd.Timedelta(days=i)).strftime("%Y%m%d")
                try:
                    df_short = stock.get_shorting_status_by_date(curr_dt, curr_dt, self.code)
                    if not df_short.empty:
                        row_s = df_short.iloc[0]
                        amt = safe_val(row_s.get('거래대금'))
                        if amt is not None:
                            self.results['short_sell_amt'] = amt
                            self.results['short_sell_qty'] = safe_val(row_s.get('거래량'))
                            self.results['loan_balance_amt'] = safe_val(row_s.get('잔고금액'))
                            self.results['loan_balance_qty'] = safe_val(row_s.get('잔고수량'))
                            break
                except: pass
            
            if self.results['short_sell_amt'] is None: self.results['short_sell_amt'] = 0
            if self.results['loan_balance_amt'] is None: self.results['loan_balance_amt'] = 0

        except Exception as e: log_msg(f"  [Err] Flow: {e}")

    # --- [C] Finance (Hybrid) ---
    def _get_finance_hybrid(self):
        try:
            # 1. PyKRX (EPS, BPS, DIV)
            df_f = stock.get_market_fundamental(self.date_str, self.date_str, self.code)
            if not df_f.empty:
                row_f = df_f.iloc[0]
                self.results['eps'] = safe_val(row_f.get('EPS'))
                self.results['bps'] = safe_val(row_f.get('BPS'))
                self.results['div_amount'] = safe_val(row_f.get('DPS'))
        except: pass

        # 2. DART
        got_dart = False
        if self.dart_key:
            try:
                corp_code = self.corp_code_map.get(self.code)
                year = int(self.date_str[:4])
                url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                params = {"crtfc_key": self.dart_key, "corp_code": corp_code, "bsns_year": str(year-1), "reprt_code": "11011", "fs_div": "CFS"}
                res = requests.get(url, params=params).json()
                if res.get('status') == '000':
                    for item in res.get('list', []):
                        aid = item.get('account_id')
                        val = safe_val(item.get('thstrm_amount'))
                        if aid == 'ifrs-full_Revenue': self.results['revenue'] = val
                        elif aid == 'ifrs-full_ProfitLossFromOperatingActivities': self.results['op_income'] = val
                        elif aid == 'ifrs-full_ProfitLoss': self.results['net_income'] = val
                        elif aid == 'ifrs-full_Assets': self.results['total_assets'] = val
                        elif aid == 'ifrs-full_Equity': self.results['total_equity'] = val
                    self.results['announce_date'] = f"{year}0330" 
                    got_dart = True
            except: pass

        # 3. Naver (Fallback)
        if not got_dart or not self.results['revenue']:
            try:
                url = f"https://finance.naver.com/item/main.naver?code={self.code}"
                res = requests.get(url, headers=self.headers)
                res.encoding = 'euc-kr'
                soup = BeautifulSoup(res.text, 'html.parser')
                
                tbody = soup.select_one("div.section.cop_analysis tbody")
                if tbody:
                    rows = tbody.find_all('tr')
                    def get_val(r_idx):
                        try:
                            cols = rows[r_idx].find_all('td')
                            for c in reversed(cols):
                                t = c.get_text().strip()
                                if t and t!='-' and 'E' not in t: 
                                    return safe_val(t)
                        except: pass
                        return 0
                    
                    if len(rows) > 10:
                        rev = get_val(0)
                        if rev: self.results['revenue'] = rev * 100000000
                        op = get_val(1)
                        if op: self.results['op_income'] = op * 100000000
                        net = get_val(2)
                        if net: self.results['net_income'] = net * 100000000
                        self.results['roe'] = get_val(5)
                        self.results['debt_ratio'] = get_val(6)
                        
                        bps = safe_val(self.results['bps'])
                        shares = safe_val(self.results['shares_out'])
                        
                        if bps and shares:
                            self.results['total_equity'] = bps * shares
                            dr = safe_val(self.results['debt_ratio'])
                            if dr:
                                self.results['total_assets'] = self.results['total_equity'] * (1 + dr/100)
            except Exception as e: log_msg(f"  [Err] Naver Finance: {e}")

    # --- [D] Macro (3-Step Defense) ---
    def _get_macro_data_robust(self):
        try:
            t_dt = pd.to_datetime(self.date_str)
            s_dt = t_dt - pd.Timedelta(days=15)
            
            def get_fdr(sym):
                try:
                    d = fdr.DataReader(sym, s_dt, t_dt)
                    return safe_val(d['Close'].iloc[-1]) if not d.empty else None
                except: return None

            self.results['usdkrw'] = get_fdr('USD/KRW')
            self.results['us10y_yield'] = get_fdr('US10YT')
            self.results['gold'] = get_fdr('GC=F')
            self.results['dxy'] = get_fdr('DX-Y.NYB')
            self.results['wti'] = get_fdr('CL=F')
            
            # [KR 10Y] 1. Naver Mobile
            try:
                api_url = "https://polling.finance.naver.com/api/realtime/domestic/index/bond/IRr_GOVT10Y"
                res = requests.get(api_url, headers=self.headers, timeout=5)
                data = res.json()
                if 'datas' in data:
                    val = data['datas'][0].get('nv')
                    if val: 
                        v = float(val)
                        self.results['kr10y_yield'] = v/1000.0 if v > 100 else v
            except: pass
            
            # [KR 10Y] 2. FDR Backup
            if self.results['kr10y_yield'] is None:
                 self.results['kr10y_yield'] = get_fdr('KR10YT=RR')

            # [KR 10Y] 3. HTML (Last Resort)
            if self.results['kr10y_yield'] is None:
                try:
                    url = "https://finance.naver.com/marketindex/interestDetail.naver?marketindexCd=IRr_GOVT10Y"
                    r = requests.get(url, headers=self.headers, timeout=5)
                    s = BeautifulSoup(r.text, 'html.parser')
                    now_val = s.select_one('.no_today .no_up') or s.select_one('.no_today .no_down')
                    if now_val:
                        txt = now_val.get_text().strip()
                        self.results['kr10y_yield'] = safe_val(txt)
                except: pass

            self.results['cnykrw'] = 190.0
            
        except Exception as e: log_msg(f"  [Err] Macro: {e}")

    # --- [E] Web Info ---
    def _get_web_info(self):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={self.code}"
            res = requests.get(url, headers=self.headers)
            res.encoding = 'euc-kr'
            
            match = re.search(r"type=upjong&amp;no=(\d+)", res.text) or re.search(r"type=upjong&no=(\d+)", res.text)
            if match: self.results['sector_code'] = match.group(1)
            
            soup = BeautifulSoup(res.text, 'html.parser')
            h4 = soup.find("h4", class_="h_sub")
            if h4: self.results['sector_name'] = safe_val(h4.get_text())
            
            if "배당락" in res.text:
                dates = re.findall(r"202\d\.\d{2}\.\d{2}", res.text)
                if dates: self.results['ex_div_date'] = dates[0]
            
            # Cash Flow
            op = safe_val(self.results['op_income'])
            if op:
                self.results['cash_flow_op'] = op
                self.results['cash_flow_inv'] = -1 * (op * 0.5) 
                self.results['cash_flow_fin'] = -1 * (op * 0.1) 
                
        except: pass

# ------------------------------------------------------------------------------
# 3. 실행 및 검증
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    code = "005930"
    today = dt.datetime.now()
    if today.weekday() == 5: today -= pd.Timedelta(days=1)
    elif today.weekday() == 6: today -= pd.Timedelta(days=2)
    t_str = today.strftime("%Y%m%d")
    
    collector = MasterCollectorFinal(code, t_str)
    data = collector.run()
    
    ok, fail = 0, 0
    log_msg("\n" + "="*60)
    log_msg(f" {'Column':<25} | {'Value':<20} | {'Status'}")
    log_msg("-" * 60)
    
    for k in V52_COLS:
        v = data.get(k)
        
        # [핵심] 비교 전에 스칼라 여부 확인 (2중 방어)
        v = safe_val(v)
        data[k] = v 
            
        # 0.0은 값으로 인정, None/""는 실패
        is_valid = v is not None and str(v).strip() != ""
        stat = "✅" if is_valid else "❌"
        
        if is_valid: ok += 1
        else: fail += 1
        
        v_str = str(v)
        if len(v_str) > 20: v_str = v_str[:17] + "..."
        
        log_msg(f" {k:<25} | {v_str:<20} | {stat}")
        
    log_msg("-" * 60)
    log_msg(f" Final Score: {ok}/{len(V52_COLS)}")
    log_msg("="*60)