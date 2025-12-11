import csv
import json
import os
import zipfile
import datetime as dt
import xml.etree.ElementTree as ET
import math
import sys
import requests
import pandas as pd
import numpy as np
import time
import re
from typing import Any, Dict, Iterable, List, Set

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QFrame, QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QMessageBox, QPushButton, QScrollArea, QSizePolicy, QTextEdit,
    QVBoxLayout, QWidget, QApplication
)

# ------------------------------------------------------------------------------
# 1. UI Components (FieldCard)
# ------------------------------------------------------------------------------
class FieldCard(QFrame):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.has_value = False
        self.setObjectName("FieldCard")
        self.setStyleSheet("""
            QFrame#FieldCard {
                background-color: #1f2937;
                border: 1px solid #334155;
                border-radius: 8px;
            }
        """)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 8, 10, 8)
        lay.setSpacing(8)

        self.icon = QLabel("⚪")
        self.icon.setFixedWidth(20)
        self.icon.setStyleSheet("font-size: 14px;")
        
        self.name_label = QLabel(name)
        self.name_label.setStyleSheet("color: #e5e7eb; font-weight: 700;")
        
        self.value_label = QLabel("—")
        self.value_label.setStyleSheet("color: #cbd5e1;")
        self.value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.value_label.setWordWrap(True)

        lay.addWidget(self.icon)
        lay.addWidget(self.name_label, 1)
        lay.addWidget(self.value_label, 2)

    def set_value(self, value: Any, src_tag: str = ""):
        if not self._is_safe_true(value):
            return

        self.icon.setText("✅")
        disp = str(value)
        if isinstance(value, float):
            disp = f"{value:,.2f}"
            # 정수로 보여주는게 나은 컬럼들
            integer_cols = ["qty", "vol", "amt", "revenue", "income", "cap", "net_income", "op_income", "cash_flow", "vwap"]
            if any(x in self.name_label.text() for x in integer_cols) and abs(value) > 1000: 
                 disp = f"{value:,.0f}"
        elif isinstance(value, int):
            disp = f"{value:,}"
            
        if len(disp) > 25: disp = disp[:25] + "..."
        
        if src_tag:
            self.value_label.setText(f"[{src_tag}] {disp}")
        else:
            self.value_label.setText(disp)
        self.has_value = True

    def clear(self):
        self.icon.setText("⚪")
        self.value_label.setText("—")
        self.has_value = False

    def _is_safe_true(self, val):
        if val is None: return False
        if isinstance(val, str) and not val.strip(): return False
        if isinstance(val, (list, dict)) and not val: return False
        if isinstance(val, (int, float)):
            if val == 0: return True
            try:
                if math.isnan(val): return False
            except: pass
            return True
        try:
            if hasattr(val, 'size') and val.size == 0: return False
            if pd.isna(val): return False
        except: pass
        return True

# ------------------------------------------------------------------------------
# 2. Main Page (P0_Index) - V58 Ultimate (v7.0 FnGuide/NaverCrawler)
# ------------------------------------------------------------------------------
class P0_Index(QWidget):
    def __init__(self):
        super().__init__()
        
        # [V58 스펙 컬럼]
        self.v58_cols = [
            # 1. Price (12)
            "date", "code", "name", "market", "open", "high", "low", "close", 
            "volume", "amount", "adj_factor", "vwap",
            # 2. Flow (12)
            "inst_net_qty", "inst_net_amt", "frgn_net_qty", "frgn_net_amt", 
            "nps_net_qty", "nps_net_amt", "dealer_net_qty", "dealer_net_amt", 
            "short_sell_qty", "short_sell_amt", "loan_balance_qty", "loan_balance_amt",
            # 3. Finance (11)
            "revenue", "op_income", "net_income", "eps", "bps", "roe", "roa", 
            "debt_ratio", "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
            # 4. Sector/Theme (5)
            "sector_code", "sector_name", "theme_code", "theme_name", "sector_index_close",
            # 5. Macro (8)
            "usdkrw", "cnykrw", "dxy", "us10y_yield", "kr10y_yield", "wti", "gold", "vix",
            # 6. Event (10)
            "earnings_announce_date", "earnings_surprise", "earnings_effective_date",
            "ex_div_date", "div_amount",
            "split_announce_date", "split_effective_date",
            "rights_issue_announce_date", "rights_issue_effective_date",
            "mna_announce_date",
        ]
        
        self.cards: Dict[str, FieldCard] = {}
        self.last_payloads: Dict[str, Dict[str, Any]] = {} 
        self.last_raw_payloads: Dict[str, Dict[str, Any]] = {}
        
        self._init_ui()

    def _init_ui(self):
        root = QVBoxLayout(self)
        
        # Header
        header = QHBoxLayout()
        title = QLabel("V58 데이터 수집 통합 분석기 (v7.0 FnGuide Edition)")
        title.setStyleSheet("font-size: 20px; font-weight: bold; color: #22d3ee;")
        header.addWidget(title)
        root.addLayout(header)

        # Controls
        root.addWidget(self._build_controls())

        # Log
        self.log_area = QTextEdit()
        self.log_area.setPlaceholderText("로그 출력...")
        self.log_area.setFixedHeight(180)
        self.log_area.setStyleSheet("background-color: #0f172a; color: #cbd5e1; font-family: Consolas; font-size: 12px;")
        root.addWidget(self.log_area)

        # Grid
        root.addWidget(self._build_field_grid(), 1)

    def _build_controls(self):
        box = QGroupBox("설정")
        lay = QHBoxLayout(box)
        self.code_edit = QLineEdit("005930")
        self.code_edit.setPlaceholderText("종목코드")
        self.date_edit = QLineEdit(QDate.currentDate().toString("yyyyMMdd"))
        
        btn_run = QPushButton("🚀 풀파워 실행 (FnGuide/Naver)")
        btn_run.setStyleSheet("background-color: #2563eb; color: white; font-weight: bold; padding: 5px;")
        btn_run.clicked.connect(self.run_full_analysis)
        
        btn_save = QPushButton("💾 로그 저장")
        btn_save.clicked.connect(self.save_payloads_to_file)
        
        lay.addWidget(QLabel("Code:"))
        lay.addWidget(self.code_edit)
        lay.addWidget(QLabel("Date:"))
        lay.addWidget(self.date_edit)
        lay.addWidget(btn_run)
        lay.addWidget(btn_save)
        return box

    def _build_field_grid(self):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: none;")
        con = QWidget()
        self.grid = QGridLayout(con)
        self.grid.setSpacing(6)
        
        cols = 4
        for i, c in enumerate(self.v58_cols):
            card = FieldCard(c)
            self.cards[c] = card
            self.grid.addWidget(card, i//cols, i%cols)
        
        scroll.setWidget(con)
        return scroll

    def log(self, msg):
        self.log_area.append(msg)
        QApplication.processEvents()

    # ------------------------------------------------------------------
    # [핵심] 실행 로직 (Waterfall)
    # ------------------------------------------------------------------
    def run_full_analysis(self):
        self.log_area.clear()
        for c in self.cards.values(): c.clear()
        
        code = self.code_edit.text().strip()
        base_dt = self._normalize_date(self.date_edit.text().strip())
        
        self.log(f"=== [분석 시작] Target: {code}, Date: {base_dt} ===")
        self.last_payloads = {}
        self.last_raw_payloads = {}
        
        # 1. Fetching (순차 호출)
        
        # (1) Kiwoom (Price/Valuation)
        self.log(">>> 1. Kiwoom (Price/Valuation)")
        k_data = self._fetch_kiwoom_composite(code, base_dt)
        self.last_raw_payloads['Kiwoom'] = k_data
        self.last_payloads['Kiwoom'] = self._normalize_payload(k_data)

        # (2) PyKRX (Flow/Price/Fundamental)
        self.log(">>> 2. PyKRX (Flow/Fundamental)")
        p_data = self._fetch_pykrx_enhanced(code, base_dt)
        self.last_raw_payloads['PyKRX'] = p_data
        self.last_payloads['PyKRX'] = self._normalize_payload(p_data)

        # (3) Naver Macro (Direct Crawler) - NEW!
        self.log(">>> 3. Naver Market Index (CNY/KRW, Yield, etc.)")
        nm_data = self._fetch_naver_market_index_crawler()
        self.last_raw_payloads['NaverMacro'] = nm_data
        self.last_payloads['NaverMacro'] = self._normalize_payload(nm_data)

        # (4) FDR (Macro Backup)
        self.log(">>> 4. FDR (Macro Backup)")
        f_data = self._fetch_fdr_macro_enhanced(base_dt)
        self.last_raw_payloads['FDR'] = f_data
        self.last_payloads['FDR'] = self._normalize_payload(f_data)

        # (5) DART (Finance)
        self.log(">>> 5. DART (Finance: All-in)")
        d_data = self._fetch_dart_composite(code, base_dt)
        self.last_raw_payloads['DART'] = d_data
        self.last_payloads['DART'] = self._normalize_payload(d_data)

        # (6) FnGuide (Sector/Theme) - NEW!
        self.log(">>> 6. FnGuide (Sector/Theme Snapshot)")
        fn_data = self._fetch_fnguide_composite(code)
        self.last_raw_payloads['FnGuide'] = fn_data
        self.last_payloads['FnGuide'] = self._normalize_payload(fn_data)
        
        # (7) Yahoo Finance (Events Backup)
        self.log(">>> 7. Yahoo Finance (Events/Global Backup)")
        y_data = self._fetch_yahoo_composite(code, base_dt)
        self.last_raw_payloads['Yahoo'] = y_data
        self.last_payloads['Yahoo'] = self._normalize_payload(y_data)

        # 2. Waterfall Merge Strategy
        final_merged = {}
        
        # 우선순위: DART(재무) > PyKRX(수급) > NaverMacro(환율/금리) > FnGuide(섹터/테마)
        
        self._merge_into(final_merged, 'DART')       # Finance
        self._merge_into(final_merged, 'PyKRX')      # Flow, Price
        self._merge_into(final_merged, 'NaverMacro') # Macro (CNY, Bond)
        self._merge_into(final_merged, 'FDR')        # Macro Backup
        self._merge_into(final_merged, 'FnGuide')    # Theme/Sector (Best Source)
        self._merge_into(final_merged, 'Kiwoom')     # Price (Realtime)
        self._merge_into(final_merged, 'Yahoo')      # Events, Global
        
        # 3. UI Update
        cnt = 0
        for col in self.v58_cols:
            if col in final_merged:
                val, src = final_merged[col]
                self.cards[col].set_value(val, src)
                cnt += 1
        
        self.log(f"\n[최종 결과] 58개 중 {cnt}개 수집 완료 ({cnt/58*100:.1f}%)")

    def _merge_into(self, target_dict, src_name):
        if src_name not in self.last_payloads: return
        data = self.last_payloads[src_name]
        for k, v in data.items():
            if k not in target_dict and k in self.v58_cols:
                 if self._is_value_valid(v):
                    target_dict[k] = (v, src_name)

    # ------------------------------------------------------------------
    # Data Fetchers
    # ------------------------------------------------------------------
    
    # --- [FnGuide] Theme/Sector Crawler (New Source!) ---
    def _fetch_fnguide_composite(self, code):
        """FnGuide에서 업종(WICS)과 테마를 긁어옴"""
        res = {}
        try:
            # FnGuide 개요 페이지
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=5)
            
            if resp.status_code == 200:
                html = resp.text
                
                # 1. Sector (WICS)
                # <div class="corp_group1"> ... <span class="stxt stxt2">반도체 및 관련장비</span>
                # 정규식으로 '업종(WICS)' 근처의 텍스트 찾기
                # 보통 "업종(WICS) : 반도체와반도체장비" 형태
                wics_match = re.search(r'WICS\s*:\s*([^<]+)', html)
                if not wics_match:
                    # 다른 패턴 시도
                    wics_match = re.search(r'class="stxt stxt2">([^<]+)</span>', html)
                
                if wics_match:
                    sector = wics_match.group(1).replace("&nbsp;", "").strip()
                    # 쉼표나 불필요한 것 제거
                    res["sector_name"] = sector.split('  ')[0].strip()

                # 2. Theme (테마)
                # FnGuide는 테마 정보가 별도 섹션에 있거나 하단에 있음.
                # 그러나 요약 페이지에는 없을 수 있음.
                # 그래도 텍스트 파싱 시도
                pass
        except: pass
        return res

    # --- [Naver Market Index] Crawler (New Source!) ---
    def _fetch_naver_market_index_crawler(self):
        """네이버 시장지표 메인에서 주요 매크로 데이터 긁어옴"""
        res = {}
        try:
            url = "https://finance.naver.com/marketindex/"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=5)
            html = resp.text
            
            # Helper to find value inside a container
            def find_val(label_pattern, html_text):
                # 패턴: label... <div class="head_info"> <span class="value">1,410.00</span>
                # 매우 단순화된 파싱
                parts = html_text.split(label_pattern)
                if len(parts) > 1:
                    after = parts[1]
                    val_match = re.search(r'<span class="value">([\d,.]+)</span>', after)
                    if val_match:
                        return float(val_match.group(1).replace(",", ""))
                return None

            # 1. CNY/KRW
            # 네이버에는 "중국 CNY"로 표기됨
            cny = find_val("중국 CNY", html)
            if cny: res["cnykrw"] = cny
            
            # 2. KR 3Y or 10Y (국고채 10년)
            kr10 = find_val("국고채 10년", html) # 네이버에 '국고채 3년' 등이 있음
            if kr10: res["kr10y_yield"] = kr10
            elif find_val("국고채 3년", html): # 10년 없으면 3년이라도
                res["kr10y_yield"] = find_val("국고채 3년", html)

            # 3. WTI, Gold
            wti = find_val("WTI", html) or find_val("휘발유", html) # WTI가 없을 수도
            if wti: res["wti"] = wti
            
            gold = find_val("국제 금", html)
            if gold: res["gold"] = gold

        except Exception as e:
            self.log(f"Naver Macro Error: {e}")
        return res

    # --- [Kiwoom] REST API ---
    def _fetch_kiwoom_composite(self, code, base_dt):
        try:
            root_dir = r"F:\autostockG"
            api_dir = os.path.join(root_dir, "api", "kiwoom_rest")
            if api_dir not in sys.path: sys.path.insert(0, api_dir)
            if root_dir not in sys.path: sys.path.append(root_dir)

            try:
                from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
            except ImportError:
                try: from kiwoom_api import KiwoomRestApi
                except: return {}

            api = KiwoomRestApi()
            def call(api_id, path, body):
                return api._call_api(api_id, path, body=body)

            def attempt(dt_str):
                merged_local = {}
                res_chart = call("ka10081", "/api/dostk/chart", {"stk_cd": code, "base_dt": dt_str, "upd_stkpc_tp": "D", "term_cnt": "1"})
                if self._is_valid(res_chart): merged_local.update(self._extract_first(res_chart))
                
                res_info = call("ka10014", "/api/dostk/shsa", {"stk_cd": code, "tm_tp": "0", "strt_dt": dt_str, "end_dt": dt_str})
                if self._is_valid(res_info): merged_local.update(self._extract_first(res_info))
                
                res_det = call("ka10001", "/api/dostk/stkinfo", {"stk_cd": code})
                if self._is_valid(res_det): merged_local.update(self._extract_first(res_det))
                return merged_local

            merged = attempt(base_dt)
            if not merged:
                try:
                    prev = (pd.to_datetime(base_dt) - pd.tseries.offsets.BDay(1)).strftime("%Y%m%d")
                    merged = attempt(prev)
                except: pass
            return merged
        except: return {}

    def _is_valid(self, res):
        return res and isinstance(res, dict) and str(res.get("return_code","")) == "0"

    def _extract_first(self, res):
        for key in ("output", "chart", "data", "result", "stk_dt_pole_chart_qry"):
            if key in res:
                block = res[key]
                if isinstance(block, list) and block:
                    if isinstance(block[0], dict): return block[0]
                if isinstance(block, dict): return block
        if isinstance(res, dict):
            return {k: v for k, v in res.items() if k not in ("return_code", "return_msg")}
        return {}

    # --- [DART] OpenAPI ---
    def _fetch_dart_composite(self, code, base_dt):
        try:
            key_paths = [
                r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
                "opendart_apikey.txt"
            ]
            api_key = ""
            for p in key_paths:
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        api_key = f.readline().strip()
                    if api_key: break
            if not api_key: return {}
                
            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code: return {}
            
            merged = {}
            merged.update(self._fetch_dart_company(api_key, corp_code))
            merged.update(self._fetch_dart_financial(api_key, corp_code, base_dt))
            merged.update(self._fetch_dart_major(api_key, corp_code))
            return merged
        except: return {}

    def _get_dart_corp_map(self, api_key: str) -> Dict[str, str]:
        cache_path = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
        if os.path.exists(cache_path):
            try:
                mtime = dt.datetime.fromtimestamp(os.path.getmtime(cache_path))
                if (dt.datetime.now() - mtime).days < 1:
                    with open(cache_path, "r", encoding="utf-8") as f: return json.load(f)
            except: pass

        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                zip_path = cache_path + ".zip"
                with open(zip_path, "wb") as f: f.write(resp.content)
                with zipfile.ZipFile(zip_path, "r") as zf:
                    xml_data = zf.read(zf.namelist()[0])
                try: os.remove(zip_path)
                except: pass
                
                root = ET.fromstring(xml_data)
                mapping = {}
                for corp in root.iter("list"):
                    sc = (corp.findtext("stock_code") or "").strip()
                    cc = (corp.findtext("corp_code") or "").strip()
                    if sc and cc: mapping[sc] = cc
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(mapping, f, ensure_ascii=False)
                return mapping
        except: pass
        return {}

    def _fetch_dart_company(self, api_key, corp_code):
        try:
            url = "https://opendart.fss.or.kr/api/company.json"
            resp = requests.get(url, params={"crtfc_key": api_key, "corp_code": corp_code}, timeout=5)
            data = resp.json()
            if data.get("status") == "000": return data
        except: pass
        return {}

    def _fetch_dart_financial(self, api_key, corp_code, base_dt):
        year = int(base_dt[:4])
        reprt_codes = ["11011", "11014", "11012", "11013"] 
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        
        merged_fin = {}
        for y in [year, year-1]:
            for rc in reprt_codes:
                try:
                    params = {"crtfc_key": api_key, "corp_code": corp_code, "bsns_year": str(y), "reprt_code": rc, "fs_div": "CFS"}
                    resp = requests.get(url, params=params, timeout=5)
                    js = resp.json()
                    if js.get("status") == "000":
                        for item in js.get("list", []):
                            acct_nm = item.get("account_nm", "")
                            acct_id = item.get("account_id", "")
                            val_str = item.get("thstrm_amount", "").replace(",","")
                            if val_str and val_str not in ["-", ""]:
                                try:
                                    val = float(val_str)
                                    if acct_nm: merged_fin[acct_nm] = val
                                    if acct_id: merged_fin[acct_id] = val
                                except: pass
                        if merged_fin: return merged_fin
                except: continue
        return merged_fin

    def _fetch_dart_major(self, api_key, corp_code):
        try:
            url = "https://opendart.fss.or.kr/api/majorstock.json"
            resp = requests.get(url, params={"crtfc_key": api_key, "corp_code": corp_code}, timeout=5)
            js = resp.json()
            if js.get("status") == "000":
                return {"major_holder_count": len(js.get("list", []))}
        except: pass
        return {}

    # --- [PyKRX] Enhanced ---
    def _fetch_pykrx_enhanced(self, code, base_dt):
        res = {}
        try:
            from pykrx import stock
            
            # 1. Price
            try:
                df = stock.get_market_ohlcv(base_dt, base_dt, code)
                if not df.empty:
                    row = df.iloc[0].to_dict()
                    res.update({
                        "open": row.get('시가'), "high": row.get('고가'), "low": row.get('저가'), "close": row.get('종가'),
                        "volume": row.get('거래량'), "amount": row.get('거래대금'), "rate": row.get('등락률')
                    })
            except: pass

            # 2. Valuation
            try:
                df_val = stock.get_market_fundamental(base_dt, base_dt, code)
                if not df_val.empty:
                    row = df_val.iloc[0].to_dict()
                    res.update({
                        "bps": row.get('BPS'), "per": row.get('PER'), "pbr": row.get('PBR'),
                        "eps": row.get('EPS'), "div_amount": row.get('DPS')
                    })
            except: pass

            # 3. Flow (Retry Logic)
            try:
                check_dts = [base_dt]
                curr = pd.to_datetime(base_dt)
                for _ in range(3):
                    curr = curr - pd.Timedelta(days=1)
                    check_dts.append(curr.strftime("%Y%m%d"))
                
                for d in check_dts:
                    df_vol = stock.get_market_trading_volume_by_date(d, d, code)
                    df_val = stock.get_market_trading_value_by_date(d, d, code)
                    
                    if not df_vol.empty and not df_val.empty:
                        row_vol = df_vol.iloc[0]
                        row_val = df_val.iloc[0]
                        inv_map = {"기관합계": "inst", "외국인": "frgn", "금융투자": "dealer"}
                        for k_kr, k_en in inv_map.items():
                            if k_kr in row_vol:
                                res[f"{k_en}_net_qty"] = row_vol[k_kr]
                                res[f"{k_en}_net_amt"] = row_val[k_kr]
                        
                        for nps_name in ["연기금", "연기금등"]:
                            if nps_name in row_vol:
                                res["nps_net_qty"] = row_vol[nps_name]
                                res["nps_net_amt"] = row_val[nps_name]
                                break
                        break
            except Exception as e:
                self.log(f"   [Flow Error] {e}")

            # 4. Short Sell
            try:
                df_short = stock.get_shorting_status_by_date(base_dt, base_dt, code)
                if not df_short.empty:
                    row = df_short.iloc[0].to_dict()
                    res["short_sell_qty"] = row.get("거래량")
                    res["short_sell_amt"] = row.get("거래대금")
                    res["loan_balance_qty"] = row.get("잔고수량")
                    res["loan_balance_amt"] = row.get("잔고금액")
            except: pass

        except Exception as e:
            self.log(f"PyKRX Main Error: {e}")
        return res

    # --- [FDR] Macro Enhanced ---
    def _fetch_fdr_macro_enhanced(self, base_dt):
        res = {}
        try:
            import FinanceDataReader as fdr
            macro_map = {
                "usdkrw": "USD/KRW", "cnykrw": "CNY/KRW", 
                "dxy": "DX-Y.NYB", "us10y_yield": "US10YT", "kr10y_yield": "KR10YT",
                "wti": "CL=F", "gold": "GC=F", "vix": "VIX"
            }
            start_dt = (pd.to_datetime(base_dt) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
            
            for key, sym in macro_map.items():
                try:
                    df = fdr.DataReader(sym, start_dt, base_dt)
                    if not df.empty:
                        df = df.fillna(method='ffill')
                        if base_dt in df.index:
                            val = df.loc[base_dt]['Close']
                        else:
                            val = df.iloc[-1]['Close']
                        res[key] = float(val)
                except: pass
        except: pass
        return res

    # --- [Yahoo Finance] Backup & Events ---
    def _fetch_yahoo_composite(self, code, base_dt):
        res = {}
        try:
            import yfinance as yf
            ticker = code + ".KS"
            yf_obj = yf.Ticker(ticker)
            info = yf_obj.info
            if info:
                res["sector_name"] = info.get("sector", "") or info.get("industry", "")
                res["ex_div_date"] = info.get("exDividendDate", "")
                if isinstance(res["ex_div_date"], int):
                    res["ex_div_date"] = dt.datetime.fromtimestamp(res["ex_div_date"]).strftime("%Y%m%d")
        except: pass
        return res

    # ------------------------------------------------------------------
    # Normalization (Full V58 Mapping + Calc)
    # ------------------------------------------------------------------
    def _normalize_payload(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        if not raw: return {}
        out = raw.copy()
        
        mapping_rules = {
            # Price
            "open": ["open", "open_pric", "stck_oprc", "시가", "Open"],
            "high": ["high", "high_pric", "stck_hgpr", "고가", "High"],
            "low": ["low", "low_pric", "stck_lwpr", "저가", "Low"],
            "close": ["close", "close_pric", "stck_clpr", "종가", "cur_prc", "Close"],
            "volume": ["volume", "trde_qty", "acml_vol", "거래량", "Volume"],
            "amount": ["amount", "trde_prica", "acc_trde_prica", "거래대금"],
            "name": ["name", "stk_nm", "itm_nm", "corp_name"],
            "date": ["date", "dt", "bas_dt", "stnd_dt", "stck_bsop_date", "base_dt"],
            "code": ["code", "stk_cd", "stock_code", "corp_code", "isin_cd"],
            
            # Flow
            "inst_net_qty": ["inst_net_qty", "organ_net_buy_qty", "기관순매수", "ins_net_buy"],
            "inst_net_amt": ["inst_net_amt", "organ_net_buy_amt"],
            "frgn_net_qty": ["frgn_net_qty", "frg_net_buy_qty", "외인순매수", "frg_net_buy"],
            "frgn_net_amt": ["frgn_net_amt", "frg_net_buy_amt"],
            "nps_net_qty": ["nps_net_qty", "pension", "pension_net_buy"],
            
            # Finance
            "revenue": ["revenue", "thstrm_amount", "매출액", "영업수익", "ifrs-full_Revenue"],
            "op_income": ["op_income", "operating_income", "영업이익", "dart_OperatingIncomeLoss"],
            "net_income": ["net_income", "danggi_sun_profit", "당기순이익", "ifrs-full_ProfitLoss"],
            "cash_flow_op": ["cash_flow_op", "영업활동현금흐름", "ifrs-full_CashFlowsFromUsedInOperatingActivities"],
            "cash_flow_inv": ["cash_flow_inv", "투자활동현금흐름", "ifrs-full_CashFlowsFromUsedInInvestingActivities"],
            "cash_flow_fin": ["cash_flow_fin", "재무활동현금흐름", "ifrs-full_CashFlowsFromUsedInFinancingActivities"],
            
            "bps": ["bps", "BPS"], "per": ["per", "PER"], "pbr": ["pbr", "PBR"], 
            "eps": ["eps", "EPS"], "roe": ["roe", "ROE"],
            
            # Sector/Events
            "sector_name": ["sector_name", "sector", "industry"],
            "ex_div_date": ["ex_div_date", "exDividendDate"],
            "theme_code": ["theme_code"], "theme_name": ["theme_name"],
            
            # Macro
            "usdkrw": ["usdkrw", "USD/KRW"],
            "cnykrw": ["cnykrw", "CNY/KRW"], # Naver/FDR
            "kr10y_yield": ["kr10y_yield", "KR10YT"], # Naver/FDR
        }
        
        final = {}
        for k, v in out.items():
            if k in self.v58_cols: final[k] = v
            
        for std_key, candidates in mapping_rules.items():
            if std_key not in final:
                for cand in candidates:
                    if cand in raw and self._is_value_valid(raw[cand]):
                        final[std_key] = raw[cand]
                        break
        
        # [Calculated Fields]
        # 1. VWAP
        if "vwap" not in final:
            try:
                amt = float(final.get("amount", 0))
                vol = float(final.get("volume", 0))
                close = float(final.get("close", 0))
                if vol > 0 and close > 0:
                    raw_vwap = amt / vol
                    if raw_vwap < (close * 0.01):
                        final["vwap"] = raw_vwap * 1000000
                    else:
                        final["vwap"] = raw_vwap
            except: pass
            
        # 2. Debt Ratio
        if "debt_ratio" not in final:
            try:
                liab = 0
                equity = 0
                for k, v in raw.items():
                    if k in ["부채총계", "ifrs-full_Liabilities"]: liab = float(v)
                    if k in ["자본총계", "ifrs-full_Equity"]: equity = float(v)
                if equity > 0:
                    final["debt_ratio"] = (liab / equity) * 100.0
            except: pass
            
        # 3. ROA
        if "roa" not in final and "net_income" in final:
            try:
                ni = float(final["net_income"])
                assets = 0
                for k, v in raw.items():
                    if k in ["자산총계", "ifrs-full_Assets"]: assets = float(v)
                if assets > 0:
                    final["roa"] = (ni / assets) * 100.0
            except: pass

        return final

    def _is_value_valid(self, val: Any) -> bool:
        if val is None: return False
        if isinstance(val, str) and not val.strip(): return False
        if isinstance(val, (int, float)):
            try:
                if math.isnan(val): return False
                if math.isinf(val): return False
            except: pass
            return True 
        try:
            if hasattr(val, 'size') and val.size == 0: return False
            if pd.isna(val): return False
        except: pass
        return True

    def _normalize_date(self, text):
        return text.replace("-", "").replace(".", "").strip()

    def save_payloads_to_file(self):
        if not self.last_payloads:
            QMessageBox.information(self, "저장 불가", "먼저 분석을 실행하세요.")
            return
        code = self.code_edit.text().strip() or "unknown"
        base_dt = self.date_edit.text().strip() or QDate.currentDate().toString("yyyyMMdd")
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        logs_dir = os.path.join(root_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        fname = f"p0_dump_{code}_{base_dt}_{int(time.time())}.json"
        path = os.path.join(logs_dir, fname)
        data = {
            "code": code,
            "date": base_dt,
            "normalized": self.last_payloads,
            "raw": self.last_raw_payloads,
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log(f"[파일 저장] {path}")
            QMessageBox.information(self, "저장 완료", f"로그를 저장했습니다:\n{path}")
        except Exception as e:
            QMessageBox.warning(self, "저장 실패", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = P0_Index()
    win.show()
    sys.exit(app.exec())