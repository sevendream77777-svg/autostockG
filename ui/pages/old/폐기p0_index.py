# -*- coding: utf-8 -*-
"""
p0_index_dart_integrated_251205.py
----------------------------------
- V58 UI (원본 p0) + DART(2014년 포함) 로버스트 통합판
- 외부 파일 없이 단일 파일만으로 동작
- 변경 핵심:
  1) _fetch_dart_composite(): corpCode 캐시 + 연도 백오프(최대 5년) + FS_DIV(CFS/OFS) + 보고서 코드 순회
  2) _fetch_dart_financial_robust(): dart_test.py의 다중 시도 로직 이식
  3) _normalize_payload(): finance 매핑에 레거시 별칭 대폭 확장

필요 패키지: requests, pandas, numpy, PySide6, (선택)beautifulsoup4, yfinance, FinanceDataReader
"""

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
from typing import Any, Dict, Iterable, List, Set, Tuple

# [System] 크롤링 필수 라이브러리 체크
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
    print("[System] bs4 모듈이 없습니다. 'pip install beautifulsoup4' 설치를 권장합니다.")

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QFrame, QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QMessageBox, QPushButton, QScrollArea, QSizePolicy, QTextEdit,
    QVBoxLayout, QWidget, QApplication, QDialog
)

# ------------------------------------------------------------------------------
# 0. DART 공통 설정 (robust)
# ------------------------------------------------------------------------------
DART_REPORT_CODES: List[Tuple[str, str]] = [
    ("11011", "사업"), ("11012", "반기"), ("11014", "3분기"), ("11013", "1분기")
]
DART_FS_DIV = ["CFS", "OFS"]

# 레거시 포함 계정명 → 표준키 확장 매핑(부분 문자열 기준)
DART_ACCOUNT_ALIASES = {
    "revenue": [
        "매출액", "영업수익", "수익(매출액)", "매출", "수익", "Revenue",
        "Ⅰ.매출액", "매출액(수익)", "매출액(매출)", "영업수익(매출)"
    ],
    "op_income": [
        "영업이익", "OperatingIncomeLoss", "영업손익", "영업손실", "이익(손실)",
        "사업이익", "매출총이익", "Ⅱ.영업손익", "영업손익(손실)", "영업이익(손실)", "영업손익(이익)"
    ],
    "net_income": [
        "당기순이익", "ProfitLoss", "순이익", "단기순이익", "분기순이익", "반기순이익",
        "당기순이익(손실)", "분기순손익", "반기순손익"
    ],
    "assets": ["자산총계", "자산", "TotalAssets", "총자산", "자산총계(원)"],
    "liabilities": ["부채총계", "부채", "TotalLiabilities", "총부채", "부채총계(원)"],
    "equity": ["자본총계", "자본", "TotalEquity", "총자본", "자본총계(원)"],
    "eps": ["주당순이익", "기본주당이익", "EarningsPerShare", "기본이익(손실)주당액", "주당순이익(손실)"],
    # 현금흐름
    "cash_flow_op": ["영업활동현금흐름"],
    "cash_flow_inv": ["투자활동현금흐름"],
    "cash_flow_fin": ["재무활동현금흐름"],
}
# IFRS 태그와 표준키 힌트(보조)
IFRS_ID_HINTS = {
    "assets": ["ifrs-full_Assets", "Assets"],
    "liabilities": ["ifrs-full_Liabilities", "Liabilities"],
    "equity": ["ifrs-full_Equity", "Equity"],
    "net_income": ["ifrs-full_ProfitLoss", "ProfitLoss"],
    "revenue": ["ifrs-full_Revenue", "Revenue", "ifrs-full_RevenueFromContractsWithCustomers"],
    "op_income": ["ifrs-full_ProfitLossFromOperatingActivities", "OperatingIncomeLoss"],
    "eps": ["ifrs-full_BasicEarningsLossPerShare", "EarningsPerShare"],
}

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
        
        # 숫자 포맷팅
        if isinstance(value, (int, float, np.number)):
            try:
                val_float = float(value)
                integer_cols = ["qty", "vol", "amt", "revenue", "income", "cap", "net_income", "op_income", "cash_flow", "vwap", "div_amount"]
                is_int_col = any(x in self.name_label.text() for x in integer_cols)
                
                if is_int_col and abs(val_float) > 100: 
                     disp = f"{val_float:,.0f}"
                else:
                     disp = f"{val_float:,.2f}"
            except: pass
            
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
        if isinstance(val, (int, float, np.number)):
            if val == 0: return True
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

# ------------------------------------------------------------------------------
# 2. Main Page (P0_Index) - V58 Complete Integration
# ------------------------------------------------------------------------------
class P0_Index(QWidget):
    def __init__(self):
        super().__init__()
        
        # [V58 스펙 58개 컬럼]
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
        title = QLabel("V58 데이터 수집 통합 분석기 (Ultimate Complete Ver.)")
        title.setStyleSheet("font-size: 20px; font-weight: bold; color: #facc15;")
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
        
        btn_run = QPushButton("🚀 풀파워 실행 (All Sources)")
        btn_run.setStyleSheet("background-color: #dc2626; color: white; font-weight: bold; padding: 6px;")
        btn_run.clicked.connect(self.run_full_analysis)
        
        btn_save = QPushButton("💾 로그 저장")
        btn_save.clicked.connect(self.save_payloads_to_file)
        
        btn_dart = QPushButton("🔎 DART Raw/Map 보기")
        btn_dart.clicked.connect(self.show_dart_raw_table)
        
        lay.addWidget(QLabel("Code:"))
        lay.addWidget(self.code_edit)
        lay.addWidget(QLabel("Date:"))
        lay.addWidget(self.date_edit)
        lay.addWidget(btn_run)
        lay.addWidget(btn_save)
        lay.addWidget(btn_dart)
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
        print(msg) 
        QApplication.processEvents()

    def _shorten(self, text: str, limit: int = 140) -> str:
        """짧은 로그용 문자열 자르기"""
        if len(text) <= limit:
            return text
        return text[:limit] + " ..."

    def _format_sample(self, data: Dict[str, Any], limit: int = 8) -> str:
        """dict 일부 샘플을 'k=v; k=v' 형태로 반환"""
        items = []
        for idx, (k, v) in enumerate(data.items()):
            if idx >= limit:
                break
            items.append(f"{k}={v}")
        return "; ".join(items)

    def _log_source_result(self, source: str, raw: Dict[str, Any], normalized: Dict[str, Any]):
        """수집/정규화된 키를 로그로 남김"""
        raw_keys = ", ".join(sorted(raw.keys())) if raw else "(empty)"
        norm_keys = ", ".join(sorted(normalized.keys())) if normalized else "(empty)"
        self.log(f"   [{source}] raw({len(raw)}): {self._shorten(raw_keys)}")
        self.log(f"   [{source}] normalized({len(normalized)}): {self._shorten(norm_keys)}")
        sample = self._format_sample(normalized)
        if sample:
            self.log(f"   [{source}] sample -> {self._shorten(sample)}")

    # ------------------------------------------------------------------
    # [핵심] 실행 로직 (Waterfall Strategy)
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
        
        # (1) Kiwoom (Price/Valuation) - [기존 유지]
        self.log(">>> 1. Kiwoom (Price/Valuation)")
        k_data = self._fetch_kiwoom_composite(code, base_dt)
        self.last_raw_payloads['Kiwoom'] = k_data
        self.last_payloads['Kiwoom'] = self._normalize_payload(k_data)
        self._log_source_result("Kiwoom", k_data, self.last_payloads['Kiwoom'])

        # (2) PyKRX (Flow/Price/Fundamental) - [기존 유지]
        self.log(">>> 2. PyKRX (Flow/Fundamental)")
        p_data = self._fetch_pykrx_enhanced(code, base_dt)
        self.last_raw_payloads['PyKRX'] = p_data
        self.last_payloads['PyKRX'] = self._normalize_payload(p_data)
        self._log_source_result("PyKRX", p_data, self.last_payloads['PyKRX'])

        # (3) Naver Macro (Direct Crawler) - [★신규 추가★]
        self.log(">>> 3. Naver Market Index (Macro/World)")
        nm_data = self._fetch_naver_market_index_crawler()
        self.last_raw_payloads['NaverMacro'] = nm_data
        self.last_payloads['NaverMacro'] = self._normalize_payload(nm_data)
        self._log_source_result("NaverMacro", nm_data, self.last_payloads['NaverMacro'])

        # (4) FDR (Macro Backup) - [기존 유지]
        self.log(">>> 4. FDR (Macro Backup)")
        f_data = self._fetch_fdr_macro_enhanced(base_dt)
        self.last_raw_payloads['FDR'] = f_data
        self.last_payloads['FDR'] = self._normalize_payload(f_data)
        self._log_source_result("FDR", f_data, self.last_payloads['FDR'])

        # (5) DART (Finance) - [로버스트 통합★]
        self.log(">>> 5. DART (Finance: Robust 2014+)")
        d_data = self._fetch_dart_composite(code, base_dt)
        self.last_raw_payloads['DART'] = d_data
        self.last_payloads['DART'] = self._normalize_payload(d_data)
        self._log_source_result("DART", d_data, self.last_payloads['DART'])

        # (6) FnGuide (Sector/Theme) - [★신규 추가★]
        self.log(">>> 6. FnGuide (Sector/Theme Snapshot)")
        fn_data = self._fetch_fnguide_composite(code)
        self.last_raw_payloads['FnGuide'] = fn_data
        self.last_payloads['FnGuide'] = self._normalize_payload(fn_data)
        self._log_source_result("FnGuide", fn_data, self.last_payloads['FnGuide'])
        
        # (7) Yahoo Finance (Events Backup) - [기존 유지]
        self.log(">>> 7. Yahoo Finance (Events/Backup)")
        y_data = self._fetch_yahoo_composite(code, base_dt)
        self.last_raw_payloads['Yahoo'] = y_data
        self.last_payloads['Yahoo'] = self._normalize_payload(y_data)
        self._log_source_result("Yahoo", y_data, self.last_payloads['Yahoo'])

        # 2. Waterfall Merge Strategy (우선순위 병합)
        final_merged = {}
        
        # 우선순위: DART(재무) > PyKRX(수급) > FnGuide(섹터) > Naver(매크로) > Kiwoom(가격)
        self._merge_into(final_merged, 'Kiwoom')     # Base Price
        self._merge_into(final_merged, 'PyKRX')      # Overwrite Price, Flow
        self._merge_into(final_merged, 'NaverMacro') # Macro
        self._merge_into(final_merged, 'FDR')        # Macro Backup
        self._merge_into(final_merged, 'FnGuide')    # Sector (Best Source)
        self._merge_into(final_merged, 'DART')       # Finance (Highest Priority)
        self._merge_into(final_merged, 'Yahoo')      # Events

        # 3. Post Process (추가 계산)
        self._post_process_calculations(final_merged, code)
        
        # 4. UI Update
        cnt = 0
        for col in self.v58_cols:
            if col in final_merged:
                val, src = final_merged[col]
                self.cards[col].set_value(val, src)
                cnt += 1
        
        self.log(f"\n[최종 결과] 58개 중 {cnt}개 수집 완료 ({cnt/58*100:.1f}%)")

    def _merge_into(self, target_dict, src_name):
        if src_name not in self.last_payloads:
            self.log(f"   [Merge] {src_name}: payload 없음, 스킵")
            return
        data = self.last_payloads[src_name]
        before = len(target_dict)
        for k, v in data.items():
            if k in self.v58_cols:
                 if self._is_value_valid(v):
                    target_dict[k] = (v, src_name)
        added = len(target_dict) - before
        self.log(f"   [Merge] {src_name}: 적용 {added}건 (누적 {len(target_dict)})")

    def _post_process_calculations(self, merged, code):
        vals = {k: v[0] for k, v in merged.items()}
        
        # Market 추론
        if "market" not in merged:
            mkt = "KOSPI" if code.isdigit() and int(code) < 100000 else "KOSDAQ"
            merged["market"] = (mkt, "Inferred")
        
        # VWAP 계산
        if "vwap" not in merged and "amount" in vals and "volume" in vals:
            try:
                amt = float(vals["amount"]); vol = float(vals["volume"])
                if vol > 0:
                    vwap = amt / vol
                    close = float(vals.get("close", 0))
                    if close > 0 and vwap < close * 0.1:
                        vwap *= 1000000
                    merged["vwap"] = (vwap, "Calc")
            except: pass

        # ROE/ROA/부채비율 보정(없으면 계산 시도)
        def _f(x): 
            try: return float(x)
            except: return 0.0
        if "roe" not in merged and "net_income" in vals and "equity" in vals:
            ni = _f(vals["net_income"]); eq = _f(vals.get("equity", 0))
            if eq != 0: merged["roe"] = ((ni/eq)*100.0, "Calc")
        if "debt_ratio" not in merged and "liabilities" in vals and "equity" in vals:
            lb = _f(vals["liabilities"]); eq = _f(vals.get("equity", 0))
            if eq != 0: merged["debt_ratio"] = ((lb/eq)*100.0, "Calc")

    # ------------------------------------------------------------------
    # Data Fetchers
    # ------------------------------------------------------------------
    # --- [FnGuide] Theme/Sector Crawler (★New) ---
    def _fetch_fnguide_composite(self, code):
        res = {}
        if not BeautifulSoup: return {}
        try:
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                wics_div = soup.find('div', class_='corp_group1')
                if wics_div:
                    h2 = wics_div.find('h2')
                    if h2: res['name'] = h2.text.strip()
                    for span in wics_div.find_all('span', class_='stxt stxt2'):
                        txt = span.text.strip()
                        if txt:
                            res["sector_name"] = txt
                            break
        except Exception as e:
            self.log(f"FnGuide Error: {e}")
        return res

    # --- [Naver Market Index] Crawler (★New) ---
    def _fetch_naver_market_index_crawler(self):
        res = {}
        if not BeautifulSoup: return {}
        try:
            url = "https://finance.naver.com/marketindex/"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=5)
            soup = BeautifulSoup(resp.text, 'html.parser')
            market1 = soup.select("#exchangeList > li")
            if len(market1) >= 4:
                usd = market1[0].select_one("span.value").text
                cny = market1[3].select_one("span.value").text
                res['usdkrw'] = float(usd.replace(",", ""))
                res['cnykrw'] = float(cny.replace(",", ""))
            oil_list = soup.select("#oilList > li")
            for item in oil_list:
                label = item.select_one("span.blind")
                if label:
                    txt = label.text.strip()
                    val_txt = item.select_one("span.value").text
                    val = float(val_txt.replace(",", ""))
                    if "WTI" in txt or "휘발유" in txt: res['wti'] = val
                    if "국제 금" in txt: res['gold'] = val
        except Exception as e:
            self.log(f"Naver Macro Error: {e}")
        return res

    # --- [Kiwoom] REST API (기존 로직 유지) ---
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

    # --- [DART] Robust 2014+ (통합판) ---
    def _fetch_dart_composite(self, code, base_dt):
        try:
            api_key = self._read_dart_key()
            if not api_key: return {}

            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code: return {}

            fin = self._fetch_dart_financial_robust(api_key, corp_code, int(base_dt[:4]))
            comp = self._fetch_dart_company(api_key, corp_code)
            major = self._fetch_dart_major(api_key, corp_code)

            merged = {}
            if comp: merged.update(comp)
            if major: merged.update(major)
            if fin: merged.update(fin)
            return merged
        except Exception as e:
            self.log(f"DART Composite Error: {e}")
            return {}

    def _read_dart_key(self) -> str:
        paths = [
            r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt",
            "opendart_apikey.txt"
        ]
        for p in paths:
            if os.path.exists(p):
                try:
                    return open(p, "r", encoding="utf-8").read().strip()
                except: pass
        return os.environ.get("DART_API_KEY", "").strip()

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

    def _fetch_dart_financial_robust(self, api_key: str, corp_code: str, base_year: int) -> Dict[str, Any]:
        """
        2014년 포함: 연도 백오프 + FS_DIV + 보고서 순회
        - 성공 시 표준키(revenue, op_income, net_income, eps, equity, liabilities, assets, cash_flow_*)로 반환
        """
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        result: Dict[str, float] = {}

        # --- D-PATCH: Debug logs + raw capture + alias extension ---
        self._dart_map_logs = []
        raw_rows = []

        DART_ACCOUNT_ALIASES.update({
            "eps": list(set(DART_ACCOUNT_ALIASES.get("eps", []) + ["EPS", "기본이익(손실)주당액", "주당순이익(기본)", "주당이익", "주당순이익(손실)", "기본주당순이익"])),
            "op_income": list(set(DART_ACCOUNT_ALIASES.get("op_income", []) + ["영업손익(이익)", "영업손익(손실)", "영업이익(손실)", "영업손익", "OperatingIncomeLoss"])),
            "equity": list(set(DART_ACCOUNT_ALIASES.get("equity", []) + ["자본총계(지배)", "지배주주지분", "EquityAttributableToOwnersOfParent"])),
            "liabilities": list(set(DART_ACCOUNT_ALIASES.get("liabilities", []) + ["부채총계(지배)", "부채", "Liabilities"])),
            "assets": list(set(DART_ACCOUNT_ALIASES.get("assets", []) + ["자산총계(지배)", "Assets"])),
            "cash_flow_op": list(set(DART_ACCOUNT_ALIASES.get("cash_flow_op", []) + ["영업활동으로인한현금흐름", "영업활동현금흐름"])),
            "cash_flow_inv": list(set(DART_ACCOUNT_ALIASES.get("cash_flow_inv", []) + ["투자활동으로인한현금흐름", "투자활동현금흐름"])),
            "cash_flow_fin": list(set(DART_ACCOUNT_ALIASES.get("cash_flow_fin", []) + ["재무활동으로인한현금흐름", "재무활동현금흐름"])),
            "bps": list(set(DART_ACCOUNT_ALIASES.get("bps", []) + ["주당순자산", "BPS"])),
        })

        chosen = None

        years = [base_year - i for i in range(0, 5)]  # base, base-1, ..., base-4
        for y in years:
            for fs in DART_FS_DIV:
                for rc, _name in DART_REPORT_CODES:
                    try:
                        params = {"crtfc_key": api_key, "corp_code": corp_code, "bsns_year": str(y), "reprt_code": rc, "fs_div": fs}
                        resp = requests.get(url, params=params, timeout=8)
                        js = resp.json()
                        if js.get("status") != "000":
                            continue
                        lst = js.get("list") or []
                        if not lst:
                            continue
                        # 관측치 매핑
                        tmp: Dict[str, float] = {}
                        
                        for item in lst:
                            # Raw extraction
                            nm = (item.get("account_nm") or "").strip()
                            aid = (item.get("account_id") or "").strip()
                            sj = (item.get("sj_div") or "").strip().upper()  # 'BS','IS','CF' 등
                            val_str = (item.get("thstrm_amount") or "").replace(",", "")
                            if not val_str or val_str == "-":
                                continue
                            try:
                                val = float(val_str)
                            except:
                                continue # <--- Line 670, IndentationError 해결
                            
                            raw_rows.append({"account_nm": nm, "account_id": aid, "sj_div": sj, "value": val})
                            
                            nm_norm = nm.replace(" ", "")
                            aid_l = aid.lower()
                            
                            # ---- 강한(오탐 방지) 매핑 세트 ----
                            STRONG = {
                            "revenue": {
                            "sj": {"IS", ""},  # 손익계산서
                            "aid_exact": {"ifrs-full_Revenue"},
                            "nm_exact": {"매출액", "Ⅰ.매출액"},
                            "nm_contains": {"매출액(수익)", "영업수익(매출)"},
                            },
                            "op_income": {
                            "sj": {"IS", ""},
                            "aid_exact": {"ifrs-full_ProfitLossFromOperatingActivities", "OperatingIncomeLoss"},
                            "nm_exact": {"영업이익", "Ⅱ.영업손익"},
                            "nm_exclude": {"매출총이익"},  # 오탐 방지
                            },
                            "net_income": {
                            "sj": {"IS", ""},
                            "aid_exact": {"ifrs-full_ProfitLoss"},
                            "nm_exact": {"당기순이익"},
                            "nm_contains": {"분기순손익", "반기순손익", "당기순이익(손실)"},
                            },
                            "eps": {
                            "sj": {"IS", ""},
                            "aid_exact": {"ifrs-full_BasicEarningsLossPerShare"},
                            "aid_contains": {"earningspershare"},
                            "nm_contains": {"주당", "기본주당순이익", "기본이익(손실)주당액"},
                            },
                            "assets": {
                            "sj": {"BS"},
                            "aid_exact": {"ifrs-full_Assets"},
                            "nm_exact": {"자산총계", "자산총계(지배)"},
                            },
                            "liabilities": {
                            "sj": {"BS"},
                            "aid_exact": {"ifrs-full_Liabilities"},
                            "nm_exact": {"부채총계", "부채총계(지배)"},
                            },
                            "equity": {
                            "sj": {"BS"},
                            "aid_exact": {"ifrs-full_Equity", "ifrs-full_EquityAttributableToOwnersOfParent"},
                            "nm_exact": {"자본총계", "자본총계(지배)", "지배주주지분"},
                            },
                            "cash_flow_op": {
                            "sj": {"CF"},
                            "aid_exact": {"ifrs-full_CashFlowsFromUsedInOperatingActivities"},
                            "nm_exact": {"영업활동현금흐름", "영업활동으로인한현금흐름"},
                            },
                            "cash_flow_inv": {
                            "sj": {"CF"},
                            "aid_exact": {"ifrs-full_CashFlowsFromUsedInInvestingActivities"},
                            "nm_exact": {"투자활동현금흐름", "투자활동으로인한현금흐름"},
                            },
                            "cash_flow_fin": {
                            "sj": {"CF"},
                            "aid_exact": {"ifrs-full_CashFlowsFromUsedInFinancingActivities"},
                            "nm_exact": {"재무활동현금흐름", "재무활동으로인한현금흐름"},
                            },
                            "bps": {
                            "sj": {"BS", ""},
                            "nm_exact": {"주당순자산"},
                            "aid_contains": {"per share", "bps"},
                            },
                            }
                            
                            def strong_match(std_key: str) -> bool:
                            rule = STRONG.get(std_key, {})
                            # sj_div gate
                            sjs = rule.get("sj")
                            if sjs and sj not in sjs:
                            return False
                            # exact matches
                            if any(aid == ex for ex in rule.get("aid_exact", set())):
                            return True
                            if any(nm == ex for ex in rule.get("nm_exact", set())):
                            return True
                            # contains (normalized)
                            if any(tok in aid_l for tok in (t.lower() for t in rule.get("aid_contains", set()))):
                            return True
                            nm_no = nm_norm
                            if any(t.replace(" ", "") in nm_no for t in rule.get("nm_contains", set())):
                            return True
                            # exclusions
                            if "nm_exclude" in rule and any(ex in nm_no for ex in rule["nm_exclude"]):
                            return False
                            return False
                            
                            # 1) 강한 매칭 우선
                            for std_key in ["revenue","op_income","net_income","eps","assets","liabilities","equity","cash_flow_op","cash_flow_inv","cash_flow_fin","bps"]:
                            if strong_match(std_key):
                            # 단, op_income은 '영업손익' 문구만 있고 '영업' 없는 경우 제외
                            if std_key == "op_income" and ("손익" in nm_norm) and ("영업" not in nm_norm):
                            pass
                            else:
                            tmp[std_key] = val
                            self._dart_map_logs.append(f"[MAP*] {std_key} <- {nm} ({aid}; sj={sj}) = {val}")
                            continue
                            
                            # 2) 보조: 2014 문자 별칭 + IFRS 힌트 (sj_div에 맞는 키만 허용)
                            ALLOW_BY_SJ = {
                            "BS": {"assets","liabilities","equity","bps"},
                            "IS": {"revenue","op_income","net_income","eps"},
                            "CF": {"cash_flow_op","cash_flow_inv","cash_flow_fin"},
                            "": {"revenue","op_income","net_income","eps","bps"},  # 일부 응답에서 sj_div 누락시
                            }
                            allowed = ALLOW_BY_SJ.get(sj, set())
                            
                            # 별칭 기반 (과거형)
                            for std, aliases in DART_ACCOUNT_ALIASES.items():
                            if std not in allowed:
                            continue
                            if any(a.replace(" ", "") in nm_norm for a in aliases):
                            # 강한 배제조건: 자산/부채/자본의 광범위한 단어 오탐 방지
                            if std in {"assets","liabilities","equity"} and nm not in {"자산총계","부채총계","자본총계","자산총계(지배)","부채총계(지배)","자본총계(지배)","지배주주지분"}:
                            continue
                            tmp.setdefault(std, val)
                            self._dart_map_logs.append(f"[ALIAS] {std} <- {nm} ({aid}; sj={sj}) = {val}")
                            break
                            
                            # IFRS 태그 힌트 (aid 기반)
                            if aid:
                            for std, hints in IFRS_ID_HINTS.items():
                            if std not in allowed:
                            continue
                            for h in hints:
                            if h.lower() in aid_l:
                            tmp.setdefault(std, val)
                            self._dart_map_logs.append(f"[HINT] {std} <- {nm} ({aid}; sj={sj}) = {val}")
                            break
                    except Exception as e:
                        self._dart_map_logs.append(f"[ERROR] map loop error: {e}")
                        continue

                    # 파생치 보정
                    eq = float(tmp.get("equity", 0.0))
                    lb = float(tmp.get("liabilities", 0.0))
                    ni = float(tmp.get("net_income", 0.0))
                    if eq != 0 and "roe" not in tmp and ni != 0:
                        tmp["roe"] = (ni/eq)*100.0
                    if eq != 0 and "debt_ratio" not in tmp and lb != 0:
                        tmp["debt_ratio"] = (lb/eq)*100.0
                    tmp.setdefault("eps", float(tmp.get("eps", 0.0)))
                    tmp.setdefault("bps", float(tmp.get("bps", 0.0)))

                    if tmp:
                        result = tmp
                        chosen = (y, fs, rc)
                        break
                if chosen: break
            if chosen: break

        # 메타 첨부
        if result:
            if chosen:
                y, fs, rc = chosen
                result["_dart_meta_year"] = y
                result["_dart_meta_fs"] = fs
                result["_dart_meta_rc"] = rc
            result["_raw_list"] = raw_rows
            result["_map_logs"] = self._dart_map_logs
        return result

    def _fetch_dart_major(self, api_key, corp_code):
        try:
            url = "https://opendart.fss.or.kr/api/majorstock.json"
            resp = requests.get(url, params={"crtfc_key": api_key, "corp_code": corp_code}, timeout=5)
            js = resp.json()
            if js.get("status") == "000":
                return {"major_holder_count": len(js.get("list", []))}
        except: pass
        return {}

    # --- [PyKRX] Enhanced (기존 로직 유지) ---
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

    # --- [FDR] Macro Enhanced (기존 로직 유지) ---
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

    # --- [Yahoo Finance] Backup & Events (기존 로직 유지) ---
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
                res["debt_ratio"] = info.get("debtToEquity")
                res["roe"] = info.get("returnOnEquity")
        except: pass
        return res

    # ------------------------------------------------------------------
    # Normalization (Full V58 Mapping + Calc, 레거시 강화)
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
            "nps_net_qty": ["nps_net_qty", "연기금", "연기금등", "pension", "pension_net_buy"],
            
            # Finance (레거시 포함 확대)
            "revenue": ["revenue", "thstrm_amount", "매출액", "영업수익", "ifrs-full_Revenue",
                        "Ⅰ.매출액", "매출액(수익)", "매출액(매출)", "영업수익(매출)"],
            "op_income": ["op_income", "operating_income", "영업이익", "dart_OperatingIncomeLoss", "OperatingIncomeLoss",
                          "영업손익", "영업손실", "이익(손실)", "사업이익", "매출총이익", "Ⅱ.영업손익", "영업손익(손실)", "영업이익(손실)"],
            "net_income": ["net_income", "danggi_sun_profit", "당기순이익", "ifrs-full_ProfitLoss",
                           "순이익", "단기순이익", "분기순이익", "반기순이익", "당기순이익(손실)", "분기순손익", "반기순손익"],
            "assets": ["assets", "자산총계", "ifrs-full_Assets", "TotalAssets", "자산총계(원)"],
            "liabilities": ["liabilities", "부채총계", "ifrs-full_Liabilities", "TotalLiabilities", "부채총계(원)"],
            "equity": ["equity", "자본총계", "ifrs-full_Equity", "TotalEquity", "자본총계(원)"],
            "cash_flow_op": ["cash_flow_op", "영업활동현금흐름", "ifrs-full_CashFlowsFromUsedInOperatingActivities"],
            "cash_flow_inv": ["cash_flow_inv", "투자활동현금흐름", "ifrs-full_CashFlowsFromUsedInInvestingActivities"],
            "cash_flow_fin": ["cash_flow_fin", "재무활동현금흐름", "ifrs-full_CashFlowsFromUsedInFinancingActivities"],
            
            "bps": ["bps", "BPS"], "per": ["per", "PER"], "pbr": ["pbr", "PBR"], 
            "eps": ["eps", "EPS", "기본주당이익", "기본이익(손실)주당액", "주당순이익(손실)"],
            "roe": ["roe", "ROE"],
            
            # Sector/Events
            "sector_name": ["sector_name", "sector", "industry"],
            "ex_div_date": ["ex_div_date", "exDividendDate"],
            "theme_code": ["theme_code"], "theme_name": ["theme_name"],
            "debt_ratio": ["debt_ratio", "debtToEquity"],
            
            # Macro
            "usdkrw": ["usdkrw", "USD/KRW"],
            "cnykrw": ["cnykrw", "CNY/KRW"],
            "kr10y_yield": ["kr10y_yield", "KR10YT"],
            "wti": ["wti", "WTI"], "gold": ["gold", "Gold"]
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
                liab = 0; equity = 0
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

    def _collect_card_results(self) -> Dict[str, Any]:
        """UI 카드에 표시된 값을 텍스트로 수집"""
        results = {}
        for key, card in self.cards.items():
            if card.has_value:
                results[key] = card.value_label.text()
        return results

    def show_dart_raw_table(self):
        """DART 수집 raw rows와 매핑 로그를 별도 창에 표시"""
        dart_norm = self.last_payloads.get("DART") or {}
        raw_payload = self.last_raw_payloads.get("DART") or {}
        raw_list = dart_norm.get("_raw_list") or raw_payload.get("_raw_list") or []
        map_logs = dart_norm.get("_map_logs") or raw_payload.get("_map_logs") or getattr(self, "_dart_map_logs", [])

        if not raw_list and not map_logs:
            QMessageBox.information(self, "DART 로그 없음", "먼저 실행한 뒤 다시 확인하세요.")
            return

        meta_parts = []
        for k in ["_dart_meta_year", "_dart_meta_fs", "_dart_meta_rc"]:
            if k in dart_norm:
                meta_parts.append(f"{k}={dart_norm[k]}")

        lines = []
        if meta_parts:
            lines.append("[Meta] " + ", ".join(meta_parts))

        lines.append(f"[Raw rows] {len(raw_list)}개")
        for row in raw_list[:120]:
            nm = row.get("account_nm", "")
            aid = row.get("account_id", "")
            val = row.get("value", "")
            lines.append(f"{nm} ({aid}) = {val}")
        if len(raw_list) > 120:
            lines.append(f"... {len(raw_list) - 120} more rows")

        lines.append(f"\n[Map logs] {len(map_logs)}개")
        for log in map_logs[:200]:
            lines.append(log)
        if len(map_logs) > 200:
            lines.append(f"... {len(map_logs) - 200} more logs")

        text = "\n".join(lines) if lines else "표시할 데이터가 없습니다."
        dlg = QDialog(self)
        dlg.setWindowTitle("DART Raw/Map 로그")
        lay = QVBoxLayout(dlg)
        te = QTextEdit(dlg)
        te.setReadOnly(True)
        te.setPlainText(text)
        lay.addWidget(te)
        dlg.resize(900, 600)
        dlg.exec()

    def save_payloads_to_file(self):
        if not self.last_payloads:
            QMessageBox.information(self, "저장 불가", "먼저 분석을 실행하세요.")
            return
        code = self.code_edit.text().strip() or "unknown"
        base_dt = self.date_edit.text().strip() or QDate.currentDate().toString("yyyyMMdd")
        root_dir = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(root_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        fname = f"p0_dump_{code}_{base_dt}_{int(time.time())}.json"
        path = os.path.join(logs_dir, fname)
        result_cards = self._collect_card_results()
        data = {
            "code": code,
            "date": base_dt,
            "result": result_cards,
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