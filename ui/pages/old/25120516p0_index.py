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
from typing import Any, Dict, Iterable, List, Set

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# ------------------------------------------------------------------------------
# 1. UI Components (FieldCard)
# ------------------------------------------------------------------------------

class FieldCard(QFrame):
    """컬럼 하나를 아이콘 + (컬럼명, 데이터) 2열로 표현."""

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
        self.icon.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.icon.setStyleSheet("font-size: 14px;")

        self.name_label = QLabel(name)
        self.name_label.setWordWrap(True)
        self.name_label.setStyleSheet("color: #e5e7eb; font-weight: 700;")

        self.value_label = QLabel("—")
        self.value_label.setWordWrap(True)
        self.value_label.setStyleSheet("color: #cbd5e1;")
        self.value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        lay.addWidget(self.icon)
        lay.addWidget(self.name_label, 1)
        lay.addWidget(self.value_label, 2)

    def set_value(self, value: Any, src_tag: str = ""):
        # 값이 유효하지 않거나 비어있는 경우 (안전한 체크)
        if not self._is_safe_true(value):
            self.icon.setText("⚪")
            self.value_label.setText("—")
            self.has_value = False
            return

        self.icon.setText("✅")
        
        # 값 포맷팅 (소수점 등 정리)
        disp = str(value)
        if isinstance(value, float):
            disp = f"{value:,.2f}"
            # 수량이나 거래대금은 소수점 제거
            if "qty" in self.name_label.text() or "vol" in self.name_label.text() or "amt" in self.name_label.text():
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
        """UI 표시용 안전한 값 체크 (NumPy/Pandas 호환)"""
        if val is None: return False
        if isinstance(val, str) and not val.strip(): return False
        if isinstance(val, (list, dict)) and not val: return False
        if isinstance(val, (int, float)):
            # 0은 값으로 인정
            if val == 0: return True
            try:
                if math.isnan(val): return False
            except: pass
            return True
        # Pandas/Numpy safe check
        try:
            if hasattr(val, 'size') and val.size == 0: return False
            if pd.isna(val): return False
        except: pass
        return True


# ------------------------------------------------------------------------------
# 2. Main Page (P0_Index)
# ------------------------------------------------------------------------------

class P0_Index(QWidget):
    """
    V58 스펙 검증 및 수집 최적화 시뮬레이터 (Final Integrated Version)
    기능: Kiwoom(REST 4종 복합) + DART + PyKRX + FDR + Yahoo + Naver
    """

    def __init__(self):
        super().__init__()
        
        # [V58 스펙 확정 컬럼]
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

    # ------------------------------------------------------------------
    # UI Layout
    # ------------------------------------------------------------------
    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setSpacing(8)

        # Header
        header_lay = QHBoxLayout()
        title = QLabel("V58 데이터 수집 통합 분석기 (Final)")
        title.setStyleSheet("font-size: 24px; font-weight: 800; color: #e5e7eb;")
        header_lay.addWidget(title)
        header_lay.addStretch(1)
        root.addLayout(header_lay)

        # Controls
        controls = self._build_controls()
        root.addWidget(controls)

        # Analysis Log Area
        self.log_area = QTextEdit()
        self.log_area.setPlaceholderText("검증 및 분석 결과가 여기에 표시됩니다...")
        self.log_area.setStyleSheet("""
            QTextEdit {
                background-color: #0f172a; 
                color: #22d3ee; 
                font-family: Consolas, monospace;
                font-size: 13px;
                border: 1px solid #334155;
            }
        """)
        self.log_area.setFixedHeight(220) 
        root.addWidget(self.log_area)

        # Field Grid
        fields_box = self._build_field_grid()
        root.addWidget(fields_box, 1)

    def _build_controls(self) -> QGroupBox:
        box = QGroupBox("테스트 설정")
        lay = QVBoxLayout(box)
        
        row1 = QHBoxLayout()
        self.code_edit = QLineEdit("005930") 
        self.code_edit.setPlaceholderText("종목코드")
        self.code_edit.setFixedWidth(100)
        
        self.date_edit = QLineEdit(QDate.currentDate().toString("yyyyMMdd"))
        self.date_edit.setPlaceholderText("YYYYMMDD")
        self.date_edit.setFixedWidth(100)

        btn_run_all = QPushButton("🚀 전체 소스 통합 분석 (Waterfall)")
        btn_run_all.setStyleSheet("background-color: #2563eb; color: white; font-weight: bold; padding: 6px 12px; border-radius: 4px;")
        btn_run_all.clicked.connect(self.run_full_analysis)

        btn_save = QPushButton("로그 저장")
        btn_save.clicked.connect(self.save_payloads_to_file)

        btn_clear = QPushButton("초기화")
        btn_clear.clicked.connect(self.clear_all)

        row1.addWidget(QLabel("종목코드:"))
        row1.addWidget(self.code_edit)
        row1.addWidget(QLabel("기준일자:"))
        row1.addWidget(self.date_edit)
        row1.addStretch(1)
        row1.addWidget(btn_run_all)
        row1.addWidget(btn_save)
        row1.addWidget(btn_clear)
        
        lay.addLayout(row1)
        return box

    def _build_field_grid(self) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: none;")
        
        container = QWidget()
        self.grid = QGridLayout(container)
        self.grid.setSpacing(6)
        
        cols = 4 
        for idx, col_name in enumerate(self.v58_cols):
            card = FieldCard(col_name)
            self.cards[col_name] = card
            self.grid.addWidget(card, idx // cols, idx % cols)
            
        scroll.setWidget(container)
        return scroll

    # ------------------------------------------------------------------
    # Core Logic: Analysis & Optimization
    # ------------------------------------------------------------------
    def run_full_analysis(self):
        """전체 소스 호출 -> 정규화 -> 커버리지 분석 -> 최적 순서 제안"""
        self.clear_all()
        code = self.code_edit.text().strip()
        base_dt = self._normalize_date(self.date_edit.text().strip())
        
        self.log(f"=== [분석 시작] Target: {code}, Date: {base_dt} ===")
        
        # 1. 각 소스별 데이터 수집 시도
        self.last_payloads = {}
        self.last_raw_payloads = {}
        
        # (1) Kiwoom - REST API (4종 복합 호출 - 원본 로직 복원)
        self.log(">>> 1. Kiwoom REST 호출 중 (4개 API 복합)...")
        k_data = self._fetch_kiwoom_composite(code, base_dt)
        self.last_raw_payloads['Kiwoom'] = k_data
        self.last_payloads['Kiwoom'] = self._normalize_payload(k_data)
        
        # (2) DART
        self.log(">>> 2. DART 호출 중...")
        d_data = self._fetch_dart_composite(code, base_dt)
        self.last_raw_payloads['DART'] = d_data
        self.last_payloads['DART'] = self._normalize_payload(d_data)
        
        # (3) PyKRX
        self.log(">>> 3. PyKRX 호출 중...")
        p_data = self._fetch_pykrx_composite(code, base_dt)
        self.last_raw_payloads['PyKRX'] = p_data
        self.last_payloads['PyKRX'] = self._normalize_payload(p_data)
        
        # (4) FDR
        self.log(">>> 4. FDR (Macro) 호출 중...")
        f_data = self._fetch_fdr_macro(base_dt)
        self.last_raw_payloads['FDR'] = f_data
        self.last_payloads['FDR'] = self._normalize_payload(f_data)

        # (5) Yahoo Finance
        self.log(">>> 5. Yahoo Finance 호출 중...")
        y_data = self._fetch_yahoo_composite(code, base_dt)
        self.last_raw_payloads['Yahoo'] = y_data
        self.last_payloads['Yahoo'] = self._normalize_payload(y_data)

        # (6) Naver
        self.log(">>> 6. Naver Finance 호출 중...")
        n_data = self._fetch_naver_composite(code)
        self.last_raw_payloads['Naver'] = n_data
        self.last_payloads['Naver'] = self._normalize_payload(n_data)

        # ----------------------------------------------------------------
        # 1.5 진단용 로그
        # ----------------------------------------------------------------
        self.log("\n[ 🔍 소스별 수집 상태 (Raw Data) ]")
        for src, data in self.last_payloads.items():
            valid_keys = [k for k, v in data.items() if self._is_value_valid(v)]
            self.log(f" - {src}: {len(valid_keys)}개 항목 수집됨")
            if src == "Kiwoom" and len(valid_keys) == 0:
                self.log("   -> 🚨 키움 실패: 경로/토큰/휴장일 확인 필요")

        # 2. 커버리지 맵 생성
        coverage_map = {src: set() for src in self.last_payloads.keys()}
        for src, data in self.last_payloads.items():
            for col in self.v58_cols:
                val = data.get(col)
                if self._is_value_valid(val):
                    coverage_map[src].add(col)

        # 3. 최적 수집 순서 계산 (Greedy Waterfall)
        self.log("\n[ 📊 최적 수집 전략 분석 결과 (Waterfall) ]")
        
        remaining_cols = set(self.v58_cols)
        final_merged = {}
        
        round_idx = 1
        while remaining_cols and coverage_map:
            best_src = None
            max_contrib = 0
            
            # 우선순위: Kiwoom > DART > PyKRX > Others
            priority_order = ["Kiwoom", "DART", "PyKRX", "FDR", "Naver", "Yahoo"]
            
            # 1. 우선순위대로 먼저 훑어서 기여도가 있으면 선택 (안전성 확보)
            for src in priority_order:
                if src in coverage_map:
                    contrib = len(coverage_map[src].intersection(remaining_cols))
                    if contrib > 0:
                        best_src = src
                        max_contrib = contrib
                        break # 우선순위 높은게 있으면 바로 선택
            
            # 2. 우선순위 목록에 없는 소스 처리 (혹은 기여도가 다 0일때)
            if best_src is None:
                for src, covered_set in coverage_map.items():
                    contrib = len(covered_set.intersection(remaining_cols))
                    if contrib > max_contrib:
                        max_contrib = contrib
                        best_src = src
            
            # 더 이상 채울 수 없으면 종료
            if best_src is None or max_contrib == 0:
                break 
                
            solved_now = coverage_map[best_src].intersection(remaining_cols)
            
            self.log(f"  ✅ {round_idx}차: {best_src} ( +{len(solved_now)}개 해결 )")
            # self.log(f"     -> 예: {list(solved_now)[:3]} ...")
            
            # 최종 데이터 병합
            for col in solved_now:
                final_merged[col] = (self.last_payloads[best_src][col], best_src)
            
            remaining_cols -= solved_now
            del coverage_map[best_src]
            round_idx += 1

        # 4. 결과 UI 반영
        filled_count = 0
        for col in self.v58_cols:
            if col in final_merged:
                val, src = final_merged[col]
                self.cards[col].set_value(val, src)
                filled_count += 1
            else:
                self.cards[col].clear()

        # 5. 리포트
        self.log(f"\n>>> 최종 달성률: {filled_count} / 58 ({(filled_count/58)*100:.1f}%)")
        if remaining_cols:
            self.log(f"⚠️ [누락된 컬럼]: {list(remaining_cols)}")
        else:
            self.log("🎉 완벽합니다! 58개 컬럼 전체 수집 가능.")

    def log(self, msg):
        self.log_area.append(msg)

    # ------------------------------------------------------------------
    # Helper: Safe Value Check (Robust Version)
    # ------------------------------------------------------------------
    def _is_value_valid(self, val: Any) -> bool:
        if val is None: return False
        if isinstance(val, str): return bool(val.strip())
        if isinstance(val, (list, dict, tuple, set)): return len(val) > 0
        if isinstance(val, (int, float)):
            try:
                if math.isnan(val): return False
                if math.isinf(val): return False
            except: pass
            return True 
        try:
            if hasattr(val, 'size'): return val.size > 0
            if pd.isna(val): return False
        except: pass
        return True

    # ------------------------------------------------------------------
    # Data Fetchers (Source Wrappers)
    # ------------------------------------------------------------------
    
    # --- [Kiwoom] REST API (4종 복합 호출 + 경로 안전 로드) ---
    def _fetch_kiwoom_composite(self, code, base_dt):
        try:
            # 1. sys.path에 폴더 추가 (token_manager import 해결)
            root_dir = r"F:\autostockG"
            api_dir = os.path.join(root_dir, "api", "kiwoom_rest")
            
            # [수정] 모듈 로드 시 경로를 확실하게 맨 앞에 추가
            if api_dir not in sys.path:
                sys.path.insert(0, api_dir)
            if root_dir not in sys.path:
                sys.path.append(root_dir)

            # 2. Import
            # 원본 코드가 kiwoom_api.py를 api.kiwoom_rest 패키지 하위로 인식하는 경우와
            # 플랫하게 인식하는 경우 모두 대응
            try:
                from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
            except ImportError:
                try:
                    from kiwoom_api import KiwoomRestApi
                except ImportError:
                    self.log("!! KiwoomRestApi import 실패. 경로 재확인 요망.")
                    return {}

            api = KiwoomRestApi()

            def call(api_id, path, body):
                res = api._call_api(api_id, path, body=body)
                rc = res.get("return_code")
                rm = res.get("return_msg")
                self.log(f"   - {api_id} rc={rc} msg={rm}")
                return res

            def attempt(dt_str):
                merged_local = {}
                res_chart = call("ka10081", "/api/dostk/chart", {"stk_cd": code, "base_dt": dt_str, "upd_stkpc_tp": "D", "term_cnt": "1"})
                if self._is_valid(res_chart): merged_local.update(self._extract_first(res_chart))

                res_info = call("ka10014", "/api/dostk/shsa", {"stk_cd": code, "tm_tp": "0", "strt_dt": dt_str, "end_dt": dt_str})
                if self._is_valid(res_info): merged_local.update(self._extract_first(res_info))

                res_inv = call("ka10058", "/api/dostk/stkinfo", {"stk_cd": code, "strt_dt": dt_str, "end_dt": dt_str, "trde_tp": "0"})
                if self._is_valid(res_inv): merged_local.update(self._extract_first(res_inv))

                res_det = call("ka10001", "/api/dostk/stkinfo", {"stk_cd": code})
                if self._is_valid(res_det): merged_local.update(self._extract_first(res_det))

                return merged_local

            merged = attempt(base_dt)
            if not merged:
                try:
                    prev = (pd.to_datetime(base_dt) - pd.tseries.offsets.BDay(1)).strftime("%Y%m%d")
                    self.log(f"   -> Kiwoom 재시도: {prev}")
                    merged = attempt(prev)
                except Exception as e:
                    self.log(f"   -> Kiwoom 재시도 실패: {e}")

            return merged
        except Exception as e:
            self.log(f"!! Kiwoom Import/Call Error: {e}")
            return {}

    # --- [DART] OpenAPI ---
    def _fetch_dart_composite(self, code, base_dt):
        try:
            # 키 파일 경로 강제 지정 (사용자 PC 기준)
            key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"
            api_key = ""
            if os.path.exists(key_path):
                with open(key_path, "r", encoding="utf-8") as f:
                    api_key = f.readline().strip()
            
            if not api_key:
                # Fallback: 현재 폴더
                if os.path.exists("opendart_apikey.txt"):
                    with open("opendart_apikey.txt", "r", encoding="utf-8") as f:
                        api_key = f.readline().strip()

            if not api_key:
                self.log("!! DART API Key 없음")
                return {}
                
            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code:
                return {}
            
            merged = {}
            merged.update(self._fetch_dart_company(api_key, corp_code))
            merged.update(self._fetch_dart_financial(api_key, corp_code, base_dt))
            merged.update(self._fetch_dart_major(api_key, corp_code))
            return merged
        except Exception as e:
            self.log(f"!! DART Error: {e}")
            return {}

    def _get_dart_corp_map(self, api_key: str) -> Dict[str, str]:
        cache_path = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
        need_fetch = True
        
        if os.path.exists(cache_path):
            try:
                mtime = dt.datetime.fromtimestamp(os.path.getmtime(cache_path))
                if (dt.datetime.now() - mtime).days < 1:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        return json.load(f)
            except: pass

        if need_fetch:
            self.log("DART 기업코드 맵 다운로드 중...")
            url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    zip_path = cache_path + ".zip"
                    with open(zip_path, "wb") as f:
                        f.write(resp.content)
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        xml_data = zf.read(zf.namelist()[0])
                    try: os.remove(zip_path)
                    except: pass
                    
                    root = ET.fromstring(xml_data)
                    mapping = {}
                    for corp in root.iter("list"):
                        stock_code = (corp.findtext("stock_code") or "").strip()
                        corp_code = (corp.findtext("corp_code") or "").strip()
                        if stock_code and corp_code:
                            mapping[stock_code] = corp_code
                    
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
                            val_str = item.get("thstrm_amount", "").replace(",","")
                            if val_str and val_str not in ["-", ""]:
                                try:
                                    val = float(val_str)
                                    merged_fin[acct_nm] = val
                                    merged_fin[item.get("account_id","")] = val
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

    # --- [PyKRX] ---
    def _fetch_pykrx_composite(self, code, base_dt):
        try:
            from pykrx import stock
            # 1. OHLCV
            df = stock.get_market_ohlcv(base_dt, base_dt, code)
            res = {}
            if df is not None and not df.empty:
                for k, v in df.iloc[0].to_dict().items():
                    res[k] = self._to_native(v)
            # 2. Fundamental
            df_f = stock.get_market_fundamental(base_dt, base_dt, code)
            if df_f is not None and not df_f.empty:
                for k, v in df_f.iloc[0].to_dict().items():
                    res[k] = self._to_native(v)
            # 3. MarketCap
            df_c = stock.get_market_cap(base_dt, base_dt, code)
            if df_c is not None and not df_c.empty:
                 for k, v in df_c.iloc[0].to_dict().items():
                    res[k] = self._to_native(v)
            return res
        except: return {}
            
    def _to_native(self, val):
        if hasattr(val, 'item'): return val.item()
        return val

    # --- [FDR] ---
    def _fetch_fdr_macro(self, base_dt):
        try:
            import FinanceDataReader as fdr
            df = fdr.DataReader('USD/KRW', base_dt, base_dt)
            if df is not None and not df.empty:
                val = df.iloc[0]['Close']
                return {"usdkrw": self._to_native(val)}
            return {}
        except: return {}

    # --- [Yahoo Finance] ---
    def _fetch_yahoo_composite(self, code, base_dt):
        try:
            import yfinance as yf
            ticker = code + ".KS"
            end = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            start = (dt.datetime.strptime(base_dt, "%Y%m%d") - dt.timedelta(days=5)).strftime("%Y-%m-%d")
            
            yf_obj = yf.Ticker(ticker)
            hist = yf_obj.history(start=start, end=end)
            
            res = {}
            if not hist.empty:
                row = hist.iloc[-1]
                res["close"] = float(row["Close"])
                res["volume"] = float(row["Volume"])
            
            info = yf_obj.info
            if info:
                res["sector_name"] = info.get("sector", "")
                res["bps"] = info.get("bookValue")
                res["per"] = info.get("trailingPE")
            
            return res
        except: return {}

    # --- [Naver] ---
    def _fetch_naver_composite(self, code):
        try:
            url = f"https://finance.naver.com/item/main.naver?code={code}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers)
            res = {}
            if resp.status_code == 200:
                res["market_status"] = "Active" 
            return res
        except: return {}

    # ------------------------------------------------------------------
    # Normalization (Full V58 Mapping)
    # ------------------------------------------------------------------
    def _normalize_payload(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        if not raw: return {}
        
        mapping_rules = {
            # Price
            "open": ["open", "open_pric", "stck_oprc", "시가", "Open"],
            "high": ["high", "high_pric", "stck_hgpr", "고가", "High"],
            "low": ["low", "low_pric", "stck_lwpr", "저가", "Low"],
            "close": ["close", "close_pric", "stck_clpr", "종가", "cur_prc", "Close"],
            "volume": ["volume", "trde_qty", "acml_vol", "거래량", "Volume"],
            "amount": ["amount", "trde_prica", "acc_trde_prica", "거래대금"],
            "market": ["market", "mkt_nm", "kospi_yn", "corp_cls"],
            "name": ["name", "stk_nm", "itm_nm", "corp_name"],
            "adj_factor": ["adj_factor", "수정계수"], 
            "vwap": ["vwap"],
            "date": ["date", "dt", "bas_dt", "stnd_dt", "stck_bsop_date", "base_dt"],
            "code": ["code", "stk_cd", "stock_code", "corp_code", "isin_cd"],
            
            # Flow (Kiwoom)
            "inst_net_qty": ["inst_net_qty", "organ_net_buy_qty", "기관순매수", "ins_net_buy", "ins_net_buy_qty"],
            "inst_net_amt": ["inst_net_amt", "organ_net_buy_amt", "ins_net_buy_amt"],
            "frgn_net_qty": ["frgn_net_qty", "frg_net_buy_qty", "외인순매수", "frg_net_buy"],
            "frgn_net_amt": ["frgn_net_amt", "frg_net_buy_amt"],
            "nps_net_qty": ["nps_net_qty", "pension", "pension_net_buy", "pension_net_buy_qty"],
            "nps_net_amt": ["nps_net_amt", "pension_net_buy_amt"],
            "dealer_net_qty": ["dealer_net_qty", "finc_inv", "finc_inv_qty"],
            "dealer_net_amt": ["dealer_net_amt", "finc_inv_amt"],
            "short_sell_qty": ["short_sell_qty", "shrts_qty", "short_qty"],
            "short_sell_amt": ["short_sell_amt", "shrts_trde_prica", "short_amt"],
            "loan_balance_qty": ["loan_balance_qty"],
            "loan_balance_amt": ["loan_balance_amt"],
            "psn_net_buy": ["psn_net_buy"],
            
            # Finance (DART/Kiwoom/Yahoo)
            "revenue": ["revenue", "thstrm_amount", "매출액", "영업수익", "totalRevenue"],
            "op_income": ["op_income", "operating_income", "영업이익", "영업손실"],
            "net_income": ["net_income", "danggi_sun_profit", "당기순이익", "당기순손실", "법인세차감전 순이익"],
            
            "per": ["per", "PER", "trailingPE"], 
            "eps": ["eps", "EPS"], 
            "pbr": ["pbr", "PBR"], 
            "bps": ["bps", "BPS", "bookValue"],
            "roe": ["roe", "ROE"],
            
            # Macro
            "usdkrw": ["usdkrw", "rate"],
            
            # Sector
            "sector_name": ["sector_name", "sector"],
        }
        
        out = {}
        for k, v in raw.items():
            if k in self.v58_cols:
                out[k] = v
        
        for std_key, candidates in mapping_rules.items():
            if std_key in out: continue 
            for cand in candidates:
                if cand in raw:
                    val = raw[cand]
                    if self._is_value_valid(val):
                        out[std_key] = val
                        break
        return out

    def _is_valid(self, res):
        return res and isinstance(res, dict) and str(res.get("return_code","")) == "0"

    def _extract_first(self, res):
        # 1) 대표 키 처리
        for key in ("output", "chart", "data", "result", "stk_dt_pole_chart_qry"):
            if key in res:
                block = res[key]
                if isinstance(block, list) and block:
                    if isinstance(block[0], dict):
                        return block[0]
                if isinstance(block, dict):
                    return block
        # 2) 기타: return_code/return_msg 같은 메타를 제외하고 본문을 그대로 반환
        if isinstance(res, dict):
            return {
                k: v for k, v in res.items()
                if k not in ("return_code", "return_msg", "response_headers", "resp_headers")
            }
        return {}

    def _normalize_date(self, text):
        return text.replace("-", "").replace(".", "").strip()

    # ------------------------------------------------------------------
    # Save payloads to file (raw + normalized)
    # ------------------------------------------------------------------
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

    def clear_all(self):
        self.log_area.clear()
        for card in self.cards.values():
            card.clear()
