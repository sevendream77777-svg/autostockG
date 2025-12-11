# -*- coding: utf-8 -*-
"""
p0_index.py — V58 통합 수집·정규화·커버리지 분석 (UI 포함, 완성본)
- Class: P0_Index (import 대상)
- PySide6 기반 UI + 23개 수정사항 반영
"""

from typing import Any, Dict, List, Set, Tuple
import os, sys, json, math, zipfile, requests, csv, time
import datetime as dt
import xml.etree.ElementTree as ET

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QGroupBox, QScrollArea,
    QLabel, QLineEdit, QPushButton, QTextEdit, QSizePolicy, QFrame, QMessageBox
)

# -----------------------------------------------------------------------------
# 0) 표준 스키마 (v58)
# -----------------------------------------------------------------------------

V58_STANDARD_COLUMNS: List[str] = [
    # 1. Price (12)
    "date","code","name","market","open","high","low","close","volume","amount","vwap","adj_factor",
    # 2. Flow (12)
    "inst_net_qty","inst_net_amt","frgn_net_qty","frgn_net_amt",
    "nps_net_qty","nps_net_amt","dealer_net_qty","dealer_net_amt",
    "short_sell_qty","short_sell_amt","loan_balance_qty","loan_balance_amt",
    # 3. Finance (11) — PIT
    "revenue","op_income","net_income","eps","bps","roe","roa","debt_ratio",
    "cash_flow_op","cash_flow_inv","cash_flow_fin",
    # 4. Sector/Theme (5) — PIT
    "sector_code","sector_name","theme_code","theme_name","sector_index_close",
    # 5. Macro (8)
    "usdkrw","cnykrw","dxy","us10y_yield","kr10y_yield","wti","gold","vix",
    # 6. Events / CA (10) — PIT
    "earnings_announce_date","earnings_surprise","earnings_effective_date",
    "ex_div_date","div_amount","split_announce_date","split_effective_date",
    "rights_issue_announce_date","rights_issue_effective_date","mna_announce_date",
]

# -----------------------------------------------------------------------------
# 1) 소스 화이트리스트 (소스 경계 고정)
# -----------------------------------------------------------------------------

SOURCE_WHITELIST: Dict[str, Set[str]] = {
    "Kiwoom": {
        # meta + 가격/거래 + 일부 수급 + (추후) 배당
        "date","code","name","market",
        "open","high","low","close","volume","amount",
        "short_sell_qty","short_sell_amt","loan_balance_qty","loan_balance_amt",
        "ex_div_date","div_amount",
    },
    "DART": {
        # 재무 11종만
        "revenue","op_income","net_income","eps","bps","roe","roa","debt_ratio",
        "cash_flow_op","cash_flow_inv","cash_flow_fin",
    },
    "PyKRX": {
        # 가격/거래 + 보조 펀더멘털(보조; 우선순위에서 뒤)
        "date","code","name","market",
        "open","high","low","close","volume","amount",
        "eps","bps","per","pbr",
    },
    "Yahoo": {
        # 제한적 사용
        "close","volume","sector_name","bps","per",
    },
    "FDR": {
        # 매크로
        "usdkrw","cnykrw","dxy","us10y_yield","kr10y_yield","wti","gold","vix",
    },
    "Naver": set(),  # 상태용, v58 기여 없음
}

MERGE_PRIORITY: List[str] = ["Kiwoom", "DART", "PyKRX", "Yahoo", "FDR", "Naver"]

# -----------------------------------------------------------------------------
# 2) UI 구성 요소
# -----------------------------------------------------------------------------

class FieldCard(QFrame):
    """컬럼 카드(아이콘 + 이름 + 값)"""
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.has_value = False

        self.setObjectName("FieldCard")
        self.setStyleSheet("""
            QFrame#FieldCard { background-color:#1f2937; border:1px solid #334155; border-radius:8px; }
        """)

        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 8, 10, 8)
        lay.setSpacing(8)

        self.icon = QLabel("⚪"); self.icon.setFixedWidth(20)
        self.icon.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.icon.setStyleSheet("font-size:14px;")
        self.name_label = QLabel(name); self.name_label.setWordWrap(True)
        self.name_label.setStyleSheet("color:#e5e7eb; font-weight:700;")
        self.value_label = QLabel("—"); self.value_label.setWordWrap(True)
        self.value_label.setStyleSheet("color:#cbd5e1;")
        self.value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        lay.addWidget(self.icon); lay.addWidget(self.name_label, 1); lay.addWidget(self.value_label, 2)

    def clear(self):
        self.icon.setText("⚪"); self.value_label.setText("—"); self.has_value = False

    def set_value(self, value: Any, src_tag: str = ""):
        if not _is_value_valid(value):
            self.clear(); return
        disp = _fmt_value(value, self.name)
        self.icon.setText("✅")
        self.value_label.setText(f"[{src_tag}] {disp}" if src_tag else disp)
        self.has_value = True

def _is_value_valid(v: Any) -> bool:
    if v is None: return False
    if isinstance(v, str): return bool(v.strip())
    if isinstance(v, (list, dict, tuple, set)): return len(v) > 0
    if isinstance(v, (int, float)):
        try:
            if math.isnan(v) or math.isinf(v): return False
        except Exception: pass
        return True
    try:
        import pandas as pd
        if hasattr(v, 'size') and v.size == 0: return False
        if 'pd' in globals() and pd.isna(v): return False
    except Exception: pass
    return True

def _fmt_value(v: Any, col_name: str) -> str:
    if isinstance(v, float):
        if any(t in col_name for t in ["qty","vol","amt","amount","volume"]): return f"{v:,.0f}"
        return f"{v:,.2f}"
    if isinstance(v, int): return f"{v:,}"
    s = str(v)
    return s if len(s) <= 48 else s[:48] + "..."

# -----------------------------------------------------------------------------
# 3) P0_Index 본체
# -----------------------------------------------------------------------------

class P0_Index(QWidget):
    """
    V58 스펙 검증 및 수집 통합기 (완성본)
    - 23개 수정 사항 반영
    - 소스별 화이트리스트 / 전용 정규화 / 우선순위 병합 / 진단 로그 분리
    """
    def __init__(self):
        super().__init__()
        self.v58_cols = V58_STANDARD_COLUMNS[:]
        self.cards: Dict[str, FieldCard] = {}
        self.last_payloads: Dict[str, Dict[str, Any]] = {}
        self.last_raw_payloads: Dict[str, Dict[str, Any]] = {}
        self._init_ui()

    # ---------------- UI ----------------
    def _init_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(12,8,12,8); root.setSpacing(8)
        header = QHBoxLayout()
        title = QLabel("V58 데이터 수집 통합 분석기 (Final)")
        title.setStyleSheet("font-size:24px; font-weight:800; color:#e5e7eb;")
        header.addWidget(title); header.addStretch(1)
        root.addLayout(header)

        root.addWidget(self._build_controls())

        self.log_area = QTextEdit()
        self.log_area.setPlaceholderText("검증 및 분석 결과가 여기에 표시됩니다...")
        self.log_area.setStyleSheet("""
            QTextEdit { background-color:#0f172a; color:#22d3ee;
                       font-family:Consolas,monospace; font-size:13px; border:1px solid #334155; }""")
        self.log_area.setFixedHeight(220)
        root.addWidget(self.log_area)

        root.addWidget(self._build_field_grid(), 1)

    def _build_controls(self) -> QGroupBox:
        box = QGroupBox("테스트 설정"); lay = QVBoxLayout(box)
        row = QHBoxLayout()
        self.code_edit = QLineEdit("005930"); self.code_edit.setFixedWidth(100)
        self.date_edit = QLineEdit(QDate.currentDate().toString("yyyyMMdd")); self.date_edit.setFixedWidth(100)

        btn_run = QPushButton("🚀 전체 소스 통합 분석 (Waterfall)")
        btn_run.setStyleSheet("background-color:#2563eb; color:white; font-weight:bold; padding:6px 12px; border-radius:4px;")
        btn_run.clicked.connect(self.run_full_analysis)

        btn_save = QPushButton("로그 저장"); btn_save.clicked.connect(self.save_payloads_to_file)
        btn_clear = QPushButton("초기화"); btn_clear.clicked.connect(self.clear_all)

        row.addWidget(QLabel("종목코드:")); row.addWidget(self.code_edit)
        row.addWidget(QLabel("기준일자:")); row.addWidget(self.date_edit)
        row.addStretch(1); row.addWidget(btn_run); row.addWidget(btn_save); row.addWidget(btn_clear)
        lay.addLayout(row)
        return box

    def _build_field_grid(self) -> QScrollArea:
        scroll = QScrollArea(); scroll.setWidgetResizable(True); scroll.setStyleSheet("border:none;")
        container = QWidget(); grid = QGridLayout(container); grid.setSpacing(6)
        self.grid = grid
        cols = 4
        for idx, col in enumerate(self.v58_cols):
            card = FieldCard(col); self.cards[col] = card
            grid.addWidget(card, idx // cols, idx % cols)
        scroll.setWidget(container); return scroll

    def log(self, msg: str): self.log_area.append(msg)

    # ---------------- RUN ----------------
    def run_full_analysis(self):
        self.clear_all()
        code = self.code_edit.text().strip()
        base_dt = self._date8(self.date_edit.text().strip())
        self.log(f"=== [분석 시작] Target: {code}, Date: {base_dt} ===")

        # 1) 수집
        self.last_payloads, self.last_raw_payloads = {}, {}

        self.log(">>> 1. Kiwoom REST 호출")
        k_raw = self._fetch_kiwoom_composite(code, base_dt)
        self.last_raw_payloads["Kiwoom"] = k_raw
        k_norm = normalize_kiwoom(k_raw)
        self.last_payloads["Kiwoom"] = k_norm

        self.log(">>> 2. DART 호출")
        d_raw = self._fetch_dart_composite(code, base_dt)
        self.last_raw_payloads["DART"] = d_raw
        d_norm = normalize_dart(d_raw)
        self.last_payloads["DART"] = d_norm

        self.log(">>> 3. PyKRX 호출")
        p_raw = self._fetch_pykrx_composite(code, base_dt)
        self.last_raw_payloads["PyKRX"] = p_raw
        p_norm = normalize_pykrx(p_raw)
        self.last_payloads["PyKRX"] = p_norm

        self.log(">>> 4. FDR 호출")
        f_raw = self._fetch_fdr_macro(base_dt)
        self.last_raw_payloads["FDR"] = f_raw
        f_norm = normalize_fdr(f_raw)
        self.last_payloads["FDR"] = f_norm

        self.log(">>> 5. Yahoo 호출")
        y_raw = self._fetch_yahoo_composite(code, base_dt)
        self.last_raw_payloads["Yahoo"] = y_raw
        y_norm = normalize_yahoo(y_raw)
        self.last_payloads["Yahoo"] = y_norm

        self.log(">>> 6. Naver 호출")
        n_raw = self._fetch_naver_composite(code)
        self.last_raw_payloads["Naver"] = n_raw
        n_norm = normalize_naver(n_raw)
        self.last_payloads["Naver"] = n_norm

        # 2) 진단
        self.log("\n[ 🔍 소스별 수집 상태 (Normalized) ]")
        for src, data in self.last_payloads.items():
            count = sum(1 for k,v in data.items() if _is_value_valid(v))
            self.log(f" - {src}: {count}개")

        # 3) 병합 (소스 화이트리스트 + 우선순위)
        merged, contributor = merge_sources(self.last_payloads, MERGE_PRIORITY)

        # 4) UI 반영
        filled = 0
        for col in self.v58_cols:
            if col in merged:
                self.cards[col].set_value(merged[col], contributor.get(col,""))
                filled += 1
            else:
                self.cards[col].clear()

        # 5) 결과
        self.log(f"\n>>> 최종 달성률: {filled} / 58 ({(filled/58)*100:.1f}%)")
        missing = [c for c in self.v58_cols if c not in merged]
        if missing: self.log(f"⚠️ 누락: {missing}")
        else: self.log("🎉 58개 전부 충족")

    # ---------------- UTIL ----------------
    def clear_all(self):
        self.log_area.clear()
        for card in self.cards.values(): card.clear()

    def save_payloads_to_file(self):
        if not self.last_payloads:
            QMessageBox.information(self, "저장 불가", "먼저 분석을 실행하세요."); return
        code = self.code_edit.text().strip() or "unknown"
        base_dt = self.date_edit.text().strip() or QDate.currentDate().toString("yyyyMMdd")
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))
        logs_dir = os.path.join(root_dir, "logs"); os.makedirs(logs_dir, exist_ok=True)
        fname = f"p0_dump_{code}_{base_dt}_{int(time.time())}.json"
        path = os.path.join(logs_dir, fname)
        data = {"code":code,"date":base_dt,"normalized":self.last_payloads,"raw":self.last_raw_payloads}
        try:
            with open(path,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)
            self.log(f"[파일 저장] {path}")
            QMessageBox.information(self, "저장 완료", f"로그 저장: {path}")
        except Exception as e:
            QMessageBox.warning(self, "저장 실패", str(e))

    def _date8(self, s: str) -> str:
        t = (s or "").replace("-", "").replace(".", "").strip()
        return t

    # ---------------- FETCHERS ----------------
    def _fetch_kiwoom_composite(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            root_dir = r"F:\autostockG"; api_dir = os.path.join(root_dir,"api","kiwoom_rest")
            if api_dir not in sys.path: sys.path.insert(0, api_dir)
            if root_dir not in sys.path: sys.path.append(root_dir)
            try:
                from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
            except ImportError:
                from kiwoom_api import KiwoomRestApi  # fallback
            api = KiwoomRestApi()

            def _call(api_id, path, body):
                res = api._call_api(api_id, path, body=body)
                self.log(f"   - {api_id} rc={res.get('return_code')} msg={res.get('return_msg')}")
                return res

            def _extract_first(res):
                for key in ("output","chart","data","result","stk_dt_pole_chart_qry"):
                    if key in res:
                        block = res[key]
                        if isinstance(block, list) and block and isinstance(block[0], dict): return block[0]
                        if isinstance(block, dict): return block
                return {k:v for k,v in res.items() if k not in ("return_code","return_msg","response_headers","resp_headers")}

            def _valid(res): return res and isinstance(res, dict) and str(res.get("return_code","")) == "0"

            def attempt(dt_str: str) -> Dict[str, Any]:
                merged = {}
                r1 = _call("ka10081","/api/dostk/chart",{"stk_cd":code,"base_dt":dt_str,"upd_stkpc_tp":"D","term_cnt":"1"})
                if _valid(r1): merged.update(_extract_first(r1))
                r2 = _call("ka10014","/api/dostk/shsa",{"stk_cd":code,"tm_tp":"0","strt_dt":dt_str,"end_dt":dt_str})
                if _valid(r2): merged.update(_extract_first(r2))
                r3 = _call("ka10058","/api/dostk/stkinfo",{"stk_cd":code,"strt_dt":dt_str,"end_dt":dt_str,"trde_tp":"0"})
                if _valid(r3): merged.update(_extract_first(r3))
                r4 = _call("ka10001","/api/dostk/stkinfo",{"stk_cd":code})
                if _valid(r4): merged.update(_extract_first(r4))
                return merged

            merged = attempt(base_dt)
            if not merged:
                import pandas as pd
                prev = (pd.to_datetime(base_dt) - pd.tseries.offsets.BDay(1)).strftime("%Y%m%d")
                self.log(f"   -> Kiwoom 재시도: {prev}")
                merged = attempt(prev)
            return merged
        except Exception as e:
            self.log(f"!! Kiwoom Error: {e}")
            return {}

    def _fetch_dart_composite(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"
            api_key = ""
            if os.path.exists(key_path):
                with open(key_path,"r",encoding="utf-8") as f: api_key = f.readline().strip()
            if not api_key and os.path.exists("opendart_apikey.txt"):
                with open("opendart_apikey.txt","r",encoding="utf-8") as f: api_key = f.readline().strip()
            if not api_key:
                self.log("!! DART API Key 없음"); return {}

            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code); 
            if not corp_code: return {}

            merged = {}
            merged.update(self._fetch_dart_company(api_key, corp_code))
            merged.update(self._fetch_dart_financial(api_key, corp_code, base_dt))
            merged.update(self._fetch_dart_major(api_key, corp_code))
            return merged
        except Exception as e:
            self.log(f"!! DART Error: {e}"); return {}

    def _get_dart_corp_map(self, api_key: str) -> Dict[str,str]:
        cache_path = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
        if os.path.exists(cache_path):
            try:
                mtime = dt.datetime.fromtimestamp(os.path.getmtime(cache_path))
                if (dt.datetime.now() - mtime).days < 1:
                    with open(cache_path,"r",encoding="utf-8") as f: return json.load(f)
            except Exception: pass
        try:
            self.log("DART 기업코드 맵 다운로드 중...")
            url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                zip_path = cache_path + ".zip"
                with open(zip_path,"wb") as f: f.write(resp.content)
                with zipfile.ZipFile(zip_path,"r") as zf:
                    xml_data = zf.read(zf.namelist()[0])
                try: os.remove(zip_path)
                except: pass
                root = ET.fromstring(xml_data); mapping = {}
                for corp in root.iter("list"):
                    stock_code = (corp.findtext("stock_code") or "").strip()
                    corp_code = (corp.findtext("corp_code") or "").strip()
                    if stock_code and corp_code: mapping[stock_code] = corp_code
                with open(cache_path,"w",encoding="utf-8") as f: json.dump(mapping,f,ensure_ascii=False)
                return mapping
        except Exception: pass
        return {}

    def _fetch_dart_company(self, api_key: str, corp_code: str) -> Dict[str,Any]:
        try:
            url = "https://opendart.fss.or.kr/api/company.json"
            js = requests.get(url, params={"crtfc_key":api_key,"corp_code":corp_code}, timeout=6).json()
            return js if js.get("status")=="000" else {}
        except Exception: return {}

    def _fetch_dart_financial(self, api_key: str, corp_code: str, base_dt: str) -> Dict[str,Any]:
        year = int(base_dt[:4]); reprt_codes = ["11011","11014","11012","11013"]
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        merged_fin: Dict[str,Any] = {}
        for y in [year, year-1]:
            for rc in reprt_codes:
                try:
                    params = {"crtfc_key":api_key,"corp_code":corp_code,"bsns_year":str(y),"reprt_code":rc,"fs_div":"CFS"}
                    js = requests.get(url, params=params, timeout=8).json()
                    if js.get("status") == "000":
                        for item in js.get("list", []):
                            acct_nm = item.get("account_nm",""); val_str = (item.get("thstrm_amount","") or "").replace(",","")
                            if val_str and val_str not in ["-",""]:
                                try:
                                    merged_fin[acct_nm] = float(val_str)
                                    if item.get("account_id"):
                                        merged_fin[item["account_id"]] = float(val_str)
                                except Exception: pass
                        if merged_fin: return merged_fin
                except Exception: continue
        return merged_fin

    def _fetch_dart_major(self, api_key: str, corp_code: str) -> Dict[str,Any]:
        try:
            url = "https://opendart.fss.or.kr/api/majorstock.json"
            js = requests.get(url, params={"crtfc_key":api_key,"corp_code":corp_code}, timeout=6).json()
            if js.get("status")=="000": return {"major_holder_count": len(js.get("list",[]))}
        except Exception: pass
        return {}

    def _fetch_pykrx_composite(self, code: str, base_dt: str) -> Dict[str,Any]:
        try:
            from pykrx import stock
            res: Dict[str,Any] = {}
            df = stock.get_market_ohlcv(base_dt, base_dt, code)
            if df is not None and not df.empty:
                d = df.iloc[0].to_dict()
                res.update({k: _to_number(v) for k,v in d.items()})
            df_f = stock.get_market_fundamental(base_dt, base_dt, code)
            if df_f is not None and not df_f.empty:
                d = df_f.iloc[0].to_dict()
                res.update({k: _to_number(v) for k,v in d.items()})
            df_c = stock.get_market_cap(base_dt, base_dt, code)
            if df_c is not None and not df_c.empty:
                d = df_c.iloc[0].to_dict()
                res.update({k: _to_number(v) for k,v in d.items()})
            return res
        except Exception:
            return {}

    def _fetch_fdr_macro(self, base_dt: str) -> Dict[str,Any]:
        try:
            import FinanceDataReader as fdr
            df = fdr.DataReader('USD/KRW', base_dt, base_dt)
            if df is not None and not df.empty:
                return {"usdkrw": _to_number(df.iloc[0]["Close"])}
            return {}
        except Exception: return {}

    def _fetch_yahoo_composite(self, code: str, base_dt: str) -> Dict[str,Any]:
        try:
            import yfinance as yf
            ticker = f"{code}.KS"
            end = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            start = (dt.datetime.strptime(base_dt, "%Y%m%d") - dt.timedelta(days=5)).strftime("%Y-%m-%d")
            yf_obj = yf.Ticker(ticker); hist = yf_obj.history(start=start, end=end)
            res: Dict[str,Any] = {}
            if hist is not None and not hist.empty:
                row = hist.iloc[-1]
                res["close"] = float(row.get("Close"))
                res["volume"] = float(row.get("Volume"))
            info = getattr(yf_obj, "info", None) or {}
            if info:
                if info.get("sector"): res["sector_name"] = info.get("sector")
                if info.get("bookValue") is not None: res["bps"] = info.get("bookValue")
                if info.get("trailingPE") is not None: res["per"] = info.get("trailingPE")
            return res
        except Exception: return {}

    def _fetch_naver_composite(self, code: str) -> Dict[str,Any]:
        try:
            url = f"https://finance.naver.com/item/main.naver?code={code}"
            resp = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=5)
            return {"market_status": "Active"} if resp.status_code == 200 else {}
        except Exception: return {}

# -----------------------------
# 4) 정규화기(소스별 전용) + 병합기
# -----------------------------

import re
_SIGNED_NUM = re.compile(r"^[\+\-]?\d+(?:\.\d+)?$")
_ONLY_SIGNS = re.compile(r"^[\+\-]+$")

def _to_number(x):
    if x is None: return None
    if isinstance(x, (int, float)):
        try:
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)): return None
        except Exception: pass
        return x
    s = str(x).strip().replace(",", "")
    if s.endswith(".") and s[:-1].isdigit(): s = s[:-1]
    if _SIGNED_NUM.match(s):
        try:
            return float(s) if "." in s else int(s)
        except Exception: return None
    return None

def _norm_price_like(v):
    if isinstance(v, (int, float)): return v
    if v is None: return None
    s = str(v).strip().replace(",", "")
    if _SIGNED_NUM.match(s): return _to_number(s)
    digits = re.sub(r"[^\d\.]", "", s)
    return _to_number(digits)

def normalize_kiwoom(raw: Dict[str,Any]) -> Dict[str,Any]:
    if not isinstance(raw, dict): return {}
    out: Dict[str,Any] = {}
    # meta
    if _is_value_valid(raw.get("stk_cd")): out["code"] = str(raw.get("stk_cd")).strip()
    if _is_value_valid(raw.get("stk_nm")): out["name"] = str(raw.get("stk_nm")).strip()
    if _is_value_valid(raw.get("dt")): out["date"] = _date8(str(raw.get("dt")))
    # OHLCV
    for src_key, dst_key in [("open_pric","open"),("high_pric","high"),("low_pric","low"),("cur_prc","close"),("trde_qty","volume")]:
        if _is_value_valid(raw.get(src_key)): out[dst_key] = _norm_price_like(raw.get(src_key))
    if _is_value_valid(raw.get("trde_prica")): out["amount"] = _to_number(str(raw.get("trde_prica")).replace(",",""))
    # hard block fundamentals
    for blocked in ("eps","per","roe","pbr","bps"):
        if blocked in out: del out[blocked]
    # whitelist
    return {k:v for k,v in out.items() if k in SOURCE_WHITELIST["Kiwoom"] and _is_value_valid(v)}

def normalize_pykrx(raw: Dict[str,Any]) -> Dict[str,Any]:
    if not isinstance(raw, dict): return {}
    out: Dict[str,Any] = {}
    # 가격/거래
    mapping = {"시가":"open","고가":"high","저가":"low","종가":"close","거래량":"volume","거래대금":"amount"}
    for k, dst in mapping.items():
        v = raw.get(k); 
        if _is_value_valid(v): out[dst] = _to_number(v)
    # 보조 펀더멘털
    vendor_map = {"EPS":"eps","BPS":"bps","PER":"per","PBR":"pbr"}
    for k, dst in vendor_map.items():
        v = raw.get(k)
        if _is_value_valid(v): out[dst] = _to_number(v)
    return {k:v for k,v in out.items() if k in SOURCE_WHITELIST["PyKRX"] and _is_value_valid(v)}

def normalize_dart(raw: Dict[str,Any]) -> Dict[str,Any]:
    if not isinstance(raw, dict): return {}
    out: Dict[str,Any] = {}
    aliases = {
        "매출액":"revenue", "ifrs-full_Revenue":"revenue",
        "영업이익":"op_income","dart_OperatingIncomeLoss":"op_income",
        "분기순이익":"net_income","ifrs-full_ProfitLoss":"net_income",
        "기본주당이익":"eps","ifrs-full_BasicEarningsLossPerShare":"eps",
        "BPS":"bps","bps":"bps",
        "roe":"roe","ROE":"roe",
        "roa":"roa","ROA":"roa",
        "부채비율":"debt_ratio","debt_ratio":"debt_ratio",
        "영업활동현금흐름":"cash_flow_op","ifrs-full_CashFlowsFromUsedInOperatingActivities":"cash_flow_op",
        "투자활동현금흐름":"cash_flow_inv","ifrs-full_CashFlowsFromUsedInInvestingActivities":"cash_flow_inv",
        "재무활동현금흐름":"cash_flow_fin","ifrs-full_CashFlowsFromUsedInFinancingActivities":"cash_flow_fin",
    }
    for rk, std in aliases.items():
        v = raw.get(rk)
        if _is_value_valid(v): out[std] = _to_number(v)
    return {k:v for k,v in out.items() if k in SOURCE_WHITELIST["DART"] and _is_value_valid(v)}

def normalize_yahoo(raw: Dict[str,Any]) -> Dict[str,Any]:
    if not isinstance(raw, dict): return {}
    out: Dict[str,Any] = {}
    for k in ("close","volume","sector_name","bps","per"):
        v = raw.get(k)
        if _is_value_valid(v): out[k] = _to_number(v) if k in {"close","volume","bps","per"} else v
    return {k:v for k,v in out.items() if k in SOURCE_WHITELIST["Yahoo"] and _is_value_valid(v)}

def normalize_fdr(raw: Dict[str,Any]) -> Dict[str,Any]:
    if not isinstance(raw, dict): return {}
    out: Dict[str,Any] = {}
    for k in SOURCE_WHITELIST["FDR"]:
        if _is_value_valid(raw.get(k)): out[k] = _to_number(raw.get(k))
    return out

def normalize_naver(raw: Dict[str,Any]) -> Dict[str,Any]:
    return {}

def merge_sources(normalized_by_source: Dict[str,Dict[str,Any]], priority: List[str]) -> Tuple[Dict[str,Any], Dict[str,str]]:
    merged: Dict[str,Any] = {}
    contributor: Dict[str,str] = {}
    for col in V58_STANDARD_COLUMNS:
        for src in priority:
            src_map = normalized_by_source.get(src, {})
            if col in src_map and _is_value_valid(src_map[col]):
                if col in SOURCE_WHITELIST.get(src, set()):
                    merged[col] = src_map[col]; contributor[col] = src; break
    return merged, contributor

def _date8(s: str) -> str:
    t = (s or "").replace("-", "").replace(".", "").strip(); return t

# (module end)
