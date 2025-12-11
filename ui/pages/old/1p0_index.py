
# -*- coding: utf-8 -*-
"""
P0 Index — Kiwoom REST 컬럼 점검 전용 페이지 (UI 위젯)
- QWidget 기반 (QStackedWidget.addWidget 가능)
- 단독 실행용 코드 없음 (UI에서만 사용)
- 외부 라이브러리/REST 오류는 UI에 안전하게 표시
"""

from __future__ import annotations

import csv
import json
import os
import zipfile
import datetime as dt
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Tuple

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QFileDialog,
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

# -------------------------------------------------
# FieldCard: 컬럼 하나를 라벨+값으로 표시
# -------------------------------------------------
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

    def set_value(self, value: Any):
        if value in (None, "", [], {}):
            self.icon.setText("⚪")
            self.value_label.setText("—")
            self.has_value = False
        else:
            self.icon.setText("✅")
            self.value_label.setText(str(value))
            self.has_value = True

    def clear(self):
        self.set_value(None)


# -------------------------------------------------
# SourcePanel: 소스별 컬럼 묶음 + 갱신 버튼
# -------------------------------------------------
class SourcePanel(QWidget):
    def __init__(self, title: str, source_id: str, v58_cols: List[str], refresh_cb):
        super().__init__()
        self.source_id = source_id
        self.v58_cols = v58_cols or []
        self.refresh_cb = refresh_cb
        self.cards: Dict[str, FieldCard] = {}
        self.card_order: List[str] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)

        header = QHBoxLayout()
        lbl = QLabel(title)
        lbl.setStyleSheet("font-weight: bold; color: #e5e7eb;")
        self.date_edit = QLineEdit(QDate.currentDate().toString("yyyyMMdd"))
        self.date_edit.setFixedWidth(88)
        self.btn_refresh = QPushButton("갱신")
        self.btn_refresh.setFixedWidth(54)
        self.btn_refresh.clicked.connect(lambda: self.refresh_cb(self.source_id))
        header.addWidget(lbl)
        header.addStretch(1)
        header.addWidget(self.date_edit)
        header.addWidget(self.btn_refresh)
        root.addLayout(header)

        self.status = QLabel("대기")
        self.status.setStyleSheet("color: #93c5fd;")
        root.addWidget(self.status)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: #0b1220; border: 1px solid #1f2937;")
        container = QWidget()
        self.vbox = QVBoxLayout(container)
        self.vbox.setContentsMargins(4, 4, 4, 4)
        self.vbox.setSpacing(4)
        self.vbox.addStretch(1)
        scroll.setWidget(container)
        root.addWidget(scroll, 1)

    def _ensure_card(self, name: str) -> FieldCard:
        if name in self.cards:
            return self.cards[name]
        card = FieldCard(name)
        card.setFixedWidth(220)
        self.cards[name] = card
        # stretch 전 위치에 삽입
        self.vbox.insertWidget(self.vbox.count() - 1, card)
        self.card_order.append(name)
        return card

    def apply_values(self, payload: Dict[str, Any]) -> Tuple[int, int, int]:
        if not isinstance(payload, dict):
            payload = {}
        # 동적 카드
        for k in payload.keys():
            self._ensure_card(str(k))

        filled = 0
        for name, card in self.cards.items():
            if name in payload and payload[name] not in (None, "", [], {}):
                card.set_value(payload[name])
                filled += 1
            else:
                card.clear()

        # v58 집계
        v58_filled = sum(1 for k in self.v58_cols if payload.get(k) not in (None, "", [], {}))
        total = len(self.cards)
        self.status.setText(f"총 {total} / 채움 {filled} / v58 {v58_filled}")
        return total, filled, v58_filled

    def clear(self):
        for c in self.cards.values():
            c.clear()
        self.status.setText(f"총 {len(self.cards)} / 채움 0 / v58 0")


# -------------------------------------------------
# P0_Index: UI 페이지 (QWidget)
# -------------------------------------------------
class P0_Index(QWidget):
    def __init__(self):
        super().__init__()
        self.cards: Dict[str, FieldCard] = {}
        self.panels: Dict[str, SourcePanel] = {}
        self.v58_cols = [
            # Price 12
            "date","code","name","market","open","high","low","close","volume","amount","adj_factor","vwap",
            # Flow 12
            "inst_net_qty","inst_net_amt","frgn_net_qty","frgn_net_amt","nps_net_qty","nps_net_amt",
            "dealer_net_qty","dealer_net_amt","short_sell_qty","short_sell_amt","loan_balance_qty","loan_balance_amt",
            # Finance 11
            "revenue","op_income","net_income","eps","bps","roe","roa","debt_ratio",
            "cash_flow_op","cash_flow_inv","cash_flow_fin",
            # Sector/Theme 5
            "sector_code","sector_name","theme_code","theme_name","sector_index_close",
            # Macro 8
            "usdkrw","cnykrw","dxy","us10y_yield","kr10y_yield","wti","gold","vix",
            # Event 10
            "earnings_announce_date","earnings_surprise","earnings_effective_date",
            "ex_div_date","div_amount",
            "split_announce_date","split_effective_date",
            "rights_issue_announce_date","rights_issue_effective_date",
            "mna_announce_date",
        ]
        self.last_payload: Dict[str, Any] = {}
        self._init_ui()

    # ---------------- UI ----------------
    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setSpacing(8)

        title = QLabel("P0 · Kiwoom REST 컬럼 테스트")
        title.setStyleSheet("font-size: 24px; font-weight: 800; color: #e5e7eb;")
        root.addWidget(title)

        sub = QLabel("REST 응답을 불러 컬럼/데이터를 즉시 점검합니다.")
        sub.setStyleSheet("color: #cbd5e1;")
        root.addWidget(sub)

        root.addWidget(self._build_controls())
        root.addWidget(self._build_sources(), 1)

    def _build_controls(self) -> QGroupBox:
        box = QGroupBox("요청 설정")
        lay = QVBoxLayout(box)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        today = QDate.currentDate().toString("yyyyMMdd")

        self.api_id_edit = QLineEdit("ka10081")
        self.api_id_edit.setFixedWidth(120)
        self.api_path_edit = QLineEdit("/api/dostk/chart")
        self.code_edit = QLineEdit("005930")
        self.code_edit.setFixedWidth(110)
        self.date_edit = QLineEdit(today)
        self.date_edit.setFixedWidth(96)
        self.date_edit.editingFinished.connect(self._propagate_date)

        row1.addWidget(QLabel("API"))
        row1.addWidget(self.api_id_edit)
        row1.addWidget(QLabel("Path"))
        row1.addWidget(self.api_path_edit, 1)
        row1.addWidget(QLabel("종목"))
        row1.addWidget(self.code_edit)
        row1.addWidget(QLabel("일자"))
        row1.addWidget(self.date_edit)
        lay.addLayout(row1)

        self.body_edit = QTextEdit()
        self.body_edit.setFixedHeight(72)
        self.body_edit.setText(f'{{"stk_cd":"005930","base_dt":"{today}","upd_stkpc_tp":"D","term_cnt":"60"}}')
        lay.addWidget(self.body_edit)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        btn_call = QPushButton("REST 호출")
        btn_call.clicked.connect(self.call_rest)
        btn_multi = QPushButton("복합 호출 (81+01+58+14)")
        btn_multi.clicked.connect(self.call_rest_multi)
        btn_json = QPushButton("JSON 파일 불러오기")
        btn_json.clicked.connect(self.load_json)
        btn_clear = QPushButton("필드 초기화")
        btn_clear.clicked.connect(self.clear_all)

        self.status_label = QLabel("대기 중")
        self.status_label.setStyleSheet("color:#93c5fd;")
        self.v58_label = QLabel("v58 매칭: -")
        self.v58_label.setStyleSheet("color:#a5b4fc; font-weight:700;")

        row2.addWidget(btn_call)
        row2.addWidget(btn_multi)
        row2.addWidget(btn_json)
        row2.addWidget(btn_clear)
        row2.addStretch(1)
        row2.addWidget(self.status_label)
        row2.addWidget(self.v58_label)
        lay.addLayout(row2)
        return box

    def _build_sources(self) -> QGroupBox:
        box = QGroupBox("소스 패널")
        box.setStyleSheet("QGroupBox { font-weight: 700; color: #e5e7eb; }")
        h = QHBoxLayout(box)
        h.setContentsMargins(6, 4, 6, 4)
        h.setSpacing(6)

        today = QDate.currentDate().toString("yyyyMMdd")
        self.sources = [
            ("kiwoom", "1) Kiwoom REST", self.v58_cols),
            ("pykrx",  "2) PyKRX", []),
            ("fdr",    "3) FinanceDataReader", []),
            ("yahoo",  "4) Yahoo Finance", []),
            ("dart",   "5) DART", []),
            ("dart_fin","6) DART 재무", []),
            ("macro",  "7) Macro (FDR)", []),
        ]
        for sid, title, v58 in self.sources:
            panel = SourcePanel(title, sid, v58, self.fetch_source)
            panel.date_edit.setText(today)
            self.panels[sid] = panel
            h.addWidget(panel, 1)
        return box

    # ---------------- Fetch helpers ----------------
    def _normalize_date(self, s: str) -> str:
        try:
            d = dt.datetime.strptime(s, "%Y%m%d").date()
        except Exception:
            d = dt.date.today()
        today = dt.date.today()
        if d > today:
            d = today
        if d.weekday() == 5: d = d - dt.timedelta(days=1)  # 토
        elif d.weekday() == 6: d = d - dt.timedelta(days=2)  # 일
        return d.strftime("%Y%m%d")

    def _build_base_body(self) -> Dict[str, Any]:
        try:
            body = json.loads(self.body_edit.toPlainText().strip() or "{}")
        except Exception:
            body = {}
        body.setdefault("stk_cd", self.code_edit.text().strip() or "005930")
        base_dt = self.date_edit.text().strip() or QDate.currentDate().toString("yyyyMMdd")
        body.setdefault("base_dt", self._normalize_date(base_dt))
        return body

    def _extract_payload(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, list):
            return data[0] if data and isinstance(data[0], dict) else {}
        if not isinstance(data, dict):
            return {}
        for key in ("output","data","result","chart","stk_dt_pole_chart_qry"):
            if key in data:
                block = data[key]
                if isinstance(block, list) and block and isinstance(block[0], dict):
                    return block[0]
                if isinstance(block, dict):
                    return block
        return data

    def _merge(self, base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(base, dict): base = {}
        if not isinstance(new, dict): return base
        out = dict(base)
        for k, v in new.items():
            if k not in out or out[k] in (None, "", [], {}):
                out[k] = v
        return out

    def _update_v58_status(self, payload: Dict[str, Any]):
        filled = sum(1 for k in self.v58_cols if payload.get(k) not in (None,"",[],{}))
        present = sum(1 for k in self.v58_cols if k in payload)
        self.v58_label.setText(f"v58 매칭: 채움 {filled}/{len(self.v58_cols)} | 키 존재 {present}")

    # ---------------- Public slots ----------------
    def fetch_source(self, source_id: str):
        code = self.code_edit.text().strip() or "005930"
        base_dt = self._normalize_date(self.panels[source_id].date_edit.text().strip() or self.date_edit.text().strip())
        self.panels[source_id].date_edit.setText(base_dt)
        panel = self.panels[source_id]
        panel.status.setText("요청 중...")

        try:
            if source_id == "kiwoom":
                self.call_rest_multi(base_dt_override=base_dt)
                panel.status.setText("갱신 완료")
                return
            if source_id == "pykrx":
                payload = self._fetch_pykrx(code, base_dt)
                payload = self._merge(payload, self._fetch_pykrx_fundamental(code, base_dt))
            elif source_id == "fdr":
                payload = self._fetch_fdr(code, base_dt)
            elif source_id == "yahoo":
                payload = self._fetch_yahoo(code, base_dt)
            elif source_id == "dart":
                payload = self._merge(self._fetch_dart(code), self._fetch_dart_major(code))
            elif source_id == "dart_fin":
                payload = self._fetch_dart_financial(code, base_dt)
            elif source_id == "macro":
                payload = self._fetch_macro(base_dt)
            else:
                payload = {}
            panel.apply_values(payload or {})
        except Exception as e:
            panel.status.setText(f"오류: {e}")

    def call_rest(self):
        api_id = (self.api_id_edit.text().strip() or "ka10081")
        path = (self.api_path_edit.text().strip() or "/api/dostk/chart")
        body = self._build_base_body()
        if api_id == "ka10081":
            body.setdefault("term_cnt", "60")
            body.setdefault("upd_stkpc_tp", "D")
        try:
            from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
            api = KiwoomRestApi()
            resp = api._call_api(api_id=api_id, url_path=path, body=body)
            payload = self._extract_payload(resp)
            if api_id == "ka10081" and not payload:
                fb = dict(body); fb["upd_stkpc_tp"] = "1"
                resp = api._call_api(api_id=api_id, url_path=path, body=fb)
                payload = self._extract_payload(resp)
        except Exception as e:
            QMessageBox.critical(self, "REST 호출 실패", str(e))
            self.status_label.setText("호출 실패")
            return

        if not payload:
            msg = (resp.get("return_msg") if isinstance(resp, dict) else "") or "표시할 데이터 없음"
            QMessageBox.information(self, "결과 없음", msg)
            self.clear_all()
            return

        self._apply_payload(payload)
        rc = (resp.get("return_code") if isinstance(resp, dict) else "?")
        rm = (resp.get("return_msg") if isinstance(resp, dict) else "")
        self.status_label.setText(f"return_code={rc} {rm} | 키 {len(self.last_payload)} / 채움 {sum(1 for v in self.last_payload.values() if v not in (None,'',[],{}))}")

    def call_rest_multi(self, base_dt_override: str | None = None):
        try:
            from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
        except Exception as e:
            QMessageBox.critical(self, "REST 호출 실패", str(e))
            self.status_label.setText("호출 실패")
            return
        base = self._build_base_body()
        if base_dt_override:
            base["base_dt"] = base_dt_override
        stk = base.get("stk_cd","005930")
        bas = base.get("base_dt", QDate.currentDate().toString("yyyyMMdd"))
        jobs = [
            ("ka10081","/api/dostk/chart",{**base,"term_cnt":"60","upd_stkpc_tp": base.get("upd_stkpc_tp","D")}),
            ("ka10001","/api/dostk/stkinfo",{"stk_cd": stk}),
            ("ka10058","/api/dostk/stkinfo",{"stk_cd": stk, "strt_dt": bas, "end_dt": bas}),
            ("ka10014","/api/dostk/stkinfo",{"stk_cd": stk, "strt_dt": bas, "end_dt": bas}),
        ]
        api = KiwoomRestApi()
        merged: Dict[str, Any] = {}
        for api_id, path, body in jobs:
            try:
                resp = api._call_api(api_id=api_id, url_path=path, body=body)
                payload = self._extract_payload(resp)
                if api_id == "ka10081" and not payload:
                    fb = dict(body); fb["upd_stkpc_tp"] = "1"
                    resp = api._call_api(api_id=api_id, url_path=path, body=fb)
                    payload = self._extract_payload(resp)
            except Exception as e:
                self.status_label.setText(f"{api_id} 오류: {e}")
                continue
            merged = self._merge(merged, payload)

        if not merged:
            QMessageBox.information(self, "결과 없음", "표시할 데이터가 없습니다.")
            self.clear_all()
            return

        self._apply_payload(merged)
        self.status_label.setText(f"복합 호출 완료 | 키 {len(self.last_payload)} / 채움 {sum(1 for v in self.last_payload.values() if v not in (None,'',[],{}))}")

    # ---------------- File/JSON ----------------
    def load_json(self):
        path, _ = QFileDialog.getOpenFileName(self, "JSON 파일 선택", "", "JSON Files (*.json);;All Files (*)")
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            QMessageBox.warning(self, "파일 오류", f"JSON 파일을 읽을 수 없습니다:\n{e}")
            return
        payload = self._extract_payload(data)
        if not payload:
            QMessageBox.information(self, "결과 없음", "표시할 컬럼이 없습니다.")
            self.clear_all()
            return
        self._apply_payload(payload)
        self.status_label.setText(f"파일 로드: {os.path.basename(path)} | 키 {len(payload)}개")

    # ---------------- Apply/Clear ----------------
    def _apply_payload(self, payload: Dict[str, Any]):
        # 기본(kiwoom) 패널에 적용
        if "kiwoom" in self.panels:
            self.panels["kiwoom"].apply_values(payload or {})
        self.last_payload = dict(payload or {})
        self._update_v58_status(self.last_payload)

    def clear_all(self):
        for p in self.panels.values():
            p.clear()
        self.status_label.setText("초기화 완료")
        self.v58_label.setText("v58 매칭: -")

    # ---------------- External fetchers ----------------
    def _fetch_pykrx(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            from pykrx import stock
            df = stock.get_market_ohlcv(base_dt, base_dt, code)
            if df is None or df.empty:
                return {"return_msg":"pykrx 응답 없음"}
            row = df.iloc[0].to_dict()
            row["dt"] = base_dt
            return {str(k): v for k, v in row.items()}
        except Exception as e:
            return {"return_msg": f"pykrx 오류: {e}"}

    def _fetch_pykrx_fundamental(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            from pykrx import stock
            df = stock.get_market_fundamental(base_dt, base_dt, code)
            if df is None or df.empty:
                return {}
            row = df.iloc[0].to_dict()
            row["dt_fund"] = base_dt
            return {str(k): v for k, v in row.items()}
        except Exception:
            return {}

    def _fetch_fdr(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            import FinanceDataReader as fdr
            s = f"{base_dt[:4]}-{base_dt[4:6]}-{base_dt[6:]}"
            e = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            df = fdr.DataReader(code, s, e)
            if df is None or df.empty:
                return {"return_msg":"FDR 응답 없음"}
            row = df.iloc[0].to_dict()
            row["dt"] = base_dt
            return {str(k): v for k, v in row.items()}
        except Exception as e:
            return {"return_msg": f"FDR 오류: {e}"}

    def _fetch_yahoo(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            import yfinance as yf
            t = code if (code.endswith(".KS") or code.endswith(".KQ")) else code+".KS"
            s = f"{base_dt[:4]}-{base_dt[4:6]}-{base_dt[6:]}"
            e = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            data = yf.Ticker(t).history(start=s, end=e)
            if data is None or data.empty:
                return {"return_msg":"yahoo 응답 없음"}
            row = data.iloc[0].to_dict()
            row["date"] = base_dt
            return {str(k): v for k, v in row.items()}
        except Exception as e:
            return {"return_msg": f"yahoo 오류: {e}"}

    def _fetch_macro(self, base_dt: str) -> Dict[str, Any]:
        try:
            import FinanceDataReader as fdr
            base = dt.datetime.strptime(base_dt, "%Y%m%d").date()
            for off in range(0, 5):
                d = base - dt.timedelta(days=off)
                fmt = d.strftime("%Y-%m-%d")
                kospi = fdr.DataReader("KS11", fmt, fmt)
                usd   = fdr.DataReader("USD/KRW", fmt, fmt)
                def last_close(df):
                    if df is None or df.empty: return None
                    return float(df.iloc[-1]["Close"]) if "Close" in df.columns else None
                k1, k2 = last_close(kospi), last_close(usd)
                if any(v is not None for v in (k1, k2)):
                    return {"dt": d.strftime("%Y%m%d"), "kospi": k1, "usdkrw": k2}
            return {"return_msg":"macro 없음"}
        except Exception as e:
            return {"return_msg": f"macro 오류: {e}"}

    def _load_dart_key(self) -> str:
        key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"
        if not os.path.exists(key_path):
            raise FileNotFoundError("dart 키 파일 없음")
        with open(key_path, "r", encoding="utf-8") as f:
            key = f.readline().strip()
        if not key:
            raise ValueError("dart 키 없음")
        return key

    def _get_dart_corp_map(self, api_key: str) -> Dict[str, str]:
        cache = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
        if os.path.exists(cache):
            try:
                with open(cache, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        import requests
        url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        tmp = cache + ".zip"
        with open(tmp, "wb") as f:
            f.write(r.content)
        with zipfile.ZipFile(tmp, "r") as zf:
            xml_name = zf.namelist()[0]
            xml_data = zf.read(xml_name)
        os.remove(tmp)
        root = ET.fromstring(xml_data)
        mp = {}
        for corp in root.iter("list"):
            stock_code = (corp.findtext("stock_code") or "").strip()
            corp_code  = (corp.findtext("corp_code") or "").strip()
            if stock_code and corp_code:
                mp[stock_code] = corp_code
        with open(cache, "w", encoding="utf-8") as f:
            json.dump(mp, f, ensure_ascii=False)
        return mp

    def _fetch_dart(self, code: str) -> Dict[str, Any]:
        try:
            key = self._load_dart_key()
            mp = self._get_dart_corp_map(key)
            corp = mp.get(code)
            if not corp:
                return {"return_msg":"corp_code 없음", "stock_code": code}
            import requests
            url = "https://opendart.fss.or.kr/api/company.json"
            r = requests.get(url, params={"crtfc_key": key, "corp_code": corp}, timeout=5)
            data = r.json()
            if data.get("status") not in ("000","013"):
                return {"return_msg": data.get("message","오류")}
            return {
                "corp_code": corp,
                "corp_name": data.get("corp_name"),
                "corp_cls": data.get("corp_cls"),
                "hm_url": data.get("hm_url"),
            }
        except Exception:
            return {"return_msg":"dart 오류"}

    def _fetch_dart_major(self, code: str) -> Dict[str, Any]:
        try:
            key = self._load_dart_key()
            mp = self._get_dart_corp_map(key)
            corp = mp.get(code)
            if not corp:
                return {}
            import requests
            url = "https://opendart.fss.or.kr/api/majorstock.json"
            r = requests.get(url, params={"crtfc_key": key, "corp_code": corp}, timeout=5)
            if r.status_code != 200:
                return {}
            js = r.json()
            if js.get("status") not in ("000","013"):
                return {}
            lst = js.get("list") or []
            return {"majorstock_count": len(lst)}
        except Exception:
            return {}

    def _fetch_dart_financial(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            key = self._load_dart_key()
            mp = self._get_dart_corp_map(key)
            corp = mp.get(code)
            if not corp:
                return {"return_msg":"corp_code 없음"}
            import requests
            year = int(base_dt[:4])
            for y in (year, year-1):
                for reprt_code in ("11011","11012"):  # 사업/반기
                    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                    params = {"crtfc_key": key, "corp_code": corp, "bsns_year": str(y), "reprt_code": reprt_code}
                    r = requests.get(url, params=params, timeout=5)
                    if r.status_code != 200:
                        continue
                    js = r.json()
                    if js.get("status") not in ("000","013"):
                        continue
                    lst = js.get("list") or []
                    if lst:
                        out = {"bsns_year": str(y), "reprt_code": reprt_code}
                        for row in lst:
                            acc = row.get("account_id") or row.get("account_nm")
                            amt = row.get("thstrm_amount")
                            if acc:
                                out[acc] = amt
                        return out
            return {"return_msg":"재무 데이터 없음"}
        except Exception:
            return {"return_msg":"dart_fin 오류"}

    # ---------------- Utilities ----------------
    def _propagate_date(self):
        val = self.date_edit.text().strip()
        if not val:
            return
        for p in self.panels.values():
            p.date_edit.setText(val)
