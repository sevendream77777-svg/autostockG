import csv
import json
import os
import zipfile
import datetime as dt
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List

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


class SourcePanel(QWidget):
    """소스별 컬럼을 세로로 모아두는 패널."""

    def __init__(self, title: str, source_id: str, columns: List[str], fetch_fn, default_date: str, v58_cols: List[str]):
        super().__init__()
        self.source_id = source_id
        self.columns = columns
        self.fetch_fn = fetch_fn
        self.v58_cols = v58_cols or []
        self.cards: Dict[str, FieldCard] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)

        header = QHBoxLayout()
        lbl = QLabel(title)
        lbl.setStyleSheet("font-weight: bold; color: #e5e7eb;")
        self.date_edit = QLineEdit(default_date)
        self.date_edit.setFixedWidth(80)
        self.date_edit.setPlaceholderText("yyyymmdd")
        self.btn_refresh = QPushButton("갱신")
        self.btn_refresh.setFixedWidth(50)
        self.btn_refresh.clicked.connect(self.fetch_fn)
        header.addWidget(lbl)
        header.addWidget(self.date_edit)
        header.addStretch(1)
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
        self.card_order: List[str] = []
        for c in columns:
            self._ensure_card(c)
        self.vbox.addStretch(1)
        scroll.setWidget(container)
        root.addWidget(scroll, 1)

    def _ensure_card(self, name: str) -> FieldCard:
        if name in self.cards:
            return self.cards[name]
        card = FieldCard(name)
        card.setFixedWidth(220)
        self.cards[name] = card
        self.card_order.append(name)
        # add before stretch (stretch at last item)
        self.vbox.insertWidget(self.vbox.count() - 1, card)
        return card

    def _reorder_cards(self):
        # sort by has_value desc, then name
        sorted_names = sorted(self.card_order, key=lambda n: (0 if self.cards[n].has_value else 1, n))
        for i in range(self.vbox.count() - 1):  # last item is stretch
            item = self.vbox.itemAt(i)
            if item and item.widget():
                self.vbox.removeWidget(item.widget())
        for name in sorted_names:
            self.vbox.insertWidget(self.vbox.count() - 1, self.cards[name])
        self.card_order = sorted_names

    def apply_values(self, payload: Dict[str, Any]):
        if not isinstance(payload, dict):
            payload = {}
        # 동적 카드 생성
        for k in payload.keys():
            self._ensure_card(str(k))

        filled_cards = 0
        for name in list(self.cards.keys()):
            card = self.cards[name]
            if name in payload and payload[name] not in (None, "", [], {}):
                card.set_value(payload[name])
                filled_cards += 1
            else:
                card.clear()

        self._reorder_cards()

        total_cards = len(self.cards)
        v58_filled = sum(1 for k in self.v58_cols if k in payload and payload.get(k) not in (None, "", [], {}))
        self.status.setText(f"총 {total_cards} / 채움 {filled_cards} / v58 {v58_filled}")
        return total_cards, filled_cards, v58_filled

    def clear(self):
        for card in self.cards.values():
            card.clear()
        total_cards = len(self.cards)
        self.status.setText(f"총 {total_cards} / 채움 0 / v58 0")


class P0_Index(QWidget):
    """P0을 REST 컬럼 테스트 전용 페이지로 사용."""

    def __init__(self):
        super().__init__()
        self.cards: Dict[str, FieldCard] = {}  # legacy grid용 (일부 함수 호환)
        self.known_fields = self._load_known_fields()
        self.field_order = list(self.known_fields)
        # v58 고정 58개 (스펙 파일 기준)
        self.v58_cols = [
            # Price 12
            "date", "code", "name", "market", "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap",
            # Flow 12
            "inst_net_qty", "inst_net_amt", "frgn_net_qty", "frgn_net_amt", "nps_net_qty", "nps_net_amt",
            "dealer_net_qty", "dealer_net_amt", "short_sell_qty", "short_sell_amt", "loan_balance_qty", "loan_balance_amt",
            # Finance 11
            "revenue", "op_income", "net_income", "eps", "bps", "roe", "roa", "debt_ratio",
            "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
            # Sector/Theme 5
            "sector_code", "sector_name", "theme_code", "theme_name", "sector_index_close",
            # Macro 8
            "usdkrw", "cnykrw", "dxy", "us10y_yield", "kr10y_yield", "wti", "gold", "vix",
            # Event 10
            "earnings_announce_date", "earnings_surprise", "earnings_effective_date",
            "ex_div_date", "div_amount",
            "split_announce_date", "split_effective_date",
            "rights_issue_announce_date", "rights_issue_effective_date",
            "mna_announce_date",
        ]
        self.last_payload: Dict[str, Any] = {}
        self.panels: Dict[str, SourcePanel] = {}
        self._init_ui()

    # ------------------------------------------------------------------
    # UI 빌드
    # ------------------------------------------------------------------
    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setSpacing(8)

        header = QLabel("P0 · Kiwoom REST 컬럼 테스트")
        header.setStyleSheet("font-size: 26px; font-weight: 800; color: #e5e7eb;")
        root.addWidget(header)

        sub = QLabel("REST 응답을 불러서 컬럼명/데이터만 빠르게 확인하는 테스트 화면입니다.")
        sub.setStyleSheet("color: #cbd5e1;")
        root.addWidget(sub)

        controls = self._build_controls()
        root.addWidget(controls)

        fields_box = self._build_field_grid()
        root.addWidget(fields_box, 1)

    def _build_controls(self) -> QGroupBox:
        box = QGroupBox("데이터 가져오기")
        lay = QVBoxLayout(box)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        today_str = QDate.currentDate().toString("yyyyMMdd")

        self.api_id_edit = QLineEdit("ka10081")
        self.api_id_edit.setPlaceholderText("API ID (예: ka10081)")
        self.api_id_edit.setFixedWidth(120)

        self.api_path_edit = QLineEdit("/api/dostk/chart")
        self.api_path_edit.setPlaceholderText("URL 경로")

        self.code_edit = QLineEdit("005930")
        self.code_edit.setPlaceholderText("종목코드")
        self.code_edit.setFixedWidth(110)

        self.date_edit = QLineEdit(today_str)
        self.date_edit.setPlaceholderText("기준일자")
        self.date_edit.setFixedWidth(100)
        self.date_edit.editingFinished.connect(self._propagate_date_to_panels)

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
        self.body_edit.setPlaceholderText('JSON Body (예: {"stk_cd":"005930","base_dt":"20250102","upd_stkpc_tp":"D"})')
        self.body_edit.setFixedHeight(70)
        self.body_edit.setText(f'{{"stk_cd":"005930","base_dt":"{today_str}","upd_stkpc_tp":"D","term_cnt":"60"}}')
        lay.addWidget(self.body_edit)

        row2 = QHBoxLayout()
        row2.setSpacing(8)

        btn_call = QPushButton("REST 호출")
        btn_call.clicked.connect(self.call_rest)
        btn_multi = QPushButton("복합 호출 (81+01+58+14)")
        btn_multi.clicked.connect(self.call_rest_multi)
        btn_file = QPushButton("JSON 파일 불러오기")
        btn_file.clicked.connect(self.load_json_file)
        btn_clear = QPushButton("필드 초기화")
        btn_clear.clicked.connect(self.clear_values)

        self.status_label = QLabel("대기 중")
        self.status_label.setStyleSheet("color: #93c5fd;")

        self.v58_label = QLabel("v58 매칭: -")
        self.v58_label.setStyleSheet("color: #a5b4fc; font-weight: bold;")

        row2.addWidget(btn_call)
        row2.addWidget(btn_multi)
        row2.addWidget(btn_file)
        row2.addWidget(btn_clear)
        row2.addStretch(1)
        row2.addWidget(self.status_label)
        row2.addWidget(self.v58_label)
        lay.addLayout(row2)

        return box

    def _build_field_grid(self) -> QGroupBox:
        box = QGroupBox(f"확인 대상 컬럼 ({len(self.known_fields)}개)")
        box.setStyleSheet("QGroupBox { font-weight: bold; color: #e5e7eb; }")
        hbox = QHBoxLayout(box)
        hbox.setContentsMargins(6, 4, 6, 4)
        hbox.setSpacing(6)

        # 소스 정의
        self.sources = [
            ("kiwoom", "1) Kiwoom REST", self.v58_cols),
            ("pykrx", "2) PyKRX", []),
            ("fdr", "3) FinanceDataReader", []),
            ("yahoo", "4) Yahoo Finance", []),
            ("dart", "5) DART", []),
            ("dart_fin", "6) DART 재무", []),
            ("macro", "7) Macro (FDR)", []),
        ]

        today_str = QDate.currentDate().toString("yyyyMMdd")
        for sid, title, cols in self.sources:
            panel = SourcePanel(title, sid, cols, lambda s=sid: self.fetch_source(s), default_date=today_str, v58_cols=self.v58_cols)
            self.panels[sid] = panel
            hbox.addWidget(panel, 1)
            # 버튼을 직접 P0 슬롯에 연결 (신호 확인 용)
            panel.btn_refresh.clicked.connect(lambda _, s=sid: self.fetch_source(s))
        return box

    # ------------------------------------------------------------------
    # 데이터 처리
    # ------------------------------------------------------------------
    def _load_known_fields(self) -> List[str]:
        """현시점 확인 가능한 컬럼 목록을 모두 합쳐서 만듭니다."""
        fields: List[str] = []
        seen = set()

        def add_many(items: Iterable[str]):
            for item in items:
                name = str(item).strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                fields.append(name)

        # 1) v58 기준 컬럼(고정)
        v58 = [
            "dt", "open", "high", "low", "close", "volume", "amount", "pred_pre", "fluc_rt",
            "per", "eps", "pbr", "bps", "roe", "ev", "sale_amt", "bus_pro", "cup_nga",
            "short_qty", "short_amt", "short_cover_qty", "short_cover_amt",
            "loan_balance_qty", "loan_balance_amt", "loan_rt",
            "psn_net_buy", "frg_net_buy", "ins_net_buy",
            "finc_inv", "insur", "trust", "etc_fin", "bank", "pension",
            "pvt_eq", "nation", "etc_corp", "frg_etc", "poss_stkcnt",
            "macro_kospi", "macro_kosdaq", "macro_fx", "macro_rate",
            "event_dividend", "event_split", "event_merger", "event_news",
            "extra_1", "extra_2", "extra_3", "extra_4", "extra_5",
            "extra_6", "extra_7", "extra_8", "extra_9", "extra_10",
        ]
        add_many(v58)

        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        # 2) XLSX에서 추출한 전체 필드 목록
        txt_path = os.path.join(root_dir, "newroomdata", "kiwoom_rest_fields_from_xlsx.txt")
        add_many(self._read_lines(txt_path))

        # 3) 최대 데이터 컬럼 정보(csv)
        max_info = os.path.join(root_dir, "api", "kiwoom_rest", "max_data_columns_info.csv")
        add_many(self._read_csv_column(max_info))

        # 4) 최신 추린 컬럼(csv)
        rel_cols = os.path.join(root_dir, "api", "kiwoom_rest", "updated_relevant_columns.csv")
        add_many(self._read_csv_column(rel_cols))

        return fields

    def _read_lines(self, path: str) -> List[str]:
        if not os.path.exists(path):
            return []
        out: List[str] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                item = line.strip()
                if item.startswith("- "):
                    item = item[2:].strip()
                if item:
                    out.append(item)
        return out

    def _merge_payloads(self, base: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """기존 값이 없을 때만 새 값을 채워넣기."""
        if not isinstance(base, dict):
            base = {}
        if not isinstance(new, dict):
            return base
        merged = dict(base)
        for k, v in new.items():
            if k not in merged or merged[k] in (None, "", [], {}):
                merged[k] = v
        return merged

    def _update_v58_status(self, payload: Dict[str, Any]):
        filled = sum(1 for k in self.v58_cols if payload.get(k) not in (None, "", [], {}))
        present = sum(1 for k in self.v58_cols if k in payload)
        self.v58_label.setText(f"v58 매칭: 채움 {filled}/{len(self.v58_cols)} | 키 존재 {present}")

    # ------------------------------------------------------------------
    # 외부 소스 페치
    # ------------------------------------------------------------------
    def _fetch_pykrx(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            from pykrx import stock
            # 일부 pykrx 버전은 market 인자를 받지 않음 -> 기본 전체 시장 조회
            df = stock.get_market_ohlcv(base_dt, base_dt, code)
            if df is None or df.empty:
                return {"return_msg": "pykrx 응답 없음"}
            row = df.iloc[0].to_dict()
            # 컬럼명 통일 시도
            row["dt"] = base_dt
            return row
        except ImportError:
            return {"return_msg": "pykrx 패키지 없음"}
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
            return row
        except Exception:
            return {}

    def _fetch_fdr(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            import FinanceDataReader as fdr
            start_fmt = f"{base_dt[:4]}-{base_dt[4:6]}-{base_dt[6:]}"
            end_fmt = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            df = fdr.DataReader(code, start_fmt, end_fmt)
            if df is None or df.empty:
                return {"return_msg": "FDR 응답 없음"}
            row = df.iloc[0].to_dict()
            row["dt"] = base_dt
            return row
        except ImportError:
            return {"return_msg": "FDR 패키지 없음"}
        except Exception:
            return {"return_msg": "FDR 오류"}

    def _fetch_yahoo(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            import yfinance as yf
            ticker = code
            if not ticker.endswith(".KS") and not ticker.endswith(".KQ"):
                ticker = code + ".KS"
            start = f"{base_dt[:4]}-{base_dt[4:6]}-{base_dt[6:]}"
            end_dt = (dt.datetime.strptime(base_dt, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            data = yf.Ticker(ticker).history(start=start, end=end_dt)
            if data is None or data.empty:
                return {}
            row = data.iloc[0].to_dict()
            tkr = yf.Ticker(ticker)
            info = {}
            try:
                info = tkr.fast_info if hasattr(tkr, "fast_info") else {}
            except Exception:
                info = {}
            try:
                info_full = tkr.get_info()
                if isinstance(info_full, dict):
                    info.update(info_full)
            except Exception:
                pass
            row_out = {k: float(v) if isinstance(v, (int, float)) else v for k, v in row.items()}
            row_out["date"] = base_dt
            if info:
                for k, v in info.items():
                    row_out[k] = v
            return row_out
        except ImportError:
            return {"return_msg": "yfinance 패키지 없음"}
        except Exception:
            return {"return_msg": "yahoo 오류"}

    def _fetch_dart(self, code: str) -> Dict[str, Any]:
        try:
            api_key = self._load_dart_key()
            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code:
                return {"return_msg": "corp_code 없음 (코드/키 확인)", "stock_code": code}
            info = self._fetch_dart_company(api_key, corp_code)
            info["corp_code"] = corp_code
            info["stock_code"] = code
            return info
        except Exception:
            return {"return_msg": "dart 호출 오류"}

    def _fetch_dart_financial(self, code: str, base_dt: str) -> Dict[str, Any]:
        try:
            api_key = self._load_dart_key()
            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code:
                return {"return_msg": "corp_code 없음", "stock_code": code}
            import requests
            reprt_codes = ["11011", "11012"]  # 사업/반기만 최소 조회
            year = int(base_dt[:4])
            for y in (year, year - 1):  # 현재/이전 연도만 시도(속도 개선)
                for reprt_code in reprt_codes:
                    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                    params = {
                        "crtfc_key": api_key,
                        "corp_code": corp_code,
                        "bsns_year": str(y),
                        "reprt_code": reprt_code,
                    }
                    r = requests.get(url, params=params, timeout=5)
                    if r.status_code != 200:
                        continue
                    js = r.json()
                    if js.get("status") not in ("000", "013"):
                        continue
                    list_data = js.get("list") or []
                    out = {"bsns_year": str(y), "reprt_code": reprt_code}
                    for row in list_data:
                        acc_id = row.get("account_id") or row.get("account_nm") or ""
                        amount = row.get("thstrm_amount")
                        if acc_id:
                            out[acc_id] = amount
                    if len(out) > 2:  # something filled
                        return out
            return {"return_msg": "재무 데이터 없음(최근 2개 연도/사업·반기 조회)"}
        except Exception:
            return {"return_msg": "dart_fin 오류"}

    def _fetch_dart_major(self, code: str) -> Dict[str, Any]:
        """주요주주/상세 지분 등 추가 정보 시도."""
        try:
            api_key = self._load_dart_key()
            corp_map = self._get_dart_corp_map(api_key)
            corp_code = corp_map.get(code)
            if not corp_code:
                return {}
            import requests
            out: Dict[str, Any] = {}
            # 주요주주
            try:
                url = "https://opendart.fss.or.kr/api/majorstock.json"
                params = {"crtfc_key": api_key, "corp_code": corp_code}
                r = requests.get(url, params=params, timeout=5)
                if r.status_code == 200:
                    js = r.json()
                    if js.get("status") in ("000", "013"):
                        lst = js.get("list") or []
                        out["majorstock_count"] = len(lst)
            except Exception:
                pass
            # 경영진/임원 지분 (간단 호출)
            try:
                url = "https://opendart.fss.or.kr/api/elst.json"
                params = {"crtfc_key": api_key, "corp_code": corp_code}
                r = requests.get(url, params=params, timeout=5)
                if r.status_code == 200:
                    js = r.json()
                    if js.get("status") in ("000", "013"):
                        out["executive_list"] = js.get("list")
            except Exception:
                pass
            return out
        except Exception:
            return {}

    def _fetch_macro(self, base_dt: str) -> Dict[str, Any]:
        try:
            import FinanceDataReader as fdr
            base = dt.datetime.strptime(base_dt, "%Y%m%d").date()
            for offset in range(0, 5):
                d = base - dt.timedelta(days=offset)
                dt_fmt = d.strftime("%Y-%m-%d")
                kospi = fdr.DataReader("KS11", dt_fmt, dt_fmt)
                kosdaq = fdr.DataReader("KQ11", dt_fmt, dt_fmt)
                usd = fdr.DataReader("USD/KRW", dt_fmt, dt_fmt)
                def last_close(df):
                    if df is None or df.empty:
                        return None
                    return float(df.iloc[-1]["Close"]) if "Close" in df.columns else None
                k1, k2, k3 = last_close(kospi), last_close(kosdaq), last_close(usd)
                if any(v is not None for v in (k1, k2, k3)):
                    return {
                        "dt": d.strftime("%Y%m%d"),
                        "kospi": k1,
                        "kosdaq": k2,
                        "usdkrw": k3,
                    }
            return {"return_msg": "macro 데이터 없음"}
        except Exception:
            return {"return_msg": "macro 오류"}

    # ------------------------------------------------------------------
    # DART Helpers
    # ------------------------------------------------------------------
    def _load_dart_key(self) -> str:
        key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\opendart_apikey.txt"
        if not os.path.exists(key_path):
            raise FileNotFoundError("dart 키 파일 없음")
        with open(key_path, "r", encoding="utf-8") as f:
            api_key = f.readline().strip()
        if not api_key:
            raise ValueError("dart 키 없음")
        return api_key

    def _get_dart_corp_map(self, api_key: str) -> Dict[str, str]:
        """stock_code -> corp_code 매핑 (1일 캐시)."""
        cache_path = os.path.join(os.path.expanduser("~"), ".dart_corp_map.json")
        need_fetch = True
        if os.path.exists(cache_path):
            mtime = dt.datetime.fromtimestamp(os.path.getmtime(cache_path))
            if (dt.datetime.now() - mtime).days < 1:
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    need_fetch = True
        if need_fetch:
            url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
            import requests
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            zip_path = cache_path + ".zip"
            with open(zip_path, "wb") as f:
                f.write(resp.content)
            with zipfile.ZipFile(zip_path, "r") as zf:
                xml_name = zf.namelist()[0]
                xml_data = zf.read(xml_name)
            os.remove(zip_path)
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
        return {}

    def _fetch_dart_company(self, api_key: str, corp_code: str) -> Dict[str, Any]:
        import requests
        url = "https://opendart.fss.or.kr/api/company.json"
        resp = requests.get(url, params={"crtfc_key": api_key, "corp_code": corp_code}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "013" and data.get("status") != "000":
            return {"return_msg": data.get("message")}
        return {
            "corp_name": data.get("corp_name"),
            "ceo_nm": data.get("ceo_nm"),
            "corp_cls": data.get("corp_cls"),
            "jurir_no": data.get("jurir_no"),
            "bizr_no": data.get("bizr_no"),
            "adres": data.get("adres"),
            "hm_url": data.get("hm_url"),
            "ir_url": data.get("ir_url"),
            "phn_no": data.get("phn_no"),
            "fax_no": data.get("fax_no"),
        }
    def _read_csv_column(self, path: str) -> List[str]:
        if not os.path.exists(path):
            return []
        out: List[str] = []
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            col_idx = 0
            if header:
                lowered = [h.lower() for h in header]
                for idx, name in enumerate(lowered):
                    if "column" in name:
                        col_idx = idx
                        break
            for row in reader:
                if len(row) > col_idx:
                    out.append(row[col_idx].strip())
        return out

    def _extract_payload(self, data: Any) -> Dict[str, Any]:
        """REST 응답에서 첫 행 딕셔너리만 추출."""
        # 리스트 전체가 응답일 때
        if isinstance(data, list):
            return data[0] if data and isinstance(data[0], dict) else {}
        if not isinstance(data, dict):
            return {}
        for key in ("output", "data", "result", "chart", "stk_dt_pole_chart_qry"):
            if key in data:
                block = data[key]
                if isinstance(block, list) and block and isinstance(block[0], dict):
                    return block[0]
                if isinstance(block, dict):
                    return block
        return data

    def _apply_values(self, payload: Dict[str, Any]):
        normalized = self._normalize_payload(payload, self.api_id_edit.text().strip())
        self.last_payload = normalized
        # 기본 패널(키움) 적용
        if "kiwoom" in self.panels:
            self.panels["kiwoom"].apply_values(normalized)
        filled = sum(1 for v in normalized.values() if v not in (None, "", [], {}))
        v58_filled = sum(1 for k in self.v58_cols if normalized.get(k) not in (None, "", [], {}))
        self.status_label.setText(f"표시 {len(normalized)} / 채움 {filled} / v58 {v58_filled}/{len(self.v58_cols)}")
        self._update_v58_status(normalized)

    def _normalize_payload(self, payload: Dict[str, Any], api_id: str) -> Dict[str, Any]:
        """자주 나오는 키를 표준 컬럼명으로 매핑하여 표시를 늘린다."""
        if not isinstance(payload, dict):
            return {}
        out = dict(payload)  # 원본 키 유지

        alias_map = {
            "dt": ["dt", "stnd_dt", "stck_bsop_date", "bas_dt", "base_dt", "date"],
            "stk_cd": ["stk_cd", "code", "itm_cd"],
            "open": ["open", "open_pric"],
            "high": ["high", "high_pric"],
            "low": ["low", "low_pric"],
            "close": ["close", "close_pric", "cur_prc", "stck_prpr", "lst_pric"],
            "volume": ["volume", "trde_qty", "tot_vlm", "acml_vol", "trd_qty", "trdvol"],
            "amount": ["amount", "trde_prica", "tr_prc", "acc_trde_prica"],
            "fluc_rt": ["fluc_rt", "flu_rt"],
            "pred_pre": ["pred_pre", "cmp_pric"],
            "per": ["per"],
            "eps": ["eps"],
            "pbr": ["pbr"],
            "bps": ["bps"],
            "roe": ["roe"],
        }

        # ka10081 전용 기본 매핑
        if api_id == "ka10081":
            alias_map["dt"] = ["dt", "stnd_dt", "stck_bsop_date", "bas_dt", "base_dt"]
            alias_map["close"] = ["close", "close_pric", "cur_prc", "stck_prpr", "lst_pric", "stck_clpr"]
            alias_map["open"] = ["open", "open_pric", "stck_oprc"]
            alias_map["high"] = ["high", "high_pric", "stck_hgpr"]
            alias_map["low"] = ["low", "low_pric", "stck_lwpr"]
            alias_map["volume"] = ["volume", "trde_qty", "tot_vlm", "acml_vol", "trd_qty", "trdvol", "acml_vol"]
        # 재무/기본정보 (ka10001 등)
        alias_map["per"] = alias_map.get("per", []) + ["stck_per"]
        alias_map["pbr"] = alias_map.get("pbr", []) + ["stck_pbr"]
        alias_map["bps"] = alias_map.get("bps", []) + ["bps"]
        alias_map["eps"] = alias_map.get("eps", []) + ["eps"]
        alias_map["roe"] = alias_map.get("roe", []) + ["roe"]
        alias_map["ev"] = alias_map.get("ev", []) + ["ev"]
        # 수급/수량 관련 확장
        alias_map["trde_prica"] = alias_map.get("trde_prica", []) + ["acc_trde_prica", "tr_prc"]
        alias_map["trde_qty"] = alias_map.get("trde_qty", []) + ["acc_trde_qty", "trqu"]

        for canonical, candidates in alias_map.items():
            if canonical in out and out[canonical] not in (None, "", [], {}):
                continue
            for cand in candidates:
                if cand in payload and payload[cand] not in (None, "", [], {}):
                    out[canonical] = payload[cand]
                    break
        return out

    def _reflow_cards(self):
        cols = 3
        # 값 있는 카드가 먼저 오도록 정렬, 그 다음 원래 순서 유지
        sorted_names = sorted(
            self.field_order,
            key=lambda n: (not self.cards[n].has_value, self.field_order.index(n))
        )
        # 기존 배치 제거
        for i in reversed(range(self.grid.count())):
            item = self.grid.itemAt(i)
            if item and item.widget():
                self.grid.removeWidget(item.widget())
        # 재배치
        for idx, name in enumerate(sorted_names):
            self.grid.addWidget(self.cards[name], idx // cols, idx % cols)

    # ------------------------------------------------------------------
    # 이벤트
    # ------------------------------------------------------------------
    def _build_base_body(self) -> Dict[str, Any]:
        body_text = self.body_edit.toPlainText().strip()
        body = json.loads(body_text) if body_text else {}
        body.setdefault("stk_cd", self.code_edit.text().strip() or "005930")
        if self.date_edit.text().strip():
            body.setdefault("base_dt", self.date_edit.text().strip())
        else:
            body.setdefault("base_dt", QDate.currentDate().toString("yyyyMMdd"))
        return body

    def fetch_source(self, source_id: str):
        code = self.code_edit.text().strip() or "005930"
        # 소스 패널별 날짜 우선, 없으면 상단 기본일자
        base_dt_raw = self.date_edit.text().strip() or QDate.currentDate().toString("yyyyMMdd")
        if source_id in self.panels:
            panel_dt = self.panels[source_id].date_edit.text().strip()
            if panel_dt:
                base_dt_raw = panel_dt
        base_dt = self._normalize_date(base_dt_raw)
        if source_id in self.panels:
            self.panels[source_id].date_edit.setText(base_dt)
        panel = self.panels.get(source_id)
        try:
            if panel:
                panel.status.setText("요청 중...")
            if source_id == "kiwoom":
                self.call_rest_multi(base_dt_override=base_dt)
                if panel:
                    panel.status.setText("갱신 완료")
            elif source_id == "pykrx":
                payload = self._fetch_pykrx(code, base_dt)
                payload_fund = self._fetch_pykrx_fundamental(code, base_dt)
                payload = self._merge_payloads(payload or {}, payload_fund or {})
                counts = self.panels["pykrx"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 패키지·거래일 확인"
                    self.panels["pykrx"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
            elif source_id == "fdr":
                payload = self._fetch_fdr(code, base_dt)
                counts = self.panels["fdr"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 패키지·거래일 확인"
                    self.panels["fdr"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
            elif source_id == "yahoo":
                payload = self._fetch_yahoo(code, base_dt)
                counts = self.panels["yahoo"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 패키지·거래일 확인"
                    self.panels["yahoo"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
            elif source_id == "dart":
                payload = self._fetch_dart(code)
                payload_fin = self._fetch_dart_financial(code, base_dt)
                payload_extra = self._fetch_dart_major(code)
                merged = self._merge_payloads(payload or {}, payload_fin or {})
                merged = self._merge_payloads(merged, payload_extra or {})
                payload = merged
                counts = self.panels["dart"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 키/코드 확인"
                    self.panels["dart"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
            elif source_id == "dart_fin":
                payload = self._fetch_dart_financial(code, base_dt)
                counts = self.panels["dart_fin"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 재무조회 실패"
                    self.panels["dart_fin"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
            elif source_id == "macro":
                payload = self._fetch_macro(base_dt)
                counts = self.panels["macro"].apply_values(payload or {})
                if counts[1] == 0:
                    msg = payload.get("return_msg") if isinstance(payload, dict) else "데이터 없음 / 휴장일?"
                    self.panels["macro"].status.setText(f"{msg} (총{counts[0]}, 채움0)")
        except Exception as e:
            self.status_label.setText(f"{source_id} 오류: {e}")
            if panel:
                panel.status.setText(f"오류: {e}")

    def _normalize_date(self, yyyymmdd: str) -> str:
        """미래/주말이면 가장 가까운 직전 평일로 보정."""
        try:
            dt_obj = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        except Exception:
            dt_obj = dt.date.today()
        today = dt.date.today()
        if dt_obj > today:
            dt_obj = today
        # 주말이면 금요일로 당겨서 요청
        if dt_obj.weekday() == 5:  # 토
            dt_obj = dt_obj - dt.timedelta(days=1)
        elif dt_obj.weekday() == 6:  # 일
            dt_obj = dt_obj - dt.timedelta(days=2)
        return dt_obj.strftime("%Y%m%d")

    def _propagate_date_to_panels(self):
        val = self.date_edit.text().strip()
        if not val:
            return
        for panel in self.panels.values():
            panel.date_edit.setText(val)

    def _call_single(self, api: "KiwoomRestApi", api_id: str, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        resp = api._call_api(api_id=api_id, url_path=path, body=body)
        payload = self._extract_payload(resp)
        # ka10081 재시도 (tp=1)
        if api_id == "ka10081" and not payload:
            fb_body = dict(body)
            fb_body["upd_stkpc_tp"] = "1"
            resp = api._call_api(api_id=api_id, url_path=path, body=fb_body)
            payload = self._extract_payload(resp)
        return payload

    def call_rest_multi(self, base_dt_override: str = None):
        try:
            from api.kiwoom_rest.kiwoom_api import KiwoomRestApi
        except Exception as e:
            QMessageBox.critical(self, "REST 호출 실패", str(e))
            self.status_label.setText("호출 실패")
            return

        base_body = self._build_base_body()
        if base_dt_override:
            base_body["base_dt"] = base_dt_override
        stk_cd = base_body.get("stk_cd", "005930")
        base_dt = base_body.get("base_dt", QDate.currentDate().toString("yyyyMMdd"))

        api = KiwoomRestApi()
        jobs = [
            ("ka10081", "/api/dostk/chart", {**base_body, "term_cnt": "60", "upd_stkpc_tp": base_body.get("upd_stkpc_tp", "D")}),
            ("ka10001", "/api/dostk/stkinfo", {"stk_cd": stk_cd}),
            ("ka10058", "/api/dostk/stkinfo", {"stk_cd": stk_cd, "strt_dt": base_dt, "end_dt": base_dt}),
            ("ka10014", "/api/dostk/stkinfo", {"stk_cd": stk_cd, "strt_dt": base_dt, "end_dt": base_dt}),
        ]

        merged: Dict[str, Any] = {}
        filled = 0
        for api_id, path, body in jobs:
            try:
                payload = self._call_single(api, api_id, path, body)
            except Exception as e:
                self.status_label.setText(f"{api_id} 오류: {e}")
                continue
            merged = self._merge_payloads(merged, payload)
            filled_now = sum(1 for v in payload.values() if v not in (None, "", [], {})) if isinstance(payload, dict) else 0
            filled += filled_now

        if not merged:
            QMessageBox.information(self, "결과 없음", "호출했으나 표시할 데이터가 없습니다.")
            self.clear_values()
            return

        self._apply_values(merged)
        keys_shown = len(self.last_payload)
        filled_shown = sum(1 for v in self.last_payload.values() if v not in (None, "", [], {}))
        self.status_label.setText(f"복합 호출 완료 | 표시 키 {keys_shown}개 / 채움 {filled_shown}개 | 합산 필드 {filled}")

    def call_rest(self):
        api_id = self.api_id_edit.text().strip() or "ka10081"
        path = self.api_path_edit.text().strip() or "/api/dostk/chart"

        try:
            body = self._build_base_body()
            # ka10081 기본 파라미터 보강
            if api_id == "ka10081":
                body.setdefault("term_cnt", "60")
                body.setdefault("upd_stkpc_tp", "D")
        except Exception as e:
            QMessageBox.warning(self, "입력 오류", f"JSON Body를 확인해주세요:\n{e}")
            return

        try:
            from api.kiwoom_rest.kiwoom_api import KiwoomRestApi  # 지연 로딩

            api = KiwoomRestApi()
            resp = api._call_api(api_id=api_id, url_path=path, body=body)
            payload = self._extract_payload(resp)
            # ka10081: 1차(D) 응답이 비면 tp=1로 재시도
            if api_id == "ka10081" and not payload:
                fb_body = dict(body)
                fb_body["upd_stkpc_tp"] = "1"
                resp = api._call_api(api_id=api_id, url_path=path, body=fb_body)
                payload = self._extract_payload(resp)
        except Exception as e:
            QMessageBox.critical(self, "REST 호출 실패", str(e))
            self.status_label.setText("호출 실패")
            return

        if not payload:
            msg = resp.get("return_msg") or "응답은 받았지만 표시할 데이터가 없습니다."
            QMessageBox.information(self, "결과 없음", msg)
            self.clear_values()
            return

        self._apply_values(payload)
        rc = resp.get("return_code", "?")
        rm = resp.get("return_msg", "")
        keys_shown = len(self.last_payload)
        filled = sum(1 for v in self.last_payload.values() if v not in (None, "", [], {}))
        self.status_label.setText(f"return_code={rc} {rm} | 표시 키 {keys_shown}개 / 채움 {filled}개")

    def load_json_file(self):
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
            self.clear_values()
            return

        self._apply_values(payload)
        self.status_label.setText(f"파일 로드: {os.path.basename(path)} | 키 {len(payload)}개")

    def clear_values(self):
        for card in self.cards.values():
            card.clear()
        self.status_label.setText("초기화 완료")
