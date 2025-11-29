# ui/pages/p4_trading.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, csv, traceback, sys
import glob
import pandas as pd
from typing import List, Dict, Any, Optional

from PySide6.QtCore import Qt, QDate, QObject, Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QSpinBox, QCheckBox, QTextEdit, QDateEdit, QTableWidget, QTableWidgetItem,
    QFileDialog, QGroupBox, QHeaderView, QMessageBox, QSplitter, QFrame
)

import requests

# ---------------------------------------------------------
# 경로 설정 및 라이브러리 로드 (KiwoomTokenManager)
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(root_dir)

try:
    from kiwoom_rest.token_manager import KiwoomTokenManager
except ImportError:
    class KiwoomTokenManager:
        def __init__(self): 
            self.config = {"base_url": "https://api.kiwoom.com"} # Fallback
        def get_token(self): return ""

# 기본 호스트 (나중에 TokenManager 설정으로 덮어씀)
DEFAULT_HOST = "https://api.kiwoom.com"

# ---------------------------------------------------------
# 로그 리다이렉터
# ---------------------------------------------------------
class LogRedirector(QObject):
    log_signal = Signal(str)
    def __init__(self):
        super().__init__()
        self.terminal = sys.stdout

    def write(self, message):
        self.terminal.write(message)
        if message.strip():
            self.log_signal.emit(message.strip())

    def flush(self):
        self.terminal.flush()

# ---------------------------------------------------------
# 유틸리티 함수
# ---------------------------------------------------------
def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

def normalize_ohlcv(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in items:
        date = r.get("dt") or r.get("date") or r.get("stnd_dt")
        _open = r.get("open_pric") or r.get("open")
        _high = r.get("high_pric") or r.get("high")
        _low  = r.get("low_pric") or r.get("low")
        _close = r.get("cur_prc") or r.get("close_pric") or r.get("close")
        _vol  = r.get("trde_qty") or r.get("volume")

        if date:
            out.append({
                "date": str(date).strip(),
                "open": str(_open).strip(),
                "high": str(_high).strip(),
                "low":  str(_low).strip(),
                "close": str(_close).strip(),
                "volume": str(_vol).strip(),
            })
    return out

def debug_post(url: str, headers: Dict[str, str], body: Dict[str, Any], timeout: int = 5):
    session = requests.Session()
    req = requests.Request("POST", url, headers=headers, json=body)
    prepped = session.prepare_request(req)

    result = {
        "outgoing_url": prepped.url,
        "outgoing_headers": dict(prepped.headers),
        "outgoing_body": body,
        "status": None,
        "json": None,
        "text": None,
        "error": None
    }

    try:
        resp = session.send(prepped, timeout=timeout)
        result["status"] = resp.status_code
        try:
            result["json"] = resp.json()
        except Exception:
            result["text"] = resp.text
    except Exception:
        result["error"] = traceback.format_exc()

    return result

# ---------------------------------------------------------
# TradingPage UI
# ---------------------------------------------------------
class TradingPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self._setup_ui()
        
        self.redirector = LogRedirector()
        self.redirector.log_signal.connect(self._log)
        sys.stdout = self.redirector 

        self._log(">>> 시스템 초기화 중... (TokenManager)")
        try:
            self.token_manager = KiwoomTokenManager()
            self.token_avail = True
            
            # [중요] Config에서 로드한 Base URL 확인 (모의 vs 실전)
            self.api_host = self.token_manager.config.get("base_url", DEFAULT_HOST)
            mode = "모의투자(Paper)" if "mock" in self.api_host else "실전투자(Real)"
            self._log(f"✅ [API 연결 성공] 접속 서버: {mode}")
            self._log(f"   └ URL: {self.api_host}")
            
        except Exception as e:
            self.token_avail = False
            self.api_host = DEFAULT_HOST
            self._log(f"❌ [초기화 실패] {e}")

        self._connect()
        self._log("[SYSTEM] P4 Trading UI Ready.")
        
        if self.token_avail:
            self._refresh_account_info()
        
        self._load_prediction_data()

    # ---------------- UI 구성 ----------------
    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # === [상단] 3단 분할 ===
        top_frame = QFrame()
        top_frame.setFixedHeight(300) 
        top_layout = QHBoxLayout(top_frame)
        top_layout.setContentsMargins(0,0,0,0)

        # 1. AI 추천
        gb_rec = QGroupBox("1. AI 추천 종목")
        rec_layout = QVBoxLayout(gb_rec)
        h_rec = QHBoxLayout()
        self.de_pred = QDateEdit(QDate.currentDate())
        self.de_pred.setCalendarPopup(True)
        self.de_pred.setDisplayFormat("yyyy-MM-dd")
        self.btn_load_pred = QPushButton("결과 찾기")
        h_rec.addWidget(self.de_pred)
        h_rec.addWidget(self.btn_load_pred)
        rec_layout.addLayout(h_rec)
        self.cmb_pred_file = QComboBox()
        rec_layout.addWidget(self.cmb_pred_file)
        self.tbl_rec = QTableWidget(0, 4)
        self.tbl_rec.setHorizontalHeaderLabels(["순위", "종목", "점수", "확률"])
        self.tbl_rec.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl_rec.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tbl_rec.setSelectionBehavior(QTableWidget.SelectRows)
        rec_layout.addWidget(self.tbl_rec)
        top_layout.addWidget(gb_rec, 1) 

        # 2. 보유 종목 (수익률)
        gb_profit = QGroupBox("2. 보유 종목 (수익률)")
        profit_layout = QVBoxLayout(gb_profit)
        h_acc = QHBoxLayout()
        self.btn_refresh_acc = QPushButton("계좌/잔고 갱신 (kt00004)")
        h_acc.addWidget(self.btn_refresh_acc)
        profit_layout.addLayout(h_acc)
        self.tbl_profit = QTableWidget(0, 3)
        self.tbl_profit.setHorizontalHeaderLabels(["종목명", "수익률(%)", "손익금"])
        self.tbl_profit.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl_profit.setEditTriggers(QTableWidget.NoEditTriggers)
        profit_layout.addWidget(self.tbl_profit)
        top_layout.addWidget(gb_profit, 1)

        # 3. 보유 종목 (시세)
        gb_market = QGroupBox("3. 보유 종목 (평가)")
        market_layout = QVBoxLayout(gb_market)
        self.lbl_summary = QLabel("예수금: - | 총평가: -")
        self.lbl_summary.setStyleSheet("font-weight: bold; color: blue;")
        market_layout.addWidget(self.lbl_summary)
        self.tbl_market = QTableWidget(0, 3)
        self.tbl_market.setHorizontalHeaderLabels(["종목명", "현재가", "평가금액"])
        self.tbl_market.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl_market.setEditTriggers(QTableWidget.NoEditTriggers)
        market_layout.addWidget(self.tbl_market)
        top_layout.addWidget(gb_market, 1)

        main_layout.addWidget(top_frame)

        # === [중단] 스플리터 ===
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)

        # 좌측: 설정
        left_widget = QWidget()
        left_box = QVBoxLayout(left_widget)
        gb_ctrl = QGroupBox("종목 및 주문 설정")
        g = QGridLayout(gb_ctrl)
        self.ed_code = QLineEdit(); self.ed_code.setPlaceholderText("종목코드")
        self.btn_chart = QPushButton("차트 조회")
        self.cmb_mkt = QComboBox(); self.cmb_mkt.addItems(["KRX", "NXT"])
        self.sp_qty = QSpinBox(); self.sp_qty.setRange(1, 1000000); self.sp_qty.setValue(1)
        self.ed_price = QLineEdit(); self.ed_price.setPlaceholderText("단가(시장가=0)")
        self.cmb_type = QComboBox(); self.cmb_type.addItems(["시장가(03)", "지정가(00)"])
        self.btn_buy = QPushButton("매수"); self.btn_buy.setStyleSheet("background-color:#ffcccc; color:red; font-weight:bold; height:40px;")
        self.btn_sell = QPushButton("매도"); self.btn_sell.setStyleSheet("background-color:#ccccff; color:blue; font-weight:bold; height:40px;")

        r=0
        g.addWidget(QLabel("종목코드"), r, 0); g.addWidget(self.ed_code, r, 1); g.addWidget(self.btn_chart, r, 2); r+=1
        g.addWidget(QLabel("거래소"), r, 0); g.addWidget(self.cmb_mkt, r, 1); g.addWidget(QLabel("수량"), r, 2); g.addWidget(self.sp_qty, r, 3); r+=1
        g.addWidget(QLabel("주문단가"), r, 0); g.addWidget(self.ed_price, r, 1); g.addWidget(QLabel("주문유형"), r, 2); g.addWidget(self.cmb_type, r, 3); r+=1
        g.addWidget(self.btn_buy, r, 0, 1, 2); g.addWidget(self.btn_sell, r, 2, 1, 2)
        left_box.addWidget(gb_ctrl)

        self.txt_log = QTextEdit(); self.txt_log.setReadOnly(True)
        left_box.addWidget(QLabel("실행 로그"))
        left_box.addWidget(self.txt_log)
        splitter.addWidget(left_widget)

        # 우측: 차트
        right_widget = QWidget()
        right_box = QVBoxLayout(right_widget)
        self.tbl_chart = QTableWidget(0, 6)
        self.tbl_chart.setHorizontalHeaderLabels(["일자","시가","고가","저가","종가","거래량"])
        self.tbl_chart.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        right_box.addWidget(QLabel("📊 일봉 차트 (ka10081)"))
        right_box.addWidget(self.tbl_chart)
        splitter.addWidget(right_widget)
        splitter.setSizes([400, 600])

    # ---------------------------------------------------------
    def _connect(self):
        self.btn_load_pred.clicked.connect(self._find_prediction_files)
        self.cmb_pred_file.currentIndexChanged.connect(self._load_selected_pred_file)
        self.btn_refresh_acc.clicked.connect(self._refresh_account_info)
        self.btn_chart.clicked.connect(self._on_chart_query)
        self.btn_buy.clicked.connect(lambda: self._on_order("BUY"))
        self.btn_sell.clicked.connect(lambda: self._on_order("SELL"))
        self.tbl_rec.cellClicked.connect(self._on_table_click)
        self.tbl_profit.cellClicked.connect(self._on_table_click)
        self.tbl_market.cellClicked.connect(self._on_table_click)

    # ---------------------------------------------------------
    # API 요청
    # ---------------------------------------------------------
    def _get_api_url(self, path: str) -> str:
        # TokenManager가 로드한 config의 base_url 사용 (실전/모의 자동 구분)
        host = self.api_host.rstrip('/')
        return f"{host}{path}"

    def _refresh_account_info(self):
        token = self._get_token()
        if not token: return

        url = self._get_api_url("/api/dostk/acnt")
        headers = {
            "api-id": "kt00004",
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}"
        }
        body = {"qry_tp": "0", "dmst_stex_tp": "KRX"}

        self._log(">>> [잔고요청] kt00004 조회 중...")
        res = debug_post(url, headers, body)
        
        if res['status'] != 200:
            msg = res.get('json', {}).get('return_msg', 'Unknown Error')
            code = res.get('json', {}).get('return_code', '')
            self._log(f"[잔고실패] {res['status']} (Code:{code}) {msg}")
            return

        data = res.get('json', {})
        deposit = int(data.get("d2_entra", 0) or 0)
        total_eval = int(data.get("tot_est_amt", 0) or 0)
        self.lbl_summary.setText(f"예수금: {deposit:,}원 | 총평가: {total_eval:,}원")

        stocks = data.get("stk_acnt_evlt_prst", [])
        self.tbl_profit.setRowCount(0)
        self.tbl_market.setRowCount(0)

        for s in stocks:
            code = s.get('stk_cd', '').strip().lstrip('A')
            name = s.get('stk_nm', '')
            pl_rate = float(s.get('pl_rt', 0) or 0)
            pl_amt = int(s.get('pl_amt', 0) or 0)
            cur_price = int(s.get('cur_prc', 0) or 0)
            eval_amt = int(s.get('evlt_amt', 0) or 0)

            # Profit Table
            r2 = self.tbl_profit.rowCount()
            self.tbl_profit.insertRow(r2)
            self.tbl_profit.setItem(r2, 0, QTableWidgetItem(name))
            item_rate = QTableWidgetItem(f"{pl_rate:.2f}%")
            item_rate.setForeground(Qt.red if pl_rate > 0 else Qt.blue)
            self.tbl_profit.setItem(r2, 1, item_rate)
            self.tbl_profit.setItem(r2, 2, QTableWidgetItem(f"{pl_amt:,}"))
            self.tbl_profit.item(r2, 0).setData(Qt.UserRole, code)

            # Market Table
            r3 = self.tbl_market.rowCount()
            self.tbl_market.insertRow(r3)
            self.tbl_market.setItem(r3, 0, QTableWidgetItem(name))
            self.tbl_market.setItem(r3, 1, QTableWidgetItem(f"{cur_price:,}"))
            self.tbl_market.setItem(r3, 2, QTableWidgetItem(f"{eval_amt:,}"))
            self.tbl_market.item(r3, 0).setData(Qt.UserRole, code)

        self._log(f"[잔고완료] 보유종목 {len(stocks)}개 확인됨")

    def _on_chart_query(self):
        code = self.ed_code.text().strip()
        if not code: return
        
        token = self._get_token()
        if not token: return

        url = self._get_api_url("/api/dostk/chart")
        headers = {
            "api-id": "ka10081",
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}"
        }
        body = {
            "stk_cd": code,
            "base_dt": QDate.currentDate().toString("yyyyMMdd"),
            "term_cnt": "60",
            "upd_stkpc_tp": "1"
        }

        res = debug_post(url, headers, body)
        if res['status'] == 200:
            data_list = res['json'].get('stk_dt_pole_chart_qry', [])
            if not data_list: data_list = res['json'].get('output', [])

            if not data_list:
                msg = res['json'].get('return_msg', 'No Data')
                code_err = res['json'].get('return_code', '')
                self._log(f"[차트오류] 데이터 0건. 응답: {code_err} - {msg}")
            
            norm = normalize_ohlcv(data_list)
            self.tbl_chart.setRowCount(0)
            for r in norm:
                i = self.tbl_chart.rowCount()
                self.tbl_chart.insertRow(i)
                self.tbl_chart.setItem(i, 0, QTableWidgetItem(str(r['date'])))
                self.tbl_chart.setItem(i, 1, QTableWidgetItem(str(r['open'])))
                self.tbl_chart.setItem(i, 2, QTableWidgetItem(str(r['high'])))
                self.tbl_chart.setItem(i, 3, QTableWidgetItem(str(r['low'])))
                self.tbl_chart.setItem(i, 4, QTableWidgetItem(str(r['close'])))
                self.tbl_chart.setItem(i, 5, QTableWidgetItem(str(r['volume'])))
            
            if norm:
                self._log(f"[차트] {code} {len(norm)}건 조회 완료")
        else:
            msg = res.get('json', {}).get('return_msg', res.get('text'))
            self._log(f"[차트실패] {res['status']} - {msg}")

    def _on_order(self, side: str):
        code = self.ed_code.text().strip()
        if not code: return
        token = self._get_token()
        if not token: return

        api_id = "kt10000" if side == "BUY" else "kt10001"
        url = self._get_api_url("/api/dostk/ordr")
        
        price = self.ed_price.text().strip()
        trde_tp = "00"
        if "시장가" in self.cmb_type.currentText():
            trde_tp = "03"
            price = "0"
        if not price: price = "0"

        headers = {
            "api-id": api_id,
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}"
        }
        body = {
            "dmst_stex_tp": self.cmb_mkt.currentText(),
            "stk_cd": code,
            "ord_qty": str(self.sp_qty.value()),
            "ord_uv": price,
            "trde_tp": trde_tp,
            "cond_uv": ""
        }

        res = debug_post(url, headers, body)
        if res['status'] == 200:
            self._log(f"[주문성공] {side} {code} 전송 완료")
        else:
            self._log(f"[주문실패] {res.get('json', {}).get('return_msg', 'Error')}")

    def _find_prediction_files(self):
        target_date = self.de_pred.date().toString("yyyy-MM-dd")
        date_str_clean = target_date.replace("-", "") 
        output_dir = os.path.join(root_dir, "MODELENGINE", "OUTPUT")
        
        if not os.path.exists(output_dir): return

        all_files = glob.glob(os.path.join(output_dir, "*.csv"))
        matched_files = []
        for fpath in all_files:
            fname = os.path.basename(fpath)
            if (target_date in fname or date_str_clean in fname) and "AI 해석" not in fname:
                matched_files.append(fpath)
        
        matched_files.sort(key=os.path.getmtime, reverse=True)
        self.cmb_pred_file.blockSignals(True)
        self.cmb_pred_file.clear()
        if not matched_files:
            self._log(f"[알림] {target_date} 관련 파일 없음")
            self.tbl_rec.setRowCount(0)
        else:
            self._log(f"[파일찾기] {len(matched_files)}개 발견")
            for f in matched_files:
                self.cmb_pred_file.addItem(os.path.basename(f), f)
            self.cmb_pred_file.blockSignals(False)
            self._load_selected_pred_file()

    def _load_prediction_data(self):
        self._find_prediction_files()

    def _load_selected_pred_file(self):
        if self.cmb_pred_file.count() == 0: return
        fpath = self.cmb_pred_file.currentData()
        if not fpath or not os.path.exists(fpath): return

        try:
            try: df = pd.read_csv(fpath, encoding='utf-8-sig')
            except: df = pd.read_csv(fpath, encoding='cp949')

            self.tbl_rec.setRowCount(0)
            for i, row in df.iterrows():
                r = self.tbl_rec.rowCount()
                self.tbl_rec.insertRow(r)
                rank = str(row.get('Rank', row.get('순위', i+1)))
                code = str(row.get('code', row.get('Code', row.get('종목코드', '')))).strip().zfill(6)
                name = str(row.get('name', row.get('Name', row.get('종목명', '')))).strip()
                score = row.get('score', row.get('Score', row.get('예측수익률(%)', 0)))
                prob = row.get('prob', row.get('Prob', row.get('상승확률(%)', 0)))
                
                self.tbl_rec.setItem(r, 0, QTableWidgetItem(rank))
                self.tbl_rec.setItem(r, 1, QTableWidgetItem(name))
                self.tbl_rec.setItem(r, 2, QTableWidgetItem(f"{float(score):.4f}"))
                self.tbl_rec.setItem(r, 3, QTableWidgetItem(f"{float(prob):.2f}%"))
                self.tbl_rec.item(r, 1).setData(Qt.UserRole, code)
            
            self._log(f"[로드완료] {os.path.basename(fpath)}")
        except Exception as e:
            self._log(f"[로드에러] {e}")

    def _on_table_click(self, row, col):
        sender = self.sender()
        if not sender: return
        code = sender.item(row, 1).data(Qt.UserRole) if sender == self.tbl_rec else sender.item(row, 0).data(Qt.UserRole)
        if code:
            self.ed_code.setText(code)
            self._on_chart_query()

    def _get_token(self):
        if not self.token_avail: return None
        return self.token_manager.get_token()

    def _log(self, msg: str):
        self.txt_log.append(str(msg))
        self.txt_log.verticalScrollBar().setValue(self.txt_log.verticalScrollBar().maximum())