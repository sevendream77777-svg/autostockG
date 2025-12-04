
# ui/pages/p6_trading.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import json
import time
import glob
import re
import traceback
import requests
import pandas as pd
from typing import List, Dict, Any

from PySide6.QtCore import Qt, QDate, QRect, QLocale, Signal, QObject
from PySide6.QtGui import QColor, QPainter
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QSpinBox, QTextEdit, QDateEdit, QTableWidget, QTableWidgetItem,
    QGroupBox, QHeaderView, QSplitter, QListWidget, QListWidgetItem,
    QCalendarWidget, QFrame
)
# ---------------------------------------------------------
# 경로 및 라이브러리 설정
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(root_dir)

# JSON 파일 경로 (절대 경로 우선, 실패시 상대 경로)
# 사용자 지정 경로: F:\autostockG\MODELENGINE\INFO\hoj_engine_info
JSON_BASE_DIR = os.path.join(root_dir, "MODELENGINE", "INFO", "hoj_engine_info")
if not os.path.exists(JSON_BASE_DIR):
    # 혹시 경로가 다르면 하드코딩된 경로 시도
    JSON_BASE_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

# 토큰 매니저 임포트 시도
try:
    from api.kiwoom_rest.token_manager import KiwoomTokenManager
except ImportError:
    # 더미 클래스 (IDE 오류 방지용)
    class KiwoomTokenManager:
        def __init__(self): self.config = {"base_url": "https://api.kiwoom.com"}
        def get_token(self): return ""

# ---------------------------------------------------------
# [Helper] REST API 통신 함수
# ---------------------------------------------------------
def debug_post(url: str, headers: Dict[str, str], body: Dict[str, Any], timeout: int = 5):
    """REST API POST 요청을 보내고 응답을 반환"""
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout)
        return {"status": resp.status_code, "json": resp.json(), "error": None}
    except Exception:
        return {"status": -1, "json": None, "error": traceback.format_exc()}

def normalize_ohlcv(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """차트 데이터 정규화"""
    out = []
    for r in items:
        dt = r.get("dt") or r.get("stnd_dt") or r.get("date")
        op = r.get("open_pric") or r.get("open")
        hi = r.get("high_pric") or r.get("high")
        lo = r.get("low_pric") or r.get("low")
        cl = r.get("cur_prc") or r.get("close_pric") or r.get("close")
        vl = r.get("trde_qty") or r.get("volume")
        if dt:
            out.append({"date": str(dt).strip(), "open": str(op).strip(), "high": str(hi).strip(), 
                        "low": str(lo).strip(), "close": str(cl).strip(), "volume": str(vl).strip()})
    return out

# ---------------------------------------------------------
# [Widget] 커스텀 달력 (예측 날짜 표시용)
# ---------------------------------------------------------
class CustomCalendar(QCalendarWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.engine_counts = {}
        self.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.setLocale(QLocale(QLocale.Korean, QLocale.SouthKorea))
        self.setStyleSheet("""
            QCalendarWidget QWidget { alternate-background-color: #444; color: white; }
            QCalendarWidget QToolButton { color: white; background-color: #333; border: none; margin: 2px; }
            QCalendarWidget QToolButton:hover { background-color: #555; border-radius: 3px; }
            QCalendarWidget QTableView { background-color: #2b2b2b; color: white; selection-background-color: #FF8C00; outline: 0; }
        """)

    def set_engine_counts(self, counts):
        self.engine_counts = counts
        self.updateCells()

    def paintCell(self, painter, rect, date):
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, False)
        
        # 배경
        bg_color = QColor("#2b2b2b")
        if date == self.selectedDate():
            bg_color = QColor("#FF8C00")
        painter.fillRect(rect, bg_color)

        # 날짜 텍스트
        text_color = QColor("white")
        if date.month() != self.monthShown():
            text_color = QColor("#777")
        
        day_rect = QRect(rect.left(), rect.top() + 2, rect.width(), rect.height() // 2)
        painter.setPen(text_color)
        painter.drawText(day_rect, Qt.AlignCenter, str(date.day()))

        # 엔진 파일 존재 표시 (+N)
        if date in self.engine_counts:
            count = self.engine_counts[date]
            count_rect = QRect(rect.left(), rect.top() + rect.height()//2, rect.width(), rect.height()//2)
            font = painter.font()
            font.setPointSize(8)
            painter.setFont(font)
            painter.setPen(QColor("#FFA500"))
            painter.drawText(count_rect, Qt.AlignCenter, f"(+{count})")

        painter.restore()

# ---------------------------------------------------------
# [Main] TradingPage 클래스
# ---------------------------------------------------------
class TradingPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # API 설정 초기화
        try:
            self.token_manager = KiwoomTokenManager()
            self.api_host = self.token_manager.config.get("base_url", "https://api.kiwoom.com")
            self.token_avail = True
        except:
            self.api_host = "https://api.kiwoom.com"
            self.token_avail = False

        self.json_files_cache = []
        
        self._setup_ui()
        self._connect()
        self._scan_files() # JSON 파일 스캔
        
        self._log(f"[시스템] REST API Host: {self.api_host}")
        self._log(f"[시스템] 추천파일 경로: {JSON_BASE_DIR}")

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # ===========================================================
        # 상단: AI 추천 (좌: 달력/리스트, 우: 종목/시세)
        # ===========================================================
        top_splitter = QSplitter(Qt.Horizontal)
        # top_splitter.setFixedHeight(350)  # 고정높이 제거 → 초기 비율만 지정(요구 7)
        top_splitter.setSizes([1, 1])

        # 1. AI 추천 엔진 선택
        gb_engine = QGroupBox("1. AI 추천 엔진 선택")
        v_eng = QVBoxLayout(gb_engine)
        h_eng = QHBoxLayout()
        self.calendar = CustomCalendar()
        self.list_engines = QListWidget()
        h_eng.addWidget(self.calendar, 1)
        h_eng.addWidget(self.list_engines, 1)
        v_eng.addLayout(h_eng)
        top_splitter.addWidget(gb_engine)

        # 2. 추천 종목 및 실시간 시세
        gb_rec = QGroupBox("2. 추천 종목 상세 (현재가 매칭)")
        v_rec = QVBoxLayout(gb_rec)
        h_rec_btn = QHBoxLayout()
        self.lbl_rec_status = QLabel("선택된 파일 없음")
        self.btn_refresh_price = QPushButton("현재가 갱신 (ka10001)")
        h_rec_btn.addWidget(self.lbl_rec_status)
        h_rec_btn.addStretch()
        h_rec_btn.addWidget(self.btn_refresh_price)
        v_rec.addLayout(h_rec_btn)

        # 테이블: 순위, 종목명, 코드, 추천가, 현재가, 등락, 점수, 확률
        self.tbl_rec = QTableWidget(0, 8)
        self.tbl_rec.setHorizontalHeaderLabels(["순위", "종목명", "코드", "추천가", "현재가", "등락률", "점수", "확률"])
        self.tbl_rec.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl_rec.setEditTriggers(QTableWidget.NoEditTriggers)

        # [추가] 전역 가독성 개선 (요구 2, 3, 4)
        self.tbl_rec.verticalHeader().setVisible(False)  # 행번호 숨김 → 앞 숫자 제거(요구 4)
        # 선택 배경 완화(빨/파 텍스트 가림 방지, styles에서 추가해도 무관)
        self.tbl_rec.setStyleSheet("QTableWidget::item:selected{background-color: rgba(255,165,0,64);} ")
        # 폭 정책: 순위/코드만 타이트, 나머지는 자동
        self.tbl_rec.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)  # 순위 좁게(요구 3)
        self.tbl_rec.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)  # 코드 좁게(요구 3)

        v_rec.addWidget(self.tbl_rec)
        top_splitter.addWidget(gb_rec)

        main_layout.addWidget(top_splitter)

        # ===========================================================
        # 중단: 보유 종목 현황 (좌: 수익률, 우: 평가금)
        # ===========================================================
        mid_splitter = QSplitter(Qt.Horizontal)
        # mid_splitter.setFixedHeight(250)  # 고정높이 제거 → 초기 비율만 지정(요구 7)
        mid_splitter.setSizes([1, 1])

        # 3. 보유 종목 수익률
        gb_yield = QGroupBox("3. 보유 종목 (수익률)")
        v_yld = QVBoxLayout(gb_yield)
        h_yld_btn = QHBoxLayout()
        self.btn_acc_refresh = QPushButton("계좌 잔고 갱신 (kt00004)")
        h_yld_btn.addStretch()
        h_yld_btn.addWidget(self.btn_acc_refresh)
        v_yld.addLayout(h_yld_btn)
        
        self.tbl_yield = QTableWidget(0, 3)
        self.tbl_yield.setHorizontalHeaderLabels(["종목명", "수익률(%)", "손익금"])
        self.tbl_yield.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v_yld.addWidget(self.tbl_yield)
        mid_splitter.addWidget(gb_yield)

        # 4. 보유 종목 평가
        gb_eval = QGroupBox("4. 보유 종목 (평가금액)")
        v_eval = QVBoxLayout(gb_eval)
        self.lbl_deposit = QLabel("예수금: - | 총평가: -")
        self.lbl_deposit.setStyleSheet("color: blue; font-weight: bold;")
        v_eval.addWidget(self.lbl_deposit)

        self.tbl_eval = QTableWidget(0, 3)
        self.tbl_eval.setHorizontalHeaderLabels(["종목명", "현재가", "평가금액"])
        self.tbl_eval.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v_eval.addWidget(self.tbl_eval)
        mid_splitter.addWidget(gb_eval)

        main_layout.addWidget(mid_splitter)

        # ===========================================================
        # 하단: 주문 설정 및 차트
        # ===========================================================
        bot_splitter = QSplitter(Qt.Horizontal)
        
        # 5. 종목 및 주문 설정
        left_w = QWidget()
        left_l = QVBoxLayout(left_w)
        gb_order = QGroupBox("5. 종목 및 주문 설정")
        g = QGridLayout(gb_order)

        self.ed_code = QLineEdit(); self.ed_code.setPlaceholderText("종목코드")
        self.btn_chart = QPushButton("일봉 조회")
        self.cmb_mkt = QComboBox(); self.cmb_mkt.addItems(["KRX", "NXT"])
        self.sp_qty = QSpinBox(); self.sp_qty.setRange(1, 999999); self.sp_qty.setValue(1)
        self.ed_price = QLineEdit(); self.ed_price.setPlaceholderText("0=시장가")
        self.cmb_type = QComboBox(); self.cmb_type.addItems(["시장가(03)", "지정가(00)"])
        self.btn_buy = QPushButton("매수"); self.btn_buy.setStyleSheet("color:red; font-weight:bold;")
        self.btn_sell = QPushButton("매도"); self.btn_sell.setStyleSheet("color:blue; font-weight:bold;")

        g.addWidget(QLabel("종목코드"), 0,0); g.addWidget(self.ed_code, 0,1); g.addWidget(self.btn_chart, 0,2)
        g.addWidget(QLabel("거래소"), 1,0); g.addWidget(self.cmb_mkt, 1,1); g.addWidget(QLabel("수량"), 1,2); g.addWidget(self.sp_qty, 1,3)
        g.addWidget(QLabel("단가"), 2,0); g.addWidget(self.ed_price, 2,1); g.addWidget(QLabel("유형"), 2,2); g.addWidget(self.cmb_type, 2,3)
        g.addWidget(self.btn_buy, 3,0,1,2); g.addWidget(self.btn_sell, 3,2,1,2)
        
        left_l.addWidget(gb_order)
        self.txt_log = QTextEdit(); self.txt_log.setReadOnly(True)
        left_l.addWidget(QLabel("실행 로그"))
        left_l.addWidget(self.txt_log)
        bot_splitter.addWidget(left_w)

        # 6. 일봉 차트
        right_w = QWidget()
        right_l = QVBoxLayout(right_w)
        self.tbl_chart = QTableWidget(0, 6)
        self.tbl_chart.setHorizontalHeaderLabels(["일자","시가","고가","저가","종가","거래량"])
        self.tbl_chart.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        right_l.addWidget(QLabel("6. 일봉 차트 (ka10081)"))
        right_l.addWidget(self.tbl_chart)
        bot_splitter.addWidget(right_w)

        bot_splitter.setSizes([400, 600])
        main_layout.addWidget(bot_splitter)

    # ---------------------------------------------------------
    # 기능 구현
    # ---------------------------------------------------------
    def _connect(self):
        self.calendar.clicked.connect(self._on_date_clicked)
        self.list_engines.itemClicked.connect(self._on_engine_selected)
        self.btn_refresh_price.clicked.connect(self._refresh_prices_ka10001)
        self.btn_acc_refresh.clicked.connect(self._refresh_account_kt00004)
        self.btn_chart.clicked.connect(self._on_chart_query)
        self.btn_buy.clicked.connect(lambda: self._on_order("BUY"))
        self.btn_sell.clicked.connect(lambda: self._on_order("SELL"))

        # 테이블 클릭 시 코드 자동 입력
        self.tbl_rec.cellClicked.connect(lambda r,c: self.ed_code.setText(self.tbl_rec.item(r,2).text()))
        self.tbl_yield.cellClicked.connect(self._on_acc_table_click)
        self.tbl_eval.cellClicked.connect(self._on_acc_table_click)

    def _get_token(self):
        if not self.token_avail:
            self._log("[오류] Token Manager가 초기화되지 않았습니다.")
            return None
        return self.token_manager.get_token()

    def _log(self, msg):
        self.txt_log.append(msg)
        self.txt_log.verticalScrollBar().setValue(self.txt_log.verticalScrollBar().maximum())

    # --- 1. 파일 스캔 ---
    def _scan_files(self):
        if not os.path.exists(JSON_BASE_DIR):
            self._log(f"[오류] 폴더 없음: {JSON_BASE_DIR}")
            return

        files = glob.glob(os.path.join(JSON_BASE_DIR, "*.json"))
        self.json_files_cache = []
        counts = {}

        for fpath in files:
            fname = os.path.basename(fpath)
            # 파일명에서 날짜/hor 추출 (예: ..._h5_..._251128.json)
            m_date = re.search(r"(\d{6})\.json$", fname)
            m_h = re.search(r"_h(\d+)_", fname)
            if m_date:
                date_str = "20" + m_date.group(1)  # 20251128
                try:
                    qdate = QDate.fromString(date_str, "yyyyMMdd")
                    # 예측기간: 파일날짜+1영업일 ~ +h영업일
                    h = int(m_h.group(1)) if m_h else 5
                    # pandas로 영업일 계산 (p4와 동일 개념)  :contentReference[oaicite:2]{index=2}
                    # → 계산은 _on_date_clicked에서 비교용 QDate로 사용
                    from pandas.tseries.offsets import BDay
                    import pandas as _pd
                    start_pd = (_pd.Timestamp(date_str) + BDay(1)).date()
                    end_pd   = (_pd.Timestamp(date_str) + BDay(h)).date()
                    start_q = QDate(start_pd.year, start_pd.month, start_pd.day)
                    end_q   = QDate(end_pd.year, end_pd.month, end_pd.day)

                    self.json_files_cache.append({
                        "date": qdate, "path": fpath, "name": fname,
                        "win_start": start_q, "win_end": end_q, "h": h
                    })
                    counts[qdate] = counts.get(qdate, 0) + 1
                except:
                    continue
        
        self.calendar.set_engine_counts(counts)
        self.calendar.setSelectedDate(QDate.currentDate())
        self._on_date_clicked(QDate.currentDate())

    def _on_date_clicked(self, date: QDate):
        self.list_engines.clear()
        # 선택한 날짜가 [파일날짜+1BD, +hBD] 구간에 포함되는 파일만 노출 (요구 8)
        matched = [
            f for f in self.json_files_cache
            if (date >= f.get("win_start", f["date"]) and date <= f.get("win_end", f["date"]))
        ]
        for m in matched:
            item = QListWidgetItem(m["name"])
            item.setData(Qt.UserRole, m["path"])
            self.list_engines.addItem(item)

    # --- 2. 추천 종목 로드 ---
    def _on_engine_selected(self, item):
        fpath = item.data(Qt.UserRole)
        self.lbl_rec_status.setText(f"파일: {item.text()}")

        # 예측기간 표기 (파일날짜 + 1BD ~ +hBD) 〔요구 5〕
        try:
            m_h = re.search(r"_h(\d+)_", item.text())
            m_d = re.search(r"(\d{6})\.json$", item.text())
            if m_d:
                date_str = "20" + m_d.group(1)
                h = int(m_h.group(1)) if m_h else 5
                from pandas.tseries.offsets import BDay
                start_pd = (pd.Timestamp(date_str) + BDay(1)).date()
                end_pd   = (pd.Timestamp(date_str) + BDay(h)).date()
                self.lbl_rec_status.setText(f"파일: {item.text()} | 예측기간: {start_pd} ~ {end_pd}")
        except Exception:
            pass
        
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            top10 = data.get("top10", [])
            self.tbl_rec.setRowCount(0)
            
            for row in top10:
                # JSON 키 매핑 (한글/영문 혼용 대응)
                rank = str(row.get("순위", row.get("rank", "")))
                name = str(row.get("종목명", row.get("name", "")))
                code = str(row.get("종목코드", row.get("code", ""))).zfill(6)
                
                # 추천가 (과거 데이터)
                price_rec = float(row.get("현재가", row.get("close", 0)))
                
                score = row.get("동시적용 기대수익(%)", row.get("예측수익률(%)", 0))
                prob = row.get("상승확률(%)", 0)

                r = self.tbl_rec.rowCount()
                self.tbl_rec.insertRow(r)
                self.tbl_rec.setItem(r, 0, QTableWidgetItem(rank))
                self.tbl_rec.setItem(r, 1, QTableWidgetItem(name))
                self.tbl_rec.setItem(r, 2, QTableWidgetItem(code))
                # 코드 가운데 정렬 (요구 3)
                if self.tbl_rec.item(r, 2):
                    self.tbl_rec.item(r, 2).setTextAlignment(Qt.AlignCenter)

                self.tbl_rec.setItem(r, 3, QTableWidgetItem(f"{price_rec:,.0f}")) # 추천가
                self.tbl_rec.setItem(r, 4, QTableWidgetItem("-")) # 현재가 (API 갱신필요)
                self.tbl_rec.setItem(r, 5, QTableWidgetItem("-")) # 등락률
                self.tbl_rec.setItem(r, 6, QTableWidgetItem(f"{float(score):.2f}"))
                self.tbl_rec.setItem(r, 7, QTableWidgetItem(f"{float(prob):.2f}%"))
                
                # 등락률 계산을 위해 원본가 저장
                self.tbl_rec.item(r, 3).setData(Qt.UserRole, price_rec)

            self._log(f"[파일로드] {len(top10)}개 종목 로딩 완료")

            # 로드 직후 자동 현재가 갱신 (요구 6)
            self._refresh_prices_ka10001()
            
        except Exception as e:
            self._log(f"[로드오류] {e}")

    # --- 2-1. 현재가 갱신 (ka10001) ---
    def _refresh_prices_ka10001(self):
        count = self.tbl_rec.rowCount()
        if count == 0: return

        token = self._get_token()
        if not token: return

        url = f"{self.api_host}/api/dostk/stk-base-info"
        headers = {"api-id": "ka10001", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        self._log(f"현재가 조회 시작 ({count}건)...")
        
        for r in range(count):
            code = self.tbl_rec.item(r, 2).text()
            rec_price = self.tbl_rec.item(r, 3).data(Qt.UserRole) # 추천가

            res = debug_post(url, headers, {"stk_cd": code})
            
            if res["status"] == 200 and res["json"]:
                out = res["json"].get("output", {})
                curr_str = out.get("close_pric") or out.get("cur_prc")
                
                if curr_str:
                    curr_val = float(curr_str)
                    self.tbl_rec.setItem(r, 4, QTableWidgetItem(f"{curr_val:,.0f}"))
                    
                    # 등락률 계산
                    if rec_price and rec_price > 0:
                        rate = ((curr_val - rec_price) / rec_price) * 100
                        item_rate = QTableWidgetItem(f"{rate:+.2f}%")
                        item_rate.setForeground(Qt.red if rate > 0 else Qt.blue)
                        self.tbl_rec.setItem(r, 5, item_rate)
            
            # 너무 빠른 호출 방지
            import time
            from PySide6.QtWidgets import QApplication
            QApplication.processEvents()
            time.sleep(0.05)
        
        self._log("현재가 갱신 완료")

    # --- 3 & 4. 계좌 조회 (kt00004) ---
    def _refresh_account_kt00004(self):
        token = self._get_token()
        if not token: return

        url = f"{self.api_host}/api/dostk/acnt"
        headers = {"api-id": "kt00004", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {"qry_tp": "0", "dmst_stex_tp": "KRX"} # 0:종목별, 1:수익률별

        res = debug_post(url, headers, body)
        if res["status"] != 200:
            self._log(f"[계좌실패] {res.get('json',{}).get('return_msg', 'Error')}")
            return
        
        data = res.get("json", {})
        deposit = int(data.get("d2_entra", 0) or 0)
        total_eval = int(data.get("tot_est_amt", 0) or 0)
        
        self.lbl_deposit.setText(f"예수금: {deposit:,}원 | 총평가: {total_eval:,}원")
        
        stocks = data.get("stk_acnt_evlt_prst", [])
        self.tbl_yield.setRowCount(0)
        self.tbl_eval.setRowCount(0)
        
        for s in stocks:
            name = s.get('stk_nm', '')
            code = s.get('stk_cd', '').strip().lstrip('A')
            pl_rate = float(s.get('pl_rt', 0) or 0)
            pl_amt = int(s.get('pl_amt', 0) or 0)
            cur_prc = int(s.get('cur_prc', 0) or 0)
            evlt_amt = int(s.get('evlt_amt', 0) or 0)
            
            # 3번 창: 수익률
            r1 = self.tbl_yield.rowCount()
            self.tbl_yield.insertRow(r1)
            self.tbl_yield.setItem(r1, 0, QTableWidgetItem(name))
            self.tbl_yield.item(r1, 0).setData(Qt.UserRole, code) # 코드 숨김 저장
            
            item_rt = QTableWidgetItem(f"{pl_rate:+.2f}%")
            item_rt.setForeground(Qt.red if pl_rate > 0 else Qt.blue)
            self.tbl_yield.setItem(r1, 1, item_rt)
            self.tbl_yield.setItem(r1, 2, QTableWidgetItem(f"{pl_amt:,}"))
            
            # 4번 창: 평가금
            r2 = self.tbl_eval.rowCount()
            self.tbl_eval.insertRow(r2)
            self.tbl_eval.setItem(r2, 0, QTableWidgetItem(name))
            self.tbl_eval.item(r2, 0).setData(Qt.UserRole, code)
            
            self.tbl_eval.setItem(r2, 1, QTableWidgetItem(f"{cur_prc:,}"))
            self.tbl_eval.setItem(r2, 2, QTableWidgetItem(f"{evlt_amt:,}"))
            
        self._log(f"계좌 조회 완료 ({len(stocks)}종목)")

    def _on_acc_table_click(self, row, col):
        sender = self.sender()
        code = sender.item(row, 0).data(Qt.UserRole)
        if code: self.ed_code.setText(code)

    # --- 5 & 6. 차트 및 주문 ---
    def _on_chart_query(self):
        code = self.ed_code.text().strip()
        if not code: return self._log("종목코드를 입력하세요.")
        token = self._get_token()
        if not token: return
        
        url = f"{self.api_host}/api/dostk/chart"
        headers = {"api-id": "ka10081", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {"stk_cd": code, "base_dt": QDate.currentDate().toString("yyyyMMdd"), "term_cnt": "60", "upd_stkpc_tp": "1"}
        
        res = debug_post(url, headers, body)
        if res["status"] == 200:
            items = res["json"].get("output", [])
            norm = normalize_ohlcv(items)
            self.tbl_chart.setRowCount(0)
            for r in norm:
                idx = self.tbl_chart.rowCount()
                self.tbl_chart.insertRow(idx)
                self.tbl_chart.setItem(idx, 0, QTableWidgetItem(r['date']))
                self.tbl_chart.setItem(idx, 1, QTableWidgetItem(r['open']))
                self.tbl_chart.setItem(idx, 2, QTableWidgetItem(r['high']))
                self.tbl_chart.setItem(idx, 3, QTableWidgetItem(r['low']))
                self.tbl_chart.setItem(idx, 4, QTableWidgetItem(r['close']))
                self.tbl_chart.setItem(idx, 5, QTableWidgetItem(r['volume']))
            self._log(f"차트 {len(norm)}건 조회 완료")
        else:
            self._log("차트 조회 실패")

    def _on_order(self, side):
        code = self.ed_code.text().strip()
        if not code: return
        token = self._get_token()
        if not token: return
        
        api_id = "kt10000" if side == "BUY" else "kt10001"
        url = f"{self.api_host}/api/dostk/ordr"
        price = self.ed_price.text().strip()
        trde_tp = "00"
        if "시장가" in self.cmb_type.currentText():
            trde_tp = "03"
            price = "0"
        if not price: price = "0"
        
        headers = {"api-id": api_id, "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "dmst_stex_tp": self.cmb_mkt.currentText(),
            "stk_cd": code,
            "ord_qty": str(self.sp_qty.value()),
            "ord_uv": price,
            "trde_tp": trde_tp,
            "cond_uv": ""
        }
        
        res = debug_post(url, headers, body)
        if res["status"] == 200:
            self._log(f"[주문성공] {side} {code} {price}원")
        else:
            self._log(f"[주문실패] {res.get('json',{}).get('return_msg')}")
