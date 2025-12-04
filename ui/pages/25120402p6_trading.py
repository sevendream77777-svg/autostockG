# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sys, json, glob, re, traceback, time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

import requests
import pandas as pd
from pandas.tseries.offsets import BDay

from PySide6.QtCore import Qt, QDate, QRect, QLocale, QPoint
from PySide6.QtGui import QColor, QPainter, QPixmap
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QSpinBox, QTextEdit, QTableWidget, QTableWidgetItem, QGroupBox,
    QHeaderView, QSplitter, QListWidget, QListWidgetItem, QCalendarWidget, QMenu,
    QApplication, QTableWidgetSelectionRange, QAbstractItemView, QAbstractScrollArea
)

# ----- NumericItem: 숫자 정렬을 위한 커스텀 아이템 (순위, 가격 정렬용) -----
class NumericItem(QTableWidgetItem):
    def __lt__(self, other):
        try:
            # UserRole에 저장된 실제 숫자값으로 비교
            a = self.data(Qt.UserRole)
            b = other.data(Qt.UserRole)
            
            # 값이 없으면 텍스트를 파싱해서 비교
            if a is None or b is None:
                def to_float(s):
                    if s is None:
                        return float("nan")
                    s = str(s).replace(",", "").replace("%", "").strip()
                    try:
                        return float(s)
                    except Exception:
                        return float("nan")
                a = to_float(self.text())
                b = to_float(other.text())
                
            return float(a) < float(b)
        except Exception:
            return super().__lt__(other)

# ---------------------------------------------------------
# 경로 및 라이브러리 설정
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(root_dir)

JSON_BASE_DIR = os.path.join(root_dir, "MODELENGINE", "INFO", "hoj_engine_info")
if not os.path.exists(JSON_BASE_DIR):
    JSON_BASE_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

# 토큰 매니저 임포트 시도
try:
    from api.kiwoom_rest.token_manager import KiwoomTokenManager
except ImportError:
    class KiwoomTokenManager:
        def __init__(self): self.config = {"base_url": "https://api.kiwoom.com"}
        def get_token(self): return ""

# ---------------------------------------------------------
# Helper
# ---------------------------------------------------------
def debug_post(url: str, headers: Dict[str, str], body: Dict[str, Any], timeout: int = 5):
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout)
        return {"status": resp.status_code, "json": resp.json(), "error": None}
    except Exception:
        return {"status": -1, "json": None, "error": traceback.format_exc()}

def normalize_ohlcv(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in items:
        dt = r.get("dt") or r.get("stnd_dt") or r.get("date")
        op = r.get("open_pric") or r.get("open")
        hi = r.get("high_pric") or r.get("high")
        lo = r.get("low_pric") or r.get("low")
        cl = r.get("cur_prc") or r.get("close_pric") or r.get("stck_prpr") or r.get("close")
        vl = r.get("trde_qty") or r.get("volume")
        if dt:
            out.append({
                "date": str(dt).strip(), "open": str(op or "").strip(), "high": str(hi or "").strip(),
                "low": str(lo or "").strip(), "close": str(cl or "").strip(), "volume": str(vl or "").strip()
            })
    return out

# ---------------------------------------------------------
# 커스텀 달력
# ---------------------------------------------------------
class CustomCalendar(QCalendarWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.engine_counts = {}      # {"yyyy-MM-dd": count}
        self.highlight_range = (None, None)  # (QDate start, QDate end)
        self.window_dates = set()    # {"yyyy-MM-dd"} 예측 가능 구간 전체
        self.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.setLocale(QLocale(QLocale.Korean, QLocale.SouthKorea))
        self.setStyleSheet("""
            QCalendarWidget QWidget { alternate-background-color: #444; color: white; }
            QCalendarWidget QToolButton { color: white; background-color: #333; border: none; margin: 2px; }
            QCalendarWidget QToolButton:hover { background-color: #555; border-radius: 3px; }
            QCalendarWidget QTableView { background-color: #2b2b2b; color: white; selection-background-color: #FF8C00; outline: 0; }
        """)

    def set_engine_counts(self, counts: Dict[str, int]):
        self.engine_counts = counts or {}
        self.updateCells()

    def set_highlight_range(self, start: QDate | None, end: QDate | None):
        self.highlight_range = (start, end)
        self.updateCells()

    def set_window_dates(self, dates: set[str]):
        self.window_dates = dates or set()
        self.updateCells()

    def paintCell(self, painter, rect, date):
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, False)
        
        # 배경 + 선택 강조
        bg_color = QColor("#2b2b2b")
        key = date.toString("yyyy-MM-dd")
        if key in self.window_dates:
            bg_color = QColor(255, 180, 120, 60)
        
        in_range = False
        start, end = self.highlight_range
        if start and end and start <= date <= end:
            in_range = True
            bg_color = QColor("#3b3f4a")
            
        if date == self.selectedDate():
            bg_color = QColor("#FF8C00")
            
        painter.fillRect(rect, bg_color)
        
        # 날짜 텍스트
        text_color = QColor("white")
        if date.month() != self.monthShown():
            text_color = QColor("#777")
        painter.setPen(text_color)
        painter.drawText(QRect(rect.left(), rect.top() + 2, rect.width(), rect.height() // 2), Qt.AlignCenter, str(date.day()))
        
        # (+N) 표시
        if key in self.engine_counts:
            count = self.engine_counts[key]
            painter.setPen(QColor("#FFA500"))
            painter.drawText(QRect(rect.left(), rect.top() + rect.height()//2, rect.width(), rect.height()//2), Qt.AlignCenter, f"(+{count})")
            
        # 범위 강조 테두리
        if in_range:
            painter.setPen(QColor("#7aa2f7"))
            painter.drawRect(rect.adjusted(1, 1, -1, -1))
            
        painter.restore()

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
class TradingPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # High-contrast up/down colors for dark backgrounds
        self.color_pos = QColor("#ff6b6b")  # bright red
        self.color_neg = QColor("#6cb8ff")  # bright blue
        
        try:
            self.token_manager = KiwoomTokenManager()
            self.api_host = self.token_manager.config.get("base_url", "https://api.kiwoom.com")
            self.token_avail = True
        except Exception:
            self.api_host = "https://api.kiwoom.com"
            self.token_avail = False
            
        self.json_files_cache = []
        self._setup_ui()
        self._connect()
        self._scan_files()
        
        self._log(f"[시스템] REST API Host: {self.api_host}")
        self._log(f"[시스템] 추천파일 경로: {JSON_BASE_DIR}")

    # ---------------- UI ----------------
    def _setup_ui(self):
        main_layout = QVBoxLayout(self)

        # 상단
        top_splitter = QSplitter(Qt.Horizontal)
        top_splitter.setSizes([600, 400])

        # 1. 엔진 선택
        gb_engine = QGroupBox("1. AI 추천 엔진 선택")
        v_eng = QVBoxLayout(gb_engine)
        v_eng_inner = QVBoxLayout()

        self.calendar = CustomCalendar()
        v_eng_inner.addWidget(self.calendar, 0, Qt.AlignTop)

        self.btn_toggle_files = QPushButton("파일 목록 펼치기")
        self.btn_toggle_files.setCheckable(True)
        v_eng_inner.addWidget(self.btn_toggle_files, 0, Qt.AlignTop)

        self.list_container = QWidget()
        list_lay = QVBoxLayout(self.list_container)
        list_lay.setContentsMargins(0, 0, 0, 0)
        self.list_engines = QListWidget()
        list_lay.addWidget(self.list_engines)
        self.list_container.setVisible(True)

        self.btn_toggle_files.setChecked(True)
        self.btn_toggle_files.setText("파일 목록 접기")

        v_eng_inner.addWidget(self.list_container)
        v_eng.addLayout(v_eng_inner)
        top_splitter.addWidget(gb_engine)

        # 2. 추천 상세
        gb_rec = QGroupBox("2. 추천 종목 상세 (현재가 매칭)")
        v_rec = QVBoxLayout(gb_rec)
        # [FIX] 하단 잘림 방지를 위해 여백 추가 (Left, Top, Right, Bottom)
        v_rec.setContentsMargins(5, 20, 5, 10)

        h_rec_btn = QHBoxLayout()
        self.lbl_rec_status = QLabel("선택된 파일 없음")
        self.btn_refresh_price = QPushButton("현재가 갱신 (ka10001)")
        self.btn_copy_text = QPushButton("텍스트 복사")
        self.btn_copy_image = QPushButton("이미지 복사")

        h_rec_btn.addWidget(self.lbl_rec_status)
        h_rec_btn.addStretch()
        h_rec_btn.addWidget(self.btn_refresh_price)
        h_rec_btn.addWidget(self.btn_copy_text)
        h_rec_btn.addWidget(self.btn_copy_image)
        v_rec.addLayout(h_rec_btn)

        self.lbl_period = QLabel("예측기간: -")
        self.lbl_period.setAlignment(Qt.AlignCenter)
        self.lbl_period.setStyleSheet("color: #4a8efc; font-weight: bold;")
        v_rec.addWidget(self.lbl_period)

        self.tbl_rec = QTableWidget(0, 10)
        self.tbl_rec.setHorizontalHeaderLabels(["순위", "종목명", "코드", "시작가", "총등락률", "현재가", "금일등락률", "거래량", "점수", "확률"])
        self.tbl_rec.setEditTriggers(QTableWidget.NoEditTriggers)
        self.tbl_rec.verticalHeader().setVisible(False)
        self.tbl_rec.setMinimumHeight(240)
        self.tbl_rec.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tbl_rec.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.tbl_rec.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.tbl_rec.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.tbl_rec.setSortingEnabled(False)

        vh = self.tbl_rec.verticalHeader()
        vh.setDefaultSectionSize(int(self.tbl_rec.fontMetrics().height() * 1.6))
        
        header = self.tbl_rec.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for col in range(3, self.tbl_rec.columnCount()):
            header.setSectionResizeMode(col, QHeaderView.Stretch)
        header.setSectionsClickable(True)
        self.tbl_rec.setSortingEnabled(True)

        self.tbl_rec.setStyleSheet("QTableWidget::item:selected{background-color: rgba(255,165,0,64);} ")
        self.tbl_rec.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tbl_rec.customContextMenuRequested.connect(self._on_rec_context_menu)
        
        v_rec.addWidget(self.tbl_rec)
        
        v_rec.setStretch(0, 0)
        v_rec.setStretch(1, 0)
        v_rec.setStretch(2, 1)
        top_splitter.addWidget(gb_rec)

        main_layout.addWidget(top_splitter)

        # 중단
        mid_splitter = QSplitter(Qt.Horizontal)
        mid_splitter.setSizes([1, 1])

        gb_yield = QGroupBox("3. 보유 종목 (수익률)")
        v_yld = QVBoxLayout(gb_yield)
        h_yld_btn = QHBoxLayout()
        self.btn_acc_refresh = QPushButton("계좌 잔고 갱신 (kt00004)")
        h_yld_btn.addStretch(); h_yld_btn.addWidget(self.btn_acc_refresh)
        v_yld.addLayout(h_yld_btn)

        self.tbl_yield = QTableWidget(0, 3)
        self.tbl_yield.setHorizontalHeaderLabels(["종목명", "수익률(%)", "손익금"])
        self.tbl_yield.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v_yld.addWidget(self.tbl_yield)
        mid_splitter.addWidget(gb_yield)

        gb_eval = QGroupBox("4. 보유 종목 (평가금액)")
        v_eval = QVBoxLayout(gb_eval)
        self.lbl_deposit = QLabel("예수금: - | 총평가: -")
        self.lbl_deposit.setStyleSheet("color: #6cb8ff; font-weight: bold;")
        v_eval.addWidget(self.lbl_deposit)

        self.tbl_eval = QTableWidget(0, 3)
        self.tbl_eval.setHorizontalHeaderLabels(["종목명", "현재가", "평가금액"])
        self.tbl_eval.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v_eval.addWidget(self.tbl_eval)
        mid_splitter.addWidget(gb_eval)

        main_layout.addWidget(mid_splitter)

        # 하단
        bot_splitter = QSplitter(Qt.Horizontal)
        left_w = QWidget(); left_l = QVBoxLayout(left_w)

        gb_order = QGroupBox("5. 종목 및 주문 설정")
        g = QGridLayout(gb_order)
        self.ed_code = QLineEdit(); self.ed_code.setPlaceholderText("종목코드")
        self.btn_chart = QPushButton("일봉 조회")
        self.cmb_mkt = QComboBox(); self.cmb_mkt.addItems(["KRX", "NXT"])
        self.sp_qty = QSpinBox(); self.sp_qty.setRange(1, 999999); self.sp_qty.setValue(1)
        self.ed_price = QLineEdit(); self.ed_price.setPlaceholderText("0=시장가")
        self.cmb_type = QComboBox(); self.cmb_type.addItems(["시장가(03)", "지정가(00)"])
        self.btn_buy = QPushButton("매수"); self.btn_buy.setStyleSheet("color:#ff6b6b; font-weight:bold;")
        self.btn_sell = QPushButton("매도"); self.btn_sell.setStyleSheet("color:#6cb8ff; font-weight:bold;")

        g.addWidget(QLabel("종목코드"), 0,0); g.addWidget(self.ed_code, 0,1); g.addWidget(self.btn_chart, 0,2)
        g.addWidget(QLabel("거래소"), 1,0); g.addWidget(self.cmb_mkt, 1,1); g.addWidget(QLabel("수량"), 1,2); g.addWidget(self.sp_qty, 1,3)
        g.addWidget(QLabel("단가"), 2,0); g.addWidget(self.ed_price, 2,1); g.addWidget(QLabel("유형"), 2,2); g.addWidget(self.cmb_type, 2,3)
        g.addWidget(self.btn_buy, 3,0,1,2); g.addWidget(self.btn_sell, 3,2,1,2)
        left_l.addWidget(gb_order)

        self.txt_log = QTextEdit(); self.txt_log.setReadOnly(True)
        left_l.addWidget(QLabel("실행 로그"))
        left_l.addWidget(self.txt_log)
        bot_splitter.addWidget(left_w)

        right_w = QWidget(); right_l = QVBoxLayout(right_w)
        self.tbl_chart = QTableWidget(0, 6)
        self.tbl_chart.setHorizontalHeaderLabels(["일자","시가","고가","저가","종가","거래량"])
        self.tbl_chart.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl_chart.setMaximumHeight(220)
        
        gb_chart = QGroupBox("6. 일봉 차트 (ka10081)")
        _gb = QVBoxLayout(gb_chart)
        _gb.addWidget(self.tbl_chart)
        right_l.addWidget(gb_chart)
        bot_splitter.addWidget(right_w)
        bot_splitter.setSizes([400, 600])

        main_layout.addWidget(bot_splitter)

        main_layout.setStretchFactor(top_splitter, 5)
        main_layout.setStretchFactor(mid_splitter, 3)
        main_layout.setStretchFactor(bot_splitter, 2)

    # ---------------- 연결 ----------------
    def _connect(self):
        self.calendar.clicked.connect(self._on_date_clicked)
        self.list_engines.itemClicked.connect(self._on_engine_selected)
        self.btn_toggle_files.toggled.connect(self._toggle_file_list)
        self.btn_refresh_price.clicked.connect(self._refresh_prices_ka10001)
        self.btn_copy_text.clicked.connect(self._copy_table_all)
        self.btn_copy_image.clicked.connect(self._copy_table_image)
        self.btn_acc_refresh.clicked.connect(self._refresh_account_kt00004)
        self.btn_chart.clicked.connect(self._on_chart_query)
        self.btn_buy.clicked.connect(lambda: self._on_order("BUY"))
        self.btn_sell.clicked.connect(lambda: self._on_order("SELL"))
        self.tbl_rec.cellClicked.connect(self._on_rec_cell_clicked)

    def _get_token(self):
        if not self.token_avail:
            self._log("[오류] Token Manager가 초기화되지 않았습니다.")
            return None
        return self.token_manager.get_token()

    def _log(self, msg):
        self.txt_log.append(msg)
        self.txt_log.verticalScrollBar().setValue(self.txt_log.verticalScrollBar().maximum())

    def _on_rec_context_menu(self, pos):
        menu = QMenu(self)
        act_copy_sel = menu.addAction("선택 복사")
        act_copy_all = menu.addAction("전체 표 복사")
        action = menu.exec_(self.tbl_rec.viewport().mapToGlobal(pos))
        if action == act_copy_sel:
            self._copy_table_ranges(self.tbl_rec.selectedRanges())
        elif action == act_copy_all:
            self._copy_table_all()

    def _on_rec_cell_clicked(self, row, col):
        item = self.tbl_rec.item(row, 2)
        if item:
            self.ed_code.setText(item.text())

    def _copy_table_ranges(self, ranges):
        if not ranges:
            return
        lines = []
        for rng in ranges:
            for row in range(rng.topRow(), rng.bottomRow() + 1):
                row_vals = []
                for col in range(rng.leftColumn(), rng.rightColumn() + 1):
                    item = self.tbl_rec.item(row, col)
                    row_vals.append(item.text() if item else "")
                lines.append("\t".join(row_vals))
        if lines:
            QApplication.clipboard().setText("\n".join(lines))

    def _copy_table_all(self):
        if self.tbl_rec.rowCount() == 0:
            return
        lines = []
        # 1) 예측기간 라벨
        if self.lbl_period.text().strip():
            period_row = [self.lbl_period.text().strip()] + [""] * (self.tbl_rec.columnCount() - 1)
            lines.append("\t".join(period_row))
        # 2) 헤더
        headers = [self.tbl_rec.horizontalHeaderItem(c).text() for c in range(self.tbl_rec.columnCount())]
        lines.append("\t".join(headers))
        # 3) 데이터 행
        for r in range(self.tbl_rec.rowCount()):
            row_vals = []
            for c in range(self.tbl_rec.columnCount()):
                item = self.tbl_rec.item(r, c)
                row_vals.append(item.text() if item else "")
            lines.append("\t".join(row_vals))
        QApplication.clipboard().setText("\n".join(lines))

    def _copy_table_image(self):
        if self.tbl_rec.rowCount() == 0:
            return
        table = self.tbl_rec
        period_text = self.lbl_period.text().strip()
        
        # 계산: 테이블 전체 내용 크기
        width_full = sum(table.columnWidth(c) for c in range(table.columnCount())) + table.frameWidth() * 2
        height_full = table.horizontalHeader().height() + sum(table.rowHeight(r) for r in range(table.rowCount())) + table.frameWidth() * 2
        fm = table.fontMetrics()
        pad = 8
        text_h = (fm.height() + pad * 2) if period_text else 0
        
        width_final = width_full
        if period_text:
            width_final = max(width_full, self.lbl_period.fontMetrics().horizontalAdvance(period_text) + pad * 2)
            
        # 테이블 크기/스크롤 임시 조정 후 렌더링
        old_size = table.size()
        old_min = table.minimumSize()
        old_hbar = table.horizontalScrollBarPolicy()
        old_vbar = table.verticalScrollBarPolicy()
        
        painter = None
        try:
            table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            table.setMinimumSize(width_full, height_full)
            table.resize(width_full, height_full)
            
            pix = QPixmap(width_final, text_h + height_full)
            pix.fill(table.palette().window().color())
            
            painter = QPainter(pix)
            if period_text:
                painter.setPen(QColor("#4a8efc"))
                painter.setFont(self.lbl_period.font())
                painter.drawText(QRect(0, pad, width_final, fm.height()), Qt.AlignCenter, period_text)
                
            table.render(painter, QPoint(0, text_h))
            QApplication.clipboard().setPixmap(pix)
        finally:
            if painter is not None and painter.isActive():
                painter.end()
            table.setMinimumSize(old_min)
            table.resize(old_size)
            table.setHorizontalScrollBarPolicy(old_hbar)
            table.setVerticalScrollBarPolicy(old_vbar)

    def _toggle_file_list(self, checked: bool):
        self.list_container.setVisible(checked)
        self.btn_toggle_files.setText("파일 목록 접기" if checked else "파일 목록 펼치기")

    # ---------------- 파일 스캔 ----------------
    def _scan_files(self):
        if not os.path.exists(JSON_BASE_DIR):
            self._log(f"[오류] 폴더 없음: {JSON_BASE_DIR}")
            return
        files = glob.glob(os.path.join(JSON_BASE_DIR, "*.json"))
        
        self.json_files_cache = []
        counts_str: Dict[str,int] = {}
        
        for fpath in files:
            if os.path.getsize(fpath) == 0:
                self._log(f"[스킵] 빈 파일: {os.path.basename(fpath)}")
                continue
            fname = os.path.basename(fpath)
            m_date = re.search(r"(\d{6})\.json$", fname)
            m_h = re.search(r"_h(\d+)_", fname)
            
            if not m_date:
                continue
                
            date_str = "20" + m_date.group(1)
            try:
                qdate = QDate.fromString(date_str, "yyyyMMdd")
                h = int(m_h.group(1)) if m_h else 5
                
                start_pd = (pd.Timestamp(date_str) + BDay(1)).date()
                end_pd   = (pd.Timestamp(date_str) + BDay(h)).date()
                start_q  = QDate(start_pd.year, start_pd.month, start_pd.day)
                end_q    = QDate(end_pd.year, end_pd.month, end_pd.day)
                
                self.json_files_cache.append({
                    "date": qdate, "path": fpath, "name": fname,
                    "win_start": start_q, "win_end": end_q, "h": h
                })
                
                start_str = start_pd.strftime("%Y-%m-%d")
                counts_str[start_str] = counts_str.get(start_str, 0) + 1
            except Exception:
                continue
                
        self.calendar.set_engine_counts(counts_str)
        self.calendar.set_window_dates(set())
        
        today = QDate.currentDate()
        self.calendar.setSelectedDate(today)
        self._on_date_clicked(today)

    def _on_date_clicked(self, date: QDate):
        self.list_engines.clear()
        matched = [
            f for f in self.json_files_cache
            if date == f.get("win_start", f["date"])
        ]
        for m in matched:
            item = QListWidgetItem(m["name"])
            item.setData(Qt.UserRole, m["path"])
            self.list_engines.addItem(item)

    def _on_engine_selected(self, item: QListWidgetItem):
        fpath = item.data(Qt.UserRole)
        self.lbl_rec_status.setText(f"파일: {item.text()}")
        start_pd = end_pd = None
        
        try:
            m_h = re.search(r"_h(\d+)_", item.text())
            m_d = re.search(r"(\d{6})\.json$", item.text())
            if m_d:
                date_str = "20" + m_d.group(1)
                h = int(m_h.group(1)) if m_h else 5
                start_pd = (pd.Timestamp(date_str) + BDay(1)).date()
                end_pd   = (pd.Timestamp(date_str) + BDay(h)).date()
                self.lbl_rec_status.setText(f"파일: {item.text()}")
                self.lbl_period.setText(f"예측기간 : {start_pd.strftime('%Y년 %m월 %d일')} 부터 ~ {end_pd.strftime('%Y년 %m월 %d일')} 까지")
                start_q = QDate(start_pd.year, start_pd.month, start_pd.day)
                end_q = QDate(end_pd.year, end_pd.month, end_pd.day)
                
                start_str = start_pd.strftime("%Y-%m-%d")
                same_start = sum(
                    1 for f in self.json_files_cache
                    if f.get("win_start") and f["win_start"].toString("yyyy-MM-dd") == start_str
                )
                counts = {start_str: same_start}
                win_dates = set(d.date().strftime("%Y-%m-%d") for d in pd.date_range(start=start_pd, end=end_pd, freq=BDay()))
                self.calendar.set_engine_counts(counts)
                self.calendar.set_window_dates(win_dates)
                self.calendar.set_highlight_range(start_q, start_q)
        except Exception:
            self.lbl_period.setText("예측기간: -")
            pass

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            top10 = data.get("top10", [])

            was_sorting = self.tbl_rec.isSortingEnabled()
            if was_sorting:
                self.tbl_rec.setSortingEnabled(False)
            
            self.tbl_rec.setRowCount(0)
            if not start_pd or not end_pd:
                self.lbl_period.setText("예측기간: -")

            for row in top10:
                rank_raw = row.get("순위", row.get("rank", ""))
                try:
                    rank_val = int(rank_raw)
                except Exception:
                    rank_val = rank_raw
                rank = str(rank_raw)

                name = str(row.get("종목명", row.get("name", "")))
                code = str(row.get("종목코드", row.get("code", ""))).zfill(6)
                
                price_rec = row.get("현재가", row.get("close", 0))
                try:
                    price_rec_f = float(price_rec) if price_rec is not None else 0.0
                except Exception:
                    price_rec_f = 0.0

                score = row.get("동시적용 기대수익(%)", row.get("예측수익률(%)", 0))
                prob  = row.get("상승확률(%)", 0)

                r = self.tbl_rec.rowCount()
                self.tbl_rec.insertRow(r)

                # [FIX] NumericItem 사용 - 순위
                rank_item = NumericItem(rank)
                rank_item.setData(Qt.UserRole, rank_val)
                rank_item.setData(Qt.EditRole, rank_val)
                self.tbl_rec.setItem(r, 0, rank_item)

                name_item = QTableWidgetItem(name)
                name_item.setForeground(QColor("#EAEAEA"))
                name_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
                self.tbl_rec.setItem(r, 1, name_item)

                code_item = QTableWidgetItem(code)
                code_item.setData(Qt.EditRole, int(code) if code.isdigit() else code)
                self.tbl_rec.setItem(r, 2, code_item)

                # [FIX] NumericItem 사용 - 시작가
                start_item = NumericItem(f"{price_rec_f:,.0f}")
                start_item.setData(Qt.EditRole, price_rec_f)
                start_item.setData(Qt.UserRole, price_rec_f)
                self.tbl_rec.setItem(r, 3, start_item)

                self.tbl_rec.setItem(r, 4, QTableWidgetItem("-"))
                self.tbl_rec.setItem(r, 5, QTableWidgetItem("-"))
                self.tbl_rec.setItem(r, 6, QTableWidgetItem("-"))
                
                # [FIX] NumericItem 사용 - 거래량
                vol_item = NumericItem("-")
                vol_item.setData(Qt.EditRole, 0)
                vol_item.setData(Qt.UserRole, 0)
                self.tbl_rec.setItem(r, 7, vol_item)

                # [FIX] NumericItem 사용 - 점수
                try:
                    score_val = float(score)
                    score_item = NumericItem(f"{score_val:.2f}")
                    score_item.setData(Qt.EditRole, score_val)
                    score_item.setData(Qt.UserRole, score_val)
                except Exception:
                    score_item = NumericItem(str(score))
                self.tbl_rec.setItem(r, 8, score_item)

                # [FIX] NumericItem 사용 - 확률
                try:
                    prob_val = float(str(prob).replace("%", ""))
                    prob_item = NumericItem(f"{prob_val:.2f}%")
                    prob_item.setData(Qt.EditRole, prob_val)
                    prob_item.setData(Qt.UserRole, prob_val)
                except Exception:
                    prob_item = NumericItem(str(prob))
                self.tbl_rec.setItem(r, 9, prob_item)

                self.tbl_rec.item(r, 0).setTextAlignment(Qt.AlignCenter)
                self.tbl_rec.item(r, 2).setTextAlignment(Qt.AlignCenter)
                
                for c in (3,4,5,6,7,8,9):
                    it = self.tbl_rec.item(r, c)
                    if it:
                        it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

            self.tbl_rec.resizeRowsToContents()
            self._resize_rec_table_to_contents()
            self.tbl_rec.scrollToTop()
            
            self.tbl_rec.setSortingEnabled(True)
            self.tbl_rec.sortItems(0, Qt.AscendingOrder)
            
            self._log(f"[파일로드] {len(top10)}개 종목 로딩 완료")
            self._refresh_prices_ka10001()

        except Exception as e:
            self._log(f"[로드오류] {os.path.basename(fpath)} : {e}")

    def _resize_rec_table_to_contents(self):
        header = self.tbl_rec.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for col in range(3, self.tbl_rec.columnCount()):
            header.setSectionResizeMode(col, QHeaderView.Stretch)

    def _refresh_prices_ka10001(self):
        data_rows = [r for r in range(self.tbl_rec.rowCount())]
        if not data_rows:
            return
            
        token = self._get_token()
        if not token:
            return
            
        url = f"{self.api_host}/api/dostk/stkinfo"
        headers = {"api-id": "ka10001", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        self.btn_refresh_price.setEnabled(False)
        orig_text = self.btn_refresh_price.text()
        self.btn_refresh_price.setText("갱신 중...")
        self._log(f"현재가 조회 시작 ({len(data_rows)}건)...")
        
        tasks = []
        for r in data_rows:
            code_item = self.tbl_rec.item(r, 2)
            price_item = self.tbl_rec.item(r, 3)
            if not code_item or not price_item:
                continue
            code = code_item.text()
            rec_price = float(price_item.data(Qt.UserRole) or 0.0)
            tasks.append((r, code, rec_price))
            
        if not tasks:
            self.btn_refresh_price.setText(orig_text)
            self.btn_refresh_price.setEnabled(True)
            return

        with ThreadPoolExecutor(max_workers=min(10, len(tasks))) as ex:
            futures = [ex.submit(debug_post, url, headers, {"stk_cd": code}) for _, code, _ in tasks]
            
            for (r, code, rec_price), fut in zip(tasks, futures):
                res = fut.result()
                if res["status"] != 200:
                    self._log(f"[ka10001 오류] {code} HTTP {res['status']} : {res.get('json')}")
                    continue
                    
                if res["json"] is None:
                    self._log(f"[ka10001 오류] {code} 응답 없음")
                    continue
                    
                ret_code = str(res["json"].get("return_code", "0"))
                ret_msg = res["json"].get("return_msg", "")
                if ret_code not in ("0", "0000", "OK", "ok"):
                    self._log(f"[ka10001 오류] {code} return_code={ret_code} msg={ret_msg}")
                    continue
                    
                out = res["json"].get("output") or res["json"]
                if not out:
                    keys = list(res["json"].keys()) if isinstance(res["json"], dict) else type(res["json"]).__name__
                    self._log(f"[ka10001 오류] {code} output 비어있음 keys={keys}")
                    continue
                    
                curr_str = (
                    out.get("stck_prpr") or out.get("cur_prc") or out.get("close_pric") or
                    out.get("tradePrice") or out.get("prpr") or out.get("last")
                )
                try:
                    curr_val = float(str(curr_str).replace(",", "")) if curr_str is not None else None
                except Exception:
                    curr_val = None
                    
                if curr_val is None:
                    self._log(f"[ka10001 오류] {code} 현재가 없음")
                    continue
                    
                curr_val = abs(curr_val)
                # [FIX] NumericItem 사용
                curr_item = NumericItem(f"{curr_val:,.0f}")
                curr_item.setData(Qt.UserRole, curr_val)
                curr_item.setData(Qt.EditRole, curr_val)
                self.tbl_rec.setItem(r, 5, curr_item)
                self.tbl_rec.item(r, 5).setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                
                if rec_price > 0:
                    rate_tot = ((curr_val - rec_price) / rec_price) * 100.0
                    item_rate_tot = NumericItem(f"{rate_tot:+.2f}%")
                    item_rate_tot.setData(Qt.UserRole, rate_tot)
                    item_rate_tot.setData(Qt.EditRole, rate_tot)
                    item_rate_tot.setForeground(self.color_pos if rate_tot > 0 else self.color_neg)
                    item_rate_tot.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    self.tbl_rec.setItem(r, 4, item_rate_tot)
                    
                day_rate_str = out.get("prdy_ctrt") or out.get("flu_rt") or out.get("day_change") or out.get("fluctuationRate")
                day_rate_val = None
                try:
                    if day_rate_str is not None:
                        day_rate_val = float(str(day_rate_str).replace("%", ""))
                except Exception:
                    day_rate_val = None
                    
                if day_rate_val is not None:
                    item_day = NumericItem(f"{day_rate_val:+.2f}%")
                    item_day.setData(Qt.UserRole, day_rate_val)
                    item_day.setData(Qt.EditRole, day_rate_val)
                    item_day.setForeground(self.color_pos if day_rate_val > 0 else self.color_neg)
                    item_day.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    self.tbl_rec.setItem(r, 6, item_day)
                    
                vol_candidates = [
                    out.get("trde_qty"), out.get("volume"), out.get("accumulatedVolume"),
                    out.get("acml_vol"), out.get("tot_vlm"), out.get("trd_qty"), out.get("trdvol")
                ]
                vol_str = next((v for v in vol_candidates if v not in (None, "")), None)
                if vol_str is not None:
                    try:
                        cleaned = re.sub(r"[^\d.-]", "", str(vol_str))
                        vol_val = float(cleaned) if cleaned != "" else None
                        text = f"{vol_val:,.0f}" if vol_val is not None else str(vol_str)
                        
                        vol_item = NumericItem(text)
                        vol_item.setData(Qt.UserRole, vol_val)
                        vol_item.setData(Qt.EditRole, vol_val)
                        self.tbl_rec.setItem(r, 7, vol_item)
                        self.tbl_rec.item(r, 7).setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    except Exception:
                        pass
                else:
                    self.tbl_rec.setItem(r, 7, NumericItem("-"))
                    
        self.btn_refresh_price.setText(orig_text)
        self.btn_refresh_price.setEnabled(True)
        self._resize_rec_table_to_contents()
        self._log("현재가 갱신 완료")

    def _refresh_account_kt00004(self):
        token = self._get_token()
        if not token: return
        url = f"{self.api_host}/api/dostk/acnt"
        headers = {"api-id": "kt00004", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {"qry_tp": "0", "dmst_stex_tp": "KRX"}
        
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
            code = (s.get('stk_cd', '') or '').strip().lstrip('A')
            pl_rate = float(s.get('pl_rt', 0) or 0)
            pl_amt = int(s.get('pl_amt', 0) or 0)
            cur_prc = int(s.get('cur_prc', 0) or 0)
            evlt_amt = int(s.get('evlt_amt', 0) or 0)
            
            r1 = self.tbl_yield.rowCount()
            self.tbl_yield.insertRow(r1)
            self.tbl_yield.setItem(r1, 0, QTableWidgetItem(name))
            self.tbl_yield.item(r1, 0).setData(Qt.UserRole, code)
            
            item_rt = QTableWidgetItem(f"{pl_rate:+.2f}%")
            item_rt.setForeground(self.color_pos if pl_rate > 0 else self.color_neg)
            item_rt.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tbl_yield.setItem(r1, 1, item_rt)
            
            amt_item = QTableWidgetItem(f"{pl_amt:,}")
            amt_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tbl_yield.setItem(r1, 2, amt_item)
            
            r2 = self.tbl_eval.rowCount()
            self.tbl_eval.insertRow(r2)
            self.tbl_eval.setItem(r2, 0, QTableWidgetItem(name))
            self.tbl_eval.item(r2, 0).setData(Qt.UserRole, code)
            
            prc_item = QTableWidgetItem(f"{cur_prc:,}")
            prc_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            evl_item = QTableWidgetItem(f"{evlt_amt:,}")
            evl_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tbl_eval.setItem(r2, 1, prc_item)
            self.tbl_eval.setItem(r2, 2, evl_item)
            
        self._log(f"계좌 조회 완료 ({len(stocks)}종목)")

    # ---------------- 차트/주문 ----------------
    def _on_chart_query(self):
        code = self.ed_code.text().strip()
        if not code:
            return self._log("종목코드를 입력하세요.")
        token = self._get_token()
        if not token: return
        
        url = f"{self.api_host}/api/dostk/chart"
        headers = {"api-id": "ka10081", "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {"stk_cd": code, "base_dt": QDate.currentDate().toString("yyyyMMdd"), "term_cnt": "60", "upd_stkpc_tp": "1"}
        
        res = debug_post(url, headers, body)
        if res["status"] != 200:
            return self._log(f"[ka10081 오류] HTTP {res['status']} : {res.get('json')}")
        if res["json"] is None:
            return self._log("[ka10081 오류] 응답 없음")
            
        ret_code = str(res["json"].get("return_code", "0"))
        ret_msg = res["json"].get("return_msg", "")
        if ret_code not in ("0", "0000", "OK", "ok"):
            return self._log(f"[ka10081 오류] return_code={ret_code} msg={ret_msg}")
            
        items = res["json"].get("output")
        if not items:
            if isinstance(res["json"], list):
                items = res["json"]
            else:
                items = (
                    res["json"].get("chart")
                    or res["json"].get("data")
                    or res["json"].get("stk_dt_pole_chart_qry")
                    or []
                )
        if not items:
            keys = list(res["json"].keys()) if isinstance(res["json"], dict) else type(res["json"]).__name__
            return self._log(f"[ka10081 오류] output 비어있음 keys={keys}")
            
        norm = normalize_ohlcv(items)[:10]
        self.tbl_chart.setRowCount(0)
        for r in norm:
            idx = self.tbl_chart.rowCount()
            self.tbl_chart.insertRow(idx)
            row_items = [
                QTableWidgetItem(r['date']),
                QTableWidgetItem(r['open']),
                QTableWidgetItem(r['high']),
                QTableWidgetItem(r['low']),
                QTableWidgetItem(r['close']),
                QTableWidgetItem(r['volume']),
            ]
            for c, it in enumerate(row_items):
                it.setTextAlignment(Qt.AlignCenter)
                self.tbl_chart.setItem(idx, c, it)
        self._log(f"차트 {len(norm)}건 조회 완료")

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
            trde_tp = "03"; price = "0"
        if not price: price = "0"
        
        headers = {"api-id": api_id, "authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "dmst_stex_tp": self.cmb_mkt.currentText(),
            "stk_cd": code, "ord_qty": str(self.sp_qty.value()),
            "ord_uv": price, "trde_tp": trde_tp, "cond_uv": ""
        }
        
        res = debug_post(url, headers, body)
        if res["status"] == 200:
            self._log(f"[주문성공] {side} {code} {price}원")
        else:
            self._log(f"[주문실패] {res.get('json',{}).get('return_msg')}")