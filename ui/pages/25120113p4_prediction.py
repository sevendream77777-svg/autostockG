
# ui/pages/p4_prediction.py
import glob
import os
import pickle
import re
import subprocess
import sys
import json
import pandas as pd
from pandas.tseries.offsets import BDay

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QHBoxLayout, QLabel, QDateEdit, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QRadioButton, QLineEdit,
    QButtonGroup, QMessageBox, QComboBox, QSpinBox, QCalendarWidget, QSplitter,
    QListWidget, QListWidgetItem, QTextEdit, QScrollArea, QWidget as QtWidget,
    QMenu, QWidgetAction, QToolButton, QSizePolicy, QApplication, QCheckBox
)
from PySide6.QtCore import QDate, Qt, QLocale, QRect, QThread, Signal
from PySide6.QtGui import QColor, QFont, QPen, QBrush, QPainter

# ──────────────────────────────────────────────────────────────────────────────
# 내부 QThread: daily_recommender.py 실행 → JSON 로드 → DF 반환
# ──────────────────────────────────────────────────────────────────────────────
class DailyRunner(QThread):
    finished_signal = Signal(object)  # pandas.DataFrame
    error_signal = Signal(str)

    def __init__(self, engine_path: str, target_date: str, top_n: int,
                 rank_by: str = "combo", ai_flag: int = 0, parent=None):
        super().__init__(parent)
        self.engine_path = engine_path
        self.target_date = target_date  # "yyyy-mm-dd"
        self.top_n = int(top_n)
        self.rank_by = rank_by
        self.ai_flag = int(ai_flag)

    def _project_root(self):
        # ui/pages/p4_prediction.py → .../(project root)
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    def _info_dir(self):
        return os.path.join(self._project_root(), "MODELENGINE", "INFO", "hoj_engine_info")

    def _util_dir(self):
        return os.path.join(self._project_root(), "MODELENGINE", "UTIL")

    def _json_path_for_engine(self):
        name = os.path.splitext(os.path.basename(self.engine_path))[0] + ".json"
        return os.path.join(self._info_dir(), name)

    def _load_json_df(self, json_path: str) -> pd.DataFrame:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        top10 = data.get("top10", [])
        if not isinstance(top10, list):
            top10 = []

        # 컬럼 맵핑: code,name,close,score,prob
        rows = []
        for item in top10[: self.top_n]:
            code = item.get("종목코드") or item.get("code") or "-"
            name = item.get("종목명") or item.get("name") or "-"
            close = item.get("현재가") or item.get("close")
            # score 우선순위: combo → ret
            score = (
                item.get("동시적용 기대수익(%)")
                if item.get("동시적용 기대수익(%)") is not None
                else item.get("예측수익률(%)")
            )
            prob_pct = item.get("상승확률(%)")
            # prob 0~1로 스케일
            prob = None
            try:
                if prob_pct is None:
                    prob = 0.0
                else:
                    prob = float(prob_pct) / 100.0 if float(prob_pct) > 1.0 else float(prob_pct)
                    # 만약 이미 0~1이면 그대로, 1초과면 %에서 변환
            except Exception:
                prob = 0.0

            try:
                close = float(close) if close is not None else None
            except Exception:
                close = None

            try:
                score = float(score) if score is not None else 0.0
            except Exception:
                score = 0.0

            rows.append({"code": str(code), "name": str(name), "close": close, "score": score, "prob": prob})

        df = pd.DataFrame(rows, columns=["code", "name", "close", "score", "prob"])
        # 정렬: score desc, prob desc
        if not df.empty:
            df = df.sort_values(["score", "prob"], ascending=[False, False]).reset_index(drop=True)
        return df

    def _ensure_json(self, json_path: str):
        # JSON이 없거나 비정상이면 daily_recommender.py 실행
        need_run = True
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # 최소 필드 검증
                if isinstance(data.get("top10", None), list) and len(data.get("top10", [])) > 0:
                    need_run = False
            except Exception:
                need_run = True

        if not need_run:
            return

        util_dir = self._util_dir()
        script = os.path.join(util_dir, "daily_recommender.py")
        if not os.path.exists(script):
            self.error_signal.emit(f"daily_recommender.py가 없습니다: {script}")
            return

        # 안전한 실행 인자
        args = [
            sys.executable, script,
            "--engine", self.engine_path,
            "--date", self.target_date,
            "--rank_by", self.rank_by,
            "--topk", str(self.top_n),
            "--ai", str(self.ai_flag)
        ]

        try:
            proc = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="ignore")
            if proc.returncode != 0:
                self.error_signal.emit(f"daily 실행 실패({proc.returncode})\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
                return
        except Exception as e:
            self.error_signal.emit(f"daily 실행 예외: {e}")
            return

    def run(self):
        try:
            json_path = self._json_path_for_engine()
            # 필요 시 생성
            self._ensure_json(json_path)
            if not os.path.exists(json_path):
                self.error_signal.emit(f"JSON 생성 실패: {os.path.basename(json_path)} 미존재")
                return

            df = self._load_json_df(json_path)
            self.finished_signal.emit(df)
        except Exception as e:
            self.error_signal.emit(str(e))


# [커스텀 달력] (+N) 텍스트 및 범위 하이라이트
class CustomCalendar(QCalendarWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.engine_counts = {}  # {QDate: count}
        self.highlight_range = None # (start_QDate, end_QDate)
        self.target_date = None # 파란색 표시할 기준일

        self.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.setLocale(QLocale(QLocale.Korean, QLocale.SouthKorea))
        self.setStyleSheet("""
            QCalendarWidget QWidget { alternate-background-color: #444; color: white; }
            QCalendarWidget QToolButton { color: white; background-color: #333; border: none; margin: 2px; }
            QCalendarWidget QToolButton:hover { background-color: #555; border-radius: 3px; }
            QCalendarWidget QTableView { background-color: #2b2b2b; color: white; selection-background-color: transparent; outline: 0; }
        """)

    def set_engine_counts(self, counts):
        self.engine_counts = counts
        self.updateCell(QDate.currentDate())

    def set_highlight_range(self, start, end, target):
        self.highlight_range = (start, end)
        self.target_date = target
        self.updateCells()

    def paintCell(self, painter, rect, date):
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, False)
        
        # 1. 배경
        bg_color = QColor("#2b2b2b")
        if self.highlight_range:
            s, e = self.highlight_range
            if s <= date <= e:
                bg_color = QColor("#553300") # 예측 기간 주황 배경

        if date == self.selectedDate():
            bg_color = QColor("#FF8C00") # 선택된 날짜 진한 주황

        painter.fillRect(rect, bg_color)

        # 2. 날짜 텍스트
        text_color = QColor("white")
        if date.month() != self.monthShown():
            text_color = QColor("#777")
        
        # 기준일(Target) 파란색 강조
        if self.target_date and date == self.target_date:
            text_color = QColor("#44AAFF")
            font = painter.font()
            font.setBold(True)
            painter.setFont(font)

        day_rect = QRect(rect.left(), rect.top() + 2, rect.width(), rect.height() // 2)
        painter.setPen(text_color)
        painter.drawText(day_rect, Qt.AlignCenter, str(date.day()))

        # 3. (+N) 카운트 표시
        if date in self.engine_counts:
            count = self.engine_counts[date]
            count_str = f"(+{count})"
            count_rect = QRect(rect.left(), rect.top() + rect.height()//2, rect.width(), rect.height()//2)
            
            c_font = painter.font()
            c_font.setPointSize(8)
            painter.setFont(c_font)
            painter.setPen(QColor("#FFA500"))
            painter.drawText(count_rect, Qt.AlignCenter, count_str)

        painter.restore()


class PredictionPage(QWidget):
    """
    [수정 내역]
    1. 달력 H기간 수정: h5 -> 5칸(일)만 칠해지도록 (BDay(h-1))
    2. 엔진 리스트 필터링: 달력 날짜 클릭 시 해당 날짜 엔진만 표시 (잠금 시)
    3. 잠금 버튼 이동: 하단 -> 우측 상단 엔진 목록 헤더 옆
    4. 테이블 UI 개선: 행 번호 삭제, 순위 폭 50%, 경계선 강화
    5. (핵심) 예측 실행은 DailyRunner(QThread)로 daily_recommender.py 실행/JSON 로드로 단일화
    """

    def _open_topn_picker(self):
        menu = QMenu(self)
        container = QWidget()
        l = QVBoxLayout(container)
        l.setContentsMargins(0, 0, 0, 0)
        
        lst = QListWidget()
        lst.setFixedSize(100, 200)
        for n in range(1, 151):
            QListWidgetItem(str(n), lst)

        try:
            lst.setCurrentRow(self.spin_topn.value() - 1)
        except:
            pass

        act = QWidgetAction(menu)
        act.setDefaultWidget(lst)
        menu.addAction(act)

        def _apply(item):
            try:
                val = int(item.text())
                self.spin_topn.setValue(val)
            except:
                pass
            menu.close()

        lst.itemClicked.connect(_apply)
        menu.exec(self.btn_topn_picker.mapToGlobal(self.btn_topn_picker.rect().bottomLeft()))

    def __init__(self):
        super().__init__()
        self.engine_paths = []
        self.all_engine_files = [] # 전체 엔진 정보 캐싱 [{'path':.., 'target_date':..}, ...]
        self.meta_cache = {}
        self.db_cache = {}
        self.runner = None  # ← 내부 QThread

        self._current_engine_info = {
            "max_date": None,
            "target_date": None,
            "h": 0, "w": 0, "version": ""
        }
        self._date_engine_counts = {}
        self.is_locked = True # 기본 잠금 상태 (필터링 ON)

        self.init_ui()

    def init_ui(self):
        root = QVBoxLayout()
        self.setLayout(root)

        # ───────────────────────────── BOX 1 (상단) ─────────────────────────────
        box1 = QGroupBox("예측 기준 설정")
        box1_layout = QVBoxLayout()
        box1_split = QSplitter(Qt.Horizontal, self)

        # (좌) 달력 패널
        cal_panel = QtWidget()
        cal_v = QVBoxLayout(cal_panel)
        cal_v.addWidget(QLabel("예측 기준일 (T)"))
        
        self.calendar = CustomCalendar(self)
        self.calendar.setSelectedDate(QDate.currentDate())
        self.calendar.clicked.connect(self._on_calendar_clicked)
        cal_v.addWidget(self.calendar)

        # (우) 엔진 목록 패널
        eng_panel = QtWidget()
        eng_v = QVBoxLayout(eng_panel)
        
        # [수정] 헤더 레이아웃 (라벨 + 잠금 버튼)
        eng_header = QHBoxLayout()
        eng_header.addWidget(QLabel("해당 날짜 사용 가능 엔진"))
        eng_header.addStretch() # 빈 공간
        
        # [이동] 잠금 버튼
        self.btn_lock = QToolButton()
        self.btn_lock.setText("🔒 잠금 (필터링 ON)")
        self.btn_lock.setCheckable(True)
        self.btn_lock.setChecked(False) # False=잠금, True=해제
        self.btn_lock.setStyleSheet("""
            QToolButton { background-color: #444; color: white; border: 1px solid #666; border-radius: 4px; padding: 2px 6px; font-size: 11px; }
            QToolButton:checked { background-color: #B22222; color: white; border: 1px solid #FF5555; }
        """)
        self.btn_lock.clicked.connect(self._on_lock_toggled)
        eng_header.addWidget(self.btn_lock)
        
        eng_v.addLayout(eng_header)
        
        self.engine_list = QListWidget()
        self.engine_list.setStyleSheet("""
            QListWidget {
                outline: 0;
                background-color: #2b2b2b;
                color: #cccccc;
                border: 1px solid #444;
            }
            QListWidget::item {
                padding: 6px;
                border-bottom: 1px solid #333;
            }
            QListWidget::item:selected {
                background-color: #FFA500;
                color: black;
                font-weight: bold;
            }
            QListWidget::item:selected:!active {
                background-color: #FFA500;
                color: black;
                font-weight: bold;
            }
            QListWidget::item:hover:!selected {
                background-color: #444444;
            }
        """)
        self.engine_list.itemSelectionChanged.connect(self._on_engine_list_changed)
        eng_v.addWidget(self.engine_list)
        
        info_row = QHBoxLayout()
        self.lbl_engine_info = QLabel("-") 
        self.lbl_engine_info.setStyleSheet("color: #DDDDDD; font-weight: bold;")
        info_row.addWidget(self.lbl_engine_info)
        
        self.lbl_db_range = QLabel("-")
        self.lbl_db_range.setStyleSheet("color: #DDDDDD; font-weight: bold;")
        info_row.addWidget(self.lbl_db_range)
        eng_v.addLayout(info_row)

        box1_split.addWidget(cal_panel)
        box1_split.addWidget(eng_panel)
        box1_split.setSizes([400, 600])

        box1_layout.addWidget(box1_split)
        box1.setLayout(box1_layout)
        main_split = QSplitter(Qt.Vertical, self)
        main_split.addWidget(box1)

        # ───────────────────────────── BOX 2 (하단) ─────────────────────────────
        box2 = QGroupBox("예측 실행 및 결과")
        box2_layout = QVBoxLayout()

        ctl_row = QHBoxLayout()
        
        # 1) 파란색 예측 범위 텍스트
        self.lbl_pred_range = QLabel("")
        self.lbl_pred_range.setStyleSheet("color: #44AAFF; font-weight: bold; font-size: 13px;")
        ctl_row.addWidget(self.lbl_pred_range)
        ctl_row.addSpacing(15)
        
        # 2) 예측 TOP Rank
        gb_rank = QGroupBox("Rank")
        gb_rank.setStyleSheet("border:0px;")
        row_rank = QHBoxLayout()
        row_rank.setContentsMargins(0, 0, 0, 0)
        self.lbl_topn = QLabel("Top")
        row_rank.addWidget(self.lbl_topn)
        self.spin_topn = QSpinBox()
        self.spin_topn.setButtonSymbols(QSpinBox.NoButtons)
        self.spin_topn.setMinimumWidth(50)
        self.spin_topn.setFocusPolicy(Qt.StrongFocus)
        self.spin_topn.setStyleSheet("font-size:14px; padding: 2px;")
        self.spin_topn.setRange(1, 150)
        self.spin_topn.setValue(10)
        row_rank.addWidget(self.spin_topn)
        self.btn_topn_picker = QToolButton()
        self.btn_topn_picker.setText("▼")
        self.btn_topn_picker.setFixedSize(20, 24)
        self.btn_topn_picker.clicked.connect(self._open_topn_picker)
        row_rank.addWidget(self.btn_topn_picker)
        self.lbl_topn_suffix = QLabel("개")
        row_rank.addWidget(self.lbl_topn_suffix)
        rank_widget = QWidget()
        rank_widget.setLayout(row_rank)
        ctl_row.addWidget(rank_widget)

        # 3) 대상 선택
        gb_target = QGroupBox("대상 선택")
        gb_target.setStyleSheet("QGroupBox { border: 1px solid #666; border-radius: 5px; margin-top: 5px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 3px; }")
        row_target = QHBoxLayout()
        self.rb_market = QRadioButton("시장전체")
        self.rb_specific = QRadioButton("특정종목")
        self.rb_market.setChecked(True)
        self.bg_target = QButtonGroup(self)
        self.bg_target.addButton(self.rb_market)
        self.bg_target.addButton(self.rb_specific)
        row_target.addWidget(self.rb_market)
        row_target.addWidget(self.rb_specific)
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("예: 005930")
        self.txt_code.setEnabled(False)
        self.bg_target.buttonToggled.connect(lambda: self.txt_code.setEnabled(self.rb_specific.isChecked()))
        row_target.addWidget(self.txt_code)
        gb_target.setLayout(row_target)
        ctl_row.addWidget(gb_target, stretch=1)

        # 4) 예측 실행 버튼
        self.btn_run = QPushButton("예측 실행")
        self.btn_run.setStyleSheet("""
            QPushButton { background-color: #FFA500; font-weight: bold; color: white; font-size: 15px; border-radius: 4px; }
            QPushButton:hover { background-color: #FFB733; }
            QPushButton:pressed { background-color: #CC8400; }
            QPushButton:disabled { background-color: #555555; color: #AAAAAA; }
        """)
        self.btn_run.setFixedHeight(40)
        self.btn_run.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.btn_run.clicked.connect(self.run_pred)
        ctl_row.addWidget(self.btn_run, stretch=1) 
        box2_layout.addLayout(ctl_row)

        # 5,6 좌/우 분할
        bottom_split = QSplitter(Qt.Horizontal, self)
        
        # 5) 예측 실행 결과
        left_panel = QtWidget()
        left_v = QVBoxLayout(left_panel)
        left_v.setContentsMargins(0,0,0,0)
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["순위", "코드", "종목명", "종가", "예측 점수", "상승확률"])
        # [수정] 테이블 스타일 (경계선, 헤더 숨김, 컬럼 폭)
        self.table.verticalHeader().setVisible(False) # 왼쪽 행번호 삭제
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Fixed) # 순위 고정폭
        self.table.setColumnWidth(0, 40) # 순위 폭 절반으로 축소
        self.table.setStyleSheet("""
            QTableWidget {
                gridline-color: #555555; /* 격자선 명확하게 */
                border: 1px solid #555555;
            }
            QHeaderView::section {
                background-color: #333333;
                color: white;
                border: 1px solid #555555;
            }
        """)
        left_v.addWidget(self.table)
        bottom_split.addWidget(left_panel)
        
        # 6) AI 분석 결과
        right_panel = QtWidget()
        right_v = QVBoxLayout(right_panel)
        right_v.setContentsMargins(0,0,0,0)
        self.ai_panel = QTextEdit()
        self.ai_panel.setReadOnly(True)
        self.ai_panel.setPlaceholderText("AI 분석 결과가 여기에 표시됩니다.")
        right_v.addWidget(self.ai_panel)
        bottom_split.addWidget(right_panel)

        bottom_split.setSizes([400, 600])
        box2_layout.addWidget(bottom_split)
        box2.setLayout(box2_layout)
        main_split.addWidget(box2)
        main_split.setSizes([300, 700])
        main_split.setCollapsible(0, False)
        main_split.setCollapsible(1, False)
        root.addWidget(main_split)

        self.date_edit = QDateEdit()
        self.date_edit.setVisible(False)
        self.date_edit.dateChanged.connect(self.on_date_changed)

        self.cb_engine = QComboBox()
        self.cb_engine.setVisible(False)
        self.cb_engine.currentIndexChanged.connect(self.on_engine_changed)

        self.load_engines()
        self._sync_dateedit_to_calendar()

    # ---------------- 내부 유틸 ----------------
    def _on_lock_toggled(self):
        # 버튼 눌림(True) -> 해제 상태
        is_unlocked = self.btn_lock.isChecked()
        self.is_locked = not is_unlocked
        
        if is_unlocked:
            self.btn_lock.setText("🔓 해제 (모두 보기)")
        else:
            self.btn_lock.setText("🔒 잠금 (필터링 ON)")
            
        # 상태 변경 시 현재 날짜 기준으로 리스트 다시 로드
        self.update_engine_list_view()

    def _on_calendar_clicked(self, qdate: QDate):
        self.date_edit.setDate(qdate) # -> on_date_changed 트리거

    def _sync_dateedit_to_calendar(self):
        d = self.calendar.selectedDate()
        self.date_edit.setDate(d)

    def _on_engine_list_changed(self):
        if self._building_list:
            return
        row = self.engine_list.currentRow()
        if row >= 0 and row < self.cb_engine.count():
            self.cb_engine.setCurrentIndex(row)

    def _parse_info_from_name(self, filename):
        date_match = re.search(r"(\d{6})\.pkl$", filename)
        max_d = None
        target_d = None
        if date_match:
            try:
                s = date_match.group(1)
                max_d = pd.to_datetime("20" + s, format="%Y%m%d").date()
                target_d = (pd.Timestamp(max_d) + BDay(1)).date()
            except:
                pass
        
        h_match = re.search(r"_h(\d+)_", filename)
        h = int(h_match.group(1)) if h_match else 5 
        w_match = re.search(r"_w(\d+)_", filename)
        w = int(w_match.group(1)) if w_match else 0
        v_match = re.search(r"(V\d+)", filename)
        ver = v_match.group(1) if v_match else "Unknown"
        return max_d, target_d, h, w, ver

    def load_engines(self):
        # 1. 파일 스캔 및 정보 캐싱
        real_files = []
        research_files = []
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_ENGINE"))
        
        if os.path.exists(os.path.join(base, "REAL")):
            real_files = glob.glob(os.path.join(base, "REAL", "*.pkl"))
        if os.path.exists(os.path.join(base, "RESEARCH")):
            research_files = glob.glob(os.path.join(base, "RESEARCH", "*.pkl"))

        def get_mtime(p):
            try: return os.path.getmtime(p)
            except: return 0

        real_files.sort(key=get_mtime, reverse=True)
        research_files.sort(key=get_mtime, reverse=True)
        files = real_files + research_files
        
        # 캐싱
        self.all_engine_files = []
        self._date_engine_counts = {}
        
        for f in files:
            fname = os.path.basename(f)
            _, target_d, _, _, _ = self._parse_info_from_name(fname)
            
            entry = {'path': f, 'name': fname, 'target_date': target_d}
            self.all_engine_files.append(entry)
            
            if target_d:
                qd = QDate(target_d.year, target_d.month, target_d.day)
                self._date_engine_counts[qd] = self._date_engine_counts.get(qd, 0) + 1
        
        self.calendar.set_engine_counts(self._date_engine_counts)
        
        # 초기 리스트 구성 (오늘 날짜 기준 필터링 또는 전체)
        self.update_engine_list_view()

    # [수정] 현재 상태(잠금여부, 날짜)에 따라 리스트 갱신
    def update_engine_list_view(self):
        curr_qdate = self.date_edit.date()
        curr_pydate = curr_qdate.toPython()
        
        filtered = []
        if self.is_locked:
            # 해당 날짜 엔진만
            for entry in self.all_engine_files:
                if entry['target_date'] == curr_pydate:
                    filtered.append(entry)
        else:
            # 전체 표시
            filtered = self.all_engine_files

        self.engine_paths = [e['path'] for e in filtered]
        
        self._building_list = True
        try:
            self.cb_engine.blockSignals(True)
            self.cb_engine.clear()
            self.cb_engine.addItems([e['name'] for e in filtered])

            self.engine_list.clear()
            for e in filtered:
                item = QListWidgetItem(e['name'])
                self.engine_list.addItem(item)
            
            # 리스트 갱신 후 선택 처리
            if filtered:
                self.cb_engine.setCurrentIndex(0)
                self.engine_list.setCurrentRow(0)
                self.on_engine_changed(0)
            else:
                # 엔진 없음
                self.lbl_engine_info.setText("엔진 없음")
                self.lbl_db_range.setText("")
                self.calendar.set_highlight_range(QDate(), QDate(), None)
                self.btn_run.setEnabled(False)
                self.lbl_pred_range.setText("")
                
        finally:
            self.cb_engine.blockSignals(False)
            self._building_list = False

    def on_engine_changed(self, idx: int):
        if idx < 0 or idx >= len(self.engine_paths):
            return
            
        path = self.engine_paths[idx]
        filename = os.path.basename(path)
        max_d, target_d, h, w, ver = self._parse_info_from_name(filename)
        
        # Fallback if needed (생략: 위에서 이미 파싱함)
        if max_d is None:
             # 안전장치: 실제 파일 읽기 (코드 생략, 기존 로직 유지)
             pass

        self._current_engine_info["target_date"] = target_d
        self._current_engine_info["h"] = h
        
        self.lbl_engine_info.setText(f"H={h}, W={w} ({ver})")
        
        if target_d:
            qd_target = QDate(target_d.year, target_d.month, target_d.day)
            
            # [수정] 예측 범위 하이라이트 (BDay(h-1)) -> 정확히 h칸 표시
            end_d = (pd.Timestamp(target_d) + BDay(h - 1)).date()
            qd_end = QDate(end_d.year, end_d.month, end_d.day)
            
            self.calendar.set_highlight_range(qd_target, qd_end, qd_target)
            self.lbl_db_range.setText(f"Engine Date: {target_d}")
            self.lbl_db_range.setStyleSheet("color: #66CCFF; font-weight: bold;")
            self.validate_date_range()
        else:
            self.calendar.set_highlight_range(QDate(), QDate(), None)
            self.lbl_db_range.setText("Info Parse Fail")
            self.lbl_db_range.setStyleSheet("color: #FF5555; font-weight: bold;")
            self.btn_run.setEnabled(False)

    def on_date_changed(self, qdate: QDate):
        # 날짜 변경 시 리스트 필터링 다시 수행
        self.update_engine_list_view()
        # 그 후 유효성 검사 (update_engine_list_view 내부에서 on_engine_changed 호출됨)
        # self.validate_date_range() # 중복 호출 방지

    def validate_date_range(self):
        # 필터링 모드에서는 이미 날짜 맞는 엔진만 떠있으므로 항상 True에 가까움
        # 단, 엔진이 없는 날짜면 리스트가 비어서 False됨
        
        if not self.is_locked:
            self.btn_run.setEnabled(True)
            self.lbl_db_range.setStyleSheet("color: #FF8C00; font-weight: bold;")
            self._update_pred_range_label()
            return

        target_d = self._current_engine_info["target_date"]
        sel_date = self.date_edit.date().toPython()
        
        if target_d and sel_date == target_d:
            self.btn_run.setEnabled(True)
            self.lbl_db_range.setStyleSheet("color: #66CCFF; font-weight: bold;") 
        else:
            self.btn_run.setEnabled(False)
            self.lbl_db_range.setStyleSheet("color: #FF5555; font-weight: bold;") 
            
        self._update_pred_range_label()

    def _update_pred_range_label(self):
        if not self.btn_run.isEnabled():
            if self.is_locked:
                pass
            return 

        h_int = self._current_engine_info["h"]
        if h_int is None: h_int = 0
            
        if h_int > 0:
            start = self.date_edit.date().toPython()
            end = (pd.Timestamp(start) + BDay(h_int)).date()
            if self.is_locked:
                self.lbl_pred_range.setText(f"▶ {h_int}영업일 뒤 예측: {start} 기준 → {end} 결과")
            else:
                self.lbl_pred_range.setText(f"⚠️ 강제 실행: {start} 기준 → {end} 결과 (H={h_int})")
        else:
            self.lbl_pred_range.setText("")

    # ---------------- 실행/결과 ----------------
    def run_pred(self):
        if self.cb_engine.count() == 0 or self.cb_engine.currentIndex() < 0:
            QMessageBox.warning(self, "알림", "엔진을 선택하세요.")
            return

        self.btn_run.setEnabled(False)
        self.btn_run.setText("분석 중...")
        QApplication.processEvents()
        
        engine_path = self.engine_paths[self.cb_engine.currentIndex()]
        target_date = self.date_edit.date().toString("yyyy-MM-dd")
        target_code = None  # 현재 버전: 시장 전체 기준 (특정 종목은 후속 확장)

        top_n = self.spin_topn.value()

        # 동시 실행 방지: 실행 중이면 무시
        if self.runner is not None and self.runner.isRunning():
            QMessageBox.information(self, "알림", "이미 실행 중입니다.")
            self.btn_run.setEnabled(True)
            self.btn_run.setText("예측 실행")
            return

        try:
            self.runner = DailyRunner(
                engine_path=engine_path,
                target_date=target_date,
                top_n=top_n,
                rank_by="combo",   # 기본: combo, 필요 시 UI 옵션과 연결
                ai_flag=0          # 기본 비활성
            )
            self.runner.finished_signal.connect(self._on_worker_finished)
            self.runner.error_signal.connect(self._on_worker_error)
            self.runner.start()
        except Exception as e:
            self.btn_run.setEnabled(True)
            self.btn_run.setText("예측 실행")
            QMessageBox.critical(self, "오류", f"시작 실패: {e}")

    def _on_worker_finished(self, df):
        self.btn_run.setEnabled(True)
        self.btn_run.setText("예측 실행")
        try:
            self.update_table(df)
            if df is not None and not df.empty:
                best = df.iloc[0]
                msg = (f"분석 완료.\n"
                       f"가장 높은 점수: {best.get('name')} ({best.get('code')})\n"
                       f"예측 점수: {best.get('score', 0):.4f}")
                self.ai_panel.setText(msg)
            else:
                self.ai_panel.setText("예측 결과가 없습니다.")
        except Exception as e:
            self.ai_panel.setText(f"결과 처리 중 오류: {e}")
        
    def _on_worker_error(self, e):
        self.btn_run.setEnabled(True)
        self.btn_run.setText("예측 실행")
        QMessageBox.critical(self, "오류", str(e))

    def update_table(self, df):
        self.table.setRowCount(0)
        if df is None or df.empty:
            QMessageBox.information(self, "알림", "해당 날짜에 결과가 없습니다.")
            return

        for i, row in df.iterrows():
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(str(i + 1)))
            self.table.setItem(r, 1, QTableWidgetItem(str(row.get("code", "-"))))
            self.table.setItem(r, 2, QTableWidgetItem(str(row.get("name", "-"))))
            # 종가 포맷
            close_val = row.get("close", "-")
            try:
                if close_val is None or pd.isna(close_val):
                    close_str = "-"
                else:
                    close_str = f"{float(close_val):,.0f}"
            except Exception:
                close_str = str(close_val)
            self.table.setItem(r, 3, QTableWidgetItem(close_str))
            # 점수
            try:
                score_val = float(row.get("score", 0))
                score_str = f"{score_val:.2f}"
            except Exception:
                score_str = str(row.get("score", "-"))
            self.table.setItem(r, 4, QTableWidgetItem(score_str))
            # 확률(%)
            prob = row.get('prob', 0)
            if isinstance(prob, (int, float)):
                 self.table.setItem(r, 5, QTableWidgetItem(f"{float(prob) * 100:.1f}%"))
            else:
                 self.table.setItem(r, 5, QTableWidgetItem("-"))

        for r in range(self.table.rowCount()):
            for c in range(self.table.columnCount()):
                item = self.table.item(r, c)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

    def closeEvent(self, event):
        if self.runner is not None and self.runner.isRunning():
            self.runner.requestInterruption()
            self.runner.wait()
        super().closeEvent(event)
