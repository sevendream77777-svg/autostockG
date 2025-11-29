# ui/pages/p3_prediction.py
import glob
import os
import pickle
import re
import pandas as pd
from pandas.tseries.offsets import BDay

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QHBoxLayout, QLabel, QDateEdit, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QRadioButton, QLineEdit,
    QButtonGroup, QMessageBox, QComboBox, QSpinBox, QCalendarWidget, QSplitter,
    QListWidget, QListWidgetItem, QTextEdit, QScrollArea, QWidget as QtWidget
)
from PySide6.QtCore import QDate, Qt
from common.workers import PredictionWorker


class PredictionPage(QWidget):
    """
    - 상단 BOX1: 좌(예측 기준일 T 달력) / 우(해당 날짜 사용 가능 엔진 목록, 스크롤)
    - 하단 BOX2: (3,4,5 한 줄) 아래 좌(예측 실행 결과 테이블) / 우(AI 분석)
    - 기존 로직(on_date_changed / on_engine_changed)은 유지. 달력이 date_edit을 대체(내부 연동).
    """
    def __init__(self):
        super().__init__()
        self.engine_paths = []
        self.meta_cache = {}
        self.db_cache = {}  # (version, tag) -> {"path":..., "min": date, "max": date}
        self._building_list = False
        self.init_ui()

    # ---------------- UI ----------------
    def init_ui(self):
        # [9] 루트 레이아웃 안전 설정
        root = QVBoxLayout()
        self.setLayout(root)

        # ───────────────────────────── BOX 1 (상단) ─────────────────────────────
        box1 = QGroupBox("예측 기준 설정")
        box1_layout = QVBoxLayout()
        # 좌/우 드래그 가능
        box1_split = QSplitter(Qt.Horizontal, self)

        # (좌) 달력 패널: 제목 + 달력
        cal_panel = QtWidget()
        cal_v = QVBoxLayout(cal_panel)
        cal_v.addWidget(QLabel("예측 기준일 (T)"))
        self.calendar = QCalendarWidget(self)
        self.calendar.setGridVisible(True)
        # 기본 날짜 = 오늘
        self.calendar.setSelectedDate(QDate.currentDate())
        self.calendar.clicked.connect(self._on_calendar_clicked)
        cal_v.addWidget(self.calendar)

        # (우) 엔진 목록 패널: 제목 + 스크롤(리스트)
        eng_panel = QtWidget()
        eng_v = QVBoxLayout(eng_panel)
        eng_v.addWidget(QLabel("해당 날짜 사용 가능 엔진"))
        # 내부적으로는 기존 QComboBox를 사용하지만, 화면에는 QListWidget 표출
        self.engine_list = QListWidget()
        self.engine_list.itemSelectionChanged.connect(self._on_engine_list_changed)
        eng_v.addWidget(self.engine_list)
        # 보조 레이블(엔진 부가정보, DB 범위)
        info_row = QHBoxLayout()
        self.lbl_engine_info = QLabel("")  # h, w
        info_row.addWidget(self.lbl_engine_info)
        self.lbl_db_range = QLabel("")
        info_row.addWidget(self.lbl_db_range)
        eng_v.addLayout(info_row)

        # 스플리터 구성 (초기 1:3 → 달력 대략 1/4)
        box1_split.addWidget(cal_panel)
        box1_split.addWidget(eng_panel)
        box1_split.setSizes([4, 6])

        box1_layout.addWidget(box1_split)
        box1.setLayout(box1_layout)
        main_split = QSplitter(Qt.Vertical, self)
        main_split.addWidget(box1)

        # ───────────────────────────── BOX 2 (하단) ─────────────────────────────
        self.lbl_pred_range = QLabel("")
        box2 = QGroupBox("예측 실행 및 결과")
        box2_layout = QVBoxLayout()

        # (3,4,5) 한 줄
        ctl_row = QHBoxLayout()

        # 3) 예측 TOP Rank
        self.lbl_topn = QLabel("예측 TOP Rank")
        ctl_row.addWidget(self.lbl_topn)
        self.spin_topn = QSpinBox()
        self.spin_topn.setButtonSymbols(QSpinBox.NoButtons)  # [1] 중복 제거
        self.spin_topn.setMinimumWidth(80)
        self.spin_topn.setFocusPolicy(Qt.StrongFocus)
        self.spin_topn.setStyleSheet("padding-left:6px; font-size:15px;")
        self.spin_topn.setRange(1, 150)
        self.spin_topn.setValue(10)
        self.spin_topn.valueChanged.connect(self._on_topn_changed)
        ctl_row.addWidget(self.spin_topn)
        self.lbl_topn_suffix = QLabel("개")
        ctl_row.addWidget(self.lbl_topn_suffix)

        ctl_row.addSpacing(12)

        # 4) 대상 선택
        gb_target = QGroupBox("대상 선택")
        row_target = QHBoxLayout()
        self.rb_market = QRadioButton("시장전체")
        self.rb_specific = QRadioButton("특정종목만 (예: 34543삼성전자)")
        self.rb_market.setChecked(True)
        self.bg_target = QButtonGroup(self)
        self.bg_target.addButton(self.rb_market)
        self.bg_target.addButton(self.rb_specific)
        row_target.addWidget(self.rb_market)
        row_target.addWidget(self.rb_specific)
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("예: 005930 (삼성전자)")
        self.txt_code.setEnabled(False)
        self.bg_target.buttonToggled.connect(lambda: self.txt_code.setEnabled(self.rb_specific.isChecked()))
        row_target.addWidget(self.txt_code)
        gb_target.setLayout(row_target)
        ctl_row.addWidget(gb_target, stretch=1)

        ctl_row.addSpacing(12)

        # 5) 예측 실행 버튼
        self.btn_run = QPushButton("예측 실행")
        self.btn_run.setStyleSheet("background-color:#FFA500; font-weight:bold;")  # [1] 중복 제거
        self.btn_run.setFixedHeight(36)
        self.btn_run.clicked.connect(self.run_pred)
        ctl_row.addWidget(self.btn_run)

        ctl_row.setStretch(0,1)
        ctl_row.setStretch(1,1)
        ctl_row.setStretch(2,3)
        box2_layout.addLayout(ctl_row)

        # 6,7 좌/우 분할(4:6)
        bottom_split = QSplitter(Qt.Horizontal, self)
        # 6) 예측 실행 결과(좌)
        left_panel = QtWidget()
        left_v = QVBoxLayout(left_panel)
        left_v.addWidget(QLabel("예측 실행 결과"))
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["순위", "코드", "종목명", "종가", "예측 점수", "상승확률"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        left_v.addWidget(self.table)
        bottom_split.addWidget(left_panel)
        # 7) AI 분석 결과(우)
        right_panel = QtWidget()
        right_v = QVBoxLayout(right_panel)
        right_v.addWidget(QLabel("AI 분석 결과"))
        self.ai_panel = QTextEdit()
        self.ai_panel.setReadOnly(True)
        self.ai_panel.setPlaceholderText("AI 분석 결과가 여기에 표시됩니다.")
        right_v.addWidget(self.ai_panel)
        bottom_split.addWidget(right_panel)
        # [2] 4:6 비율 반영
        bottom_split.setSizes([4, 6])

        box2_layout.addWidget(bottom_split)
        box2.setLayout(box2_layout)
        main_split.addWidget(box2)
        main_split.setSizes([3,7])
        root.addWidget(main_split)

        # 내부용: 기존 로직 유지 위한 숨김 date_edit + 콤보박스
        self.date_edit = QDateEdit()
        self.date_edit.setVisible(False)
        self.date_edit.dateChanged.connect(self.on_date_changed)

        self.cb_engine = QComboBox()
        self.cb_engine.setVisible(False)
        self.cb_engine.currentIndexChanged.connect(self.on_engine_changed)

        # 초기 로드
        self.load_engines()
        # 초기 날짜 동기화 (달력 → date_edit)
        self._sync_dateedit_to_calendar()

    # ---------------- 내부 유틸 ----------------
    def _on_topn_changed(self, v:int):
        # label은 이미 "개"로 뒤에 붙여 표기 중. 추가 가공 필요 없음.
        pass

    def _on_calendar_clicked(self, qdate: QDate):
        # 달력 클릭 -> 숨김 date_edit 갱신 → 기존 로직 그대로 작동
        self.date_edit.setDate(qdate)

    def _sync_dateedit_to_calendar(self):
        d = self.calendar.selectedDate()
        self.date_edit.setDate(d)

    def _on_engine_list_changed(self):
        if self._building_list:
            return
        row = self.engine_list.currentRow()
        if row >= 0 and row < self.cb_engine.count():
            self.cb_engine.setCurrentIndex(row)

    def _engine_dirs(self):
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_ENGINE"))
        return [os.path.join(base, "REAL"), os.path.join(base, "RESEARCH")]

    def _db_dir(self):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_DB"))

    def _extract_tag(self, path: str):
        try:
            tags = re.findall(r"(\d{6})", os.path.basename(path))
            return tags[-1] if tags else None
        except Exception:
            return None

    def _pick_db_path(self, version: str, tag: str | None):
        db_dir = self._db_dir()
        candidates = []
        if tag:
            candidates.append(os.path.join(db_dir, f"HOJ_DB_{version}_{tag}.parquet"))
        candidates.extend(sorted(glob.glob(os.path.join(db_dir, f"HOJ_DB_{version}_*.parquet")), reverse=True))
        candidates.append(os.path.join(db_dir, f"HOJ_DB_{version}.parquet"))
        candidates.append(os.path.join(db_dir, "HOJ_DB.parquet"))
        for c in candidates:
            if os.path.exists(c):
                return c
        return None

    def _load_db_info(self, version, tag=None):
        key = (version, tag)
        if key in self.db_cache:
            return self.db_cache[key]

        path = self._pick_db_path(version, tag)
        if not path:
            info = {"path": None, "min": None, "max": None}
            self.db_cache[key] = info
            return info

        try:
            df_date = pd.read_parquet(path, columns=["Date"])
            df_date["Date"] = pd.to_datetime(df_date["Date"]).dt.date
            min_d = df_date["Date"].min() if not df_date.empty else None
            max_d = df_date["Date"].max() if not df_date.empty else None
            info = {"path": path, "min": min_d, "max": max_d}
        except Exception:
            info = {"path": path, "min": None, "max": None}
        self.db_cache[key] = info
        return info

    # ---------------- 엔진/날짜 로직 ----------------
    def load_engines(self):
        files = []
        for d in self._engine_dirs():
            if os.path.exists(d):
                files.extend(glob.glob(os.path.join(d, "*.pkl")))
        files = sorted(files, reverse=True)

        # REAL 우선 정렬
        def _sort_key(p):
            return (0 if "HOJ_ENGINE\\REAL" in p or "HOJ_ENGINE/REAL" in p else 1, os.path.basename(p))
        files = sorted(files, key=_sort_key)

        self.engine_paths = files

        # 콤보박스(숨김) & 리스트(표시) 동시 구성
        self._building_list = True
        try:
            self.cb_engine.blockSignals(True)
            self.cb_engine.clear()
            self.cb_engine.addItems([os.path.basename(f) for f in files])

            self.engine_list.clear()
            for f in files:
                item = QListWidgetItem(os.path.basename(f))
                self.engine_list.addItem(item)

            if files:
                self.cb_engine.setCurrentIndex(0)
                self.engine_list.setCurrentRow(0)
                self.on_engine_changed(0)
        finally:
            self.cb_engine.blockSignals(False)
            self._building_list = False

    def on_engine_changed(self, idx: int):
        if idx < 0 or idx >= len(self.engine_paths):
            self.lbl_engine_info.setText("")
            self.lbl_db_range.setText("")
            return
        path = self.engine_paths[idx]
        meta = self._get_meta(path)
        tag = self._extract_tag(path)
        version = meta.get("version", "V31")
        db_info = self._load_db_info(version, tag)
        min_d, max_d = db_info.get("min"), db_info.get("max")

        h = meta.get("horizon")
        w = meta.get("input_window")
        self.lbl_engine_info.setText(f"h={h}, w={w}")
        if min_d and max_d:
            self.lbl_db_range.setText(f"DB: {min_d} ~ {max_d}")
            # 기준일 허용 범위: max_d+1영업일 ~ max_d+5영업일
            start = (pd.Timestamp(max_d) + BDay(1)).date()
            end = (pd.Timestamp(max_d) + BDay(5)).date()
            # 달력 최소/최대는 강제하지 않음(시각적 제한 대신 on_date_changed로 판별)
            # 선택 날짜가 범위 밖이면 최소일로 리셋
            cur = self.calendar.selectedDate().toPython()
            if not (start <= cur <= end):
                self.calendar.setSelectedDate(QDate(start.year, start.month, start.day))
                self._sync_dateedit_to_calendar()
        else:
            self.lbl_db_range.setText("DB 범위 없음")
        self._update_pred_range_label()

    def _get_meta(self, path):
        if path in self.meta_cache:
            return self.meta_cache[path]
        meta = {}
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
            if isinstance(data, dict):
                meta = data.get("meta", {})
        except Exception:
            meta = {}
        self.meta_cache[path] = meta
        return meta

    def on_date_changed(self, qdate: QDate):
        # 기준일 변경 시 선택된 엔진이 없으면 무시
        if self.cb_engine.count() == 0:
            return
        idx = self.cb_engine.currentIndex()
        if idx < 0:
            return
        # 기준일이 허용 범위 밖이면 엔진 목록 초기화(기존 로직 유지)
        path = self.engine_paths[idx]
        meta = self._get_meta(path)
        tag = self._extract_tag(path)
        version = meta.get("version", "V31")
        db_info = self._load_db_info(version, tag)
        max_d = db_info.get("max")
        if not max_d:
            self.cb_engine.clear()
            self.engine_list.clear()
            self.lbl_engine_info.setText("해당 날짜 데이터 없음")
            self.lbl_db_range.setText("")
            self.lbl_pred_range.setText("")
            return
        allow_start = (pd.Timestamp(max_d) + BDay(1)).date()
        allow_end = (pd.Timestamp(max_d) + BDay(5)).date()
        target = qdate.toPython()
        if not (allow_start <= target <= allow_end):
            self.cb_engine.clear()
            self.engine_list.clear()
            self.lbl_engine_info.setText("해당 날짜 데이터 없음")
            self.lbl_db_range.setText("")
            self.lbl_pred_range.setText("")
            return
        # 허용 범위면 엔진 유지
        self._update_pred_range_label()

    # ---------------- 실행/결과 ----------------
    def run_pred(self):
        if self.cb_engine.count() == 0 or self.cb_engine.currentIndex() < 0:
            QMessageBox.warning(self, "알림", "엔진을 선택하세요.")
            return
        engine_path = self.engine_paths[self.cb_engine.currentIndex()]
        target_date = self.calendar.selectedDate().toString("yyyy-MM-dd")
        target_code = None

        if self.rb_specific.isChecked():
            target_code = self.txt_code.text().strip()
            if not target_code:
                QMessageBox.warning(self, "알림", "종목 코드를 입력하세요.")
                return

        top_n = self.spin_topn.value()
        self.worker = PredictionWorker(
            engine_path=engine_path,
            target_date=target_date,
            top_n=top_n,
            specific_code=target_code,
        )
        # [8] 종료/에러 시 정리
        self.worker.finished_signal.connect(self._on_worker_finished)
        self.worker.error_signal.connect(self._on_worker_error)
        self.worker.start()

    def _on_worker_finished(self, df):
        try:
            self.update_table(df)
        finally:
            try:
                self.worker.deleteLater()
            except Exception:
                pass
            self.worker = None

    def _on_worker_error(self, e):
        try:
            QMessageBox.critical(self, "오류", str(e))
        finally:
            try:
                self.worker.deleteLater()
            except Exception:
                pass
            self.worker = None

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
            self.table.setItem(r, 3, QTableWidgetItem(str(row.get("close", "-"))))
            self.table.setItem(r, 4, QTableWidgetItem(f"{row.get('score', 0):.4f}"))
            self.table.setItem(r, 5, QTableWidgetItem(f"{row.get('prob', 0) * 100:.1f}%"))
        for r in range(self.table.rowCount()):
            for c in range(self.table.columnCount()):
                item = self.table.item(r, c)
                if item:
                    item.setTextAlignment(Qt.AlignCenter)

    def _update_pred_range_label(self):
        if self.cb_engine.count() == 0 or self.cb_engine.currentIndex() < 0:
            self.lbl_pred_range.setText("")
            return
        path = self.engine_paths[self.cb_engine.currentIndex()]
        meta = self._get_meta(path)
        h = meta.get("horizon")

        # [4] horizon 안전 처리
        h_int = None
        if isinstance(h, (int, float)):
            try:
                h_int = int(h)
            except Exception:
                h_int = None
        if h_int is None or h_int <= 0:
            self.lbl_pred_range.setText("")
            return

        try:
            start = self.calendar.selectedDate().toPython()
            end = (pd.Timestamp(start) + BDay(h_int)).date()
            self.lbl_pred_range.setText(f"{h_int}영업일 예측: {start} ~ {end}")
        except Exception:
            self.lbl_pred_range.setText("")
