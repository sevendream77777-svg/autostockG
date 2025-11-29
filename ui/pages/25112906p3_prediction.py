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
    QButtonGroup, QMessageBox, QComboBox, QSpinBox
)
from PySide6.QtCore import QDate, Qt
from common.workers import PredictionWorker


class PredictionPage(QWidget):
    """
    - 엔진 리스트: REAL 우선, 최신 DB를 가진 엔진만 기준일에 노출
    - 엔진 선택 시 해당 엔진 DB만 읽어 min/max 계산
    - 기준일은 DB max+1영업일 ~ max+5영업일만 선택 가능
    - 기준일이 범위 밖이면 엔진 없음/메시지
    """
    def __init__(self):
        super().__init__()
        self.engine_paths = []
        self.meta_cache = {}
        self.db_cache = {}  # (version, tag) -> {"path":..., "min": date, "max": date}
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 1. 예측 설정
        gb_setting = QGroupBox("🔮 예측 설정")
        v_box = QVBoxLayout()

        # 기준일
        h_date = QHBoxLayout()
        h_date.addWidget(QLabel("예측 기준일 (T):"))
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate())
        self.date_edit.dateChanged.connect(self.on_date_changed)
        h_date.addWidget(self.date_edit)
        h_date.addStretch()
        v_box.addLayout(h_date)

        # 예측 기간 안내
        self.lbl_pred_range = QLabel("")
        v_box.addWidget(self.lbl_pred_range)

        # 엔진 선택
        h_engine = QHBoxLayout()
        h_engine.addWidget(QLabel("해당 날짜 사용 가능 엔진:"))
        self.cb_engine = QComboBox()
        self.cb_engine.currentIndexChanged.connect(self.on_engine_changed)
        h_engine.addWidget(self.cb_engine, stretch=1)
        self.lbl_engine_info = QLabel("")  # h, w 표시
        h_engine.addWidget(self.lbl_engine_info)
        self.lbl_db_range = QLabel("")     # DB 범위 표시
        h_engine.addWidget(self.lbl_db_range)
        v_box.addLayout(h_engine)

        # Top N
        h_topn = QHBoxLayout()
        h_topn.addWidget(QLabel("Top N:"))
        self.spin_topn = QSpinBox()
        self.spin_topn.setRange(1, 150)
        self.spin_topn.setValue(10)
        h_topn.addWidget(self.spin_topn)
        h_topn.addStretch()
        v_box.addLayout(h_topn)

        # 대상 선택
        gb_target = QGroupBox("대상 선택")
        h_target = QHBoxLayout()
        self.rb_market = QRadioButton("시장 전체 (Top N 추천)")
        self.rb_specific = QRadioButton("특정 종목만")
        self.rb_market.setChecked(True)
        self.bg_target = QButtonGroup(self)
        self.bg_target.addButton(self.rb_market)
        self.bg_target.addButton(self.rb_specific)
        h_target.addWidget(self.rb_market)
        h_target.addWidget(self.rb_specific)
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("예: 005930 (삼성전자)")
        self.txt_code.setEnabled(False)
        self.bg_target.buttonToggled.connect(lambda: self.txt_code.setEnabled(self.rb_specific.isChecked()))
        h_target.addWidget(self.txt_code)
        gb_target.setLayout(h_target)
        v_box.addWidget(gb_target)

        # 실행 버튼
        self.btn_run = QPushButton("🚀 예측 실행")
        self.btn_run.setFixedHeight(45)
        self.btn_run.setStyleSheet("background-color: #5e81ac; font-weight: bold;")
        self.btn_run.clicked.connect(self.run_pred)
        v_box.addWidget(self.btn_run)

        gb_setting.setLayout(v_box)
        layout.addWidget(gb_setting)

        # 결과 테이블
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["순위", "코드", "종목명", "종가", "예측 점수", "상승확률"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

        self.load_engines()

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
        self.cb_engine.clear()
        self.cb_engine.addItems([os.path.basename(f) for f in files])
        if files:
            self.cb_engine.setCurrentIndex(0)
            self.on_engine_changed(0)

    def on_engine_changed(self, idx: int):
        if idx < 0 or idx >= len(self.engine_paths):
            self.lbl_engine_info.setText("")
            self.lbl_db_range.setText("")
            self.lbl_pred_range.setText("")
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
            min_q = QDate(start.year, start.month, start.day)
            max_q = QDate(end.year, end.month, end.day)
            self.date_edit.blockSignals(True)
            self.date_edit.setMinimumDate(min_q)
            self.date_edit.setMaximumDate(max_q)
            # 현재 선택이 범위 밖이면 최소일로 리셋
            cur = self.date_edit.date().toPython()
            if not (start <= cur <= end):
                self.date_edit.setDate(min_q)
            self.date_edit.blockSignals(False)
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
        # 기준일이 허용 범위 밖이면 엔진 목록 비움
        path = self.engine_paths[idx]
        meta = self._get_meta(path)
        tag = self._extract_tag(path)
        version = meta.get("version", "V31")
        db_info = self._load_db_info(version, tag)
        max_d = db_info.get("max")
        if not max_d:
            self.cb_engine.clear()
            self.lbl_engine_info.setText("해당 날짜 데이터 없음")
            self.lbl_db_range.setText("")
            self.lbl_pred_range.setText("")
            return
        allow_start = (pd.Timestamp(max_d) + BDay(1)).date()
        allow_end = (pd.Timestamp(max_d) + BDay(5)).date()
        target = qdate.toPython()
        if not (allow_start <= target <= allow_end):
            self.cb_engine.clear()
            self.lbl_engine_info.setText("해당 날짜 데이터 없음")
            self.lbl_db_range.setText("")
            self.lbl_pred_range.setText("")
            return
        # 허용 범위면 엔진 유지
        self._update_pred_range_label()

    def run_pred(self):
        if self.cb_engine.count() == 0 or self.cb_engine.currentIndex() < 0:
            QMessageBox.warning(self, "알림", "엔진을 선택하세요.")
            return
        engine_path = self.engine_paths[self.cb_engine.currentIndex()]
        target_date = self.date_edit.date().toString("yyyy-MM-dd")
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
        self.worker.finished_signal.connect(self.update_table)
        self.worker.error_signal.connect(lambda e: QMessageBox.critical(self, "오류", str(e)))
        self.worker.start()

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
        if not h or not isinstance(h, (int, float)):
            self.lbl_pred_range.setText("")
            return
        try:
            start = self.date_edit.date().toPython()
            end = (pd.Timestamp(start) + BDay(int(h))).date()
            self.lbl_pred_range.setText(f"{int(h)}영업일 예측: {start} ~ {end}")
        except Exception:
            self.lbl_pred_range.setText("")
