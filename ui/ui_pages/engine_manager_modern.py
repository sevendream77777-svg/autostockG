# ============================================================
# Engine Manager UI (Unified Workbench) - Final V32 - MODERNIZED
#   - Refactored into a single file with Inner Classes for pages
#   - Applied a Modern Dark Theme (QSS)
#   - ADDED: "0단계: 데이터 파이프라인" 탭 안에 수동 선택 다운로드 UI
#            (종목 직접 입력/파일선택 + 기간 선택 + 실행 + 실시간 로그)
#   - Calls: RAW/pykrx_full_dump_resumable_v2.py (parameter mode)
# ============================================================

import os
import sys
import glob
import re
import pickle
import time
import subprocess
import shlex
import pandas as pd
from datetime import datetime

# PySide6
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QTabWidget,
    QLabel, QComboBox, QSpinBox, QPushButton, QTextEdit, QLineEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QSplitter, QMessageBox, QDateEdit,
    QProgressBar, QFileDialog, QCheckBox
)
from PySide6.QtCore import Qt, QThread, Signal, QDate

# ------------------------------------------------------------
# 프로젝트 경로 설정
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
ui_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(ui_dir)
sys.path.append(root_dir)

# Backend path aliases
model_engine_dir = os.path.join(root_dir, "MODELENGINE")
util_dir = os.path.join(model_engine_dir, "UTIL")
raw_dir = os.path.join(model_engine_dir, "RAW")

sys.path.append(util_dir)
sys.path.append(raw_dir)

# Backend Import (guarded)
try:
    from MODELENGINE.UTIL.train_engine_unified import run_unified_training
    from MODELENGINE.UTIL.predict_daily_top10 import run_prediction
    from MODELENGINE.UTIL.config_paths import get_path
    import update_raw_data
    import build_features
    import build_unified_db
    import make_kospi_index_10y
    BACKEND_READY = True
except Exception:
    BACKEND_READY = False
    def run_unified_training(mode, horizon, valid_days, n_estimators, version): time.sleep(1)
    def run_prediction(engine_path, target_date, top_n):
        time.sleep(1)
        return pd.DataFrame({
            'Code': ['005930','035420','005380'],
            'Name': ['삼성전자','NAVER','현대차'],
            'Close': [70000,200000,250000],
            'Pred_Score': [0.95,0.88,0.79],
            'Pred_Prob': [0.85,0.75,0.65]
        })
    def get_path(key):
        return os.path.join(root_dir, 'MODELENGINE', 'HOJ_ENGINE', 'RESEARCH')

# ------------------------------------------------------------
# QSS (Nord-like)
# ------------------------------------------------------------

def get_modern_qss():
    return """
    QWidget { background-color: #2e3440; color: #d8dee9; font-size: 10pt; }
    QTabWidget::pane { border: 1px solid #4c566a; border-top: 1px solid #3b4252; }
    QTabBar::tab { background: #3b4252; color: #eceff4; padding: 12px 25px; border: none; min-width: 150px; }
    QTabBar::tab:selected { background: #4c566a; color: #88c0d0; font-weight: bold; border-bottom: 2px solid #88c0d0; }
    QGroupBox { font-size: 11pt; font-weight: bold; border: 1px solid #4c566a; border-radius: 5px; margin-top: 10px; padding-top: 15px; color: #a3be8c; }
    QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 5px; background: #2e3440; }
    QPushButton { background-color: #5e81ac; color: #eceff4; border-radius: 6px; padding: 10px; font-weight: bold; border: 1px solid #4c566a; }
    QPushButton:hover { background-color: #81a1c1; }
    QPushButton:disabled { background-color: #3b4252; color: #4c566a; }
    #data_step_btn { background-color: #4c566a; min-height: 40px; }
    #data_all_btn  { background-color: #b48ead; min-height: 40px; }
    QProgressBar { border: 1px solid #4c566a; border-radius: 5px; text-align: center; background: #3b4252; }
    QProgressBar::chunk { background-color: #a3be8c; border-radius: 5px; }
    QTextEdit, QLineEdit, QComboBox, QSpinBox, QDateEdit { background: #3b4252; border: 1px solid #4c566a; border-radius: 4px; padding: 5px; color: #eceff4; }
    QHeaderView::section { background: #4c566a; color: #88c0d0; padding: 5px; border: 1px solid #3b4252; font-weight: bold; }
    QTableWidget::item:selected { background-color: #5e81ac; color: #eceff4; }
    """

# ------------------------------------------------------------
# Workers
# ------------------------------------------------------------

class DataUpdateWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
    def run(self):
        try:
            total = len(self.tasks)
            for i, task in enumerate(self.tasks):
                self.progress_signal.emit(int(i/total*100))
                if not BACKEND_READY:
                    time.sleep(0.5)
                    self.log_signal.emit(f"[MOCK] {task} 단계 실행")
                else:
                    if task == 'stock':
                        self.log_signal.emit("📈 개별 시세(RAW) 업데이트")
                        update_raw_data.main()
                    elif task == 'kospi':
                        self.log_signal.emit("🇰🇷 KOSPI 지수 수집")
                        make_kospi_index_10y.main()
                    elif task == 'feature':
                        self.log_signal.emit("🧮 피처(Feature) 계산")
                        build_features.main()
                    elif task == 'db':
                        self.log_signal.emit("📦 통합 DB 생성")
                        build_unified_db.build_unified_db()
                self.log_signal.emit(f"✅ {task} 단계 완료")
            self.progress_signal.emit(100)
            self.finished_signal.emit("데이터 파이프라인 완료")
        except Exception as e:
            self.error_signal.emit(str(e))

class ManualDownloadWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    def __init__(self, codes, start_yyyymmdd, end_yyyymmdd, out_dir, script_path):
        super().__init__()
        self.codes = codes
        self.start = start_yyyymmdd
        self.end = end_yyyymmdd
        self.out_dir = out_dir
        self.script_path = script_path
    def run(self):
        try:
            if not os.path.exists(self.script_path):
                raise FileNotFoundError(f"다운로드 스크립트 없음: {self.script_path}")
            cmd = [sys.executable, self.script_path, "--out", self.out_dir, "--start", self.start, "--end", self.end, "--codes"] + self.codes
            self.log_signal.emit("실행 명령:\n" + shlex.join(cmd))
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
                for line in p.stdout:
                    self.log_signal.emit(line.rstrip())
                p.wait()
                if p.returncode != 0:
                    raise RuntimeError(f"스크립트 종료 코드: {p.returncode}")
            self.finished_signal.emit("선택 다운로드 완료")
        except Exception as e:
            self.error_signal.emit(str(e))

class TrainingWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    def __init__(self, params):
        super().__init__()
        self.params = params
    def run(self):
        try:
            self.log_signal.emit(f"학습 시작: {self.params}")
            run_unified_training(
                mode=self.params['mode'],
                horizon=self.params['horizon'],
                valid_days=self.params['valid_days'],
                n_estimators=self.params['n_estimators'],
                version=self.params['version']
            )
            self.finished_signal.emit("엔진 생성 완료")
        except Exception as e:
            self.error_signal.emit(str(e))

class PredictionWorker(QThread):
    finished_signal = Signal(object)
    error_signal = Signal(str)
    def __init__(self, engine_path, target_date, top_n):
        super().__init__()
        self.engine_path = engine_path
        self.target_date = target_date
        self.top_n = top_n
    def run(self):
        try:
            df = run_prediction(self.engine_path, self.target_date, self.top_n)
            self.finished_signal.emit(df)
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# Pages
# ------------------------------------------------------------

class _UIDataUpdatePage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()
    def init_ui(self):
        layout = QVBoxLayout(self)
        info = QLabel("RAW→KOSPI→FEATURE→DB 순차 실행 / + 수동 선택 다운로드")
        info.setStyleSheet("font-weight: bold; color:#88c0d0")
        layout.addWidget(info)
        # Buttons
        grp = QGroupBox("▶ 데이터 파이프라인 단계별 실행")
        h = QHBoxLayout()
        self.b1 = QPushButton("1. 시세(RAW)"); self.b1.setObjectName("data_step_btn")
        self.b2 = QPushButton("2. KOSPI");     self.b2.setObjectName("data_step_btn")
        self.b3 = QPushButton("3. 피처");      self.b3.setObjectName("data_step_btn")
        self.b4 = QPushButton("4. DB");        self.b4.setObjectName("data_step_btn")
        self.bAll = QPushButton("⚡ 전체 실행 (1~4)"); self.bAll.setObjectName("data_all_btn")
        for b in (self.b1,self.b2,self.b3,self.b4): b.setFixedHeight(42)
        self.bAll.setFixedHeight(46)
        self.b1.clicked.connect(lambda: self.manager.run_data_task(['stock']))
        self.b2.clicked.connect(lambda: self.manager.run_data_task(['kospi']))
        self.b3.clicked.connect(lambda: self.manager.run_data_task(['feature']))
        self.b4.clicked.connect(lambda: self.manager.run_data_task(['db']))
        self.bAll.clicked.connect(lambda: self.manager.run_data_task(['stock','kospi','feature','db']))
        for w in (self.b1,self.b2,self.b3,self.b4,self.bAll): h.addWidget(w)
        grp.setLayout(h)
        layout.addWidget(grp)
        # Progress + Log
        self.progress = QProgressBar(); layout.addWidget(self.progress)
        self.log = QTextEdit(); self.log.setReadOnly(True); self.log.setPlaceholderText("데이터 작업 로그...")
        layout.addWidget(self.log)
        # Manual Download UI
        dl = QGroupBox("📥 수동 선택 다운로드 (종목/기간 지정)")
        v = QVBoxLayout()
        r1 = QHBoxLayout()
        r1.addWidget(QLabel("종목코드(쉼표):"))
        self.edit_codes = QLineEdit(); self.edit_codes.setPlaceholderText("예: 000020,091440,005930")
        r1.addWidget(self.edit_codes)
        self.btn_pick_file = QPushButton("파일 선택(txt/json)")
        self.btn_pick_file.clicked.connect(self.manager.on_pick_codes_file)
        r1.addWidget(self.btn_pick_file)
        v.addLayout(r1)
        r2 = QHBoxLayout()
        r2.addWidget(QLabel("시작일:"))
        self.date_start = QDateEdit(); self.date_start.setCalendarPopup(True); self.date_start.setDisplayFormat("yyyyMMdd"); self.date_start.setDate(QDate.currentDate().addDays(-30))
        r2.addWidget(self.date_start)
        r2.addWidget(QLabel("종료일:"))
        self.date_end = QDateEdit(); self.date_end.setCalendarPopup(True); self.date_end.setDisplayFormat("yyyyMMdd"); self.date_end.setDate(QDate.currentDate())
        r2.addWidget(self.date_end)
        self.chk_single = QCheckBox("단일일자"); self.chk_single.stateChanged.connect(self.manager.on_toggle_single_day)
        r2.addWidget(self.chk_single)
        v.addLayout(r2)
        r3 = QHBoxLayout()
        r3.addWidget(QLabel("저장폴더:"))
        self.edit_out = QLineEdit(); self.edit_out.setPlaceholderText("기본: RAW/manual_download")
        r3.addWidget(self.edit_out)
        btn_out = QPushButton("폴더 선택"); btn_out.clicked.connect(self.manager.on_pick_outdir)
        r3.addWidget(btn_out)
        v.addLayout(r3)
        r4 = QHBoxLayout()
        self.btn_run = QPushButton("📥 선택 다운로드 실행"); self.btn_run.setFixedHeight(44); self.btn_run.clicked.connect(self.manager.start_manual_download)
        r4.addStretch(1); r4.addWidget(self.btn_run)
        v.addLayout(r4)
        self.dl_log = QTextEdit(); self.dl_log.setReadOnly(True); self.dl_log.setPlaceholderText("선택 다운로드 로그…")
        v.addWidget(self.dl_log)
        dl.setLayout(v)
        layout.addWidget(dl)

class _UITrainingPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()
    def init_ui(self):
        layout = QVBoxLayout(self)
        g = QGroupBox("⚙ 엔진 학습 파라미터")
        h = QHBoxLayout()
        h.addWidget(QLabel("모드:")); self.cb_mode = QComboBox(); self.cb_mode.addItems(["research","real"]); h.addWidget(self.cb_mode)
        h.addWidget(QLabel("Horizon:")); self.sp_h = QSpinBox(); self.sp_h.setRange(1,60); self.sp_h.setValue(5); self.sp_h.setSuffix("일"); h.addWidget(self.sp_h)
        h.addWidget(QLabel("검증기간:")); self.sp_v = QSpinBox(); self.sp_v.setRange(30,1000); self.sp_v.setValue(365); self.sp_v.setSuffix("일"); h.addWidget(self.sp_v)
        h.addWidget(QLabel("Trees:")); self.sp_t = QSpinBox(); self.sp_t.setRange(100,10000); self.sp_t.setValue(1000); self.sp_t.setSingleStep(100); h.addWidget(self.sp_t)
        h.addWidget(QLabel("버전:")); self.cb_ver = QComboBox(); self.cb_ver.addItems(["V31","V32","TEST"]); self.cb_ver.setEditable(True); h.addWidget(self.cb_ver)
        g.setLayout(h); layout.addWidget(g)
        self.btn = QPushButton("🚀 학습 시작"); self.btn.setFixedHeight(46); self.btn.clicked.connect(self.manager.start_training); layout.addWidget(self.btn)
        self.log = QTextEdit(); self.log.setReadOnly(True); self.log.setPlaceholderText("학습 로그…"); layout.addWidget(self.log)

class _UIManagerPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()
    def init_ui(self):
        spl = QSplitter(Qt.Horizontal)
        left = QWidget(); lv = QVBoxLayout(left)
        lv.addWidget(QLabel("📂 엔진 목록"))
        self.tbl = QTableWidget(); self.tbl.setColumnCount(1); self.tbl.setHorizontalHeaderLabels(["Engine File"]); self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tbl.itemClicked.connect(self.manager.load_engine_metadata)
        lv.addWidget(self.tbl)
        btn = QPushButton("🔄 새로고침"); btn.setObjectName("data_step_btn"); btn.clicked.connect(self.manager.refresh_engine_list); lv.addWidget(btn)
        right = QGroupBox("📋 엔진 상세"); rv = QVBoxLayout(); self.info = QTextEdit(); self.info.setReadOnly(True); rv.addWidget(self.info); right.setLayout(rv)
        spl.addWidget(left); spl.addWidget(right); spl.setSizes([320, 760])
        main = QVBoxLayout(self); main.addWidget(spl)

class _UIPredictPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()
    def init_ui(self):
        layout = QVBoxLayout(self)
        g = QGroupBox("🔮 예측 실행")
        h = QHBoxLayout()
        h.addWidget(QLabel("Horizon:")); self.sp_h = QSpinBox(); self.sp_h.setRange(1,60); self.sp_h.setValue(5); self.sp_h.valueChanged.connect(self.manager.filter_engines_by_horizon); h.addWidget(self.sp_h)
        h.addWidget(QLabel("기준일:")); self.date = QDateEdit(); self.date.setCalendarPopup(True); self.date.setDisplayFormat("yyyy-MM-dd"); self.date.setDate(QDate.currentDate().addDays(-1)); h.addWidget(self.date)
        h.addWidget(QLabel("엔진:")); self.cb_engine = QComboBox(); self.cb_engine.setMinimumWidth(300); h.addWidget(self.cb_engine)
        h.addWidget(QLabel("Top N:")); self.sp_top = QSpinBox(); self.sp_top.setRange(1,100); self.sp_top.setValue(10); h.addWidget(self.sp_top)
        self.btn = QPushButton("⚡ 예측"); self.btn.clicked.connect(self.manager.start_prediction); h.addWidget(self.btn)
        g.setLayout(h); layout.addWidget(g)
        self.tbl = QTableWidget(); self.tbl.setColumnCount(5); self.tbl.setHorizontalHeaderLabels(["Code","Name","Close","Score","Prob"]); self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch); layout.addWidget(self.tbl)

# ------------------------------------------------------------
# Main Window
# ------------------------------------------------------------

class EngineManager(QWidget):
    def __init__(self):
        super().__init__()
        self.all_engines = []
        self.setup_ui()
        self.refresh_engine_list()
    def setup_ui(self):
        self.setStyleSheet(get_modern_qss())
        v = QVBoxLayout(self)
        self.tabs = QTabWidget()
        # pages
        self.page_data = _UIDataUpdatePage(self)
        self.page_train = _UITrainingPage(self)
        self.page_manage = _UIManagerPage(self)
        self.page_predict = _UIPredictPage(self)
        self.tabs.addTab(self.page_data, "💾 0단계: 데이터 파이프라인")
        self.tabs.addTab(self.page_train, "🏭 1단계: 모델 학습실")
        self.tabs.addTab(self.page_manage, "📊 2단계: 엔진 분석실")
        self.tabs.addTab(self.page_predict, "🔮 3단계: 예측 및 검증")
        v.addWidget(self.tabs)
        self.setWindowTitle("HOJ Engine Manager (Unified V32)")
        self.resize(1280, 860)
        # shortcuts
        self.data_log = self.page_data.log
        self.data_progress = self.page_data.progress
        self.btn_all = self.page_data.bAll
        # download refs
        self.edit_codes = self.page_data.edit_codes
        self.btn_pick_file = self.page_data.btn_pick_file
        self.date_start = self.page_data.date_start
        self.date_end = self.page_data.date_end
        self.chk_single = self.page_data.chk_single
        self.edit_out = self.page_data.edit_out
        self.btn_run = self.page_data.btn_run
        self.dl_log = self.page_data.dl_log
        # train
        self.train_log = self.page_train.log
        self.train_btn = self.page_train.btn
        self.train_mode = self.page_train.cb_mode
        self.train_h = self.page_train.sp_h
        self.train_v = self.page_train.sp_v
        self.train_t = self.page_train.sp_t
        self.train_ver = self.page_train.cb_ver
        # manage/predict
        self.tbl_eng = self.page_manage.tbl
        self.info_eng = self.page_manage.info
        self.cb_engine = self.page_predict.cb_engine
        self.pred_h = self.page_predict.sp_h
        self.pred_date = self.page_predict.date
        self.pred_top = self.page_predict.sp_top
        self.pred_btn = self.page_predict.btn
    # --- Data Tab ---
    def run_data_task(self, tasks):
        self.data_log.clear(); self.data_progress.setValue(0)
        for b in (self.page_data.b1,self.page_data.b2,self.page_data.b3,self.page_data.b4,self.page_data.bAll): b.setEnabled(False)
        self.worker = DataUpdateWorker(tasks)
        self.worker.log_signal.connect(self.data_log.append)
        self.worker.progress_signal.connect(self.data_progress.setValue)
        self.worker.finished_signal.connect(self._on_data_finish)
        self.worker.error_signal.connect(self._on_data_error)
        self.worker.start()
    def _on_data_finish(self, msg):
        for b in (self.page_data.b1,self.page_data.b2,self.page_data.b3,self.page_data.b4,self.page_data.bAll): b.setEnabled(True)
        self.data_log.append("\n✅ " + msg)
        QMessageBox.information(self, "완료", msg)
    def _on_data_error(self, err):
        for b in (self.page_data.b1,self.page_data.b2,self.page_data.b3,self.page_data.b4,self.page_data.bAll): b.setEnabled(True)
        self.data_log.append("\n❌ " + err)
        QMessageBox.critical(self, "오류", err)
    # manual download
    def on_toggle_single_day(self, state):
        if state == Qt.Checked:
            self.date_end.setDate(self.date_start.date()); self.date_end.setEnabled(False)
        else:
            self.date_end.setEnabled(True)
    def on_pick_codes_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "종목 리스트 파일 선택 (txt/json)", root_dir, "Text/JSON (*.txt *.json)")
        if path: self.edit_codes.setText(path)
    def on_pick_outdir(self):
        path = QFileDialog.getExistingDirectory(self, "저장 폴더 선택", raw_dir)
        if path: self.edit_out.setText(path)
    def start_manual_download(self):
        raw_codes = self.edit_codes.text().strip()
        if not raw_codes:
            QMessageBox.warning(self, "입력 오류", "종목코드를 입력하거나 txt/json를 선택하세요.")
            return
        codes = []
        if os.path.isfile(raw_codes):
            ext = os.path.splitext(raw_codes)[1].lower()
            try:
                if ext == '.txt':
                    with open(raw_codes, encoding='utf-8') as f:
                        codes = [x.strip() for x in f if x.strip()]
                elif ext == '.json':
                    import json
                    with open(raw_codes, encoding='utf-8') as f:
                        codes = json.load(f)
                else:
                    QMessageBox.warning(self, "형식 오류", "txt/json만 지원"); return
            except Exception as e:
                QMessageBox.critical(self, "파일 로딩 오류", str(e)); return
        else:
            codes = [c.strip() for c in raw_codes.split(',') if c.strip()]
        if not codes:
            QMessageBox.warning(self, "입력 오류", "대상 종목이 없습니다."); return
        s = self.date_start.date().toString('yyyyMMdd')
        e = self.date_end.date().toString('yyyyMMdd')
        if self.chk_single.isChecked(): e = s
        out_dir = self.edit_out.text().strip() or os.path.join(raw_dir, 'manual_download')
        os.makedirs(out_dir, exist_ok=True)
        script_path = os.path.join(raw_dir, 'pykrx_full_dump_resumable_v2.py')
        self.btn_run.setEnabled(False)
        self.dl_log.clear(); self.dl_log.append(f"▶ 다운로드 시작: {codes}\n기간 {s}~{e}\n저장: {out_dir}")
        self.dl_worker = ManualDownloadWorker(codes, s, e, out_dir, script_path)
        self.dl_worker.log_signal.connect(self.dl_log.append)
        self.dl_worker.finished_signal.connect(self._on_dl_finish)
        self.dl_worker.error_signal.connect(self._on_dl_error)
        self.dl_worker.start()
    def _on_dl_finish(self, msg):
        self.btn_run.setEnabled(True)
        self.dl_log.append("\n✅ " + msg)
        QMessageBox.information(self, "완료", msg)
    def _on_dl_error(self, err):
        self.btn_run.setEnabled(True)
        self.dl_log.append("\n❌ " + err)
        QMessageBox.critical(self, "오류", err)
    # --- Training Tab ---
    def start_training(self):
        params = {
            'mode': self.train_mode.currentText(),
            'horizon': self.train_h.value(),
            'valid_days': self.train_v.value(),
            'n_estimators': self.train_t.value(),
            'version': self.train_ver.currentText()
        }
        self.train_log.clear(); self.train_log.append(f"요청: {params}")
        self.train_btn.setEnabled(False); self.train_btn.setText("⏳ 학습 중…")
        self.tr_worker = TrainingWorker(params)
        self.tr_worker.log_signal.connect(self.train_log.append)
        self.tr_worker.finished_signal.connect(self._on_tr_finish)
        self.tr_worker.error_signal.connect(self._on_tr_error)
        self.tr_worker.start()
    def _on_tr_finish(self, msg):
        self.train_btn.setEnabled(True); self.train_btn.setText("🚀 학습 시작")
        self.train_log.append("\n✅ " + msg)
        QMessageBox.information(self, "완료", msg)
        self.refresh_engine_list()
    def _on_tr_error(self, err):
        self.train_btn.setEnabled(True); self.train_btn.setText("🚀 학습 시작")
        self.train_log.append("\n❌ " + err)
        QMessageBox.critical(self, "오류", err)
    # --- Manage & Predict ---
    def refresh_engine_list(self):
        base = get_path('HOJ_ENGINE')
        files = sorted(glob.glob(os.path.join(base, '**', '*.pkl'), recursive=True), key=os.path.getmtime, reverse=True)
        self.all_engines = []
        self.tbl_eng.setRowCount(0)
        for f in files:
            name = os.path.basename(f)
            m = re.search(r'_h(\d+)_', name)
            horizon = int(m.group(1)) if m else -1
            self.all_engines.append({'name': name, 'path': f, 'horizon': horizon})
            r = self.tbl_eng.rowCount(); self.tbl_eng.insertRow(r)
            it = QTableWidgetItem(name); it.setData(Qt.UserRole, f); self.tbl_eng.setItem(r, 0, it)
        self.filter_engines_by_horizon()
        self.info_eng.setText(f"총 {len(self.all_engines)}개 엔진")
    def filter_engines_by_horizon(self):
        h = self.pred_h.value(); self.cb_engine.clear(); cnt=0
        for e in self.all_engines:
            if e['horizon'] == h:
                self.cb_engine.addItem(e['name'], e['path']); cnt+=1
        self.pred_btn.setEnabled(cnt>0)
        if cnt==0: self.cb_engine.addItem(f"(H{h} 엔진 없음)", None)
    def load_engine_metadata(self, item):
        path = item.data(Qt.UserRole)
        try:
            with open(path,'rb') as f: data = pickle.load(f)
            meta = data.get('meta',{})
            lines = ["=== 엔진 상세 ===",
                     f"파일: {os.path.basename(path)}",
                     f"생성일: {meta.get('train_date','N/A')}",
                     f"데이터 기준일: {meta.get('data_date','N/A')}",
                     f"Horizon: {meta.get('horizon','?')}일",
                     "",
                     "=== 성과 지표 ===",
                     f"ACC: {meta.get('metrics',{}).get('acc',0)*100:.2f}%",
                     f"F1 : {meta.get('metrics',{}).get('f1',0):.4f}",
                     f"AUC: {meta.get('metrics',{}).get('auc',0):.4f}",
                     f"Pos: {meta.get('metrics',{}).get('pos_rate',0)*100:.2f}%"]
            self.info_eng.setText("\n".join(lines))
        except Exception as e:
            self.info_eng.setText(f"정보 로드 실패: {e}")
    def start_prediction(self):
        path = self.cb_engine.currentData()
        if not path:
            QMessageBox.warning(self, "경고", "사용 가능한 엔진이 없습니다."); return
        date = self.pred_date.date().toString('yyyy-MM-dd')
        topn = self.pred_top.value()
        self.pred_btn.setEnabled(False); self.pred_btn.setText("⏳ 계산…")
        self.pw = PredictionWorker(path, date, topn)
        self.pw.finished_signal.connect(self._on_pred_ok)
        self.pw.error_signal.connect(self._on_pred_err)
        self.pw.start()
    def _on_pred_ok(self, df):
        self.pred_btn.setEnabled(True); self.pred_btn.setText("⚡ 예측")
        self.page_predict.tbl.setRowCount(0)
        if df is None or df.empty:
            QMessageBox.information(self, "알림", "결과 없음/휴장일"); return
        for _, row in df.iterrows():
            r = self.page_predict.tbl.rowCount(); self.page_predict.tbl.insertRow(r)
            self.page_predict.tbl.setItem(r,0,QTableWidgetItem(str(row.get('Code',''))))
            self.page_predict.tbl.setItem(r,1,QTableWidgetItem(str(row.get('Name',''))))
            self.page_predict.tbl.setItem(r,2,QTableWidgetItem(f"{row.get('Close',0):,}"))
            self.page_predict.tbl.setItem(r,3,QTableWidgetItem(f"{row.get('Pred_Score',0):.4f}"))
            self.page_predict.tbl.setItem(r,4,QTableWidgetItem(f"{row.get('Pred_Prob',0)*100:.1f}%"))
    def _on_pred_err(self, err):
        self.pred_btn.setEnabled(True); self.pred_btn.setText("⚡ 예측")
        QMessageBox.critical(self, "오류", err)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = EngineManager()
    w.show()
    sys.exit(app.exec())
