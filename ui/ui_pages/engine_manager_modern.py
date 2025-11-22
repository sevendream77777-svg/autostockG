# ============================================================
# Engine Manager UI (Unified Workbench) - Final V32 - MODERNIZED
#   - Refactored into a single file with Inner Classes for pages
#   - Applied a Modern Dark Theme (QSS)
# ============================================================

import os
import sys
import glob
import re
import pickle
import time
import pandas as pd
from datetime import datetime

# 필수 PySide6 모듈
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QTabWidget,
    QLabel, QComboBox, QSpinBox, QPushButton, QTextEdit, 
    QTableWidget, QTableWidgetItem, QHeaderView, QSplitter, QMessageBox, QDateEdit,
    QProgressBar, QSizePolicy
)
from PySide6.QtCore import Qt, QThread, Signal, QDate, QSize

# ------------------------------------------------------------
# 프로젝트 경로 설정 (기존 로직 유지)
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
ui_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(ui_dir)
sys.path.append(root_dir)

# Backend 스크립트 동적 임포트용 경로 설정
model_engine_dir = os.path.join(root_dir, "MODELENGINE")
util_dir = os.path.join(model_engine_dir, "UTIL")
raw_dir = os.path.join(model_engine_dir, "RAW")

sys.path.append(util_dir)
sys.path.append(raw_dir)

# Backend Import (Mock for external tool dependency)
try:
    from MODELENGINE.UTIL.train_engine_unified import run_unified_training
    from MODELENGINE.UTIL.predict_daily_top10 import run_prediction
    from MODELENGINE.UTIL.config_paths import get_path
    
    # 데이터 업데이트 모듈 (지연 임포트 또는 여기서 확인)
    import update_raw_data
    import build_features
    import build_unified_db
    import make_kospi_index_10y # Assumed to be in raw_dir
    BACKEND_READY = True
except ImportError as e:
    # print(f"⚠️ Backend Import Warning: {e}")
    BACKEND_READY = False
    # Mock functions for UI display only
    def run_unified_training(mode, horizon, valid_days, n_estimators, version): time.sleep(2); print("Mock Training Finished")
    def run_prediction(engine_path, target_date, top_n): 
        time.sleep(1)
        data = {'Code': ['005930', '035420', '005380'], 'Name': ['삼성전자', 'NAVER', '현대차'], 
                'Close': [70000, 200000, 250000], 'Pred_Score': [0.95, 0.88, 0.79], 'Pred_Prob': [0.85, 0.75, 0.65]}
        return pd.DataFrame(data)
    def get_path(key): return os.path.join(os.path.dirname(os.path.abspath(__file__)), "MODELENGINE/HOJ_ENGINE/RESEARCH")


# ------------------------------------------------------------
# 공용 QSS 스타일 정의 (Dark Theme)
# ------------------------------------------------------------
def get_modern_qss():
    # Nord Theme Inspired Dark QSS
    return """
        /* General Style */
        QWidget {
            background-color: #2e3440; /* Dark Slate Background */
            color: #d8dee9; /* Light Text */
            font-size: 10pt;
            font-family: "Malgun Gothic", "Noto Sans KR", sans-serif;
        }

        /* QTabWidget - Tab Bar */
        QTabWidget::pane { 
            border: 1px solid #4c566a; /* Darker border */
            border-top: 1px solid #3b4252;
            background-color: #2e3440;
        }
        QTabBar::tab { 
            background: #3b4252; /* Slightly Lighter Tab Background */
            color: #eceff4;
            padding: 12px 25px; /* Bigger Padding */
            border: none;
            margin-right: 1px;
            min-width: 150px;
            font-weight: 500;
        }
        QTabBar::tab:selected { 
            background: #4c566a; /* Dark Accent for Selected */
            color: #88c0d0; /* Bright Accent Text */
            font-weight: bold;
            border-bottom: 2px solid #88c0d0; /* Highlight line */
        }
        
        /* QGroupBox */
        QGroupBox {
            font-size: 11pt;
            font-weight: bold;
            border: 1px solid #4c566a;
            border-radius: 5px;
            margin-top: 10px;
            padding-top: 15px;
            color: #a3be8c; /* Green Accent Title */
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top center;
            padding: 0 5px;
            background-color: #2e3440;
        }

        /* QPushButton - General */
        QPushButton {
            background-color: #5e81ac; /* Primary Blue Accent */
            color: #eceff4;
            border-radius: 6px;
            padding: 10px;
            font-weight: bold;
            border: 1px solid #4c566a;
        }
        QPushButton:hover {
            background-color: #81a1c1; /* Lighter on hover */
        }
        QPushButton:pressed {
            background-color: #5e81ac;
        }
        QPushButton:disabled {
            background-color: #3b4252;
            color: #4c566a;
        }

        /* Special Buttons for Data Tab */
        #data_step_btn {
            background-color: #4c566a;
            font-size: 9pt;
            min-height: 40px;
        }
        #data_step_btn:hover {
            background-color: #5e81ac;
        }
        #data_all_btn {
            background-color: #b48ead; /* Purple Accent for Critical Action */
            font-size: 11pt;
            min-height: 40px;
        }
        #data_all_btn:hover {
            background-color: #d08770; 
        }

        /* QProgressBar */
        QProgressBar { 
            border: 1px solid #4c566a; 
            border-radius: 5px; 
            text-align: center; 
            color: #eceff4;
            background-color: #3b4252;
        } 
        QProgressBar::chunk { 
            background-color: #a3be8c; /* Green Success Color */
            border-radius: 5px; 
        }

        /* QTextEdit, QLineEdit, QComboBox, QSpinBox, QDateEdit */
        QTextEdit, QLineEdit, QComboBox, QSpinBox, QDateEdit {
            background-color: #3b4252; /* Dark Input Fields */
            border: 1px solid #4c566a;
            border-radius: 4px;
            padding: 5px;
            color: #eceff4;
        }
        QDateEdit::drop-down, QComboBox::drop-down {
            border: none;
            background-color: #4c566a;
            width: 20px;
        }
        QComboBox:on {
            padding-top: 2px;
            padding-left: 4px;
            border-image: url(":/icons/down_arrow.png"); /* Example: Custom arrow icon */
        }

        /* QTableWidget */
        QTableWidget {
            gridline-color: #4c566a;
            background-color: #2e3440;
            alternate-background-color: #3b4252;
            border: 1px solid #4c566a;
        }
        QHeaderView::section {
            background-color: #4c566a;
            color: #88c0d0;
            padding: 5px;
            border: 1px solid #3b4252;
            font-weight: bold;
        }
        QTableWidget QTableCornerButton::section {
            background: #4c566a;
        }
        QTableWidget::item:selected {
            background-color: #5e81ac; /* Accent for selection */
            color: #eceff4;
        }
        
        /* QLabel for Info/Status */
        QLabel {
            color: #d8dee9;
        }
        .info_label {
            color: #a3be8c; /* Sub-info green */
        }
    """

# ------------------------------------------------------------
# [Worker 0] 데이터 업데이트 스레드 (순차 실행) - 기존 로직 유지
# ------------------------------------------------------------
class DataUpdateWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks # 실행할 작업 리스트 ['stock', 'kospi', 'feature', 'db']

    def run(self):
        if not BACKEND_READY:
            self.error_signal.emit("⚠️ Backend modules are not fully imported. Running Mock mode.")
            time.sleep(1)
            # Fallback for mock run
            self.tasks = ['stock', 'kospi', 'feature', 'db']
            
        try:
            total = len(self.tasks)
            for idx, task in enumerate(self.tasks):
                step_num = idx + 1
                self.progress_signal.emit(int((idx / total) * 100))
                
                if task == 'stock':
                    self.log_signal.emit(f"[{step_num}/{total}] 📈 개별 시세(RAW) 업데이트 중...")
                    update_raw_data.main()
                    
                elif task == 'kospi':
                    self.log_signal.emit(f"[{step_num}/{total}] 🇰🇷 KOSPI 지수 수집 중...")
                    sys.path.append(raw_dir)
                    make_kospi_index_10y.main()
                    
                elif task == 'feature':
                    self.log_signal.emit(f"[{step_num}/{total}] 🧮 피처(Feature) 계산 중...")
                    build_features.main()
                    
                elif task == 'db':
                    self.log_signal.emit(f"[{step_num}/{total}] 📦 통합 DB(Unified) 생성 중...")
                    build_unified_db.build_unified_db()
                
                self.log_signal.emit(f"   ✅ {task.upper()} 단계 완료.")
                time.sleep(0.5) # UI 갱신 여유

            self.progress_signal.emit(100)
            self.finished_signal.emit("모든 데이터 파이프라인 작업이 성공적으로 완료되었습니다!")
            
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# [Worker 1] 학습용 스레드 - 기존 로직 유지
# ------------------------------------------------------------
class TrainingWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, params):
        super().__init__()
        self.params = params

    def run(self):
        try:
            self.log_signal.emit(f"🚀 엔진 모델 학습 시작... (설정: {self.params})")
            run_unified_training(
                mode=self.params['mode'],
                horizon=self.params['horizon'],
                valid_days=self.params['valid_days'],
                n_estimators=self.params['n_estimators'],
                version=self.params['version']
            )
            self.log_signal.emit("✅ 학습 프로세스 정상 종료. 새로운 엔진이 생성되었습니다.")
            self.finished_signal.emit("엔진 생성이 완료되었습니다!")
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# [Worker 2] 예측용 스레드 - 기존 로직 유지
# ------------------------------------------------------------
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
            df_result = run_prediction(
                engine_path=self.engine_path, 
                target_date=self.target_date, 
                top_n=self.top_n
            )
            self.finished_signal.emit(df_result)
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# 각 탭의 UI/로직을 캡슐화한 내부 클래스 (리팩토링)
# ------------------------------------------------------------
class _UIDataUpdatePage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 안내문
        info_label = QLabel("데이터 파이프라인 관리: RAW 데이터 수집부터 통합 DB 생성까지 순차 실행")
        info_label.setObjectName("info_label")
        info_label.setStyleSheet("font-weight: bold; font-size: 13px; color: #88c0d0;")
        layout.addWidget(info_label)

        # 버튼 그룹
        btn_group = QGroupBox("▶️ 데이터 파이프라인 단계별 실행")
        btn_layout = QHBoxLayout()
        
        self.btn_step1 = QPushButton("1. 시세(RAW)")
        self.btn_step2 = QPushButton("2. KOSPI")
        self.btn_step3 = QPushButton("3. 피처생성")
        self.btn_step4 = QPushButton("4. DB통합")
        self.btn_step_all = QPushButton("⚡ 전체 실행 (1~4) - 권장")

        # 버튼 디자인 식별자 (QSS 적용을 위함)
        for btn in [self.btn_step1, self.btn_step2, self.btn_step3, self.btn_step4]:
            btn.setObjectName("data_step_btn")
            btn.setFixedHeight(45)
        self.btn_step_all.setObjectName("data_all_btn")
        self.btn_step_all.setFixedHeight(50)

        # 이벤트 연결 (메인 매니저의 함수에 위임)
        self.btn_step1.clicked.connect(lambda: self.manager.run_data_task(['stock']))
        self.btn_step2.clicked.connect(lambda: self.manager.run_data_task(['kospi']))
        self.btn_step3.clicked.connect(lambda: self.manager.run_data_task(['feature']))
        self.btn_step4.clicked.connect(lambda: self.manager.run_data_task(['db']))
        self.btn_step_all.clicked.connect(lambda: self.manager.run_data_task(['stock', 'kospi', 'feature', 'db']))

        btn_layout.addWidget(self.btn_step1)
        btn_layout.addWidget(self.btn_step2)
        btn_layout.addWidget(self.btn_step3)
        btn_layout.addWidget(self.btn_step4)
        btn_layout.addWidget(self.btn_step_all)
        btn_group.setLayout(btn_layout)
        
        layout.addWidget(btn_group)

        # 진행바
        self.data_progress = QProgressBar()
        self.data_progress.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.data_progress)

        # 로그창
        self.data_log = QTextEdit()
        self.data_log.setReadOnly(True)
        self.data_log.setPlaceholderText("데이터 작업 로그가 여기에 실시간으로 표시됩니다...")
        self.data_log.setStyleSheet("color: #a3be8c; font-family: Consolas;") # 로그 전용 색상
        layout.addWidget(self.data_log)

class _UITrainingPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        group = QGroupBox("⚙️ 엔진 학습 파라미터 설정")
        form = QHBoxLayout()

        # Input Widgets
        form.addWidget(QLabel("모드:"))
        self.combo_mode = QComboBox()
        self.combo_mode.addItems(["research", "real"])
        self.combo_mode.currentTextChanged.connect(lambda t: self.spin_valid.setEnabled(t == 'research'))
        form.addWidget(self.combo_mode)

        form.addWidget(QLabel("예측일(Horizon):"))
        self.spin_horizon = QSpinBox()
        self.spin_horizon.setRange(1, 60)
        self.spin_horizon.setValue(5)
        self.spin_horizon.setSuffix("일 뒤")
        form.addWidget(self.spin_horizon)

        form.addWidget(QLabel("검증기간:"))
        self.spin_valid = QSpinBox()
        self.spin_valid.setRange(30, 1000)
        self.spin_valid.setValue(365)
        self.spin_valid.setSuffix("일")
        form.addWidget(self.spin_valid)

        form.addWidget(QLabel("나무(Trees):"))
        self.spin_trees = QSpinBox()
        self.spin_trees.setRange(100, 10000)
        self.spin_trees.setValue(1000)
        self.spin_trees.setSingleStep(100)
        form.addWidget(self.spin_trees)

        form.addWidget(QLabel("버전태그:"))
        self.edit_version = QComboBox()
        self.edit_version.addItems(["V31", "V32", "TEST"])
        self.edit_version.setEditable(True)
        form.addWidget(self.edit_version)

        group.setLayout(form)
        layout.addWidget(group)

        self.btn_train = QPushButton("🚀 엔진 생산 시작 (Start Training)")
        self.btn_train.setFixedHeight(50)
        self.btn_train.setStyleSheet("background-color: #a3be8c; color: #2e3440; font-size: 16px; font-weight: bold;")
        self.btn_train.clicked.connect(self.manager.start_training)
        layout.addWidget(self.btn_train)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("모델 학습 로그가 여기에 실시간으로 표시됩니다.")
        self.log_text.setStyleSheet("color: #88c0d0; font-family: Consolas;")
        layout.addWidget(self.log_text)

class _UIManagerPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()

    def init_ui(self):
        # Use QSplitter for responsive side-by-side layout
        splitter = QSplitter(Qt.Horizontal)
        
        # Left Panel (Engine List)
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.addWidget(QLabel("📂 보유 엔진 목록 (클릭 시 상세 스펙 표시)"))
        
        self.table_engines = QTableWidget()
        self.table_engines.setColumnCount(1)
        self.table_engines.setHorizontalHeaderLabels(["Engine Filename"])
        self.table_engines.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_engines.setSelectionBehavior(QTableWidget.SelectRows)
        self.table_engines.itemClicked.connect(self.manager.load_engine_metadata)
        left_layout.addWidget(self.table_engines)
        
        btn_refresh = QPushButton("🔄 목록 새로고침")
        btn_refresh.setObjectName("data_step_btn")
        btn_refresh.clicked.connect(self.manager.refresh_engine_list)
        left_layout.addWidget(btn_refresh)
        
        # Right Panel (Engine Info/Specs)
        right_panel = QGroupBox("📋 엔진 상세 스펙 (성과표)")
        vbox = QVBoxLayout()
        self.txt_engine_info = QTextEdit()
        self.txt_engine_info.setReadOnly(True)
        self.txt_engine_info.setPlaceholderText("엔진을 선택하면 상세 정보가 여기에 로드됩니다.")
        self.txt_engine_info.setStyleSheet("font-size: 10pt; line-height: 1.6; color: #eceff4;")
        vbox.addWidget(self.txt_engine_info)
        right_panel.setLayout(vbox)

        # Add to splitter
        splitter.addWidget(left_widget)
        splitter.addWidget(right_panel)
        splitter.setSizes([300, 700]) # Initial ratio

        main_layout = QHBoxLayout(self)
        main_layout.addWidget(splitter)

class _UIPredictPage(QWidget):
    def __init__(self, manager):
        super().__init__()
        self.manager = manager
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        ctl_group = QGroupBox("🔮 예측 조건 설정 및 실행")
        ctl_layout = QHBoxLayout()
        
        # 1. 예측 기간
        ctl_layout.addWidget(QLabel("Horizon:"))
        self.spin_pred_horizon = QSpinBox()
        self.spin_pred_horizon.setRange(1, 60)
        self.spin_pred_horizon.setValue(5)
        self.spin_pred_horizon.setSuffix(" 일 뒤")
        self.spin_pred_horizon.valueChanged.connect(self.manager.filter_engines_by_horizon)
        self.spin_pred_horizon.setMaximumWidth(100)
        ctl_layout.addWidget(self.spin_pred_horizon)
        
        ctl_layout.addSpacing(20)

        # 2. 기준 날짜
        ctl_layout.addWidget(QLabel("기준 날짜:"))
        self.date_picker = QDateEdit()
        self.date_picker.setCalendarPopup(True)
        self.date_picker.setDate(QDate.currentDate().addDays(-1))
        self.date_picker.setDisplayFormat("yyyy-MM-dd")
        self.date_picker.setMaximumWidth(150)
        ctl_layout.addWidget(self.date_picker)
        
        ctl_layout.addSpacing(20)

        # 3. 엔진 선택
        ctl_layout.addWidget(QLabel("엔진 선택:"))
        self.combo_engine_sel = QComboBox()
        self.combo_engine_sel.setMinimumWidth(300)
        ctl_layout.addWidget(self.combo_engine_sel)

        # 4. 출력 개수
        ctl_layout.addWidget(QLabel("Top N:"))
        self.spin_top = QSpinBox()
        self.spin_top.setRange(1, 100)
        self.spin_top.setValue(10)
        self.spin_top.setMaximumWidth(60)
        ctl_layout.addWidget(self.spin_top)
        
        ctl_layout.addStretch(1)

        # 5. 예측 실행 버튼
        self.btn_predict = QPushButton("⚡ 예측 실행")
        self.btn_predict.setFixedWidth(150)
        self.btn_predict.setFixedHeight(40)
        self.btn_predict.setStyleSheet("background-color: #88c0d0; color: #2e3440;") # Cyan accent for prediction
        self.btn_predict.clicked.connect(self.manager.start_prediction)
        ctl_layout.addWidget(self.btn_predict)

        ctl_group.setLayout(ctl_layout)
        layout.addWidget(ctl_group)

        layout.addWidget(QLabel("📈 예측 결과 (Top N 종목)"))
        
        self.table_result = QTableWidget()
        self.table_result.setColumnCount(5)
        self.table_result.setHorizontalHeaderLabels(["종목코드", "종목명", "현재가 (W)", "예측점수 (Score)", "상승확률 (Prob)"])
        self.table_result.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table_result)

# ------------------------------------------------------------
# 메인 UI 클래스 (메인 창 및 로직 통합)
# ------------------------------------------------------------
class EngineManager(QWidget):
    def __init__(self):
        super().__init__()
        self.all_engines = [] 
        self.setup_ui()
        self.refresh_engine_list()

    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        self.setStyleSheet(get_modern_qss())
        
        self.tabs = QTabWidget()

        # UI Page Instances
        self.data_page = _UIDataUpdatePage(self)
        self.train_page = _UITrainingPage(self)
        self.manage_page = _UIManagerPage(self)
        self.predict_page = _UIPredictPage(self)
        
        self.data_log = self.data_page.data_log
        self.data_progress = self.data_page.data_progress
        self.btn_step_all = self.data_page.btn_step_all
        
        self.log_text = self.train_page.log_text
        self.btn_train = self.train_page.btn_train
        
        self.table_engines = self.manage_page.table_engines
        self.txt_engine_info = self.manage_page.txt_engine_info
        
        self.combo_engine_sel = self.predict_page.combo_engine_sel
        self.spin_pred_horizon = self.predict_page.spin_pred_horizon
        self.date_picker = self.predict_page.date_picker
        self.spin_top = self.predict_page.spin_top
        self.btn_predict = self.predict_page.btn_predict
        self.table_result = self.predict_page.table_result

        # Add Tabs
        self.tabs.addTab(self.data_page, "💾 0단계: 데이터 파이프라인")
        self.tabs.addTab(self.train_page, "🏭 1단계: 모델 학습실")
        self.tabs.addTab(self.manage_page, "📊 2단계: 엔진 분석실")
        self.tabs.addTab(self.predict_page, "🔮 3단계: 예측 및 검증")

        main_layout.addWidget(self.tabs)
        self.setWindowTitle("HOJ Engine Manager (Unified V32) - Modern")
        self.resize(1200, 800)

    # ----------------------------------------------------------------
    # 로직 메서드 (기존 로직 유지 및 연결)
    # ----------------------------------------------------------------
    
    # [Tab 0] 데이터 업데이트 실행
    def run_data_task(self, tasks):
        self.data_log.clear()
        self.data_log.append(f"=== 🚀 데이터 작업 시작: {tasks} ===")
        self.data_progress.setValue(0)
        
        # 버튼 잠금
        self.data_page.btn_step_all.setEnabled(False)
        for btn in [self.data_page.btn_step1, self.data_page.btn_step2, self.data_page.btn_step3, self.data_page.btn_step4]:
             btn.setEnabled(False)
        
        self.data_worker = DataUpdateWorker(tasks)
        self.data_worker.log_signal.connect(self.data_log.append)
        self.data_worker.progress_signal.connect(self.data_progress.setValue)
        self.data_worker.finished_signal.connect(self.on_data_finished)
        self.data_worker.error_signal.connect(self.on_data_error)
        self.data_worker.start()

    def on_data_finished(self, msg):
        self.data_page.btn_step_all.setEnabled(True)
        for btn in [self.data_page.btn_step1, self.data_page.btn_step2, self.data_page.btn_step3, self.data_page.btn_step4]:
             btn.setEnabled(True)
        self.data_log.append(f"\n✅ {msg}")
        QMessageBox.information(self, "완료", msg)

    def on_data_error(self, err):
        self.data_page.btn_step_all.setEnabled(True)
        for btn in [self.data_page.btn_step1, self.data_page.btn_step2, self.data_page.btn_step3, self.data_page.btn_step4]:
             btn.setEnabled(True)
        self.data_log.append(f"\n❌ 에러 발생: {err}")
        QMessageBox.critical(self, "오류", str(err))

    # [Tab 1] 학습
    def start_training(self):
        params = {
            "mode": self.train_page.combo_mode.currentText(),
            "horizon": self.train_page.spin_horizon.value(),
            "valid_days": self.train_page.spin_valid.value(),
            "n_estimators": self.train_page.spin_trees.value(),
            "version": self.train_page.edit_version.currentText()
        }
        self.log_text.clear()
        self.log_text.append(f"=== 🚀 학습 요청 시작 ===\n설정: {params}")
        self.btn_train.setEnabled(False)
        self.btn_train.setText("⏳ 학습 진행 중... (Wait)")

        self.worker = TrainingWorker(params)
        self.worker.log_signal.connect(self.log_text.append)
        self.worker.finished_signal.connect(self.on_train_finished)
        self.worker.error_signal.connect(self.on_train_error)
        self.worker.start()

    def on_train_finished(self, msg):
        self.btn_train.setEnabled(True)
        self.btn_train.setText("🚀 엔진 생산 시작 (Start Training)")
        self.log_text.append(f"\n✅ {msg}")
        QMessageBox.information(self, "완료", msg)
        self.refresh_engine_list()

    def on_train_error(self, err):
        self.btn_train.setEnabled(True)
        self.btn_train.setText("🚀 엔진 생산 시작 (Start Training)")
        self.log_text.append(f"\n❌ 오류 발생: {err}")
        QMessageBox.critical(self, "오류", str(err))

    # [Tab 2 & 3] 엔진 관리 및 필터링
    def refresh_engine_list(self):
        base_path = get_path("HOJ_ENGINE")
        pattern = os.path.join(base_path, "**", "*.pkl")
        files = glob.glob(pattern, recursive=True)
        files.sort(key=os.path.getmtime, reverse=True)
        
        self.all_engines = []
        self.table_engines.setRowCount(0)
        
        for f in files:
            name = os.path.basename(f)
            h_val = -1
            match = re.search(r"_h(\d+)_", name)
            if match:
                h_val = int(match.group(1))
            
            self.all_engines.append({'name': name, 'path': f, 'horizon': h_val})

            row = self.table_engines.rowCount()
            self.table_engines.insertRow(row)
            item = QTableWidgetItem(name)
            item.setData(Qt.UserRole, f)
            self.table_engines.setItem(row, 0, item)
            
        # Update predictor list after refreshing
        self.filter_engines_by_horizon()
        self.txt_engine_info.setText(f"총 {len(self.all_engines)}개의 엔진이 로드되었습니다.")

    def filter_engines_by_horizon(self):
        target_h = self.spin_pred_horizon.value()
        self.combo_engine_sel.clear()
        
        found_count = 0
        for eng in self.all_engines:
            if eng['horizon'] == target_h:
                self.combo_engine_sel.addItem(eng['name'], eng['path'])
                found_count += 1
        
        if found_count == 0:
            self.combo_engine_sel.addItem(f"(Horizon {target_h} 엔진 없음)", None)
            self.btn_predict.setEnabled(False)
        else:
             self.btn_predict.setEnabled(True)

    def load_engine_metadata(self, item):
        path = item.data(Qt.UserRole)
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
            
            meta = data.get("meta", {})
            
            info = f"=== 📁 엔진 상세 정보 ===\n"
            info += f"  - **파일명**: {os.path.basename(path)}\n"
            info += f"  - **생성일**: {meta.get('train_date', 'N/A')}\n"
            info += f"  - **데이터 기준일**: {meta.get('data_date', 'N/A')}\n"
            info += f"  - **예측 기간 (Horizon)**: {meta.get('horizon', '?')}일\n"
            
            metrics = meta.get('metrics', {})
            info += "\n=== 📊 주요 성과 지표 ===\n"
            info += f"  - **정확도 (ACC)**: {metrics.get('acc', 0)*100:.2f}%\n"
            info += f"  - **F1 Score**: {metrics.get('f1', 0):.4f}\n"
            info += f"  - **AUC Score**: {metrics.get('auc', 0):.4f}\n"
            info += f"  - **Positive Rate**: {metrics.get('pos_rate', 0)*100:.2f}%\n"
            
            self.txt_engine_info.setText(info)
        except Exception as e:
            self.txt_engine_info.setText(f"❌ 정보 로드 실패: {e}")

    def start_prediction(self):
        engine_path = self.combo_engine_sel.currentData()
        if not engine_path:
            QMessageBox.warning(self, "경고", "사용 가능한 엔진이 없습니다.")
            return
            
        target_date = self.date_picker.date().toString("yyyy-MM-dd")
        top_n = self.spin_top.value()
        
        self.btn_predict.setEnabled(False)
        self.btn_predict.setText("⏳ 예측 계산 중...")
        
        self.pred_worker = PredictionWorker(engine_path, target_date, top_n)
        self.pred_worker.finished_signal.connect(self.on_predict_result)
        self.pred_worker.error_signal.connect(self.on_predict_error)
        self.pred_worker.start()

    def on_predict_result(self, df):
        self.btn_predict.setEnabled(True)
        self.btn_predict.setText("⚡ 예측 실행")
        
        if df is None or df.empty:
            QMessageBox.warning(self, "알림", "해당 날짜의 데이터가 없거나 휴장일입니다.")
            self.table_result.setRowCount(0)
            return
            
        self.table_result.setRowCount(0)
        for _, row in df.iterrows():
            r_idx = self.table_result.rowCount()
            self.table_result.insertRow(r_idx)
            # Apply formatting to improve readability
            self.table_result.setItem(r_idx, 0, QTableWidgetItem(str(row['Code'])))
            self.table_result.setItem(r_idx, 1, QTableWidgetItem(str(row.get('Name', 'Unknown'))))
            
            close_item = QTableWidgetItem(f"{row['Close']:,}")
            close_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.table_result.setItem(r_idx, 2, close_item)
            
            score_item = QTableWidgetItem(f"{row['Pred_Score']:.4f}")
            score_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.table_result.setItem(r_idx, 3, score_item)
            
            prob_item = QTableWidgetItem(f"{row['Pred_Prob']*100:.1f}%")
            prob_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            # Highlight high probability in green
            if row['Pred_Prob'] > 0.7:
                 prob_item.setForeground(Qt.GlobalColor.darkCyan)
            self.table_result.setItem(r_idx, 4, prob_item)

    def on_predict_error(self, err):
        self.btn_predict.setEnabled(True)
        self.btn_predict.setText("⚡ 예측 실행")
        QMessageBox.critical(self, "오류", str(err))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = EngineManager()
    window.show()
    sys.exit(app.exec())