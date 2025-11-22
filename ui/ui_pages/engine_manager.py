# ============================================================
# Engine Manager UI (Unified Workbench) - Final V32
#   - Tab 0: 💾 데이터 공장 (New! 데이터 업데이트)
#   - Tab 1: 🏭 엔진 공장 (학습)
#   - Tab 2: 📊 엔진 분석실 (관리)
#   - Tab 3: 🔮 타임머신 예측기 (필터링 적용)
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
    QProgressBar
)
from PySide6.QtCore import Qt, QThread, Signal, QDate

# ------------------------------------------------------------
# 프로젝트 경로 설정
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

# Backend Import
try:
    from MODELENGINE.UTIL.train_engine_unified import run_unified_training
    from MODELENGINE.UTIL.predict_daily_top10 import run_prediction
    from MODELENGINE.UTIL.config_paths import get_path
    
    # 데이터 업데이트 모듈 (지연 임포트 또는 여기서 확인)
    import update_raw_data
    import build_features
    import build_unified_db
    # make_kospi_index_10y는 RAW 폴더에 있어서 동적 임포트 필요할 수 있음
except ImportError as e:
    print(f"⚠️ Backend Import Warning: {e}")

# ------------------------------------------------------------
# [Worker 0] 데이터 업데이트 스레드 (순차 실행)
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
                    # RAW 폴더의 모듈을 동적으로 불러와 실행
                    sys.path.append(raw_dir)
                    import make_kospi_index_10y
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
            self.finished_signal.emit("모든 데이터 작업이 완료되었습니다!")
            
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# [Worker 1] 학습용 스레드
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
            self.log_signal.emit(f"🚀 엔진 공장 가동 시작... (설정: {self.params})")
            run_unified_training(
                mode=self.params['mode'],
                horizon=self.params['horizon'],
                valid_days=self.params['valid_days'],
                n_estimators=self.params['n_estimators'],
                version=self.params['version']
            )
            self.log_signal.emit("✅ 학습 프로세스 정상 종료.")
            self.finished_signal.emit("엔진 생성이 완료되었습니다!")
        except Exception as e:
            self.error_signal.emit(str(e))

# ------------------------------------------------------------
# [Worker 2] 예측용 스레드
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
# 메인 UI 클래스
# ------------------------------------------------------------
class EngineManager(QWidget):
    def __init__(self):
        super().__init__()
        self.all_engines = [] 
        self.init_ui()
        self.refresh_engine_list()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #444; }
            QTabBar::tab { background: #333; color: #AAA; padding: 8px 20px; }
            QTabBar::tab:selected { background: #1565C0; color: #FFF; font-weight: bold; }
        """)

        self.tab_data = self.create_data_tab()     # [0단계]
        self.tab_train = self.create_train_tab()   # [1단계]
        self.tab_manage = self.create_manage_tab() # [2단계]
        self.tab_predict = self.create_predict_tab() # [3단계]

        self.tabs.addTab(self.tab_data, "💾 0단계: 데이터 공장 (Data)")
        self.tabs.addTab(self.tab_train, "🏭 1단계: 엔진 공장 (Training)")
        self.tabs.addTab(self.tab_manage, "📊 2단계: 엔진 분석실 (Manage)")
        self.tabs.addTab(self.tab_predict, "🔮 3단계: 타임머신 예측 (Predict)")

        main_layout.addWidget(self.tabs)

    # ----------------------------------------------------------------
    # [Tab 0] 데이터 공장 (Data Factory) - NEW!
    # ----------------------------------------------------------------
    def create_data_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # 안내문
        info_label = QLabel("📉 주식 데이터 업데이트 파이프라인 (순서대로 진행하세요)")
        info_label.setStyleSheet("color: #DDD; font-weight: bold; font-size: 14px; margin-bottom: 10px;")
        layout.addWidget(info_label)

        # 버튼 그룹
        btn_layout = QHBoxLayout()
        
        self.btn_step1 = QPushButton("1. 시세(RAW)")
        self.btn_step2 = QPushButton("2. KOSPI")
        self.btn_step3 = QPushButton("3. 피처생성")
        self.btn_step4 = QPushButton("4. DB통합")
        self.btn_step_all = QPushButton("⚡ 전체 실행 (1~4)")

        # 버튼 스타일링
        for btn in [self.btn_step1, self.btn_step2, self.btn_step3, self.btn_step4]:
            btn.setFixedHeight(50)
            btn.setStyleSheet("background-color: #444; color: white; font-weight: bold;")
        
        self.btn_step_all.setFixedHeight(50)
        self.btn_step_all.setStyleSheet("background-color: #D32F2F; color: white; font-weight: bold; font-size: 13px;")

        # 이벤트 연결
        self.btn_step1.clicked.connect(lambda: self.run_data_task(['stock']))
        self.btn_step2.clicked.connect(lambda: self.run_data_task(['kospi']))
        self.btn_step3.clicked.connect(lambda: self.run_data_task(['feature']))
        self.btn_step4.clicked.connect(lambda: self.run_data_task(['db']))
        self.btn_step_all.clicked.connect(lambda: self.run_data_task(['stock', 'kospi', 'feature', 'db']))

        btn_layout.addWidget(self.btn_step1)
        btn_layout.addWidget(self.btn_step2)
        btn_layout.addWidget(self.btn_step3)
        btn_layout.addWidget(self.btn_step4)
        btn_layout.addWidget(self.btn_step_all)
        
        layout.addLayout(btn_layout)

        # 진행바
        self.data_progress = QProgressBar()
        self.data_progress.setAlignment(Qt.AlignCenter)
        self.data_progress.setStyleSheet("QProgressBar { border: 1px solid #555; border-radius: 5px; text-align: center; } QProgressBar::chunk { background-color: #388E3C; }")
        layout.addWidget(self.data_progress)

        # 로그창
        self.data_log = QTextEdit()
        self.data_log.setReadOnly(True)
        self.data_log.setStyleSheet("background-color: #1E1E1E; color: #00E676; font-family: Consolas;")
        self.data_log.setPlaceholderText("데이터 작업 로그가 여기에 표시됩니다.")
        layout.addWidget(self.data_log)

        return widget

    # ----------------------------------------------------------------
    # [Tab 1] 엔진 공장
    # ----------------------------------------------------------------
    def create_train_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        group = QGroupBox("🛠️ 엔진 생산 설정")
        form = QHBoxLayout()

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
        self.btn_train.setFixedHeight(45)
        self.btn_train.setStyleSheet("background-color: #2E7D32; color: white; font-size: 14px; font-weight: bold;")
        self.btn_train.clicked.connect(self.start_training)
        layout.addWidget(self.btn_train)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("background-color: #111; color: #0F0; font-family: Consolas;")
        self.log_text.setPlaceholderText("대기 중... 학습 로그가 여기에 표시됩니다.")
        layout.addWidget(self.log_text)

        return widget

    # ----------------------------------------------------------------
    # [Tab 2] 엔진 분석실
    # ----------------------------------------------------------------
    def create_manage_tab(self):
        widget = QWidget()
        layout = QHBoxLayout(widget)

        left_panel = QVBoxLayout()
        left_panel.addWidget(QLabel("📂 보유 엔진 목록 (최신순)"))
        
        self.table_engines = QTableWidget()
        self.table_engines.setColumnCount(1)
        self.table_engines.setHorizontalHeaderLabels(["Engine Filename"])
        self.table_engines.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_engines.setSelectionBehavior(QTableWidget.SelectRows)
        self.table_engines.itemClicked.connect(self.load_engine_metadata)
        left_panel.addWidget(self.table_engines)
        
        btn_refresh = QPushButton("🔄 목록 새로고침")
        btn_refresh.clicked.connect(self.refresh_engine_list)
        left_panel.addWidget(btn_refresh)

        layout.addLayout(left_panel, 1)

        right_panel = QGroupBox("📋 엔진 상세 스펙 (성적표)")
        vbox = QVBoxLayout()
        self.txt_engine_info = QTextEdit()
        self.txt_engine_info.setReadOnly(True)
        self.txt_engine_info.setStyleSheet("font-size: 13px; line-height: 1.4;")
        vbox.addWidget(self.txt_engine_info)
        right_panel.setLayout(vbox)

        layout.addWidget(right_panel, 1)

        return widget

    # ----------------------------------------------------------------
    # [Tab 3] 타임머신 예측
    # ----------------------------------------------------------------
    def create_predict_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)

        ctl_group = QGroupBox("🔮 예측 조건 설정")
        ctl_layout = QHBoxLayout()
        
        ctl_layout.addWidget(QLabel("1. 예측 기간:"))
        self.spin_pred_horizon = QSpinBox()
        self.spin_pred_horizon.setRange(1, 60)
        self.spin_pred_horizon.setValue(5)
        self.spin_pred_horizon.setSuffix(" 일 뒤")
        self.spin_pred_horizon.valueChanged.connect(self.filter_engines_by_horizon)
        ctl_layout.addWidget(self.spin_pred_horizon)

        ctl_layout.addWidget(QLabel("2. 기준 날짜:"))
        self.date_picker = QDateEdit()
        self.date_picker.setCalendarPopup(True)
        self.date_picker.setDate(QDate.currentDate().addDays(-1))
        self.date_picker.setDisplayFormat("yyyy-MM-dd")
        ctl_layout.addWidget(self.date_picker)

        ctl_layout.addWidget(QLabel("3. 엔진 선택:"))
        self.combo_engine_sel = QComboBox()
        self.combo_engine_sel.setMinimumWidth(250)
        ctl_layout.addWidget(self.combo_engine_sel)

        ctl_layout.addWidget(QLabel("4. 출력 개수:"))
        self.spin_top = QSpinBox()
        self.spin_top.setRange(1, 100)
        self.spin_top.setValue(10)
        ctl_layout.addWidget(self.spin_top)

        self.btn_predict = QPushButton("⚡ 예측 실행")
        self.btn_predict.setFixedWidth(120)
        self.btn_predict.setStyleSheet("background-color: #1976D2; color: white; font-weight: bold;")
        self.btn_predict.clicked.connect(self.start_prediction)
        ctl_layout.addWidget(self.btn_predict)

        ctl_group.setLayout(ctl_layout)
        layout.addWidget(ctl_group)

        self.table_result = QTableWidget()
        self.table_result.setColumnCount(5)
        self.table_result.setHorizontalHeaderLabels(["종목코드", "종목명", "현재가", "예측점수(Score)", "상승확률(Prob)"])
        self.table_result.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table_result)

        return widget

    # ----------------------------------------------------------------
    # 로직 메서드
    # ----------------------------------------------------------------
    
    # [Tab 0] 데이터 업데이트 실행
    def run_data_task(self, tasks):
        self.data_log.clear()
        self.data_log.append(f"🚀 데이터 작업 시작: {tasks}")
        self.data_progress.setValue(0)
        
        # 버튼 잠금
        self.btn_step_all.setEnabled(False)
        
        self.data_worker = DataUpdateWorker(tasks)
        self.data_worker.log_signal.connect(self.data_log.append)
        self.data_worker.progress_signal.connect(self.data_progress.setValue)
        self.data_worker.finished_signal.connect(self.on_data_finished)
        self.data_worker.error_signal.connect(self.on_data_error)
        self.data_worker.start()

    def on_data_finished(self, msg):
        self.btn_step_all.setEnabled(True)
        self.data_log.append(f"\n✅ {msg}")
        QMessageBox.information(self, "완료", msg)

    def on_data_error(self, err):
        self.btn_step_all.setEnabled(True)
        self.data_log.append(f"\n❌ 에러 발생: {err}")
        QMessageBox.critical(self, "오류", str(err))

    # [Tab 1] 학습
    def start_training(self):
        params = {
            "mode": self.combo_mode.currentText(),
            "horizon": self.spin_horizon.value(),
            "valid_days": self.spin_valid.value(),
            "n_estimators": self.spin_trees.value(),
            "version": self.edit_version.currentText()
        }
        self.log_text.clear()
        self.log_text.append(f"=== 학습 요청 시작 ===\n설정: {params}")
        self.btn_train.setEnabled(False)
        self.btn_train.setText("⏳ 학습 진행 중...")

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

    # [Tab 2 & 3] 엔진 관리
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
            
        self.filter_engines_by_horizon()

    def filter_engines_by_horizon(self):
        target_h = self.spin_pred_horizon.value()
        self.combo_engine_sel.clear()
        
        found_count = 0
        for eng in self.all_engines:
            if eng['horizon'] == target_h:
                self.combo_engine_sel.addItem(eng['name'], eng['path'])
                found_count += 1
        
        if found_count == 0:
            self.combo_engine_sel.addItem(f"(h{target_h} 엔진 없음)", None)

    def load_engine_metadata(self, item):
        path = item.data(Qt.UserRole)
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
            
            meta = data.get("meta", {})
            features = data.get("features", [])
            
            info = f"📁 파일명: {os.path.basename(path)}\n"
            info += f"📅 생성일: {meta.get('train_date', 'N/A')}\n"
            info += f"💾 데이터기준: {meta.get('data_date', 'N/A')}\n"
            info += f"🎯 Horizon: {meta.get('horizon', '?')}일\n"
            if meta.get('metrics'):
                info += f"📊 정확도: {meta['metrics'].get('acc', 0)*100:.2f}%\n"
            
            self.txt_engine_info.setText(info)
        except Exception as e:
            self.txt_engine_info.setText(f"정보 로드 실패: {e}")

    def start_prediction(self):
        engine_path = self.combo_engine_sel.currentData()
        if not engine_path:
            QMessageBox.warning(self, "경고", "사용 가능한 엔진이 없습니다.")
            return
            
        target_date = self.date_picker.date().toString("yyyy-MM-dd")
        top_n = self.spin_top.value()
        
        self.btn_predict.setEnabled(False)
        self.btn_predict.setText("⏳ 계산 중...")
        
        self.pred_worker = PredictionWorker(engine_path, target_date, top_n)
        self.pred_worker.finished_signal.connect(self.on_predict_result)
        self.pred_worker.error_signal.connect(self.on_train_error)
        self.pred_worker.start()

    def on_predict_result(self, df):
        self.btn_predict.setEnabled(True)
        self.btn_predict.setText("⚡ 예측 실행")
        
        if df is None or df.empty:
            QMessageBox.warning(self, "알림", "해당 날짜의 데이터가 없거나 휴장일입니다.")
            return
            
        self.table_result.setRowCount(0)
        for _, row in df.iterrows():
            r_idx = self.table_result.rowCount()
            self.table_result.insertRow(r_idx)
            self.table_result.setItem(r_idx, 0, QTableWidgetItem(str(row['Code'])))
            self.table_result.setItem(r_idx, 1, QTableWidgetItem(str(row.get('Name', 'Unknown'))))
            self.table_result.setItem(r_idx, 2, QTableWidgetItem(f"{row['Close']:,}"))
            self.table_result.setItem(r_idx, 3, QTableWidgetItem(f"{row['Pred_Score']:.4f}"))
            self.table_result.setItem(r_idx, 4, QTableWidgetItem(f"{row['Pred_Prob']*100:.1f}%"))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = EngineManager()
    window.setWindowTitle("HOJ Engine Manager (Unified V32)")
    window.resize(1000, 700)
    window.show()
    sys.exit(app.exec())