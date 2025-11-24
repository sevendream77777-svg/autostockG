# ui/pages/p1_training.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QLabel, QComboBox, QSpinBox, QPushButton, QTextEdit, 
                               QMessageBox, QRadioButton, QButtonGroup, QFormLayout)
from PySide6.QtCore import Qt
from common.workers import TrainingWorker

class TrainingPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # --- [A] 데이터 관련 설정 ---
        gb_data = QGroupBox("A. 데이터 설정 (Data Config)")
        layout_data = QHBoxLayout()
        
        # Horizon (라벨 윈도우)
        layout_data.addWidget(QLabel("예측 목표(Horizon):"))
        self.cb_horizon = QComboBox()
        self.cb_horizon.addItems(["1일", "2일", "5일", "10일", "20일", "60일"])
        self.cb_horizon.setCurrentText("5일") # 기본값
        layout_data.addWidget(self.cb_horizon)

        # Input Window (입력 기간)
        layout_data.addWidget(QLabel("입력 윈도우:"))
        self.cb_input_window = QComboBox()
        self.cb_input_window.addItems(["20일", "60일", "120일"])
        self.cb_input_window.setCurrentText("60일")
        layout_data.addWidget(self.cb_input_window)

        gb_data.setLayout(layout_data)
        layout.addWidget(gb_data)

        # --- [B] & [C] 피처 및 엔진 선택 ---
        gb_model = QGroupBox("B & C. 모델 엔진 구성 (Engine Config)")
        layout_model = QFormLayout()

        # 피처 그룹 선택
        self.cb_feature_group = QComboBox()
        self.cb_feature_group.addItems(["HOJ (가격기반)", "SLE (펀더멘탈)", "Combo (HOJ+SLE)"])
        layout_model.addRow("피처 그룹:", self.cb_feature_group)

        # 엔진 알고리즘 선택
        self.cb_engine_type = QComboBox()
        self.cb_engine_type.addItems(["XGBoost (Standard)", "LightGBM", "RandomForest", "Meta Model (Ensemble)"])
        layout_model.addRow("엔진 알고리즘:", self.cb_engine_type)

        gb_model.setLayout(layout_model)
        layout.addWidget(gb_model)

        # --- [D] 학습 기간 및 모드 ---
        gb_mode = QGroupBox("D. 학습 모드 (Training Mode)")
        layout_mode = QHBoxLayout()
        
        self.bg_mode = QButtonGroup(self)
        self.rb_research = QRadioButton("🧪 연구 모드 (9년 학습 + 1년 검증)")
        self.rb_real = QRadioButton("🚀 실전 모드 (전체 10년 학습)")
        self.rb_research.setChecked(True)
        
        self.bg_mode.addButton(self.rb_research)
        self.bg_mode.addButton(self.rb_real)
        
        layout_mode.addWidget(self.rb_research)
        layout_mode.addWidget(self.rb_real)
        
        gb_mode.setLayout(layout_mode)
        layout.addWidget(gb_mode)

        # 실행 버튼
        self.btn_train = QPushButton("🔥 엔진 학습 시작 (Start Training)")
        self.btn_train.setFixedHeight(50)
        self.btn_train.setStyleSheet("background-color: #bf616a; font-weight: bold; font-size: 12pt;")
        self.btn_train.clicked.connect(self.start_train)
        layout.addWidget(self.btn_train)
        
        # 로그창
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("background-color: #2e3440; color: #d8dee9; font-family: Consolas;")
        layout.addWidget(self.log)

    def start_train(self):
        # UI에서 파라미터 수집
        h_val = int(self.cb_horizon.currentText().replace("일", ""))
        iw_val = int(self.cb_input_window.currentText().replace("일", ""))
        mode_val = "real" if self.rb_real.isChecked() else "research"
        
        params = {
            'mode': mode_val,
            'horizon': h_val,
            'input_window': iw_val,
            'feature_group': self.cb_feature_group.currentText(),
            'engine_type': self.cb_engine_type.currentText(),
            'n_estimators': 1000, # 고정값 또는 추가 설정 가능
            'version': f"V34_UI_{mode_val.upper()}"
        }
        
        self.log.append(f"🚀 학습 요청 시작...\n설정: {params}")
        self.log.append("-" * 40)
        
        self.worker = TrainingWorker(params)
        self.worker.log_signal.connect(self.log.append)
        self.worker.finished_signal.connect(lambda m: QMessageBox.information(self, "학습 완료", m))
        self.worker.error_signal.connect(lambda e: self.log.append(f"❌ 에러 발생: {e}"))
        self.worker.start()