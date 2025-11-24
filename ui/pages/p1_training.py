# ui/pages/p1_training.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QLabel, QComboBox, QSpinBox, QPushButton, QTextEdit, QMessageBox)
from common.workers import TrainingWorker

class TrainingPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        gb = QGroupBox("⚙️ 모델 학습 설정")
        form = QHBoxLayout()
        
        self.cb_mode = QComboBox(); self.cb_mode.addItems(["research", "real"])
        form.addWidget(QLabel("모드:"))
        form.addWidget(self.cb_mode)
        
        self.sb_horizon = QSpinBox(); self.sb_horizon.setValue(5); self.sb_horizon.setSuffix("일")
        form.addWidget(QLabel("예측일(H):"))
        form.addWidget(self.sb_horizon)
        
        self.btn_train = QPushButton("🚀 학습 시작")
        self.btn_train.setFixedHeight(40)
        self.btn_train.clicked.connect(self.start_train)
        
        gb.setLayout(form)
        layout.addWidget(gb)
        layout.addWidget(self.btn_train)
        
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setPlaceholderText("학습 로그가 여기에 표시됩니다...")
        layout.addWidget(self.log)

    def start_train(self):
        params = {
            'mode': self.cb_mode.currentText(),
            'horizon': self.sb_horizon.value(),
            'valid_days': 365,   # 기본값
            'n_estimators': 1000, # 기본값
            'version': 'V32_UI'
        }
        self.log.append(f"학습 요청: {params}")
        self.worker = TrainingWorker(params)
        self.worker.log_signal.connect(self.log.append)
        self.worker.finished_signal.connect(lambda m: QMessageBox.information(self, "완료", m))
        self.worker.start()