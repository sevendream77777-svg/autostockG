# ui/pages/p6_settings.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QGroupBox, QFormLayout, 
                               QLineEdit, QCheckBox, QPushButton, QLabel, QFileDialog)

class SettingsPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        
        # 1. 경로 설정
        gb_path = QGroupBox("📁 경로 설정 (Paths)")
        f_path = QFormLayout()
        
        self.txt_engine = QLineEdit()
        self.txt_engine.setText(r"F:\autostockG\MODELENGINE")
        btn_find = QPushButton("찾기")
        btn_find.clicked.connect(self.find_path)
        
        f_path.addRow("MODELENGINE Root:", self.txt_engine)
        f_path.addRow("", btn_find)
        gb_path.setLayout(f_path)
        layout.addWidget(gb_path)
        
        # 2. API 설정
        gb_api = QGroupBox("🔑 증권사 API 설정 (Kiwoom)")
        f_api = QFormLayout()
        self.txt_id = QLineEdit()
        self.txt_pw = QLineEdit()
        self.txt_pw.setEchoMode(QLineEdit.Password)
        self.chk_mock = QCheckBox("모의투자 접속 (Mock Trading)")
        self.chk_mock.setChecked(True)
        
        f_api.addRow("아이디:", self.txt_id)
        f_api.addRow("비밀번호:", self.txt_pw)
        f_api.addRow("", self.chk_mock)
        gb_api.setLayout(f_api)
        layout.addWidget(gb_api)
        
        # 3. 자동화 설정
        gb_auto = QGroupBox("🤖 자동매매 스케줄")
        f_auto = QFormLayout()
        self.chk_auto_start = QCheckBox("프로그램 시작 시 자동 접속")
        self.chk_daily_routine = QCheckBox("장 마감 후 자동 데이터 수집 및 학습 (15:40~)")
        f_auto.addRow(self.chk_auto_start)
        f_auto.addRow(self.chk_daily_routine)
        gb_auto.setLayout(f_auto)
        layout.addWidget(gb_auto)
        
        # 저장 버튼
        btn_save = QPushButton("설정 저장 (Save Config)")
        btn_save.setFixedHeight(50)
        layout.addWidget(btn_save)
        
        layout.addStretch()

    def find_path(self):
        d = QFileDialog.getExistingDirectory(self, "Select Folder")
        if d: self.txt_engine.setText(d)