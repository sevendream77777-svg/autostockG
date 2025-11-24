# ui/pages/p3_prediction.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QGroupBox, QHBoxLayout, 
                               QLabel, QDateEdit, QPushButton, QTableWidget, 
                               QTableWidgetItem, QHeaderView, QRadioButton, QLineEdit, QButtonGroup, QMessageBox)
from PySide6.QtCore import QDate, Qt
from common.workers import PredictionWorker

class PredictionPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 1. 설정 섹션
        gb_setting = QGroupBox("🔮 예측 시뮬레이션 설정")
        v_box = QVBoxLayout()
        
        # 기준일
        h_date = QHBoxLayout()
        h_date.addWidget(QLabel("예측 기준일 (T):"))
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate())
        h_date.addWidget(self.date_edit)
        h_date.addStretch()
        v_box.addLayout(h_date)

        # 대상 선택 (전체 vs 특정 종목)
        gb_target = QGroupBox("예측 대상")
        gb_target.setStyleSheet("border: 1px dotted #88c0d0; margin: 5px;")
        h_target = QHBoxLayout()
        
        self.rb_market = QRadioButton("시장 전체 (Top 10 추천)")
        self.rb_specific = QRadioButton("특정 종목 지정")
        self.rb_market.setChecked(True)
        
        self.bg_target = QButtonGroup(self)
        self.bg_target.addButton(self.rb_market)
        self.bg_target.addButton(self.rb_specific)
        
        h_target.addWidget(self.rb_market)
        h_target.addWidget(self.rb_specific)
        
        # 종목 코드 입력
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("예: 005930 (삼성전자)")
        self.txt_code.setEnabled(False)
        self.bg_target.buttonToggled.connect(lambda: self.txt_code.setEnabled(self.rb_specific.isChecked()))
        h_target.addWidget(self.txt_code)
        
        gb_target.setLayout(h_target)
        v_box.addWidget(gb_target)
        
        # 실행 버튼
        self.btn_run = QPushButton("🔮 예측 실행 (Run Prediction)")
        self.btn_run.setFixedHeight(45)
        self.btn_run.setStyleSheet("background-color: #5e81ac; font-weight: bold;")
        self.btn_run.clicked.connect(self.run_pred)
        v_box.addWidget(self.btn_run)
        
        gb_setting.setLayout(v_box)
        layout.addWidget(gb_setting)
        
        # 2. 결과 테이블
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["순위", "종목코드", "종목명", "예측 점수", "상승 확률"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

    def run_pred(self):
        target_date = self.date_edit.date().toString("yyyy-MM-dd")
        target_code = None
        
        if self.rb_specific.isChecked():
            target_code = self.txt_code.text().strip()
            if not target_code:
                QMessageBox.warning(self, "경고", "종목 코드를 입력해주세요.")
                return
        
        # 엔진 경로는 자동 탐색하도록 None 전달 (Worker에서 처리)
        self.worker = PredictionWorker(engine_path=None, target_date=target_date, top_n=10, specific_code=target_code)
        self.worker.finished_signal.connect(self.update_table)
        self.worker.error_signal.connect(lambda e: QMessageBox.critical(self, "오류", e))
        self.worker.start()

    def update_table(self, df):
        self.table.setRowCount(0)
        if df is None or df.empty:
            QMessageBox.information(self, "알림", "해당 조건의 예측 결과가 없습니다.")
            return
            
        for i, row in df.iterrows():
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(str(i + 1)))
            self.table.setItem(r, 1, QTableWidgetItem(str(row.get('code', '-'))))
            self.table.setItem(r, 2, QTableWidgetItem(str(row.get('name', '-'))))
            self.table.setItem(r, 3, QTableWidgetItem(f"{row.get('score', 0):.4f}"))
            self.table.setItem(r, 4, QTableWidgetItem(f"{row.get('prob', 0)*100:.1f}%"))