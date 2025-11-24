# ui/pages/p3_prediction.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QGroupBox, QHBoxLayout, 
                               QLabel, QDateEdit, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView)
from PySide6.QtCore import QDate
from common.workers import PredictionWorker

class PredictionPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        gb = QGroupBox("🔮 예측 시뮬레이션")
        h = QHBoxLayout()
        self.date_edit = QDateEdit(); self.date_edit.setCalendarPopup(True); self.date_edit.setDate(QDate.currentDate())
        h.addWidget(QLabel("기준일:"))
        h.addWidget(self.date_edit)
        
        btn = QPushButton("예측 실행")
        btn.clicked.connect(self.run_pred)
        h.addWidget(btn)
        gb.setLayout(h)
        layout.addWidget(gb)
        
        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["종목코드", "종목명", "예측값"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

    def run_pred(self):
        # 간단 예시
        self.worker = PredictionWorker(None, self.date_edit.date().toString("yyyy-MM-dd"), 10)
        self.worker.finished_signal.connect(self.update_table)
        self.worker.start()

    def update_table(self, df):
        self.table.setRowCount(0)
        if df is not None and not df.empty:
            for i, row in df.iterrows():
                r = self.table.rowCount()
                self.table.insertRow(r)
                self.table.setItem(r, 0, QTableWidgetItem(str(row['Code'])))
                self.table.setItem(r, 1, QTableWidgetItem(str(row['Name'])))
                self.table.setItem(r, 2, QTableWidgetItem(str(row.get('Pred', '-'))))