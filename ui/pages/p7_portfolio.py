# ui/pages/p5_portfolio.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QGroupBox, QTableWidget, 
                               QHeaderView, QLabel, QHBoxLayout)

class PortfolioPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        
        # 상단 요약
        gb_summary = QGroupBox("📊 자산 현황")
        h = QHBoxLayout()
        # 예시 데이터
        labels = [
            ("총 매입금액", "50,000,000"),
            ("총 평가금액", "52,500,000"),
            ("총 손익", "+2,500,000"),
            ("수익률", "+5.0%")
        ]
        for title, val in labels:
            v = QVBoxLayout()
            v.addWidget(QLabel(title))
            l_val = QLabel(val)
            l_val.setStyleSheet("font-size: 14pt; font-weight: bold;")
            if "+" in val: l_val.setStyleSheet(l_val.styleSheet() + " color: #bf616a;")
            v.addWidget(l_val)
            h.addLayout(v)
            
        gb_summary.setLayout(h)
        layout.addWidget(gb_summary)
        
        # 보유 종목 리스트
        gb_list = QGroupBox("📂 보유 종목 상세")
        v_list = QVBoxLayout()
        
        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["종목명", "보유수량", "매입가", "현재가", "평가손익", "수익률", "비중"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        v_list.addWidget(self.table)
        gb_list.setLayout(v_list)
        layout.addWidget(gb_list)