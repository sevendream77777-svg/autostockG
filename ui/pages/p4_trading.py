# ui/pages/p4_trading.py
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QLabel, QPushButton, QTableWidget, QTableWidgetItem, 
                               QHeaderView, QDateEdit, QLineEdit, QFormLayout, QSplitter, QComboBox)
from PySide6.QtCore import Qt, QDate

class TradingPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 상단: 시장 지표 & 컨트롤 패널
        h_top = QHBoxLayout()
        
        # KOSPI 지수 (더미)
        gb_market = QGroupBox("📊 Market Index")
        h_market = QHBoxLayout()
        self.lbl_kospi = QLabel("KOSPI: 2,500.00 (▲ 10.5)")
        self.lbl_kospi.setStyleSheet("color: #bf616a; font-weight: bold; font-size: 14pt;")
        h_market.addWidget(self.lbl_kospi)
        gb_market.setLayout(h_market)
        h_top.addWidget(gb_market)
        
        # 추천 날짜 선택
        gb_date = QGroupBox("📅 추천 기준일")
        h_date = QHBoxLayout()
        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate())
        h_date.addWidget(self.date_edit)
        btn_load = QPushButton("추천 불러오기")
        h_date.addWidget(btn_load)
        gb_date.setLayout(h_date)
        h_top.addWidget(gb_date)
        
        # 계좌 요약
        gb_account = QGroupBox("💰 내 계좌 요약")
        h_acc = QHBoxLayout()
        h_acc.addWidget(QLabel("예수금: 10,000,000원"))
        h_acc.addWidget(QLabel(" | "))
        h_acc.addWidget(QLabel("총손익: +50,000원 (+0.5%)"))
        gb_account.setLayout(h_acc)
        h_top.addWidget(gb_account)
        
        layout.addLayout(h_top)

        # 메인 스플리터 (좌: 추천목록, 중: 차트/정보, 우: 주문)
        splitter = QSplitter(Qt.Horizontal)

        # [좌측] Top 10 추천 리스트
        gb_left = QGroupBox("🏆 AI Top 10 추천")
        v_left = QVBoxLayout()
        self.table_top10 = QTableWidget()
        self.table_top10.setColumnCount(3)
        self.table_top10.setHorizontalHeaderLabels(["종목", "점수", "등락"])
        self.table_top10.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        v_left.addWidget(self.table_top10)
        gb_left.setLayout(v_left)
        splitter.addWidget(gb_left)

        # [중앙] 종목 상세 정보 & 차트 (플레이스홀더)
        gb_center = QGroupBox("📈 종목 상세 분석")
        v_center = QVBoxLayout()
        self.lbl_stock_name = QLabel("종목을 선택하세요")
        self.lbl_stock_name.setStyleSheet("font-size: 18pt; font-weight: bold; color: #eceff4;")
        self.lbl_stock_info = QLabel("현재가: - | 전일비: -")
        v_center.addWidget(self.lbl_stock_name)
        v_center.addWidget(self.lbl_stock_info)
        
        chart_area = QLabel("[ 차트 영역 (Matplotlib/PyQtGraph 예정) ]")
        chart_area.setAlignment(Qt.AlignCenter)
        chart_area.setStyleSheet("background-color: #2e3440; border: 1px solid #4c566a; min-height: 300px;")
        v_center.addWidget(chart_area)
        gb_center.setLayout(v_center)
        splitter.addWidget(gb_center)

        # [우측] 주문 및 체결
        gb_right = QGroupBox("⚡ 주식 주문 (Order)")
        v_right = QVBoxLayout()
        
        form = QFormLayout()
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("종목코드")
        form.addRow("종목코드:", self.txt_code)
        
        self.combo_type = QComboBox()
        self.combo_type.addItems(["지정가", "시장가"])
        form.addRow("주문구분:", self.combo_type)
        
        self.spin_qty = QLineEdit() # SpinBox로 교체 가능
        self.spin_qty.setPlaceholderText("수량")
        form.addRow("수량:", self.spin_qty)
        
        self.spin_price = QLineEdit()
        self.spin_price.setPlaceholderText("단가")
        form.addRow("단가:", self.spin_price)
        
        v_right.addLayout(form)
        
        h_btns = QHBoxLayout()
        btn_buy = QPushButton("매수 (Buy)")
        btn_buy.setStyleSheet("background-color: #bf616a; color: white; font-weight: bold; padding: 10px;")
        btn_sell = QPushButton("매도 (Sell)")
        btn_sell.setStyleSheet("background-color: #5e81ac; color: white; font-weight: bold; padding: 10px;")
        h_btns.addWidget(btn_buy)
        h_btns.addWidget(btn_sell)
        v_right.addLayout(h_btns)
        
        v_right.addWidget(QLabel("📋 실시간 체결/미체결"))
        self.table_orders = QTableWidget()
        self.table_orders.setColumnCount(4)
        self.table_orders.setHorizontalHeaderLabels(["시간", "종목", "구분", "상태"])
        v_right.addWidget(self.table_orders)
        
        gb_right.setLayout(v_right)
        splitter.addWidget(gb_right)
        
        # 비율 설정
        splitter.setSizes([300, 500, 300])
        layout.addWidget(splitter)