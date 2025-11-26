import sys
import os
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QLabel, QPushButton, QTableWidget, QTableWidgetItem, 
                               QHeaderView, QDateEdit, QLineEdit, QFormLayout, 
                               QSplitter, QComboBox, QMessageBox)
from PySide6.QtCore import Qt, QDate, QTimer, Slot, QThread, Signal

# ---------------------------------------------------------
# [필수] 루트 경로 설정
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(root_dir)

# Kiwoom REST API 모듈 임포트
try:
    from kiwoom_rest.kiwoom_api import KiwoomRestApi
except ImportError:
    KiwoomRestApi = None

# ==========================================================
# [백그라운드] 데이터 수집 스레드 (멈춤 방지)
# ==========================================================
class DataFetcher(QThread):
    data_received = Signal(dict, dict) # kospi, account

    def __init__(self, api):
        super().__init__()
        self.api = api

    def run(self):
        """API 문서에 맞춘 정확한 데이터 요청"""
        if not self.api: return

        # --------------------------------------------------
        # 1. KOSPI 조회 (ka20003) - 문서 기준 수정
        # --------------------------------------------------
        kospi_data = {}
        try:
            # [문서] 필수 파라미터: inds_cd
            res = self.api._call_api(
                api_id="ka20003", 
                url_path="/api/dostk/sect", 
                body={"inds_cd": "001"}, 
                method="POST"
            )
            
            # [문서] 응답 구조: {"all_inds_idex": [...], "return_code": 0}
            if res and str(res.get("return_code")) == "0":
                data_list = res.get("all_inds_idex", [])
                if data_list and len(data_list) > 0:
                    kospi_data = data_list[0] # 리스트 첫번째 요소가 KOSPI
            else:
                print(f"[Error] KOSPI 실패: {res.get('return_msg', res)}") 
        except Exception as e:
            print(f"[Critical] KOSPI 예외: {e}")

        # --------------------------------------------------
        # 2. 예수금 조회 (kt00001) - 문서 기준 수정
        # --------------------------------------------------
        account_data = {}
        try:
            # [문서] Body: qry_tp
            res = self.api.get_deposit_details(qry_tp="2")
            
            # [문서] 응답 구조: {"entr": "...", "return_code": 0} -> output 래퍼 없음!
            if res and str(res.get("return_code")) == "0":
                account_data = res # 전체 응답을 그대로 전달 (entr이 루트에 있음)
            else:
                print(f"[Error] 예수금 실패: {res.get('return_msg', res)}")
        except Exception as e:
            print(f"[Critical] 예수금 예외: {e}")
        
        self.data_received.emit(kospi_data, account_data)

class TradingPage(QWidget):
    def __init__(self):
        super().__init__()
        
        # API 초기화
        self.api = None
        if KiwoomRestApi:
            try:
                self.api = KiwoomRestApi()
                print("[System] Kiwoom REST API 준비 완료")
            except Exception as e:
                print(f"[System] API 준비 실패: {e}")

        # 스레드 설정
        self.worker = None
        if self.api:
            self.worker = DataFetcher(self.api)
            self.worker.data_received.connect(self.on_data_update)

        # UI 구성
        self.init_ui()
        self.init_signals()
        
        # 자동 갱신 (5초)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.start_background_worker)
        self.refresh_timer.start(5000) 

        # 초기 실행 (1초 후)
        QTimer.singleShot(1000, self.start_background_worker)

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # --- 상단 ---
        h_top = QHBoxLayout()
        
        gb_market = QGroupBox("📊 Market Index (KOSPI)")
        h_market = QHBoxLayout()
        self.lbl_kospi = QLabel("KOSPI: 조회 대기...")
        self.lbl_kospi.setStyleSheet("color: #bf616a; font-weight: bold; font-size: 14pt;")
        h_market.addWidget(self.lbl_kospi)
        gb_market.setLayout(h_market)
        h_top.addWidget(gb_market)
        
        gb_account = QGroupBox("💰 내 계좌 (예수금)")
        h_acc = QHBoxLayout()
        self.lbl_deposit = QLabel("예수금: - 원")
        self.lbl_deposit.setStyleSheet("font-weight: bold; color: #ebcb8b;")
        h_acc.addWidget(self.lbl_deposit)
        
        btn_refresh = QPushButton("🔄")
        btn_refresh.setFixedWidth(30)
        btn_refresh.clicked.connect(self.start_background_worker)
        h_acc.addWidget(btn_refresh)
        gb_account.setLayout(h_acc)
        h_top.addWidget(gb_account)
        
        layout.addLayout(h_top)

        # --- 중앙 ---
        splitter = QSplitter(Qt.Horizontal)

        # [좌측] 추천
        gb_left = QGroupBox("🏆 추천")
        v_left = QVBoxLayout()
        self.table_top10 = QTableWidget()
        self.table_top10.setColumnCount(3)
        self.table_top10.setHorizontalHeaderLabels(["코드", "종목명", "점수"])
        self.table_top10.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_top10.setSelectionBehavior(QTableWidget.SelectRows)
        v_left.addWidget(self.table_top10)
        gb_left.setLayout(v_left)
        splitter.addWidget(gb_left)

        # [중앙] 시세
        gb_center = QGroupBox("📈 시세")
        v_center = QVBoxLayout()
        self.lbl_stock_name = QLabel("종목 선택")
        self.lbl_stock_name.setStyleSheet("font-size: 16pt; font-weight: bold;")
        self.lbl_current_price = QLabel("현재가: -")
        self.lbl_current_price.setStyleSheet("font-size: 14pt; color: #a3be8c;")
        self.lbl_rate = QLabel("등락률: -")
        v_center.addWidget(self.lbl_stock_name)
        v_center.addWidget(self.lbl_current_price)
        v_center.addWidget(self.lbl_rate)
        
        self.chart_area = QLabel("차트 영역")
        self.chart_area.setAlignment(Qt.AlignCenter)
        self.chart_area.setStyleSheet("background: #2e3440; border: 1px solid #4c566a; min-height: 200px;")
        v_center.addWidget(self.chart_area)
        
        gb_center.setLayout(v_center)
        splitter.addWidget(gb_center)

        # [우측] 주문
        gb_right = QGroupBox("⚡ 주문")
        v_right = QVBoxLayout()
        form = QFormLayout()
        self.txt_code = QLineEdit()
        self.txt_code.setPlaceholderText("종목코드")
        form.addRow("코드:", self.txt_code)
        self.combo_type = QComboBox()
        self.combo_type.addItems(["지정가", "시장가"])
        form.addRow("구분:", self.combo_type)
        self.spin_qty = QLineEdit()
        self.spin_qty.setPlaceholderText("수량")
        form.addRow("수량:", self.spin_qty)
        self.spin_price = QLineEdit()
        self.spin_price.setPlaceholderText("단가")
        form.addRow("단가:", self.spin_price)
        v_right.addLayout(form)
        
        h_btns = QHBoxLayout()
        self.btn_buy = QPushButton("매수")
        self.btn_sell = QPushButton("매도")
        h_btns.addWidget(self.btn_buy)
        h_btns.addWidget(self.btn_sell)
        v_right.addLayout(h_btns)
        
        self.table_orders = QTableWidget()
        self.table_orders.setColumnCount(4)
        self.table_orders.setHorizontalHeaderLabels(["주문번호", "종목", "구분", "수량"])
        v_right.addWidget(self.table_orders)
        
        gb_right.setLayout(v_right)
        splitter.addWidget(gb_right)
        
        splitter.setSizes([200, 400, 200])
        layout.addWidget(splitter)

    def init_signals(self):
        self.table_top10.cellClicked.connect(self.on_table_cell_clicked)
        self.btn_buy.clicked.connect(lambda: self.send_order("buy"))
        self.btn_sell.clicked.connect(lambda: self.send_order("sell"))
        self.txt_code.returnPressed.connect(self.on_code_entered)
        self.load_mock_recommendations()

    def on_code_entered(self):
        code = self.txt_code.text().strip()
        if len(code) == 6:
            self.fetch_stock_price(code)

    def start_background_worker(self):
        if self.worker and not self.worker.isRunning():
            self.worker.start()

    @Slot(dict, dict)
    def on_data_update(self, kospi_data, account_data):
        """데이터 수신 시 UI 업데이트 (필드명 문서 매칭)"""
        # 1. KOSPI [ka20003]
        # 문서 필드명: cur_prc(현재가), flu_rt(등락률)
        if kospi_data:
            price = kospi_data.get("cur_prc", "-")
            rate = kospi_data.get("flu_rt", "0.0")
            
            # 값 포맷팅
            self.lbl_kospi.setText(f"KOSPI: {price} ({rate}%)")
            
            try:
                if float(rate) > 0:
                    self.lbl_kospi.setStyleSheet("color: #bf616a; font-weight: bold; font-size: 14pt;")
                else:
                    self.lbl_kospi.setStyleSheet("color: #5e81ac; font-weight: bold; font-size: 14pt;")
            except: pass
        
        # 2. 예수금 [kt00001]
        # 문서 필드명: entr(예수금) -> 루트에 존재
        if account_data:
            deposit = account_data.get("entr", "0")
            # 혹시 못가져오면 구형 필드명(dnca_tot_amt)도 체크
            if deposit == "0" or not deposit:
                deposit = account_data.get("dnca_tot_amt", "0")

            try:
                deposit_val = int(deposit)
                self.lbl_deposit.setText(f"예수금: {deposit_val:,} 원")
            except:
                self.lbl_deposit.setText(f"예수금: {deposit}")

    def fetch_stock_price(self, code):
        if not self.api: return
        try:
            # ka10007: 시세표성정보
            res = self.api._call_api("ka10007", "/api/dostk/mrkcond", body={"stk_cd": code})
            if res and str(res.get("return_code")) == "0":
                output = res.get("output", {})
                # ka10007은 보통 output 안에 prc, flt_rt 사용
                price = output.get("prc", "-")
                rate = output.get("flt_rt", "0.0") 
                name = output.get("stk_nm", "")

                if name: self.lbl_stock_name.setText(f"{name} ({code})")
                
                fmt_price = price
                if str(price).lstrip('-').isdigit():
                    fmt_price = f"{int(price):,}"
                
                self.lbl_current_price.setText(f"현재가: {fmt_price}원")
                self.lbl_rate.setText(f"등락률: {rate}%")
            else:
                print(f"[Error] 시세 조회 실패: {res.get('return_msg', res)}")
        except Exception as e:
            print(f"[Critical] 시세 조회 중 에러: {e}")

    def send_order(self, order_type):
        if not self.api: return
        
        code = self.txt_code.text().strip()
        qty = self.spin_qty.text().strip()
        price = self.spin_price.text().strip()
        
        if not code or not qty:
            QMessageBox.warning(self, "입력 오류", "종목코드와 수량을 입력하세요.")
            return

        trde_tp = "00" if self.combo_type.currentIndex() == 0 else "03"
        if trde_tp == "03": price = "0"

        target_market = "0"

        try:
            if order_type == "buy":
                res = self.api.buy_order(target_market, code, qty, price, trde_tp)
            else:
                res = self.api.sell_order(target_market, code, qty, price, trde_tp)
                
            if res and str(res.get("return_code")) == "0":
                output = res.get("output", {})
                ord_no = output.get("ord_no", "접수") 
                if not output:
                    ord_no = res.get("ord_no", "접수")
                    
                QMessageBox.information(self, "주문 성공", f"주문번호: {ord_no}")
                self.add_order_log(ord_no, code, order_type, qty)
            else:
                msg = res.get("return_msg", "오류")
                QMessageBox.warning(self, "주문 실패", f"{msg}")
        except Exception as e:
            QMessageBox.critical(self, "에러", str(e))

    def load_mock_recommendations(self):
        self.table_top10.setRowCount(0)
        data = [("005930", "삼성전자", "95.5"), ("000660", "SK하이닉스", "92.1")]
        for r, (c, n, s) in enumerate(data):
            self.table_top10.insertRow(r)
            self.table_top10.setItem(r, 0, QTableWidgetItem(c))
            self.table_top10.setItem(r, 1, QTableWidgetItem(n))
            self.table_top10.setItem(r, 2, QTableWidgetItem(s))

    def on_table_cell_clicked(self, row, col):
        code = self.table_top10.item(row, 0).text()
        self.txt_code.setText(code)
        self.fetch_stock_price(code)

    def add_order_log(self, ord_no, code, type_str, qty):
        row = self.table_orders.rowCount()
        self.table_orders.insertRow(row)
        self.table_orders.setItem(row, 0, QTableWidgetItem(str(ord_no)))
        self.table_orders.setItem(row, 1, QTableWidgetItem(code))
        self.table_orders.setItem(row, 2, QTableWidgetItem("매수" if type_str=="buy" else "매도"))
        self.table_orders.setItem(row, 3, QTableWidgetItem(qty))