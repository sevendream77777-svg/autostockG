# ui/main_launcher.py
import sys
import os
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QPushButton, QStackedWidget, QLabel, QGridLayout)
from PySide6.QtCore import Qt

# 모듈 경로 설정 (현재 ui 폴더 기준 상위도 인식)
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 공통 스타일
from common.styles import get_modern_qss

# 페이지 로드 (없으면 빈 위젯)
try:
    from pages.p0_data_pipeline import DataPage
    from pages.p1_training import TrainingPage
    from pages.p2_analysis import AnalysisPage
    from pages.p3_prediction import PredictionPage
    from pages.p4_trading import TradingPage
    from pages.p5_portfolio import PortfolioPage
    from pages.p6_settings import SettingsPage
except ImportError as e:
    print(f"❌ 페이지 로딩 에러: {e}")
    # 에러 발생 시 더미 클래스 생성
    DataPage = TrainingPage = AnalysisPage = PredictionPage = \
    TradingPage = PortfolioPage = SettingsPage = QWidget

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HOJ Pro Manager (Unified V2)")
        self.resize(1400, 900)
        self.setStyleSheet(get_modern_qss())

        # 중앙 위젯 설정
        self.central_widget = QStackedWidget()
        self.setCentralWidget(self.central_widget)

        # --- [Page 0] 홈 메뉴 (대시보드) ---
        self.home_widget = QWidget()
        self.init_home_ui()
        self.central_widget.addWidget(self.home_widget) # Index 0

        # --- [Page 1~7] 기능별 화면 ---
        self.pages = [
            DataPage(),         # Index 1 (P0)
            TrainingPage(),     # Index 2 (P1)
            AnalysisPage(),     # Index 3 (P2)
            PredictionPage(),   # Index 4 (P3)
            TradingPage(),    # Index 5 (P4)
            PortfolioPage(),    # Index 6 (P5)
            SettingsPage()      # Index 7 (P6)
        ]
        
        for p in self.pages:
            self.central_widget.addWidget(p)

        # 툴바 생성
        self.create_toolbar()

    def init_home_ui(self):
        layout = QVBoxLayout(self.home_widget)
        
        # 타이틀
        title = QLabel("HOJ SYSTEM COMMANDER")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 32pt; font-weight: bold; color: #88c0d0; margin-top: 20px; margin-bottom: 40px;")
        layout.addWidget(title)

        # 그리드 메뉴
        grid = QGridLayout()
        grid.setSpacing(20)
        layout.addLayout(grid)

        # 메뉴 정의: (이름, 아이콘/설명, 이동할 페이지 Index)
        # Index 0은 홈이므로, 실제 페이지는 1부터 시작
        menus = [
            ("🔄 P0. 데이터 구축\n(Data Pipeline)", "시세 수집, DB 통합", 1),
            ("🔥 P1. 엔진 학습\n(Model Training)", "AI 모델 훈련/갱신", 2),
            ("📊 P2. 엔진 분석\n(Model Analysis)", "성능 지표, 백테스팅", 3),
            ("🔮 P3. 과거 예측\n(Simulation)", "과거 시점 예측 검증", 4),
            ("📈 P4. 실전 매매\n(Live Trading)", "Top10 추천 & 주문", 5),
            ("💰 P5. 포트폴리오\n(My Account)", "잔고, 수익률 관리", 6),
            ("⚙️ P6. 설정\n(Settings)", "경로, API 설정", 7)
        ]

        row, col = 0, 0
        for name, desc, idx in menus:
            btn = QPushButton(f"{name}\n\n{desc}")
            btn.setObjectName("menu_btn") # 스타일시트 적용
            btn.setFixedSize(280, 180)
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #3b4252; 
                    color: #eceff4; 
                    font-size: 14pt; 
                    border-radius: 15px;
                    border: 2px solid #4c566a;
                    text-align: center;
                }
                QPushButton:hover {
                    background-color: #4c566a;
                    border: 2px solid #88c0d0;
                }
            """)
            btn.clicked.connect(lambda checked, i=idx: self.switch_page(i))
            
            grid.addWidget(btn, row, col)
            col += 1
            if col > 3: # 4열 배치
                col = 0
                row += 1
        
        layout.addStretch()
        
        # 하단 상태바
        version_lbl = QLabel("System Version: 2.0 | Engine: Ready | API: Disconnected")
        version_lbl.setStyleSheet("color: #6f7788;")
        version_lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(version_lbl)

    def switch_page(self, index):
        self.central_widget.setCurrentIndex(index)

    def go_home(self):
        self.central_widget.setCurrentIndex(0)

    def create_toolbar(self):
        toolbar = self.addToolBar("Navigation")
        toolbar.setMovable(False)
        
        btn_home = QPushButton("🏠 HOME")
        btn_home.setStyleSheet("font-weight: bold; font-size: 11pt; padding: 5px 15px;")
        btn_home.clicked.connect(self.go_home)
        toolbar.addWidget(btn_home)
        
        toolbar.addSeparator()
        
        # 툴바에도 바로가기 추가 (선택사항)
        shortcuts = [("데이터", 1), ("학습", 2), ("매매", 5)]
        for name, idx in shortcuts:
            btn = QPushButton(name)
            btn.clicked.connect(lambda checked, i=idx: self.switch_page(i))
            toolbar.addWidget(btn)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())