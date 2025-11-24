# ui/main_launcher.py
import sys
import os
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QPushButton, QStackedWidget, QLabel, QGridLayout)
from PySide6.QtCore import Qt

# 모듈 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 공통 스타일 및 페이지 로드
from common.styles import get_modern_qss
try:
    from pages.p0_data_pipeline import DataPage
    from pages.p1_training import TrainingPage
    from pages.p2_analysis import AnalysisPage
    from pages.p3_prediction import PredictionPage
except ImportError as e:
    print(f"페이지 로딩 에러: {e}")
    # 에러나면 빈 위젯으로 대체 (프로그램 꺼짐 방지)
    DataPage = TrainingPage = AnalysisPage = PredictionPage = QWidget

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HOJ Pro Manager (Modular V1)")
        self.resize(1280, 800)
        self.setStyleSheet(get_modern_qss())

        # 중앙 위젯 설정 (스택 위젯: 카드 돌리기 방식)
        self.central_widget = QStackedWidget()
        self.setCentralWidget(self.central_widget)

        # --- 0번 페이지: 홈 메뉴 (아이콘 그리드) ---
        self.home_widget = QWidget()
        self.init_home_ui()
        self.central_widget.addWidget(self.home_widget) # Index 0

        # --- 1~N번 페이지: 기능별 화면 ---
        self.page_data = DataPage()
        self.page_train = TrainingPage()
        self.page_analysis = AnalysisPage()
        self.page_pred = PredictionPage()

        self.central_widget.addWidget(self.page_data)     # Index 1
        self.central_widget.addWidget(self.page_train)    # Index 2
        self.central_widget.addWidget(self.page_analysis) # Index 3
        self.central_widget.addWidget(self.page_pred)     # Index 4

    def init_home_ui(self):
        layout = QVBoxLayout(self.home_widget)
        
        # 타이틀
        title = QLabel("HOJ SYSTEM MANAGER")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 24pt; font-weight: bold; color: #88c0d0; margin-bottom: 30px;")
        layout.addWidget(title)

        # 그리드 메뉴
        grid = QGridLayout()
        layout.addLayout(grid)

        # 메뉴 정의 (이름, 연결할 페이지 인덱스)
        menus = [
            ("💾 데이터 파이프라인", 1),
            ("🏭 모델 학습", 2),
            ("📊 엔진 분석", 3),
            ("🔮 예측 시뮬레이션", 4),
            ("📈 매매 시스템(준비중)", None), # 연결 없음
            ("⚙️ 설정(준비중)", None)
        ]

        row, col = 0, 0
        for name, idx in menus:
            btn = QPushButton(name)
            btn.setObjectName("menu_btn") # 스타일 적용용 ID
            btn.setFixedSize(250, 150)
            if idx is not None:
                btn.clicked.connect(lambda checked, i=idx: self.switch_page(i))
            else:
                btn.setEnabled(False)
            
            grid.addWidget(btn, row, col)
            col += 1
            if col > 2: # 3열 배치
                col = 0
                row += 1
        
        layout.addStretch()

    def switch_page(self, index):
        self.central_widget.setCurrentIndex(index)
        # 페이지로 이동하면 상단에 '홈으로' 버튼 추가가 필요할 수 있음
        # 이번 구조에서는 각 페이지 상단에 '홈으로' 버튼을 넣는 방식을 추천합니다.
        # (현재는 예시로 윈도우 타이틀바나 별도 네비게이션이 없으므로, 
        # 각 페이지 파일 __init__에 홈 버튼을 추가하는 로직을 넣거나, 
        # Main에서 Toolbar를 쓰는게 좋습니다. 일단 간단히 Toolbar 추가)
        
    def go_home(self):
        self.central_widget.setCurrentIndex(0)

    def create_toolbar(self):
        # 상단 툴바 (어디서든 홈으로 가기 위해)
        toolbar = self.addToolBar("Navigation")
        btn_home = QPushButton("🏠 HOME")
        btn_home.setObjectName("home_btn")
        btn_home.clicked.connect(self.go_home)
        toolbar.addWidget(btn_home)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.create_toolbar() # 툴바 생성
    win.show()
    sys.exit(app.exec())