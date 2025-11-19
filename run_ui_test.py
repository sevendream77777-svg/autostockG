import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget

# --- 1. UI 창을 정의하는 클래스 ---
# QMainWindow: '메인 창' 역할을 하는 클래스
class MyMainWindow(QMainWindow):
    
    def __init__(self):
        super().__init__()
        
        # 1. 윈도우 제목 및 크기 설정
        self.setWindowTitle("AI 주식 자동매매 프로그램 (V0.1)")
        self.resize(400, 300) # (가로 400, 세로 300)

        # 2. 버튼 생성
        self.button = QPushButton("오늘의 Top 10 추천 받기")
        
        # (★★★ 나중에 이 버튼에 'daily_recommender.py'의 로직을 연결할 것입니다 ★★★)
        self.button.clicked.connect(self.on_button_click) 

        # 3. 레이아웃 설정
        # (버튼을 창 중앙에 배치하기 위한 설정)
        layout = QVBoxLayout()
        layout.addWidget(self.button)
        
        central_widget = QWidget()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

    # 4. 버튼 클릭 시 실행될 함수 (지금은 'Hello'만 출력)
    def on_button_click(self):
        print("버튼 클릭! (여기에 AI 예측 로직이 연결됩니다.)")
        # (나중에는 여기에 get_today_prediction_list() 함수를 호출)
        
        # (예측 결과를 보여줄 새 창을 띄우거나, 텍스트 상자를 추가할 수 있음)
        # ...


# --- 2. 프로그램을 실행하는 메인 부분 ---
if __name__ == "__main__":
    # QApplication: 프로그램을 실행시키는 필수 객체
    app = QApplication(sys.argv)
    
    # MyMainWindow 클래스(우리가 설계한 창)를 화면에 띄움
    window = MyMainWindow()
    window.show()
    
    # 프로그램이 종료되지 않고 계속 실행되도록 함
    sys.exit(app.exec())