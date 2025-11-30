# -*- coding: utf-8 -*-
import sys
import os
import ctypes  # [추가] 윈도우 작업표시줄 아이콘 분리용

# ------------------------------------------------------------------------------
# [시스템 경로 설정]
# ------------------------------------------------------------------------------
current_path = os.path.abspath(__file__)           # F:\autostockG\ui\ui.py
ui_dir = os.path.dirname(current_path)             # F:\autostockG\ui
project_root = os.path.dirname(ui_dir)             # F:\autostockG

# 1. 프로젝트 루트 추가
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. ui 폴더 추가
if ui_dir not in sys.path:
    sys.path.append(ui_dir)
# ------------------------------------------------------------------------------

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QListWidget, QListWidgetItem, QSizePolicy, QMessageBox
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QIcon  # [추가] 아이콘 기능을 위해 필요

# ==============================================================================
# 페이지 모듈 임포트
# ==============================================================================
try:
    from pages.p0_index import P0_Index
    from pages.p1_data_pipeline import P1_DataPipeline
    from pages.p2_training import P2_Training
    from pages.p3_analysis import P3_Analysis
    from pages.p4_prediction import PredictionPage as P4_Prediction
    from pages.p5_send import P5_Send
    from pages.p6_trading import TradingPage as P6_Trading
    from pages.p7_portfolio import PortfolioPage as P7_Portfolio
    from pages.p_setup import SettingsPage as P_Setup
except ImportError as e:
    print(f"\n[CRITICAL ERROR] 페이지 모듈 로드 실패: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==============================================================================
# UI 스타일
# ==============================================================================
APP_STYLE = """
    QMainWindow { background-color: #f5f6fa; }
    #SideNav { background-color: #2c3e50; border: none; }
    QListWidget { background-color: #2c3e50; border: none; outline: none; }
    QListWidget::item { color: #bdc3c7; padding: 15px 20px; border-bottom: 1px solid #34495e; font-size: 14px; font-weight: bold; }
    QListWidget::item:hover { background-color: #34495e; color: #ecf0f1; }
    QListWidget::item:selected { background-color: #2980b9; color: #ffffff; border-left: 5px solid #3498db; }
    #MainWindowContent { background-color: #ffffff; }
"""

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("G2Garage - Trading System")
        self.setGeometry(100, 100, 1280, 800)
        self.setStyleSheet(APP_STYLE)

        # [추가] 메인 윈도우 아이콘 설정 (안전장치)
        icon_path = os.path.join(project_root, 'image', 'G2G.ico')
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_hbox = QHBoxLayout(central_widget)
        main_hbox.setContentsMargins(0, 0, 0, 0)
        main_hbox.setSpacing(0)

        # 1. 사이드 메뉴
        self.nav_list = QListWidget()
        self.nav_list.setObjectName("SideNav")
        self.nav_list.setFixedWidth(240)
        main_hbox.addWidget(self.nav_list)

        # 2. 메인 콘텐츠
        self.stacked_widget = QStackedWidget()
        self.stacked_widget.setObjectName("MainWindowContent")
        main_hbox.addWidget(self.stacked_widget)

        self.setup_pages()
        self.setup_navigation()
        self.nav_list.setCurrentRow(0)

    def setup_pages(self):
        self.pages_info = [
            ("P0Index 화면", "🏠", P0_Index),
            ("P1데이터 구축", "💾", P1_DataPipeline),
            ("P2엔진 학습", "🧠", P2_Training),
            ("P3엔진 분석", "🔍", P3_Analysis),
            ("P4종목 예측", "📈", P4_Prediction),
            ("P5자료 전송", "📤", P5_Send),
            ("P6실전 매매", "🚀", P6_Trading),
            ("P7포트폴리오", "💼", P7_Portfolio),
            ("설정", "⚙️", P_Setup),
        ]

        for name, icon, cls in self.pages_info:
            try:
                page_widget = cls()
                self.stacked_widget.addWidget(page_widget)
            except Exception as e:
                err = QLabel(f"❌ {name} 로드 오류: {e}")
                err.setStyleSheet("color: red; font-size: 16px; padding: 20px;")
                self.stacked_widget.addWidget(err)
                print(f"[ERROR] {name} 로드 실패: {e}")

    def setup_navigation(self):
        for name, icon, _ in self.pages_info:
            item = QListWidgetItem(self.nav_list)
            item.setText(f"  {icon}   {name}")
            item.setSizeHint(QSize(200, 60))

        self.nav_list.currentRowChanged.connect(self.stacked_widget.setCurrentIndex)

if __name__ == '__main__':
    # [추가] 1. 윈도우 작업표시줄 아이콘 그룹 분리 (작업관리자에 아이콘 뜨게 함)
    myappid = 'g2garage.autostock.trading.1.0'
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except Exception:
        pass # 일부 윈도우 버전 호환성 무시

    app = QApplication(sys.argv)

    # [추가] 2. 앱 전체 아이콘 설정
    # F:\autostockG\image\G2G.ico 경로 자동 계산
    icon_path = os.path.join(project_root, 'image', 'G2G.ico')
    
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))
    else:
        print(f"[WARNING] 아이콘 파일을 찾을 수 없습니다: {icon_path}")

    window = MainWindow()
    window.show()
    sys.exit(app.exec_())