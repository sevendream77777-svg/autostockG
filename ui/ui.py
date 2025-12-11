# -*- coding: utf-8 -*-
import sys, os, ctypes
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QListWidget, QListWidgetItem,
    QSizePolicy, QFrame, QGridLayout
)
from PySide6.QtCore import Qt, QSize, QTimer
from PySide6.QtGui import QIcon, QGuiApplication

# ----------------------------------------------------------
# 경로 설정
# ----------------------------------------------------------
current_path = os.path.abspath(__file__)
ui_dir = os.path.dirname(current_path)
project_root = os.path.dirname(ui_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if ui_dir not in sys.path:
    sys.path.append(ui_dir)

from ui.pages.p0_index import P0_Index
from ui.pages.p1_data_pipeline import P1_DataPipeline
from ui.pages.p2_training import P2_Training
from ui.pages.p3_analysis import P3_Analysis
from ui.pages.p4_prediction import PredictionPage as P4_Prediction
from ui.pages.p5_send import P5SendPage
from ui.pages.p6_trading import TradingPage as P6_Trading
from ui.pages.p7_portfolio import PortfolioPage as P7_Portfolio
from ui.pages.p_setup import SettingsPage as P_Setup

from common.styles import build_qss


# ============================================================
# NavCard
# ============================================================
class NavCard(QWidget):
    def __init__(self, color_bg: str, icon_text: str, title_text: str):
        super().__init__()
        self.base_color = color_bg

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.inner_card = QFrame()
        self.inner_card.setObjectName("NavCard")
        self.layout.addWidget(self.inner_card)

        lay = QGridLayout(self.inner_card)
        lay.setContentsMargins(6, 6, 6, 6)
        lay.setSpacing(2)

        self.icon = QLabel(icon_text)
        self.icon.setObjectName("NavIcon")
        self.icon.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        self.title = QLabel(title_text)
        self.title.setObjectName("NavTitle")
        self.title.setWordWrap(False)
        self.title.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)

        self.subtitle = QLabel()
        self.subtitle.setObjectName("NavSubtitle")
        self.subtitle.setWordWrap(True)
        self.subtitle.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        lay.addWidget(self.icon, 0, 0, 1, 1)
        lay.addWidget(self.title, 0, 1, 1, 1)
        lay.addWidget(self.subtitle, 1, 0, 1, 2)

        self._apply_card_style(self.base_color)

    def _apply_card_style(self, bg_hex):
        self.inner_card.setStyleSheet(f"""
            background-color: {bg_hex};
            border-radius: 10px;
        """)

        self.icon.setStyleSheet("font-size: 20px; padding-left:4px; color: #f5f5f5;")
        self.title.setStyleSheet("font-size: 17px; font-weight:800; padding-left:4px; color: #f5f5f5;")
        self.subtitle.setStyleSheet("font-size: 13px; font-weight:600; padding-left:4px; padding-bottom:4px; color: #e8e8e8;")

    def setSelected(self, selected: bool):
        if selected:
            self.inner_card.setStyleSheet(f"""
                background-color: {self.base_color};
                border-radius: 10px;
                border: none;
                border-left: 5px solid #FFD166;
            """)
            self.icon.setStyleSheet("font-size: 24px; padding-left:4px; color: #fff; font-weight:900;")
            self.title.setStyleSheet("font-size: 21px; font-weight:900; padding-left:4px; color: #fff; letter-spacing: 0.3px;")
            self.subtitle.setStyleSheet("font-size: 15px; font-weight:800; padding-left:4px; padding-bottom:4px; color: #f0f0f0;")
        else:
            self._apply_card_style(self.base_color)


# ============================================================
# MainWindow
# ============================================================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("G2Garage - Trading System")

        screen = QGuiApplication.primaryScreen().availableGeometry()
        self.setGeometry(
            screen.x() + (screen.width() - 1280)//2,
            screen.y() + (screen.height() - 800)//2,
            1280, 800
        )
        self.setMinimumSize(1100, 650)

        central = QWidget()
        self.setCentralWidget(central)
        hbox = QHBoxLayout(central)
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(0)

        # ------------------------------------------------------
        # 사이드 메뉴 (반응형)
        # ------------------------------------------------------
        self.nav_list = QListWidget()
        self.nav_list.setObjectName("SideNav")
        self.nav_list.setSpacing(0)
        self.nav_list.setContentsMargins(0, 0, 0, 0)
        self.nav_list.viewport().setContentsMargins(0, 0, 0, 0)

        self.nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        hbox.addWidget(self.nav_list)

        # ------------------------------------------------------
        # 메인 화면
        # ------------------------------------------------------
        self.stack = QStackedWidget()
        self.stack.setObjectName("MainWindowContent")
        hbox.addWidget(self.stack)

        hbox.setStretch(0, 1)
        hbox.setStretch(1, 5)

        self._setup_pages()
        self._setup_nav()

        self.nav_list.setCurrentRow(0)
        self._sync_selection(0)

        self.nav_list.currentRowChanged.connect(self._on_nav_changed)

        self.apply_theme("nord", None, None)

    # ----------------------------------------------------------
    def _setup_pages(self):
        self.pages_info = [
            ("Index 화면", "🏠",  "#3B82F6", "대시보드", P0_Index),
            ("데이터 구축", "💾", "#22C55E", "시세 수집, DB 통합", P1_DataPipeline),
            ("엔진 학습", "🧠", "#F59E0B", "AI 모델 훈련/갱신", P2_Training),
            ("엔진 분석", "🔍", "#A855F7", "성능 지표, 백테스팅", P3_Analysis),
            ("종목 예측", "📈", "#EF4444", "과거 시점 예측 검증", P4_Prediction),
            ("자료 전송", "📤", "#14B8A6", "각종 자료 전송", P5SendPage),
            ("실전 매매", "🚀", "#60A5FA", "Top10 추천 & 주문", P6_Trading),
            ("포트폴리오", "💼", "#9CA3AF", "잔고, 수익률 관리", P7_Portfolio),
            ("설정",     "⚙️", "#6C7280", "환경 설정", P_Setup),
        ]

        for _, _, _, _, cls in self.pages_info:
            self.stack.addWidget(cls())

    # ----------------------------------------------------------
    def _setup_nav(self):
        self.cards = []
        self.nav_items = []

        for name, icon, color, subtitle, _ in self.pages_info:
            item = QListWidgetItem()

            # 최소 높이만 제안 (강제 X)
            item.setSizeHint(QSize(160, 70))

            card = NavCard(color, icon, name)
            card.subtitle.setText(subtitle)

            self.nav_list.addItem(item)
            self.nav_list.setItemWidget(item, card)

            self.cards.append(card)
            self.nav_items.append(item)

    # ----------------------------------------------------------
    # 반응형 (C 옵션: 70~150px 범위 자동)
    # ----------------------------------------------------------
    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._apply_nav_layout()

    def showEvent(self, event):
        super().showEvent(event)
        # 최초 표시 시 레이아웃을 한 번 더 적용 (초기 높이 0 문제 방지)
        QTimer.singleShot(0, self._apply_nav_layout)

    def _apply_nav_layout(self):
        total_w = max(1, self.width())
        nav_w = int(total_w * 0.18)  # 가로 자동 비율
        nav_w = max(150, min(nav_w, 300))
        self.nav_list.setFixedWidth(nav_w)

        count = len(self.nav_items)
        if count == 0:
            return

        # 세로 자동 조절: 렌더링 전 viewport 높이가 0이 되는 현상 대비
        vh = max(self.nav_list.viewport().height(), self.nav_list.height(), self.height() - 40)
        if vh <= 0:
            vh = 1

        base_h = vh // count
        base_h = max(70, min(base_h, 150))

        for item, card in zip(self.nav_items, self.cards):
            item.setSizeHint(QSize(nav_w, base_h))

    # ----------------------------------------------------------
    def _on_nav_changed(self, row: int):
        self.stack.setCurrentIndex(row)
        self._sync_selection(row)

    def _sync_selection(self, row: int):
        for i, c in enumerate(self.cards):
            c.setSelected(i == row)

    def apply_theme(self, theme, p, t, overrides=None):
        qss = build_qss(theme, p, t, overrides)
        self.setStyleSheet(qss)


# ============================================================

if __name__ == "__main__":
    myappid = 'g2garage.autostock.trading.1.0'
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except:
        pass

    app = QApplication(sys.argv)

    icon_path = os.path.join(project_root, "image", "G2G.ico")
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))

    w = MainWindow()
    w.show()
    sys.exit(app.exec())
