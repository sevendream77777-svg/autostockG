# -*- coding: utf-8 -*-
import sys, os, ctypes
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QListWidget, QListWidgetItem, QSizePolicy, QFrame, QGridLayout
)
from PySide6.QtCore import Qt, QSize, QTimer
from PySide6.QtGui import QIcon, QGuiApplication

current_path = os.path.abspath(__file__)
ui_dir = os.path.dirname(current_path)
project_root = os.path.dirname(ui_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if ui_dir not in sys.path:
    sys.path.append(ui_dir)

try:
    from pages.p0_index import P0_Index
    from pages.p1_data_pipeline import P1_DataPipeline
    from pages.p2_training import P2_Training
    from pages.p3_analysis import P3_Analysis
    from pages.p4_prediction import PredictionPage as P4_Prediction
    from pages.p5_send import P5SendPage
    from pages.p6_trading import TradingPage as P6_Trading
    from pages.p7_portfolio import PortfolioPage as P7_Portfolio
    from pages.p_setup import SettingsPage as P_Setup
except ImportError as e:
    print(f"\n[CRITICAL ERROR] 페이지 모듈 로드 실패: {e}")
    import traceback; traceback.print_exc(); sys.exit(1)

from common.styles import build_qss


# ============================================================
# NavCard
# ============================================================
class NavCard(QWidget):
    def __init__(self, color_bg: str, icon_text: str, title_text: str):
        super().__init__()
        self.base_color = color_bg
        self._scale = 1.0

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.inner_card = QFrame()
        self.inner_card.setObjectName("NavCard")
        self.layout.addWidget(self.inner_card)

        lay = QGridLayout(self.inner_card)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)

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

    def _apply_card_style(self, bg_hex: str):
        self.inner_card.setStyleSheet(f"""
            background-color: {bg_hex};
            border: none;
            border-radius: 10px;
        """)

        icon_sz = int(18 * self._scale)
        title_sz = int(14 * self._scale)
        sub_sz = int(11 * self._scale)

        self.icon.setStyleSheet(f"font-size: {icon_sz}px; padding-left:6px;")
        self.title.setStyleSheet(f"font-size: {title_sz}px; font-weight:700; padding-left:6px;")
        self.subtitle.setStyleSheet(f"font-size: {sub_sz}px; padding-left:6px; padding-bottom:4px;")

    def setSelected(self, selected: bool):
        if selected:
            self._apply_card_style(self._mix(self.base_color, "#000000", 0.15))
        else:
            self._apply_card_style(self.base_color)

    def _mix(self, c1, c2, ratio):
        def to_rgb(h):
            h = h.lstrip("#")
            return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
        def to_hex(rgb):
            return "#{:02X}{:02X}{:02X}".format(*rgb)
        r1, g1, b1 = to_rgb(c1); r2, g2, b2 = to_rgb(c2)
        r = int(r1*(1-ratio) + r2*ratio)
        g = int(g1*(1-ratio) + g2*ratio)
        b = int(b1*(1-ratio) + b2*ratio)
        return to_hex((r, g, b))

    def setScale(self, scale: float):
        self._scale = max(0.6, min(scale, 1.6))
        self._apply_card_style(self.base_color)


# ============================================================
# MainWindow
# ============================================================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("G2Garage - Trading System")

        screen = QGuiApplication.primaryScreen()
        screen_geom = screen.availableGeometry()

        initial_w = 1280
        initial_h = 800
        if initial_h > screen_geom.height():
            initial_h = screen_geom.height() - 40

        x = screen_geom.x() + (screen_geom.width() - initial_w) // 2
        y = screen_geom.y() + (screen_geom.height() - initial_h) // 2
        self.setGeometry(x, y, initial_w, initial_h)

        central = QWidget()
        self.setCentralWidget(central)
        hbox = QHBoxLayout(central)
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(0)

        # ------------------------------------------------------
        # 사이드 메뉴
        # ------------------------------------------------------
        self.nav_list = QListWidget()
        self.nav_list.setObjectName("SideNav")
        self.nav_list.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.nav_list.setMinimumWidth(144)
        self.nav_list.setMaximumWidth(10000)

        self.nav_list.setSpacing(0)
        self.nav_list.setContentsMargins(0, 0, 0, 0)
        self.nav_list.viewport().setContentsMargins(0, 0, 0, 0)

        self.nav_list.setStyleSheet("padding:0px; margin:0px; border:0px;")
        self.nav_list.viewport().setStyleSheet("padding:0px; margin:0px; border:0px;")

        self.nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        hbox.addWidget(self.nav_list)

        # ------------------------------------------------------
        # 중앙 페이지
        # ------------------------------------------------------
        self.stack = QStackedWidget()
        self.stack.setObjectName("MainWindowContent")
        hbox.addWidget(self.stack)

        hbox.setStretch(0, 1)
        hbox.setStretch(1, 4)

        self._setup_pages()
        self._setup_nav()

        self.nav_list.setCurrentRow(0)
        self._sync_selection(0)
        self.nav_list.currentRowChanged.connect(self._on_nav_changed)

        self.apply_theme("nord", None, None)

        QTimer.singleShot(0, lambda: self.resizeEvent(None))

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
            try:
                self.stack.addWidget(cls())
            except Exception as e:
                err = QLabel(f"❌ 페이지 로드 오류: {e}")
                err.setStyleSheet("color:#ff5555; padding:16px; font-size:14px;")
                self.stack.addWidget(err)

    # ----------------------------------------------------------
    def _setup_nav(self):
        self.cards = []
        self.nav_items = []

        for name, icon, color, subtitle, _ in self.pages_info:
            item = QListWidgetItem()
            item.setSizeHint(QSize(200, 85))

            card = NavCard(color, icon, name)
            card.subtitle.setText(subtitle)

            self.nav_list.addItem(item)
            self.nav_list.setItemWidget(item, card)

            self.cards.append(card)
            self.nav_items.append(item)

    # ----------------------------------------------------------
    # 반응형 사이즈 계산
    # ----------------------------------------------------------
    def resizeEvent(self, event):
        super().resizeEvent(event)

        # ⬇⬇⬇ 가로 고정폭(원하면 숫자만 바꾸면 됨)
        nav_width = 220
        self.nav_list.setFixedWidth(nav_width)

        # 세로 자동(강제 없음)
        vh = self.nav_list.viewport().height()
        if vh <= 0:
            vh = self.nav_list.height()

        # 카드 스케일 조절만 유지
        for item, card in zip(self.nav_items, self.cards):
            card.setScale(1.0)

        self.nav_list.updateGeometry()
        self.nav_list.repaint()

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
    except Exception:
        pass

    app = QApplication(sys.argv)
    icon_path = os.path.join(project_root, 'image', 'G2G.ico')
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))

    w = MainWindow()
    w.show()
    sys.exit(app.exec())
