# -*- coding: utf-8 -*-
# Source: :contentReference[oaicite:0]{index=0}
import sys, os, ctypes
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QListWidget, QListWidgetItem, QSizePolicy
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QIcon, QFont

# ------------------------------------------------------------
# 경로 설정
# ------------------------------------------------------------
current_path = os.path.abspath(__file__)
ui_dir = os.path.dirname(current_path)
project_root = os.path.dirname(ui_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if ui_dir not in sys.path:
    sys.path.append(ui_dir)

# 페이지 모듈
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
    import traceback; traceback.print_exc(); sys.exit(1)

# 스타일
from common.styles import build_qss

# ------------------------------------------------------------
# NavCard
# ------------------------------------------------------------
from PySide6.QtWidgets import QFrame, QGridLayout

class NavCard(QFrame):
    def __init__(self, color_bg: str, icon_text: str, title_text: str):
        super().__init__()
        self.base_color = color_bg
        self.setObjectName("NavCard")
        self.setProperty("selected", False)
        self.setProperty("hover", False)

        lay = QGridLayout(self)
        # 여백/간격 최소화: 박스 안에 글씨만 보이도록
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setHorizontalSpacing(8)
        lay.setVerticalSpacing(2)

        self.icon = QLabel(icon_text)
        self.icon.setObjectName("NavIcon")
        self.icon.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        self.title = QLabel(title_text)
        self.title.setObjectName("NavTitle")
        self.title.setWordWrap(False)          # 제목은 한 줄
        self.title.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)

        self.subtitle = QLabel()
        self.subtitle.setObjectName("NavSubtitle")
        self.subtitle.setWordWrap(True)        # 설명은 자동 줄바꿈
        self.subtitle.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        # 아이콘+제목 같은 줄, 설명은 다음 줄(2줄 구조)
        lay.addWidget(self.icon,     0, 0, 1, 1)
        lay.addWidget(self.title,    0, 1, 1, 1)
        lay.addWidget(self.subtitle, 1, 0, 1, 2)

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        # ------------------------------------------------
        # 🔥 반응형 스케일 파라미터 — 반드시 먼저 선언
        # ------------------------------------------------
        self._scale = 1.0

        # ------------------------------------------------
        # 🔥 기본 스타일 적용 (테두리/내부선 제거)
        # ------------------------------------------------
        self._apply_card_style(self.base_color)

        # 기준 높이(오토핏 기반, resize 시 동적으로 재계산)
        self.setMinimumHeight(68)
        self.setFixedHeight(92)

    def _apply_card_style(self, bg_hex: str):
        # 내부 검정 줄/선 제거: border/hover border 모두 제거
        self.setStyleSheet(f"""
            QFrame#NavCard {{
                background-color: {bg_hex};
                border: none;                    /* 테두리 제거 */
                border-radius: 10px;
            }}
            QFrame#NavCard:hover {{
                border: none;                    /* 호버 테두리 제거 */
            }}
            QLabel#NavIcon {{
                font-size: {int(18 * self._scale)}px;
            }}
            QLabel#NavTitle {{
                font-size: {int(14 * self._scale)}px;
                font-weight: 600;
                padding-left: 6px;               /* 아이콘과 간격 */
            }}
        """)

    def setSelected(self, selected: bool):
        # 선택 시 배경만 아주 살짝 어둡게(테두리 없이)
        def _mix(c1, c2, ratio):
            def to_rgb(h):
                h = h.lstrip("#"); return tuple(int(h[i:i+2],16) for i in (0,2,4))
            def to_hex(rgb):
                return "#{:02X}{:02X}{:02X}".format(*rgb)
            r1,g1,b1 = to_rgb(c1); r2,g2,b2 = to_rgb(c2)
            r = int(r1*(1-ratio) + r2*ratio)
            g = int(g1*(1-ratio) + g2*ratio)
            b = int(b1*(1-ratio) + b2*ratio)
            return to_hex((r,g,b))

        if selected:
            self._apply_card_style(_mix(self.base_color, "#000000", 0.08))
        else:
            self._apply_card_style(self.base_color)

    def setScale(self, scale: float):
        # 창 높이에 연동되는 스케일
        self._scale = max(0.85, min(scale, 1.6))
        # 텍스트/아이콘 스케일 재적용
        self._apply_card_style(self.base_color)
        # 내용 높이에 맞추어 오토핏(설명 줄바꿈 고려)
        base = 64
        extra = int(18 * (self._scale - 1.0))   # 스케일 증가 시 보정
        h = max(self.minimumHeight(), base + extra)
        self.setFixedHeight(h)

# ------------------------------------------------------------
# MainWindow
# ------------------------------------------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("G2Garage - Trading System")
        self.setGeometry(100, 100, 1280, 800)

        self._theme_name = "nord"
        self._theme_primary = None
        self._theme_text = None

        icon_path = os.path.join(project_root, 'image', 'G2G.ico')
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        central = QWidget(); self.setCentralWidget(central)
        hbox = QHBoxLayout(central); hbox.setContentsMargins(0,0,0,0); hbox.setSpacing(0)

        self.nav_list = QListWidget()
        self.nav_list.setObjectName("SideNav")
        self.nav_list.setFixedWidth(280)
        self.nav_list.setVerticalScrollMode(QListWidget.ScrollPerPixel)
        self.nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        hbox.addWidget(self.nav_list)

        self.stack = QStackedWidget()
        self.stack.setObjectName("MainWindowContent")
        hbox.addWidget(self.stack)

        self._setup_pages()
        self._setup_nav()

        self.nav_list.setCurrentRow(0)
        self._sync_selection(0)

        self.nav_list.currentRowChanged.connect(self._on_nav_changed)

        # 초기 테마 적용
        self.apply_theme(self._theme_name, self._theme_primary, self._theme_text)

    # --------------------------------------------------------
    def _setup_pages(self):
        self.pages_info = [
            ("Index 화면", "🏠",  "#3B82F6", "대시보드", P0_Index),
            ("데이터 구축", "💾", "#22C55E", "시세 수집, DB 통합", P1_DataPipeline),
            ("엔진 학습", "🧠", "#F59E0B", "AI 모델 훈련/갱신", P2_Training),
            ("엔진 분석", "🔍", "#A855F7", "성능 지표, 백테스팅", P3_Analysis),
            ("종목 예측", "📈", "#EF4444", "과거 시점 예측 검증", P4_Prediction),
            ("자료 전송", "📤", "#14B8A6", "각종 자료 전송", P5_Send),
            ("실전 매매", "🚀", "#60A5FA", "Top10 추천 & 주문", P6_Trading),
            ("포트폴리오", "💼", "#9CA3AF", "잔고, 수익률 관리", P7_Portfolio),
            ("설정",     "⚙️", "#94A3B8", "환경 설정", P_Setup),
        ]
        for _, _, _, _, cls in self.pages_info:
            try:
                self.stack.addWidget(cls())
            except Exception as e:
                err = QLabel(f"❌ 페이지 로드 오류: {e}")
                err.setStyleSheet("color:#ff6b6b; font-size: 15px; padding: 16px;")
                self.stack.addWidget(err)

        # 설정 페이지가 테마 이벤트 발신
        try:
            settings_widget = self.stack.widget(len(self.pages_info)-1)
            if hasattr(settings_widget, "themeChanged"):
                settings_widget.themeChanged.connect(self.apply_theme_from_settings)
        except Exception:
            pass

    # --------------------------------------------------------
    def _setup_nav(self):
        self.cards = []
        self.nav_items = []
        for name, icon, color, subtitle, _ in self.pages_info:
            item = QListWidgetItem()
            # 최초에는 카드의 sizeHint 사용(오토핏)
            card = NavCard(color, icon, name)
            card.subtitle.setText(subtitle)

            item.setSizeHint(card.sizeHint())
            self.nav_list.addItem(item)
            self.nav_list.setItemWidget(item, card)

            self.cards.append(card)
            self.nav_items.append(item)

    # --------------------------------------------------------
    def _on_nav_changed(self, row: int):
        self.stack.setCurrentIndex(row)
        self._sync_selection(row)

    def _sync_selection(self, row: int):
        for i, c in enumerate(self.cards):
            c.setSelected(i == row)

    # --------------------------------------------------------
    def resizeEvent(self, event):
        super().resizeEvent(event)
        h = max(640, self.height())
        scale = h / 800.0
        # 카드 스케일 적용 + 아이템 높이도 카드 sizeHint로 동기화
        for i, c in enumerate(getattr(self, "cards", [])):
            c.setScale(scale)
            if i < len(self.nav_items):
                self.nav_items[i].setSizeHint(c.sizeHint())

    # --------------------------------------------------------
    def apply_theme(self, theme_name: str, primary_color: str | None, text_color: str | None):
        self._theme_name = theme_name
        self._theme_primary = primary_color
        self._theme_text = text_color
        qss = build_qss(theme_name, primary_color, text_color)
        self.setStyleSheet(qss)

    def apply_theme_from_settings(self, payload: dict):
        theme = payload.get("theme", "nord")
        primary = payload.get("primary")
        text = payload.get("text")
        self.apply_theme(theme, primary, text)

# ------------------------------------------------------------
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
