# -*- coding: utf-8 -*-
import sys, os, ctypes
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QListWidget, QListWidgetItem, QSizePolicy, QFrame, QGridLayout
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QIcon, QGuiApplication

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

# 페이지 모듈 로드
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

# 스타일
from common.styles import build_qss

# ------------------------------------------------------------
# NavCard (반응형 카드 위젯)
# ------------------------------------------------------------
class NavCard(QWidget):
    def __init__(self, color_bg: str, icon_text: str, title_text: str):
        super().__init__()
        self.base_color = color_bg
        self._scale = 1.0

        # 카드 사이의 간격: 아래쪽 4px 여백 (간격 조정)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 4) 
        self.layout.setSpacing(0)

        # 실제 색상이 들어가는 카드 본체
        self.inner_card = QFrame()
        self.inner_card.setObjectName("NavCard")
        self.layout.addWidget(self.inner_card)

        # 카드 내부 내용물 (여백 최적화)
        lay = QGridLayout(self.inner_card)
        # 내용이 잘리지 않도록 내부 여백을 줄임 (상하 6px)
        lay.setContentsMargins(10, 6, 10, 6) 
        lay.setHorizontalSpacing(8)
        lay.setVerticalSpacing(2)

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

        lay.addWidget(self.icon,     0, 0, 1, 1)
        lay.addWidget(self.title,    0, 1, 1, 1)
        lay.addWidget(self.subtitle, 1, 0, 1, 2)

        self._apply_card_style(self.base_color)

    def _apply_card_style(self, bg_hex: str):
        self.inner_card.setStyleSheet(
            "background-color: {bg};"
            "border: none;"
            "border-radius: 10px;"
            .format(bg=bg_hex)
        )
        
        # 폰트 크기 반응형
        icon_sz = int(18 * self._scale)
        title_sz = int(14 * self._scale)
        sub_sz = int(11 * self._scale)
        
        self.icon.setStyleSheet(f"font-size: {icon_sz}px; background: transparent;")
        self.title.setStyleSheet(f"font-size: {title_sz}px; font-weight:700; padding-left:4px; background: transparent;")
        self.subtitle.setStyleSheet(f"font-size: {sub_sz}px; background: transparent;")

    def setSelected(self, selected: bool):
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
            self._apply_card_style(_mix(self.base_color, "#000000", 0.15))
        else:
            self._apply_card_style(self.base_color)

    def setScale(self, scale: float):
        self._scale = max(0.85, min(scale, 1.5)) 
        self._apply_card_style(self.base_color)

# ------------------------------------------------------------
# MainWindow
# ------------------------------------------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("G2Garage - Trading System")
        
        # [작업표시줄 가림 방지]
        screen = QGuiApplication.primaryScreen()
        screen_geom = screen.availableGeometry() 
        
        initial_w = 1280
        initial_h = 800
        
        if initial_h > screen_geom.height():
            initial_h = screen_geom.height() - 40 
        
        x = screen_geom.x() + (screen_geom.width() - initial_w) // 2
        y = screen_geom.y() + (screen_geom.height() - initial_h) // 2
        
        self.setGeometry(x, y, initial_w, initial_h)

        self._theme_name = "nord"
        self._theme_primary = None
        self._theme_text = None
        self._overrides = None

        # [수정됨] 아이콘 설정 개선
        icon_path = os.path.join(project_root, 'image', 'G2G.ico')
        if os.path.exists(icon_path):
            icon = QIcon(icon_path)
            self.setWindowIcon(icon)
            QApplication.setWindowIcon(icon)

        central = QWidget(); self.setCentralWidget(central)
        hbox = QHBoxLayout(central); hbox.setContentsMargins(0,0,0,0); hbox.setSpacing(0)

        # [수정됨] 사이드 메뉴 리스트 (반응형 크기)
        self.nav_list = QListWidget()
        self.nav_list.setObjectName("SideNav")
        # 메뉴는 창 크기에 따라 유동적으로 늘어나도록 폭 제약을 최소화하고
        # 최소 폭만 지정한다. 최대 폭은 매우 크게 설정하여 사실상 제한이 없다.
        self.nav_list.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        self.nav_list.setMinimumWidth(200)
        # 최대 폭을 적절하게 제한하여 화면을 과도하게 차지하지 않도록 함
        self.nav_list.setMaximumWidth(320)
        # 스크롤바가 표시되지 않도록 정책 변경
        self.nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.nav_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        # 픽셀 단위 스크롤 모드를 유지 (항상 스크롤바는 숨겨져 있음)
        self.nav_list.setVerticalScrollMode(QListWidget.ScrollPerPixel)
        hbox.addWidget(self.nav_list)

        self.stack = QStackedWidget()
        self.stack.setObjectName("MainWindowContent")
        hbox.addWidget(self.stack)

        # [수정됨] 메뉴:본문 비율을 1:4로 설정
        hbox.setStretch(0, 1)
        hbox.setStretch(1, 4)

        self._setup_pages()
        self._setup_nav()

        self.nav_list.setCurrentRow(0)
        self._sync_selection(0)
        self.nav_list.currentRowChanged.connect(self._on_nav_changed)

        self.apply_theme(self._theme_name, self._theme_primary, self._theme_text, overrides=self._overrides)

    # --------------------------------------------------------
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
                err.setStyleSheet("color:#ff6b6b; font-size: 15px; padding: 16px;")
                self.stack.addWidget(err)

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
            card = NavCard(color, icon, name)
            card.subtitle.setText(subtitle)

            self.nav_list.addItem(item)
            self.nav_list.setItemWidget(item, card)

            self.cards.append(card)
            self.nav_items.append(item)

    # --------------------------------------------------------
    # [수정됨] 창 너비에 따른 메뉴 폭 동적 계산 추가
    # --------------------------------------------------------
    def resizeEvent(self, event):
        super().resizeEvent(event)
        # [수정됨] 창 너비에 따라 메뉴 폭을 동적으로 계산
        total_w = self.contentsRect().width()
        # 화면 1/5 이하로 줄이기 위해 비율을 18%로 설정
        nav_width = int(total_w * 0.18)
        # 최소 폭 보장
        if nav_width < self.nav_list.minimumWidth():
            nav_width = self.nav_list.minimumWidth()
        # 최대 폭은 nav_list.setMaximumWidth에 의해 제한된다. 적용하여 너무 넓어지지 않도록 한다.
        if nav_width > self.nav_list.maximumWidth():
            nav_width = self.nav_list.maximumWidth()
        self.nav_list.setFixedWidth(nav_width)

        # [수정됨] nav_list 높이 계산 및 카드 스케일링
        total_h = self.contentsRect().height()
        count = len(self.nav_items)
        if count == 0:
            return

        # NavCard마다 4px의 아래 여백이 있으므로 개수만큼 여유 공간에서 빼준다.
        # 추가적으로 2px 정도 더 빼 여백을 확보해 스크롤바가 생기지 않도록 한다.
        # 카드 사이의 하단 여백(4px) 외에도 상단/하단 여백 등을 고려하여 조금 더 빼준다.
        available_h = total_h - (count * 4) - 8
        if available_h < 0:
            available_h = 0

        # N등분 계산
        item_h = int(available_h / count) if count > 0 else 0
        # 최소 높이 60px (이보다 작아지면 글자가 잘리므로 방어)
        if item_h < 60:
            item_h = 60

        # 폰트 스케일 계산 (88px 기준)
        scale = item_h / 88.0
        # 각 항목 크기 및 폰트 스케일 적용
        for item, card in zip(self.nav_items, self.cards):
            item.setSizeHint(QSize(self.nav_list.width(), item_h))
            card.setScale(scale)

    # --------------------------------------------------------
    def _on_nav_changed(self, row: int):
        self.stack.setCurrentIndex(row)
        self._sync_selection(row)

    def _sync_selection(self, row: int):
        for i, c in enumerate(self.cards):
            c.setSelected(i == row)

    def apply_theme(self, theme_name: str, primary_color: str | None, text_color: str | None, overrides: dict | None = None):
        self._theme_name = theme_name
        self._theme_primary = primary_color
        self._theme_text = text_color
        self._overrides = overrides
        qss = build_qss(theme_name, primary_color, text_color, overrides=overrides)
        self.setStyleSheet(qss)

    def apply_theme_from_settings(self, payload: dict):
        theme = payload.get("theme", "nord")
        primary = payload.get("primary")
        text = payload.get("text")
        keys = ["bg","surface","surface_alt","border","muted","text","subtext","primary","primary_alt","accent","danger","shadow"]
        overrides = {k: v for k, v in payload.items() if k in keys and v}
        self.apply_theme(theme, primary, text, overrides=overrides if overrides else None)

if __name__ == "__main__":
    myappid = 'g2garage.autostock.trading.1.0'
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except Exception:
        pass

    app = QApplication(sys.argv)
    # [추가] 전역 아이콘 설정: QApplication 생성 후 바로 설정하여 초기 실행 시 작업표시줄 아이콘이 누락되는 문제를 방지한다.
    try:
        icon_path = os.path.join(project_root, 'image', 'G2G.ico')
        if os.path.exists(icon_path):
            app_icon = QIcon(icon_path)
            app.setWindowIcon(app_icon)
    except Exception:
        pass
    # 메인 윈도우 생성 후 표시
    w = MainWindow()
    w.show()
    sys.exit(app.exec())