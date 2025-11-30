# -*- coding: utf-8 -*-
# pages/p_setup.py
from PySide6.QtCore import Signal, Qt
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QGroupBox, QFormLayout, QHBoxLayout,
    QLineEdit, QCheckBox, QPushButton, QLabel, QFileDialog, QRadioButton, QColorDialog,
    QScrollArea, QFrame
)

class SettingsPage(QWidget):
    themeChanged = Signal(dict)

    def __init__(self):
        super().__init__()
        
        # 메인 레이아웃: 스크롤 영역만 담음
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        
        # 실제 내용이 담길 위젯
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(10, 10, 10, 10)

        # ----- [A] 경로 설정 -----
        gb_path = QGroupBox("📁 경로 설정 (Paths)")
        f_path = QFormLayout()
        self.txt_engine = QLineEdit()
        self.txt_engine.setText(r"F:\autostockG\MODELENGINE")
        btn_find = QPushButton("찾기")
        btn_find.clicked.connect(self.find_path)
        f_path.addRow("MODELENGINE Root:", self.txt_engine)
        f_path.addRow("", btn_find)
        gb_path.setLayout(f_path)
        layout.addWidget(gb_path)

        # ----- [B] API 설정 -----
        gb_api = QGroupBox("🔑 증권사 API 설정 (Kiwoom)")
        f_api = QFormLayout()
        self.txt_id = QLineEdit()
        self.txt_pw = QLineEdit(); self.txt_pw.setEchoMode(QLineEdit.Password)
        self.chk_mock = QCheckBox("모의투자 접속 (Mock Trading)")
        self.chk_mock.setChecked(True)
        f_api.addRow("아이디:", self.txt_id)
        f_api.addRow("비밀번호:", self.txt_pw)
        f_api.addRow("", self.chk_mock)
        gb_api.setLayout(f_api)
        layout.addWidget(gb_api)

        # ----- [C] 자동화 -----
        gb_auto = QGroupBox("🤖 자동매매 스케줄")
        f_auto = QFormLayout()
        self.chk_auto_start = QCheckBox("프로그램 시작 시 자동 접속")
        self.chk_daily_routine = QCheckBox("장 마감 후 자동 데이터 수집 및 학습 (15:40~)")
        f_auto.addRow(self.chk_auto_start)
        f_auto.addRow(self.chk_daily_routine)
        gb_auto.setLayout(f_auto)
        layout.addWidget(gb_auto)

        # ----- [D] 테마/스킨 -----
        gb_theme = QGroupBox("🎨 스킨 & 컬러")
        f_theme = QFormLayout()

        row_skin = QHBoxLayout()
        self.rb_nord = QRadioButton("Nord Dark")
        self.rb_black = QRadioButton("VSCode Black")
        self.rb_nord.setChecked(True)
        row_skin.addWidget(self.rb_nord)
        row_skin.addWidget(self.rb_black)

        def mk_color_row(title):
            h = QHBoxLayout()
            lbl = QLabel("현재: 기본")
            btn = QPushButton(f"{title} 선택")
            return h, lbl, btn

        self._colors = {
            "primary": None, "text": None, "bg": None,
            "surface": None, "surface_alt": None, "border": None,
            "subtext": None, "accent": None,
        }
        self._labels = {}
        
        # 컬러 선택 버튼들 생성
        r_primary, self.lbl_primary, btn_primary = mk_color_row("주요색")
        btn_primary.clicked.connect(lambda: self.pick_color("primary", self.lbl_primary))

        r_text, self.lbl_text, btn_text = mk_color_row("글자색")
        btn_text.clicked.connect(lambda: self.pick_color("text", self.lbl_text))

        r_bg, self.lbl_bg, btn_bg = mk_color_row("배경(bg)")
        btn_bg.clicked.connect(lambda: self.pick_color("bg", self.lbl_bg))

        r_surface, self.lbl_surface, btn_surface = mk_color_row("카드(surface)")
        btn_surface.clicked.connect(lambda: self.pick_color("surface", self.lbl_surface))

        r_surface_alt, self.lbl_surface_alt, btn_surface_alt = mk_color_row("보조배경(surface_alt)")
        btn_surface_alt.clicked.connect(lambda: self.pick_color("surface_alt", self.lbl_surface_alt))

        r_border, self.lbl_border, btn_border = mk_color_row("테두리(border)")
        btn_border.clicked.connect(lambda: self.pick_color("border", self.lbl_border))

        r_subtext, self.lbl_subtext, btn_subtext = mk_color_row("보조글자(subtext)")
        btn_subtext.clicked.connect(lambda: self.pick_color("subtext", self.lbl_subtext))

        r_accent, self.lbl_accent, btn_accent = mk_color_row("포인트(accent)")
        btn_accent.clicked.connect(lambda: self.pick_color("accent", self.lbl_accent))

        self._labels = {
            "primary": self.lbl_primary, "text": self.lbl_text, "bg": self.lbl_bg,
            "surface": self.lbl_surface, "surface_alt": self.lbl_surface_alt,
            "border": self.lbl_border, "subtext": self.lbl_subtext, "accent": self.lbl_accent,
        }

        for row, lbl, btn in [
            (row_skin, None, None),
            (r_primary, self.lbl_primary, btn_primary),
            (r_text, self.lbl_text, btn_text),
            (r_bg, self.lbl_bg, btn_bg),
            (r_surface, self.lbl_surface, btn_surface),
            (r_surface_alt, self.lbl_surface_alt, btn_surface_alt),
            (r_border, self.lbl_border, btn_border),
            (r_subtext, self.lbl_subtext, btn_subtext),
            (r_accent, self.lbl_accent, btn_accent),
        ]:
            if lbl and btn:
                row.addWidget(lbl)
                row.addWidget(btn)
            f_theme.addRow("" if lbl else "스킨", row)

        row_apply = QHBoxLayout()
        btn_apply = QPushButton("적용")
        btn_reset = QPushButton("기본값으로")
        btn_apply.clicked.connect(self.apply_theme_clicked)
        btn_reset.clicked.connect(self.reset_colors)
        row_apply.addWidget(btn_apply)
        row_apply.addWidget(btn_reset)

        f_theme.addRow("", row_apply)
        gb_theme.setLayout(f_theme)
        layout.addWidget(gb_theme)

        btn_save = QPushButton("설정 저장 (Save Config)")
        btn_save.setFixedHeight(44)
        layout.addWidget(btn_save)

        layout.addStretch()

        # 스크롤 세팅
        scroll.setWidget(content_widget)
        main_layout.addWidget(scroll)

    def find_path(self):
        d = QFileDialog.getExistingDirectory(self, "Select Folder")
        if d:
            self.txt_engine.setText(d)

    def _pick(self):
        color = QColorDialog.getColor()
        if color.isValid():
            return color.name()
        return None

    def pick_color(self, key, label):
        c = self._pick()
        if c:
            self._colors[key] = c
            label.setText(f"현재: {c}")

    def reset_colors(self):
        for k in list(self._colors.keys()):
            self._colors[k] = None
        for k, lbl in self._labels.items():
            lbl.setText("현재: 기본")

    def apply_theme_clicked(self):
        theme = "black" if self.rb_black.isChecked() else "nord"
        payload = {"theme": theme}
        payload["primary"] = self._colors.get("primary")
        payload["text"] = self._colors.get("text")
        for k, v in self._colors.items():
            if v and k not in ("primary","text"):
                payload[k] = v
        self.themeChanged.emit(payload)