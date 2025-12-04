# -*- coding: utf-8 -*-
# common/styles.py
from __future__ import annotations

def get_theme_nord_dark():
    return {
        "bg": "#2E3440", "surface": "#3B4252", "surface_alt": "#434C5E",
        "border": "#4C566A", "muted": "#D8DEE9", "text": "#ECEFF4",
        "subtext": "#BFC7D5", "primary": "#5E81AC", "primary_alt": "#81A1C1",
        "accent": "#A3BE8C", "danger": "#E06C75", "shadow": "rgba(0,0,0,0.25)",
    }

def get_theme_black_vscode():
    return {
        "bg": "#1E1E1E", "surface": "#252526", "surface_alt": "#2C2C2C",
        "border": "#3C3C3C", "muted": "#C8CCD0", "text": "#E7E7E7",
        "subtext": "#A8ACB0", "primary": "#007ACC", "primary_alt": "#3498DB",
        "accent": "#16A085", "danger": "#E06C75", "shadow": "rgba(0,0,0,0.35)",
    }

def _form_controls_qss(p):
    chk_icon = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='white' stroke-width='3' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='20 6 9 17 4 12'/%3E%3C/svg%3E"
    rad_icon = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='white'%3E%3Ccircle cx='12' cy='12' r='6'/%3E%3C/svg%3E"

    return f"""
    QWidget {{
        background-color: {p['bg']}; color: {p['text']};
        font-family: 'Segoe UI', 'Malgun Gothic', 'Apple SD Gothic Neo'; font-size: 10pt;
        padding: 0px;  /* 전역 패딩 0으로 강제 */
    }}
    QPushButton {{
        background-color: {p['surface_alt']}; border: 1px solid {p['primary']};
        padding: 6px 12px; border-radius: 6px; color: {p['text']};
    }}
    QPushButton:hover {{ background-color: {p['primary']}; }}
    QPushButton:pressed {{ background-color: {p['primary_alt']}; }}
    QPushButton:disabled {{ color: {p['subtext']}; border-color: {p['border']}; background-color: {p['surface']}; }}
    QLineEdit, QComboBox, QDateEdit {{
        background-color: {p['surface']}; border: 1px solid {p['border']};
        padding: 6px 8px; border-radius: 6px; color: {p['text']};
        selection-background-color: {p['primary_alt']}; selection-color: #000000;
    }}
    QComboBox QAbstractItemView {{
        background: {p['surface']}; color: {p['text']};
        border: 1px solid {p['border']};
        selection-background-color: {p['primary_alt']}; selection-color: #000000;
    }}
    QTableWidget::item:selected {{ background-color: rgba(255,255,255,0.18); 
    }}
    QTextEdit {{
        background-color: {p['surface']}; border: 1px solid {p['border']};
        padding: 8px; border-radius: 8px; color: {p['text']};
    }}
    QCheckBox {{ spacing: 8px; color: {p['text']}; }}
    QCheckBox::indicator {{
        width: 18px; height: 18px; border-radius: 4px;
        border: 2px solid {p['primary']}; background-color: {p['surface_alt']};
    }}
    QCheckBox::indicator:checked {{
        background-color: {p['primary']}; border-color: {p['accent']}; image: url("{chk_icon}");
    }}
    QRadioButton {{ spacing: 8px; color: {p['text']}; }}
    QRadioButton::indicator {{ width: 18px; height: 18px; }}
    QRadioButton::indicator:unchecked {{
        border: 2px solid {p['primary']}; border-radius: 9px; background-color: {p['surface_alt']};
    }}
    QRadioButton::indicator:checked {{
        border: 2px solid {p['accent']}; background-color: {p['primary']};
        border-radius: 9px; image: url("{rad_icon}");
    }}
    QGroupBox {{
        border: 1px solid {p['border']}; border-radius: 8px;
        margin-top: 14px; padding-top: 10px; background: {p['surface']};
    }}
    QGroupBox::title {{
        subcontrol-origin: margin; left: 12px; padding: 0 6px; color: {p['muted']};
    }}
    QScrollBar:vertical {{ background: {p['surface']}; width: 10px; margin: 0px; }}
    QScrollBar::handle:vertical {{ background: {p['border']}; min-height: 24px; border-radius: 4px; }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height:0; }}
    QScrollBar:horizontal {{ height: 10px; background: {p['surface']}; }}
    QScrollBar::handle:horizontal {{ background: {p['border']}; min-width: 24px; border-radius: 4px; }}
    """

def get_nav_card_qss(p):
    return f"""
    #SideNav {{ background-color: {p['bg']}; border: none; padding: 0px; }}

    /* 추가됨 — 숨겨진 QListWidget·viewport 여백 완전 제거 */
    QListWidget {{ padding:0px; margin:0px; border:0px; }}
    QListWidget::viewport {{ padding:0px; margin:0px; border:0px; }}

    QListWidget::item {{ background: transparent; border: none; margin: 0px; padding: 0px; }}
    QListWidget::item:selected {{ background: transparent; border: none; }}
    QLabel#NavTitle, QLabel#NavSubtitle, QLabel#NavIcon {{ background: transparent; }}
    QLabel#NavSubtitle {{ font-size: 11px; color: {p['subtext']}; }}
    #MainWindowContent {{ background-color: {p['surface']}; }}
    """

def build_qss(theme: str, primary_color: str | None = None, text_color: str | None = None, overrides: dict | None = None) -> str:
    if theme == "black":
        pal = get_theme_black_vscode()
    else:
        pal = get_theme_nord_dark()
    if primary_color:
        pal["primary"] = primary_color; pal["primary_alt"] = primary_color
    if text_color:
        pal["text"] = text_color
    if overrides:
        for k, v in overrides.items():
            if k in pal and v:
                pal[k] = v
    return _form_controls_qss(pal) + get_nav_card_qss(pal)
