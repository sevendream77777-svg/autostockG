# ui/common/styles.py

def get_modern_qss():
    # [수정] SVG 인코딩 문제 해결 (공백 -> %20 변환, viewBox 기준 스케일링)
    # 체크 아이콘 (Check)
    chk_icon = "data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2024%2024'%20fill='none'%20stroke='white'%20stroke-width='3'%20stroke-linecap='round'%20stroke-linejoin='round'%3E%3Cpolyline%20points='20%206%209%2017%204%2012'/%3E%3C/svg%3E"
    
    # 라디오 아이콘 (Circle)
    rad_icon = "data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2024%2024'%20fill='white'%3E%3Ccircle%20cx='12'%20cy='12'%20r='6'/%3E%3C/svg%3E"

    return f"""
    QWidget {{
        background-color: #2e3440;
        color: #e5e9f0;
        font-family: 'Segoe UI';
        font-size: 10pt;
    }}

    /* -------------------------
       CHECKBOX 기본 스타일
       ------------------------- */
    QCheckBox {{
        spacing: 6px;
        color: #e5e9f0;
    }}

    QCheckBox::indicator {{
        width: 18px;
        height: 18px;
        border-radius: 3px;
        border: 2px solid #88c0d0;
        background-color: #3b4252;
    }}

    QCheckBox::indicator:checked {{
        background-color: #81a1c1;
        border-color: #a3be8c;
        image: url("{chk_icon}");
    }}

    QCheckBox::indicator:hover {{
        border-color: #81a1c1;
    }}

    /* -------------------------
       disabled 표시 유지 (전체선택 시 문제 해결)
       ------------------------- */
    QCheckBox::indicator:disabled {{
        background-color: #4c566a;
        border-color: #616e88;
        opacity: 1;
    }}

    QCheckBox::indicator:disabled:checked {{
        background-color: #5e81ac;
        border-color: #a3be8c;
        image: url("{chk_icon}");
        opacity: 1;
    }}

    /* -------------------------
       RADIO BUTTON
       ------------------------- */
    QRadioButton {{
        spacing: 6px;
        color: #e5e9f0;
    }}

    QRadioButton::indicator {{
        width: 16px;
        height: 16px;
    }}

    QRadioButton::indicator:unchecked {{
        border: 2px solid #88c0d0;
        border-radius: 8px;
        background-color: #3b4252;
    }}

    QRadioButton::indicator:checked {{
        border: 2px solid #a3be8c;
        background-color: #5e81ac;
        border-radius: 8px;
        image: url("{rad_icon}");
    }}

    QRadioButton::indicator:disabled {{
        background-color: #4c566a;
        border-color: #616e88;
    }}

    QRadioButton:disabled {{
        color: #7f8fa6;
    }}

    /* -------------------------
       BUTTON
       ------------------------- */
    QPushButton {{
        background-color: #4c566a;
        border: 1px solid #5e81ac;
        padding: 5px 10px;
        border-radius: 4px;
        color: #e5e9f0;
    }}

    QPushButton:hover {{
        background-color: #5e81ac;
    }}

    QPushButton:pressed {{
        background-color: #81a1c1;
    }}

    /* -------------------------
       INPUT
       ------------------------- */

    QLineEdit, QComboBox, QDateEdit {{
        background-color: #3b4252;
        border: 1px solid #5e81ac;
        padding: 4px;
        border-radius: 4px;
        color: #eceff4;
    }}

    QTextEdit {{
        background-color: #3b4252;
        border: 1px solid #4c566a;
        padding: 4px;
        border-radius: 4px;
        color: #eceff4;
    }}
    """