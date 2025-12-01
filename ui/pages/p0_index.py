# -*- coding: utf-8 -*-
from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel, QGridLayout, QFrame
from PySide6.QtCore import Qt

class P0_Index(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(40, 40, 40, 40)
        layout.setSpacing(20)

        header = QLabel("AutoStockG Dashboard")
        header.setStyleSheet("font-size: 36px; font-weight: bold; color: #E2E8F0;")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        desc = QLabel("데이터 수집부터 AI 예측, 자동 매매까지 통합된 관리 시스템")
        desc.setStyleSheet("font-size: 18px; color: #AEBBCC; margin-bottom: 30px;")
        desc.setAlignment(Qt.AlignCenter)
        layout.addWidget(desc)

        grid = QGridLayout()
        grid.setSpacing(20)

        items = [
            ("데이터 구축", "P1", "데이터 수집/전처리"),
            ("엔진 학습", "P2", "AI 모델 학습"),
            ("엔진 분석", "P3", "성능 분석/백테스팅"),
            ("종목 예측", "P4", "Top 추천 종목"),
            ("자료 전송", "P5", "알림 발송 (SMS/TG)"),
            ("실전 매매", "P6", "자동 매매 실행"),
            ("포트폴리오", "P7", "계좌 잔고/수익률"),
            ("설정", "Setup", "환경 설정")
        ]

        for i, (name, code, sub) in enumerate(items):
            card = QFrame()
            card.setStyleSheet("background-color: white; border: 1px solid #ecf0f1; border-radius: 10px;")
            vbox = QVBoxLayout(card)
            
            lbl_code = QLabel(code)
            lbl_code.setStyleSheet("color: #3498db; font-weight: bold;")
            lbl_name = QLabel(name)
            lbl_name.setStyleSheet("font-size: 18px; font-weight: bold; color: #2c3e50;")
            lbl_sub = QLabel(sub)
            lbl_sub.setStyleSheet("color: #95a5a6; font-size: 12px;")
            
            vbox.addWidget(lbl_code)
            vbox.addWidget(lbl_name)
            vbox.addWidget(lbl_sub)
            grid.addWidget(card, i // 4, i % 4)

        layout.addLayout(grid)
        layout.addStretch(1)