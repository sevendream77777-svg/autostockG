# -*- coding: utf-8 -*-
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QGroupBox, QRadioButton, QTextEdit,
    QPushButton, QHBoxLayout, QMessageBox, QComboBox, QFileDialog, QCheckBox,
    QSplitter, QSizePolicy
)
from PySide6.QtGui import QPixmap
from PySide6.QtCore import Qt
import os
import subprocess
import glob

# 경로 설정
INFO_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"
BEST_TOP_DIR = r"F:\autostockG\MODELENGINE\INFO\best_top"
ALERT_SCRIPT = r"F:\autostockG\alert_notify.py"   # alert_notify.py 최종버전 위치

class P5_Send(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    # ============================================
    # UI 구성
    # ============================================
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(25)
        layout.setContentsMargins(40, 40, 40, 40)
        layout.setAlignment(Qt.AlignTop)

        title = QLabel("P5. 자료 전송")
        title.setStyleSheet("font-size: 30px; font-weight: bold; color: #2c3e50;")
        layout.addWidget(title)

        # ------------------------------
        # JSON 파일 선택
        # ------------------------------
        group_json = QGroupBox("엔진(JSON) 선택")
        vj = QVBoxLayout()

        self.combo_json = QComboBox()
        self.load_json_list()

        vj.addWidget(self.combo_json)
        group_json.setLayout(vj)
        layout.addWidget(group_json)

        # ------------------------------
        # 전송 옵션 + 버튼 묶기 (상단으로 이동)
        # ------------------------------
        group_opt = QGroupBox("전송 옵션")
        opt_layout = QHBoxLayout()       # 좌: 옵션 / 우: 버튼 2개
        opt_layout.setSpacing(20)

        # ====== 옵션 영역 ======
        opt_left = QVBoxLayout()

        # 모드: text / image / both
        mode_box = QHBoxLayout()
        mode_box.addWidget(QLabel("전송 모드:"))
        self.rb_text = QRadioButton("텍스트만")
        self.rb_image = QRadioButton("이미지만")
        self.rb_both = QRadioButton("둘 다")
        self.rb_both.setChecked(True)
        mode_box.addWidget(self.rb_text)
        mode_box.addWidget(self.rb_image)
        mode_box.addWidget(self.rb_both)
        opt_left.addLayout(mode_box)

        # 채널: 카카오 / 텔레그램 / SMS
        ch_box = QHBoxLayout()
        ch_box.addWidget(QLabel("전송 채널:"))
        self.cb_kakao = QCheckBox("카카오톡")
        self.cb_tg = QCheckBox("텔레그램")
        self.cb_sms = QCheckBox("SMS")
        ch_box.addWidget(self.cb_kakao)
        ch_box.addWidget(self.cb_tg)
        ch_box.addWidget(self.cb_sms)
        opt_left.addLayout(ch_box)

        opt_layout.addLayout(opt_left)

        # ====== 버튼 영역(오른쪽 정렬) ======
        opt_right = QVBoxLayout()
        opt_right.setAlignment(Qt.AlignRight | Qt.AlignTop)

        self.btn_preview = QPushButton("미리보기 생성")
        self.btn_preview.setStyleSheet("padding: 8px; font-weight: bold;")
        self.btn_preview.clicked.connect(self.make_preview)
        opt_right.addWidget(self.btn_preview)

        self.btn_send = QPushButton("전송 실행")
        self.btn_send.setStyleSheet("background:#2980b9; color:white; padding:10px; font-weight:bold;")
        self.btn_send.clicked.connect(self.run_send)
        opt_right.addWidget(self.btn_send)

        opt_layout.addLayout(opt_right)

        group_opt.setLayout(opt_layout)
        layout.addWidget(group_opt)

        # -----------------------------------------------------
        # 이미지 미리보기 + 로그 → Splitter로 좌/우 배치하기
        # -----------------------------------------------------
        split_group = QGroupBox("미리보기 / 로그")
        split_layout = QHBoxLayout()

        splitter = QSplitter(Qt.Horizontal)

        # ===== 왼쪽: 이미지 미리보기 =====
        left_widget = QWidget()
        left_layout = QVBoxLayout()

        self.lbl_img = QLabel("이미지 없음")
        self.lbl_img.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.lbl_img.setStyleSheet("background: #f0f0f0; border: 1px solid #ccc;")
        self.lbl_img.setFixedSize(350, 480)
        left_layout.addWidget(self.lbl_img)
        left_widget.setLayout(left_layout)

        # ===== 오른쪽: 로그 =====
        right_widget = QWidget()
        right_layout = QVBoxLayout()
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        right_layout.addWidget(self.txt_log)
        right_widget.setLayout(right_layout)

        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setSizes([350, 900])

        split_layout.addWidget(splitter)
        split_group.setLayout(split_layout)
        layout.addWidget(split_group)

    # ============================================
    # JSON 목록 로드
    # ============================================
    def load_json_list(self):
        files = glob.glob(os.path.join(INFO_DIR, "HOJ_ENGINE_REAL_*.json"))
        files = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)

        self.combo_json.clear()
        for f in files:
            self.combo_json.addItem(os.path.basename(f), f)

    # ============================================
    # 미리보기 생성(alert_notify.py 실행)
    # ============================================
    def make_preview(self):
        json_path = self.combo_json.currentData()
        if not json_path:
            self.txt_log.append("[오류] JSON 선택되지 않음")
            return

        cmd = ["python", ALERT_SCRIPT, "--json", json_path, "--make-only"]
        self.txt_log.append("▶ 미리보기 생성 중...")
        self.txt_log.append(" ".join(cmd))

        try:
            subprocess.run(cmd, capture_output=True, text=True, shell=False)
        except Exception as e:
            self.txt_log.append(f"[오류] 실행 중 오류 발생: {e}")
            return

        base = os.path.splitext(os.path.basename(json_path))[0]
        img_path = os.path.join(BEST_TOP_DIR, base + ".png")

        if os.path.exists(img_path):
            pix = QPixmap(img_path).scaled(
                self.lbl_img.width(), self.lbl_img.height(), Qt.KeepAspectRatio)
            self.lbl_img.setPixmap(pix)
            self.txt_log.append(f"[완료] 이미지 생성됨: {img_path}")
        else:
            self.lbl_img.setText("생성된 이미지 없음")
            self.txt_log.append("[오류] 이미지 생성 실패")

    # ============================================
    # 전송 실행(alert_notify.py 호출)
    # ============================================
    def run_send(self):
        json_path = self.combo_json.currentData()
        if not json_path:
            self.txt_log.append("[오류] JSON 선택되지 않음")
            return

        # 모드
        if self.rb_text.isChecked():
            mode = "text"
        elif self.rb_image.isChecked():
            mode = "image"
        else:
            mode = "both"

        # 채널
        channels = []
        if self.cb_kakao.isChecked():
            channels.append("kakao")
        if self.cb_tg.isChecked():
            channels.append("telegram")
        if self.cb_sms.isChecked():
            channels.append("sms")

        if not channels:
            self.txt_log.append("[오류] 최소 1개 채널 선택해야 함")
            return

        cmd = [
            "python", ALERT_SCRIPT,
            "--json", json_path,
            "--channels", ",".join(channels),
            "--mode", mode
        ]

        self.txt_log.append("▶ 전송 실행")
        self.txt_log.append(" ".join(cmd))

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, shell=False)
            self.txt_log.append(result.stdout)
            if result.stderr:
                self.txt_log.append(result.stderr)
        except Exception as e:
            self.txt_log.append(f"[오류] 실행중 오류: {e}")
            return

        self.txt_log.append("✅ 전송 완료")
