# -*- coding: utf-8 -*-
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QGroupBox, QRadioButton, QTextEdit,
    QPushButton, QHBoxLayout, QMessageBox, QComboBox, QFileDialog, QCheckBox
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
        # 전송모드 / 채널 선택
        # ------------------------------
        group_opt = QGroupBox("전송 옵션")
        vo = QVBoxLayout()

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

        vo.addLayout(mode_box)

        # 채널: 카카오 / 텔레그램 / SMS
        ch_box = QHBoxLayout()
        ch_box.addWidget(QLabel("전송 채널:"))
        self.cb_kakao = QCheckBox("카카오톡")
        self.cb_tg = QCheckBox("텔레그램")
        self.cb_sms = QCheckBox("SMS")

        ch_box.addWidget(self.cb_kakao)
        ch_box.addWidget(self.cb_tg)
        ch_box.addWidget(self.cb_sms)

        vo.addLayout(ch_box)
        group_opt.setLayout(vo)
        layout.addWidget(group_opt)

        # ------------------------------
        # 이미지 미리보기
        # ------------------------------
        group_img = QGroupBox("이미지 미리보기")
        vi = QVBoxLayout()

        self.lbl_img = QLabel("이미지 없음")
        self.lbl_img.setAlignment(Qt.AlignCenter)
        self.lbl_img.setStyleSheet("background: #f0f0f0; border: 1px solid #ccc;")
        self.lbl_img.setFixedHeight(350)
        vi.addWidget(self.lbl_img)

        btn_preview = QPushButton("미리보기 생성")
        btn_preview.setStyleSheet("padding: 8px; font-weight: bold;")
        btn_preview.clicked.connect(self.make_preview)
        vi.addWidget(btn_preview)

        group_img.setLayout(vi)
        layout.addWidget(group_img)

        # ------------------------------
        # 로그
        # ------------------------------
        group_log = QGroupBox("로그")
        vl = QVBoxLayout()
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        vl.addWidget(self.txt_log)
        group_log.setLayout(vl)
        layout.addWidget(group_log)

        # ------------------------------
        # 전송 버튼
        # ------------------------------
        btn_send = QPushButton("전송 실행")
        btn_send.setStyleSheet("background:#2980b9; color:white; padding:12px; font-weight:bold;")
        btn_send.clicked.connect(self.run_send)
        layout.addWidget(btn_send)

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

        # 생성된 이미지 로드
        base = os.path.splitext(os.path.basename(json_path))[0]
        img_path = os.path.join(BEST_TOP_DIR, base + ".png")

        if os.path.exists(img_path):
            pix = QPixmap(img_path).scaled(self.lbl_img.width(), self.lbl_img.height(), Qt.KeepAspectRatio)
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
