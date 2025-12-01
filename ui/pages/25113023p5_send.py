# -*- coding: utf-8 -*-
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QGroupBox, QRadioButton, QTextEdit,
    QPushButton, QHBoxLayout, QComboBox, QCheckBox, QSplitter,
    QSizePolicy, QScrollArea, QDialog
)
from PySide6.QtGui import QPixmap, QMouseEvent
from PySide6.QtCore import Qt
import os
import subprocess
import glob

# 경로 설정
INFO_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"
BEST_TOP_DIR = r"F:\autostockG\MODELENGINE\INFO\best_top"
# 경로 교정: alert_notify.py 최종 위치
ALERT_SCRIPT = r"F:\autostockG\MODELENGINE\UTIL\alert_notify.py"

class P5_Send(QWidget):
    def __init__(self):
        super().__init__()
        self.current_img_path = ""
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
        # 전송 옵션 + 버튼(우측)
        # ------------------------------
        group_opt = QGroupBox("전송 옵션")
        opt_layout = QHBoxLayout()
        opt_layout.setSpacing(20)

        # 좌: 옵션
        opt_left = QVBoxLayout()

        mode_box = QHBoxLayout()
        mode_box.addWidget(QLabel("전송 모드:"))
        self.rb_text  = QRadioButton("텍스트만")
        self.rb_image = QRadioButton("이미지만")
        self.rb_both  = QRadioButton("둘 다"); self.rb_both.setChecked(True)
        mode_box.addWidget(self.rb_text); mode_box.addWidget(self.rb_image); mode_box.addWidget(self.rb_both)
        opt_left.addLayout(mode_box)

        ch_box = QHBoxLayout()
        ch_box.addWidget(QLabel("전송 채널:"))
        self.cb_kakao = QCheckBox("카카오톡")
        self.cb_tg    = QCheckBox("텔레그램")
        self.cb_sms   = QCheckBox("SMS")
        ch_box.addWidget(self.cb_kakao); ch_box.addWidget(self.cb_tg); ch_box.addWidget(self.cb_sms)
        opt_left.addLayout(ch_box)

        opt_layout.addLayout(opt_left)

        # 우: 버튼
        opt_right = QVBoxLayout(); opt_right.setAlignment(Qt.AlignRight | Qt.AlignTop)
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
        # 3분할: [왼] 이미지(스크롤) / [중] 전송 텍스트 / [오] 로그
        # -----------------------------------------------------
        split_group = QGroupBox("미리보기 / 전송 텍스트 / 로그")
        split_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)   # ★ 추가
        split_layout = QHBoxLayout()

        splitter = QSplitter(Qt.Horizontal)
        splitter.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)      # ★ 추가

        # ===== 왼쪽: 이미지(스크롤) =====
        self.lbl_img = QLabel("이미지 없음")
        self.lbl_img.mousePressEvent = self.open_full_image
        self.lbl_img.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.lbl_img.setStyleSheet("background: #f5f7fa; border: 1px solid #d8dee9;")

        self.img_scroll = QScrollArea()
        self.img_scroll.setWidgetResizable(True)
        self.img_scroll.setWidget(self.lbl_img)
        self.img_scroll.setMinimumWidth(360)
        self.img_scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)  # ★ 추가

        # ===== 가운데: 전송 텍스트 =====
        self.txt_message = QTextEdit()
        self.txt_message.setReadOnly(True)
        self.txt_message.setPlaceholderText("전송 텍스트가 여기 표시됩니다.")
        self.txt_message.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding) # ★ 추가

        # ===== 오른쪽: 실행 로그 =====
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setPlaceholderText("프로그램 실행 내용(Log)")
        self.txt_log.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)    # ★ 추가

        splitter.addWidget(self.img_scroll)
        splitter.addWidget(self.txt_message)
        splitter.addWidget(self.txt_log)

        splitter.setSizes([380, 520, 520])
        # 추가: 분할기 스트레치 팩터(좌 1 : 중 2 : 우 2)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 2)

        split_layout.addWidget(splitter)
        split_group.setLayout(split_layout)
        layout.addWidget(split_group)
       # 아래 두 줄 추가 (상단 그룹은 고정, 하단 3분할은 확장)
        group_json.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        group_opt.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

     # 전체 레이아웃 스트레치: title(0), json(1), opt(2) 고정 / split(3) 확장
        layout.setStretch(0, 0)
        layout.setStretch(1, 0)
        layout.setStretch(2, 0)
        layout.setStretch(3, 1)

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
    # 미리보기 생성(alert_notify.py 호출)
    #  - 이미지/텍스트 파일 생성
    #  - 텍스트는 --dump-text 로 stdout도 수신 → 가운데 패널 표시
    # ============================================
    def make_preview(self):
        json_path = self.combo_json.currentData()
        if not json_path:
            self.txt_log.append("[오류] JSON 선택되지 않음")
            return

        cmd = ["python", ALERT_SCRIPT, "--json", json_path, "--make-only", "--dump-text"]
        self.txt_log.append("▶ 미리보기 생성 중...")
        self.txt_log.append(" ".join(cmd))

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, shell=False)
        except Exception as e:
            self.txt_log.append(f"[오류] 실행 중 오류 발생: {e}")
            return

        # 가운데 텍스트: stdout을 그대로 출력(알림 텍스트)
        msg_text = result.stdout.strip()
        if msg_text:
            self.txt_message.setPlainText(msg_text)
        # 추가: stdout이 비면 best_top/<base>.txt 읽어 보강
        if not msg_text:
            base = os.path.splitext(os.path.basename(json_path))[0]
            txt_path = os.path.join(BEST_TOP_DIR, base + ".txt")
            if os.path.exists(txt_path):
                with open(txt_path, "r", encoding="utf-8") as f:
                    msg_text = f.read().strip()
            if msg_text:
                self.txt_message.setPlainText(msg_text)


        # 이미지 로드
        base = os.path.splitext(os.path.basename(json_path))[0]
        img_path = os.path.join(BEST_TOP_DIR, base + ".png")
        self.current_img_path = img_path if os.path.exists(img_path) else ""

        if self.current_img_path:
            pix = QPixmap(self.current_img_path)
            self.lbl_img.setPixmap(pix)  # 스크롤 영역이 알아서 처리
            self.txt_log.append(f"[완료] 이미지 생성됨: {self.current_img_path}")
        else:
            self.lbl_img.setText("생성된 이미지 없음")
            self.txt_log.append("[오류] 이미지 생성 실패")

        # 로그에 표준출력/표준에러는 참고용으로만 추가
        if result.stderr:
            self.txt_log.append(result.stderr)

    # ============================================
    # 전송 실행(alert_notify.py 호출)
    #  - 먼저 미리보기 갱신(텍스트/이미지 최신 상태)
    #  - 이후 실제 전송 실행
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
        if self.cb_kakao.isChecked(): channels.append("kakao")
        if self.cb_tg.isChecked():    channels.append("telegram")
        if self.cb_sms.isChecked():   channels.append("sms")

        if not channels:
            self.txt_log.append("[오류] 최소 1개 채널 선택해야 함")
            return

        # 1) 최신 미리보기 갱신
        self.make_preview()

        # 2) 실제 전송 실행
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
            if result.stdout: self.txt_log.append(result.stdout)
            if result.stderr: self.txt_log.append(result.stderr)
        except Exception as e:
            self.txt_log.append(f"[오류] 실행중 오류: {e}")
            return

        self.txt_log.append("✅ 전송 완료")

    # ============================================
    # 이미지 원본 확대 보기
    # ============================================
    def open_full_image(self, event: QMouseEvent):
        if not self.current_img_path or not os.path.exists(self.current_img_path):
            return
        dlg = QDialog(self)
        dlg.setWindowTitle("원본 이미지")
        dlg.resize(900, 1200)

        label = QLabel()
        label.setPixmap(QPixmap(self.current_img_path))
        label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        sa = QScrollArea(dlg)
        sa.setWidgetResizable(True)
        sa.setWidget(label)

        v = QVBoxLayout(dlg)
        v.addWidget(sa)
        dlg.setLayout(v)
        dlg.exec()
