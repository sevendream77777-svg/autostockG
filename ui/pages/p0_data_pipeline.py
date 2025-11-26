# ui/pages/p0_data_pipeline.py
import os
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QPushButton, QTextEdit, QProgressBar, QLabel, QMessageBox,
                               QLineEdit, QDateEdit, QCheckBox, QFileDialog, QRadioButton, QButtonGroup)
from PySide6.QtCore import QDate, Qt
from common.workers import DataUpdateWorker, ManualDownloadWorker

class DataPage(QWidget):
    def __init__(self):
        super().__init__()
        # MODELENGINE 경로 자동 탐색
        self.base_path = self.find_modelengine_path()
        self.init_ui()

    def find_modelengine_path(self):
        # 1. 현재 파일 기준 상위 폴더 탐색
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path_candidate = os.path.abspath(os.path.join(current_dir, "../../MODELENGINE"))
        
        if os.path.exists(path_candidate):
            return path_candidate
        
        # 2. 실패 시 하드코딩 경로
        return r"F:\autostockG\MODELENGINE"

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 1. 파이프라인 섹션
        gb_pipe = QGroupBox("🔄 데이터 파이프라인 (Data Factory)")
        v_pipe = QVBoxLayout()
        
        # 경로 확인용 라벨
        lbl_path = QLabel(f"엔진 경로: {self.base_path}")
        lbl_path.setStyleSheet("color: gray; font-size: 8pt;")
        v_pipe.addWidget(lbl_path)

        lbl_info = QLabel("각 단계를 개별 실행하거나, 전체 실행을 통해 한 번에 처리할 수 있습니다.")
        lbl_info.setStyleSheet("color: #88c0d0; margin-bottom: 10px;")
        v_pipe.addWidget(lbl_info)
        
        h_btns1 = QHBoxLayout()
        self.btn_step1 = QPushButton("1. 시세 업데이트"); self.btn_step2 = QPushButton("2. KOSPI 생성")
        self.btn_step3 = QPushButton("3. 피처 생성"); self.btn_step4 = QPushButton("4. DB 통합")
        for b in [self.btn_step1, self.btn_step2, self.btn_step3, self.btn_step4]: 
            b.setFixedHeight(40)
            h_btns1.addWidget(b)
        v_pipe.addLayout(h_btns1)
        
        self.btn_all = QPushButton("🚀 전체 실행 (Run All)")
        self.btn_all.setFixedHeight(50)
        self.btn_all.setStyleSheet("background-color: #b48ead; font-weight: bold;")
        v_pipe.addWidget(self.btn_all)
        
        self.progress = QProgressBar()
        self.progress.setValue(0)
        v_pipe.addWidget(self.progress)
        
        # [수정] 로그창 상단에 '로그 지우기' 버튼 추가 (너무 많이 쌓이면 지울 수 있게)
        h_log_ctrl = QHBoxLayout()
        h_log_ctrl.addWidget(QLabel("📋 실행 로그"))
        h_log_ctrl.addStretch()
        btn_clear_log = QPushButton("로그 지우기")
        btn_clear_log.setFixedSize(80, 25)
        btn_clear_log.clicked.connect(self.clear_log_view)
        h_log_ctrl.addWidget(btn_clear_log)
        v_pipe.addLayout(h_log_ctrl)

        self.log_pipe = QTextEdit()
        self.log_pipe.setReadOnly(True)
        self.log_pipe.setMaximumHeight(200) # 로그창 높이 조금 키움
        self.log_pipe.setStyleSheet("background-color: #2e3440; color: #d8dee9; font-family: Consolas;")
        v_pipe.addWidget(self.log_pipe)
        
        gb_pipe.setLayout(v_pipe)
        layout.addWidget(gb_pipe)

        # 2. 수동 다운로드 섹션 (기존 유지)
        gb_manual = QGroupBox("📥 수동 선택 다운로드 (Manual Download)")
        v_manual = QVBoxLayout()
        
        # 종목 선택
        gb_target = QGroupBox("대상 종목 선택")
        gb_target.setStyleSheet("QGroupBox { border: 1px dotted #4c566a; margin-top: 5px; padding: 5px; }")
        v_target = QVBoxLayout()
        h_radio = QHBoxLayout()
        self.rb_all = QRadioButton("모든 종목 (KOSPI + KOSDAQ)")
        self.rb_all.setChecked(True) 
        self.rb_select = QRadioButton("종목 지정 (코드 입력/파일)")
        h_radio.addWidget(self.rb_all)
        h_radio.addWidget(self.rb_select)
        v_target.addLayout(h_radio)
        
        self.bg_target = QButtonGroup(self)
        self.bg_target.addButton(self.rb_all)
        self.bg_target.addButton(self.rb_select)
        self.bg_target.buttonToggled.connect(self.toggle_code_input)
        
        self.widget_input = QWidget()
        h_input = QHBoxLayout(self.widget_input)
        h_input.setContentsMargins(0, 5, 0, 0)
        h_input.addWidget(QLabel("코드/파일:"))
        self.txt_codes = QLineEdit()
        self.txt_codes.setPlaceholderText("예: 005930, 000660 또는 종목리스트.txt")
        h_input.addWidget(self.txt_codes)
        self.btn_file = QPushButton("파일선택")
        self.btn_file.clicked.connect(self.pick_code_file)
        h_input.addWidget(self.btn_file)
        
        v_target.addWidget(self.widget_input)
        gb_target.setLayout(v_target)
        v_manual.addWidget(gb_target)
        self.toggle_code_input()

        # 기간 선택
        h2 = QHBoxLayout()
        h2.addWidget(QLabel("기간:"))
        
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.setDate(QDate.currentDate())
        
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.setDate(QDate.currentDate())
        
        h2.addWidget(self.date_start)
        h2.addWidget(QLabel("~"))
        h2.addWidget(self.date_end)
        
        self.chk_one_day = QCheckBox("1일만")
        self.chk_one_day.stateChanged.connect(lambda s: self.date_end.setEnabled(s == 0))
        h2.addWidget(self.chk_one_day)
        v_manual.addLayout(h2)

        # 컬럼 선택
        gb_cols = QGroupBox("저장할 컬럼 설정")
        gb_cols.setStyleSheet("QGroupBox { border: 1px dotted #4c566a; margin-top: 5px; padding: 5px; }")
        v_cols = QVBoxLayout()
        
        self.chk_all_original = QCheckBox("📦 원본 컬럼 모두 저장 (추천)")
        self.chk_all_original.setChecked(True)
        self.chk_all_original.setStyleSheet("font-weight: bold; color: #ebcb8b;")
        self.chk_all_original.stateChanged.connect(self.toggle_col_selection)
        v_cols.addWidget(self.chk_all_original)
        
        self.widget_col_select = QWidget()
        h_cols = QHBoxLayout(self.widget_col_select)
        h_cols.setContentsMargins(0,0,0,0)
        
        self.chk_cols = {}
        for c in ["Open", "High", "Low", "Close", "Volume", "Amount", "Change"]:
            chk = QCheckBox(c)
            chk.setChecked(True)
            self.chk_cols[c] = chk
            h_cols.addWidget(chk)
            
        btn_toggle = QPushButton("반전")
        btn_toggle.setFixedSize(60, 25)
        btn_toggle.clicked.connect(self.toggle_individual_cols)
        h_cols.addWidget(btn_toggle)
        
        v_cols.addWidget(self.widget_col_select)
        gb_cols.setLayout(v_cols)
        v_manual.addWidget(gb_cols)
        
        self.toggle_col_selection()

        # 실행 버튼
        self.btn_down_run = QPushButton("다운로드 실행")
        self.btn_down_run.setFixedHeight(45)
        v_manual.addWidget(self.btn_down_run)
        
        self.log_manual = QTextEdit()
        self.log_manual.setReadOnly(True)
        self.log_manual.setStyleSheet("background-color: #2e3440; color: #d8dee9; font-family: Consolas;")
        v_manual.addWidget(self.log_manual)
        
        gb_manual.setLayout(v_manual)
        layout.addWidget(gb_manual)

        # 시그널 연결
        self.btn_step1.clicked.connect(lambda: self.run_pipeline(['stock']))
        self.btn_step2.clicked.connect(lambda: self.run_pipeline(['kospi']))
        self.btn_step3.clicked.connect(lambda: self.run_pipeline(['feature']))
        self.btn_step4.clicked.connect(lambda: self.run_pipeline(['db']))
        self.btn_all.clicked.connect(lambda: self.run_pipeline(['stock', 'kospi', 'feature', 'db']))
        self.btn_down_run.clicked.connect(self.run_manual_download)

    # --- 로직 ---
    def clear_log_view(self):
        self.log_pipe.clear()

    def run_pipeline(self, tasks):
        # [수정] 기존 로그를 지우지 않고 구분선만 추가
        # self.log_pipe.clear()  <-- 삭제됨
        self.log_pipe.append("\n" + "="*50)
        self.log_pipe.append(f"🚀 작업을 시작합니다: {tasks}")
        self.log_pipe.append("="*50 + "\n")
        
        self.progress.setValue(0)
        
        # 워커 생성
        self.worker = DataUpdateWorker(tasks, base_path=self.base_path)
        
        self.worker.log_signal.connect(self.log_pipe.append)
        self.worker.progress_signal.connect(self.progress.setValue)
        self.worker.finished_signal.connect(lambda m: QMessageBox.information(self, "완료", m))
        self.worker.error_signal.connect(lambda e: QMessageBox.critical(self, "오류", e))
        self.worker.start()

    def pick_code_file(self):
        f, _ = QFileDialog.getOpenFileName(self, "종목 리스트 파일", "", "Txt/Json (*.txt *.json)")
        if f: self.txt_codes.setText(f)

    def toggle_code_input(self):
        enabled = self.rb_select.isChecked()
        self.widget_input.setEnabled(enabled)
        if not enabled: self.txt_codes.clear()

    def toggle_col_selection(self):
        if self.chk_all_original.isChecked():
            for chk in self.chk_cols.values():
                chk.setChecked(True)
        self.widget_col_select.setEnabled(True)

    def toggle_individual_cols(self):
        for chk in self.chk_cols.values():
            chk.setChecked(not chk.isChecked())

    def run_manual_download(self):
        # 1. 종목 결정
        codes = None
        if self.rb_all.isChecked(): 
            codes = None
            target_msg = "전체 종목 (KOSPI+KOSDAQ)"
        else:
            raw = self.txt_codes.text().strip()
            if not raw: 
                QMessageBox.warning(self, "입력 오류", "종목코드나 파일을 지정해주세요.")
                return
            if os.path.isfile(raw): 
                codes = [raw]
            else: 
                codes = [c.strip() for c in raw.split(',') if c.strip()]
            target_msg = f"{len(codes) if isinstance(codes, list) and not os.path.isfile(codes[0]) else '파일'} 지정"

        # 2. 기간 및 컬럼
        s = self.date_start.date().toString("yyyyMMdd")
        e = self.date_end.date().toString("yyyyMMdd")
        if self.chk_one_day.isChecked(): e = s
        
        final_cols = None
        col_msg = "ALL (Original)"
        
        if not self.chk_all_original.isChecked():
            selected = [col for col, chk in self.chk_cols.items() if chk.isChecked()]
            if not selected: 
                QMessageBox.warning(self, "설정 오류", "컬럼을 하나 이상 선택하세요.")
                return
            final_cols = selected
            col_msg = str(final_cols)

        # 3. 경로 설정 및 실행
        script = os.path.join(self.base_path, "RAW", "시세다운로드full단독", "pykrx_full_dump_resumable.py")
        out = os.path.join(self.base_path, "RAW", "시세다운로드full단독", "raw_only_down_ui")
        
        if not os.path.exists(script):
             QMessageBox.critical(self, "오류", f"스크립트를 찾을 수 없습니다:\n{script}")
             return
             
        if not os.path.exists(out): os.makedirs(out, exist_ok=True)

        # [수정] 수동 다운로드 로그도 누적되도록 변경
        # self.log_manual.clear() <-- 삭제됨
        self.log_manual.append("\n" + "-"*40)
        self.log_manual.append(f"📥 다운로드 요청 시작...")
        self.log_manual.append(f" - 대상: {target_msg}")
        self.log_manual.append(f" - 기간: {s} ~ {e}")
        self.log_manual.append(f" - 컬럼: {col_msg}")
        self.log_manual.append(f" - 저장 경로: {out}")
        self.log_manual.append(f" - 실행 파일: {script}")
        
        self.md_worker = ManualDownloadWorker(codes, s, e, out, script, columns=final_cols)
        self.md_worker.log_signal.connect(self.log_manual.append)
        self.md_worker.finished_signal.connect(lambda m: QMessageBox.information(self, "완료", m))
        self.md_worker.error_signal.connect(lambda e: self.log_manual.append(f"❌ {e}"))
        self.md_worker.start()