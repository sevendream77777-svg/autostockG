# ui/pages/p1_training.py
import os
import glob
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, 
                               QLabel, QComboBox, QSpinBox, QPushButton, QTextEdit, 
                               QMessageBox, QFrame, QSizePolicy)
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont
from common.workers import TrainingWorker

class P2_Training(QWidget):
    def __init__(self):
        super().__init__()
        self.base_path = self.find_modelengine_path()
        self.worker = None
        self.init_ui()
        # 초기 로딩
        self.refresh_data_files()

    def find_modelengine_path(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path_candidate = os.path.abspath(os.path.join(current_dir, "../../MODELENGINE"))
        return path_candidate if os.path.exists(path_candidate) else r"F:\autostockG\MODELENGINE"

    def init_ui(self):
        # 전체 레이아웃: 위에서 아래로 순차 진행 (VBoxLayout)
        layout = QVBoxLayout(self)
        layout.setSpacing(15)

        # --- [Step 1] 데이터 선택 ---
        gb_step1 = QGroupBox("1단계: 학습 데이터 선택 (Base Data)")
        gb_step1.setStyleSheet("QGroupBox { font-weight: bold; border: 1px solid #88c0d0; }")
        h_step1 = QHBoxLayout()
        
        h_step1.addWidget(QLabel("사용할 DB 파일:"))
        self.cb_db_files = QComboBox()
        self.cb_db_files.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        h_step1.addWidget(self.cb_db_files)
        
        btn_refresh = QPushButton("새로고침")
        btn_refresh.clicked.connect(self.refresh_data_files)
        h_step1.addWidget(btn_refresh)
        
        gb_step1.setLayout(h_step1)
        layout.addWidget(gb_step1)

        # --- [Step 2] 연구 설정 및 실행 ---
        gb_step2 = QGroupBox("2단계: 연구 설계 및 검증 (Research)")
        gb_step2.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #81a1c1; }")
        v_step2 = QVBoxLayout()
        
        # 파라미터 행
        h_param_res = QHBoxLayout()
        
        # Horizon
        h_param_res.addWidget(QLabel("목표기간(H):"))
        self.cb_h_res = QComboBox()
        self.setup_horizon_combo(self.cb_h_res)
        h_param_res.addWidget(self.cb_h_res)
        
        # Window
        h_param_res.addWidget(QLabel("입력윈도우(W):"))
        self.cb_w_res = QComboBox()
        self.setup_window_combo(self.cb_w_res)
        h_param_res.addWidget(self.cb_w_res)
        
        # Iteration
        h_param_res.addWidget(QLabel("반복횟수(N):"))
        self.spin_n_res = QSpinBox()
        self.spin_n_res.setRange(100, 10000); self.spin_n_res.setSingleStep(100); self.spin_n_res.setValue(1000)
        h_param_res.addWidget(self.spin_n_res)
        
        # Valid Days
        h_param_res.addWidget(QLabel("검증기간(일):"))
        self.spin_val_res = QSpinBox()
        self.spin_val_res.setRange(30, 2000); self.spin_val_res.setValue(365)
        h_param_res.addWidget(self.spin_val_res)
        
        v_step2.addLayout(h_param_res)
        
        # 경고 메시지 라벨
        self.lbl_warn_res = QLabel("")
        self.lbl_warn_res.setStyleSheet("color: #bf616a; font-weight: normal;")
        v_step2.addWidget(self.lbl_warn_res)
        
        # 실행 버튼
        self.btn_run_res = QPushButton("🧪 연구 학습 시작 (Start Research)")
        self.btn_run_res.setStyleSheet("background-color: #5e81ac; font-weight: bold; color: white; padding: 8px;")
        self.btn_run_res.clicked.connect(self.run_research)
        v_step2.addWidget(self.btn_run_res)
        
        gb_step2.setLayout(v_step2)
        layout.addWidget(gb_step2)

        # --- [Step 3] 연구 결과 리포트 (중요!) ---
        gb_report = QGroupBox("3단계: 연구 결과 자동 요약")
        gb_report.setStyleSheet("QGroupBox { font-weight: bold; border: 1px solid #ebcb8b; }")
        v_report = QVBoxLayout()
        
        self.txt_report = QTextEdit()
        self.txt_report.setReadOnly(True)
        self.txt_report.setMaximumHeight(100)
        self.txt_report.setPlaceholderText("연구 학습이 완료되면 이곳에 핵심 요약(AUC, 성능평가)이 표시됩니다.")
        self.txt_report.setStyleSheet("background-color: #3b4252; color: #ebcb8b; font-size: 10pt; border: none;")
        v_report.addWidget(self.txt_report)
        
        gb_report.setLayout(v_report)
        layout.addWidget(gb_report)

        # --- [Step 4] 실전 엔진 생성 ---
        gb_step4 = QGroupBox("4단계: 실전 엔진 생성 (Real Production)")
        gb_step4.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #bf616a; }")
        v_step4 = QVBoxLayout()
        
        # 설명 & 파라미터 (활성화 상태)
        h_param_real = QHBoxLayout()
        h_param_real.addWidget(QLabel("설정값(자동상속):"))
        
        self.cb_h_real = QComboBox(); self.setup_horizon_combo(self.cb_h_real)
        h_param_real.addWidget(self.cb_h_real)
        
        self.cb_w_real = QComboBox(); self.setup_window_combo(self.cb_w_real)
        h_param_real.addWidget(self.cb_w_real)
        
        self.spin_n_real = QSpinBox()
        self.spin_n_real.setRange(100, 10000); self.spin_n_real.setSingleStep(100); self.spin_n_real.setValue(1000)
        h_param_real.addWidget(self.spin_n_real)
        
        h_param_real.addWidget(QLabel("(검증 없음)"))
        v_step4.addLayout(h_param_real)
        
        # 실행 버튼
        self.btn_run_real = QPushButton("🚀 실전 엔진 생성 (Create Real Engine)")
        self.btn_run_real.setStyleSheet("background-color: #bf616a; font-weight: bold; color: white; padding: 8px;")
        self.btn_run_real.clicked.connect(self.run_real_manual)
        v_step4.addWidget(self.btn_run_real)
        
        gb_step4.setLayout(v_step4)
        layout.addWidget(gb_step4)

        # --- [Step 5] 로그창 ---
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setStyleSheet("background-color: #2e3440; color: #d8dee9; font-family: Consolas; font-size: 9pt;")
        layout.addWidget(self.log_view)

        # 이벤트 연결 (경고 메시지용)
        self.cb_h_res.currentIndexChanged.connect(self.update_warnings)
        self.cb_w_res.currentIndexChanged.connect(self.update_warnings)

    # --- 콤보박스 셋업 ---
    def setup_horizon_combo(self, cb):
        items = [1, 2, 5, 10, 20, 40, 60]
        for val in items:
            cb.addItem(f"{val}일", val)
            if val >= 40: cb.setItemData(cb.count()-1, QColor("#bf616a"), Qt.ForegroundRole)
        cb.setCurrentText("5일")

    def setup_window_combo(self, cb):
        items = [5, 10, 20, 40, 60, 90, 120]
        for val in items:
            cb.addItem(f"{val}일", val)
            if val <= 10: cb.setItemData(cb.count()-1, QColor("#bf616a"), Qt.ForegroundRole)
        cb.setCurrentText("60일")

    def update_warnings(self):
        h_val = self.cb_h_res.currentData()
        w_val = self.cb_w_res.currentData()
        msg = []
        if h_val >= 40: msg.append("⚠️ Horizon 40일 이상은 샘플 부족 위험")
        if w_val <= 10: msg.append("⚠️ Window 10일 이하는 학습 효과 미미")
        self.lbl_warn_res.setText(" | ".join(msg))

    # --- 데이터 파일 갱신 ---
    def refresh_data_files(self):
        self.cb_db_files.clear()
        db_path = os.path.join(self.base_path, "HOJ_DB")
        files = glob.glob(os.path.join(db_path, "HOJ_DB_V31_*.parquet"))
        files.sort(key=lambda x: os.path.basename(x), reverse=True) # 최신순

        if not files:
            self.cb_db_files.addItem("데이터 파일 없음")
            return

        for f in files:
            name = os.path.basename(f)
            self.cb_db_files.addItem(name, f) # data=full_path

    # --- 실행 로직 ---
    def get_db_check(self):
        if self.cb_db_files.count() == 0 or "없음" in self.cb_db_files.currentText():
            QMessageBox.warning(self, "경고", "학습할 데이터 파일이 없습니다.")
            return False
        return True

    def run_research(self):
        if not self.get_db_check(): return
        
        params = {
            'mode': 'research',
            'horizon': self.cb_h_res.currentData(),
            'input_window': self.cb_w_res.currentData(),
            'n_estimators': self.spin_n_res.value(),
            'valid_days': self.spin_val_res.value()
        }
        self.start_worker(params)

    def run_real_manual(self):
        if not self.get_db_check(): return
        
        params = {
            'mode': 'real',
            'horizon': self.cb_h_real.currentData(),
            'input_window': self.cb_w_real.currentData(),
            'n_estimators': self.spin_n_real.value(),
            'valid_days': 0
        }
        self.start_worker(params)

    def start_worker(self, params):
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "대기", "현재 작업이 진행 중입니다.")
            return

        self.log_view.append("\n" + "="*50)
        self.log_view.append(f"🚀 [{params['mode'].upper()}] 학습 시작")
        self.log_view.append(f"설정: H={params['horizon']}, W={params['input_window']}, N={params['n_estimators']}")
        if params['mode'] == 'research': self.txt_report.clear() # 리포트 초기화
        
        self.worker = TrainingWorker(params, base_path=self.base_path)
        self.worker.log_signal.connect(self.log_view.append)
        self.worker.finished_signal.connect(self.on_finished)
        self.worker.error_signal.connect(lambda e: self.log_view.append(f"❌ {e}"))
        self.worker.start()

    def on_finished(self, result):
        mode = result['mode']
        params = result['params']
        
        if mode == 'research':
            # 1. 결과 리포트 생성 및 표시
            summary = self.generate_summary(result.get('last_lines', []))
            self.txt_report.setText(summary)
            
            # 2. 실전 연결 제안 팝업
            reply = QMessageBox.question(
                self, "연구 완료", 
                f"연구 학습이 완료되었습니다.\n\n[결과 요약]\n{summary}\n\n🚀 이 설정으로 '실전 엔진'을 바로 생성하시겠습니까?",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.run_real_auto(params)
        else:
            QMessageBox.information(self, "완료", "실전 엔진 생성이 완료되었습니다!\n이제 P3 예측 메뉴를 사용할 수 있습니다.")

    def generate_summary(self, last_lines):
        """로그 마지막 줄에서 핵심 지표 추출"""
        text = ""
        found = False
        for line in reversed(last_lines):
            if any(k in line.lower() for k in ['auc', 'rmse', 'score', 'valid']):
                text += line + "\n"
                found = True
                if len(text) > 200: break
        return text if found else "주요 성능 지표를 찾을 수 없습니다. (로그 확인 필요)"

    def run_real_auto(self, res_params):
        """연구 설정값을 실전 UI에 반영하고 자동 시작"""
        # UI 동기화 (사용자가 보기에 값이 바뀌도록)
        self.cb_h_real.setCurrentText(f"{res_params['horizon']}일")
        self.cb_w_real.setCurrentText(f"{res_params['input_window']}일")
        self.spin_n_real.setValue(res_params['n_estimators'])
        
        self.log_view.append("\n>>> 연구 설정 상속 -> 실전 엔진 생성 자동 시작...")
        self.run_real_manual() # 변경된 UI 값으로 실행