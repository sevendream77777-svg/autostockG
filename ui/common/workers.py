# ui/common/workers.py
import os
import sys
import time
import subprocess
import pandas as pd
from PySide6.QtCore import QThread, Signal

# ---------------------------------------------------------
# 경로 설정 (전역)
# ---------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
ui_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(ui_dir) # F:\autostockG
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, "MODELENGINE", "UTIL"))
sys.path.append(os.path.join(root_dir, "MODELENGINE", "RAW"))

# ---------------------------------------------------------
# 1. 데이터 파이프라인 워커
# ---------------------------------------------------------
class DataUpdateWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, task_list): 
        super().__init__()
        self.task_list = task_list

    def run(self):
        try:
            # Lazy import to avoid circular dependency or startup lag
            import update_raw_data
            import make_kospi_index_10y
            import build_features
            import build_unified_db
        except ImportError as e: 
            self.error_signal.emit(f"스크립트 로딩 실패: {e}")
            return

        try:
            total = len(self.task_list)
            for i, task in enumerate(self.task_list):
                self.progress_signal.emit(int((i/total)*100))
                
                if task == 'stock': 
                    self.log_signal.emit("\n>>> [1] 시세(RAW) 업데이트...")
                    update_raw_data.main()
                elif task == 'kospi': 
                    self.log_signal.emit("\n>>> [2] KOSPI 지수 생성...")
                    make_kospi_index_10y.main()
                elif task == 'feature': 
                    self.log_signal.emit("\n>>> [3] 피처 엔지니어링...")
                    build_features.main()
                elif task == 'db': 
                    self.log_signal.emit("\n>>> [4] DB 통합...")
                    build_unified_db.build_unified_db()
                
                self.log_signal.emit(f"✅ {task} 단계 완료")
                time.sleep(0.5)
            
            self.progress_signal.emit(100)
            self.finished_signal.emit("모든 데이터 작업이 완료되었습니다.")
        except Exception as e: 
            import traceback
            self.error_signal.emit(f"오류 발생: {traceback.format_exc()}")

# ---------------------------------------------------------
# 2. 수동 다운로드 워커
# ---------------------------------------------------------
class ManualDownloadWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    
    def __init__(self, codes, start_date, end_date, out_dir, script_path, columns=None):
        super().__init__()
        self.codes = codes
        self.start_date = start_date
        self.end_date = end_date
        self.out_dir = out_dir
        self.script_path = script_path
        self.columns = columns

    def run(self):
        try:
            if not os.path.exists(self.script_path): 
                self.error_signal.emit(f"스크립트 파일 없음: {self.script_path}")
                return
            
            cmd = [sys.executable, self.script_path, "--out", self.out_dir, "--start", self.start_date, "--end", self.end_date]

            if self.codes:
                cmd.append("--codes")
                if isinstance(self.codes, list): 
                    cmd.extend(self.codes)
                else: 
                    cmd.extend([c.strip() for c in self.codes.split(',') if c.strip()])
            
            if self.columns: 
                cmd.append("--columns")
                cmd.extend(self.columns)
            
            self.log_signal.emit(f"실행 명령:\n{' '.join(cmd)}")
            
            # Run subprocess
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, encoding='utf-8', errors='replace') as p:
                for line in p.stdout: 
                    self.log_signal.emit(line.rstrip())
                p.wait()
                if p.returncode != 0: 
                    raise RuntimeError(f"프로세스 종료 코드: {p.returncode}")
            
            self.finished_signal.emit("다운로드 완료")
        except Exception as e: 
            self.error_signal.emit(str(e))

# ---------------------------------------------------------
# 3. 학습 워커 (파라미터 확장 적용됨)
# ---------------------------------------------------------
class TrainingWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    
    def __init__(self, params): 
        super().__init__()
        self.params = params
        
    def run(self):
        try: 
            # 통합 트레이너 임포트
            from train_engine_unified import run_unified_training
        except ImportError as e: 
            self.error_signal.emit(f"학습 스크립트 로딩 실패: {e}")
            return
            
        try:
            self.log_signal.emit(f"▶ 학습 프로세스 시작: {self.params}")
            
            # UI 파라미터를 실제 함수 인자로 매핑
            # (train_engine_unified.py가 해당 인자를 받을 수 있어야 함. 
            # 현재는 표준 인자만 전달하고, 나머지는 로직 내에서 처리되거나 기본값 사용)
            run_unified_training(
                mode=self.params['mode'],
                horizon=self.params['horizon'],
                valid_days=365 if self.params['mode'] == 'research' else 0, 
                n_estimators=self.params.get('n_estimators', 1000),
                version=self.params['version']
            )
            
            self.finished_signal.emit(f"엔진 생성 완료 ({self.params['version']})")
        except Exception as e: 
            import traceback
            self.error_signal.emit(f"학습 중 오류: {str(e)}\n{traceback.format_exc()}")

# ---------------------------------------------------------
# 4. 예측 워커 (특정 종목 지원 적용됨)
# ---------------------------------------------------------
class PredictionWorker(QThread):
    finished_signal = Signal(object)
    error_signal = Signal(str)
    
    def __init__(self, engine_path, target_date, top_n, specific_code=None): 
        super().__init__()
        self.eng = engine_path
        self.date = target_date
        self.n = top_n
        self.code = specific_code
        
    def run(self):
        try: 
            from predict_daily_top10 import run_prediction
        except ImportError as e: 
            self.error_signal.emit(f"예측 스크립트 로딩 실패: {e}")
            return
            
        try:
            # 엔진 경로가 없으면 최신 엔진 자동 탐색 로직이 내부에 있다고 가정
            df = run_prediction(self.eng, self.date, self.n)
            
            # 특정 종목 필터링 (결과가 DataFrame일 경우)
            if self.code and df is not None and not df.empty:
                filtered_df = pd.DataFrame()
                
                # 컬럼명 대소문자 대응
                code_col = None
                for col in df.columns:
                    if col.lower() == 'code':
                        code_col = col
                        break
                
                if code_col:
                    filtered_df = df[df[code_col].astype(str) == str(self.code)]
                
                # 필터링 결과가 있으면 덮어쓰기, 없으면 빈 데이터프레임
                df = filtered_df
            
            self.finished_signal.emit(df)
        except Exception as e: 
            import traceback
            self.error_signal.emit(str(e))