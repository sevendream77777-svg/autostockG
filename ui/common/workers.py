# ui/common/workers.py
import os
import sys
import time
import subprocess
import datetime
from PySide6.QtCore import QThread, Signal

# ---------------------------------------------------------
# 1. 데이터 파이프라인 워커 (기존 유지)
# ---------------------------------------------------------
class DataUpdateWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, task_list, base_path=None): 
        super().__init__()
        self.task_list = task_list
        if base_path:
            self.base_path = base_path
        else:
            self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../MODELENGINE"))

    def run(self):
        script_map = {
            'stock':   os.path.join(self.base_path, "RAW", "raw_patch.py"),
            'kospi':   os.path.join(self.base_path, "RAW", "make_kospi_index_10y.py"),
            'feature': os.path.join(self.base_path, "UTIL", "build_features.py"),
            'db':      os.path.join(self.base_path, "UTIL", "build_unified_db.py")
        }
        task_names = {
            'stock': "1. 시세 수집 (RAW)", 'kospi': "2. KOSPI 지수 갱신",
            'feature': "3. 피처 생성", 'db': "4. DB 통합"
        }
        total = len(self.task_list)
        try:
            for i, task in enumerate(self.task_list):
                script_path = script_map.get(task)
                display_name = task_names.get(task, task)

                if not script_path or not os.path.exists(script_path):
                    self.error_signal.emit(f"❌ 파일을 찾을 수 없음: {script_path}")
                    continue

                self.log_signal.emit(f"\n>>> [{display_name}] 실행 중... ({script_path})")
                self.progress_signal.emit(int((i / total) * 100))

                cmd = [sys.executable, script_path]
                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, encoding='utf-8', errors='replace',
                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
                )
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None: break
                    if line: self.log_signal.emit(line.strip())

                if process.poll() == 0:
                    self.log_signal.emit(f"✅ [{display_name}] 완료")
                else:
                    self.error_signal.emit(f"⚠️ [{display_name}] 중단됨 (코드: {process.poll()})")
                time.sleep(0.5)

            self.progress_signal.emit(100)
            self.finished_signal.emit("요청하신 모든 작업이 완료되었습니다.")
        except Exception as e:
            self.error_signal.emit(f"시스템 오류: {str(e)}")

# ---------------------------------------------------------
# 2. 학습 워커 (기능 강화: 결과 딕셔너리 전달)
# ---------------------------------------------------------
class TrainingWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(dict) # 변경: 단순 문자열 대신 결과 dict 전달
    error_signal = Signal(str)
    
    def __init__(self, params, base_path=None): 
        super().__init__()
        self.params = params
        if base_path:
            self.base_path = base_path
        else:
            self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../MODELENGINE"))
        
    def run(self):
        script_path = os.path.join(self.base_path, "UTIL", "train_engine_unified.py")
        
        if not os.path.exists(script_path):
            self.error_signal.emit(f"❌ 학습 스크립트 없음: {script_path}")
            return

        # 로그 저장 경로 설정
        log_dir = os.path.join(self.base_path, "HOJ_ENGINE", self.params['mode'].upper(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"Log_{self.params['mode'].upper()}_{now_str}.txt"
        log_path = os.path.join(log_dir, log_filename)

        start_msg = f"🔥 학습 프로세스 시작 (모드: {self.params['mode'].upper()})..."
        self.log_signal.emit(start_msg)
        self.save_log(log_path, start_msg)

        try:
            cmd = [
                sys.executable, script_path,
                "--mode", str(self.params['mode']),
                "--horizon", str(self.params['horizon']),
                "--input_window", str(self.params['input_window']),
                "--valid_days", str(self.params['valid_days']),
                "--n_estimators", str(self.params['n_estimators']),
                "--version", "V31"
            ]
            
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', errors='replace',
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )

            last_lines = []
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None: break
                if line:
                    msg = line.strip()
                    self.log_signal.emit(msg)
                    self.save_log(log_path, msg)
                    last_lines.append(msg)
                    if len(last_lines) > 20: last_lines.pop(0) # 마지막 20줄 보관

            if process.poll() == 0:
                # 완료 시 UI로 넘길 정보 패키징
                result_package = {
                    "status": "success",
                    "message": f"엔진 학습 완료 ({self.params['mode']})",
                    "mode": self.params['mode'],
                    "log_path": log_path,
                    "last_lines": last_lines,
                    "params": self.params # 다음 단계 상속용
                }
                self.finished_signal.emit(result_package)
                self.save_log(log_path, "[SUCCESS] Process Finished")
            else:
                self.error_signal.emit(f"학습 중단됨 (Exit Code: {process.poll()})")
                self.save_log(log_path, f"[ERROR] Exit Code {process.poll()}")

        except Exception as e:
            self.error_signal.emit(f"실행 오류: {str(e)}")
            self.save_log(log_path, f"[EXCEPTION] {str(e)}")

    def save_log(self, path, content):
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(content + "\n")
        except: pass

# ---------------------------------------------------------
# [기존 유지] 수동 다운로드 & 예측 워커
# ---------------------------------------------------------
class ManualDownloadWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)
    def __init__(self, codes, start_date, end_date, out_dir, script_path, columns=None):
        super().__init__()
        self.codes, self.s, self.e, self.out, self.script, self.cols = codes, start_date, end_date, out_dir, script_path, columns
    def run(self):
        try:
            if not os.path.exists(self.script): 
                self.error_signal.emit(f"파일 없음: {self.script}")
                return
            cmd = [sys.executable, self.script, "--out", self.out, "--start", self.s, "--end", self.e]
            if self.codes:
                cmd.append("--codes")
                if isinstance(self.codes, list): cmd.extend(self.codes)
                else: cmd.extend([c.strip() for c in self.codes.split(',') if c.strip()])
            if self.cols: 
                cmd.append("--columns")
                cmd.extend(self.cols)
            self.log_signal.emit(f"실행: {' '.join(cmd)}")
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace') as p:
                for line in p.stdout: self.log_signal.emit(line.rstrip())
                p.wait()
            self.finished_signal.emit("다운로드 완료")
        except Exception as e: self.error_signal.emit(str(e))

class PredictionWorker(QThread):
    finished_signal = Signal(object)
    error_signal = Signal(str)
    def __init__(self, engine_path, target_date, top_n, specific_code=None): 
        super().__init__()
        self.eng = engine_path; self.date = target_date; self.n = top_n; self.code = specific_code
    def run(self):
        try: 
            sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "MODELENGINE", "UTIL"))
            from predict_daily_top10 import run_prediction
            df = run_prediction(self.eng, self.date, self.n)
            if self.code and df is not None and not df.empty:
                code_col = next((c for c in df.columns if c.lower() == 'code'), None)
                if code_col: df = df[df[code_col].astype(str) == str(self.code)]
            self.finished_signal.emit(df)
        except Exception as e: self.error_signal.emit(f"예측 실패: {str(e)}")