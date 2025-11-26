# ui/common/workers.py
import os
import sys
import time
import subprocess
import datetime
import pickle
import glob
import re
import pandas as pd
from PySide6.QtCore import QThread, Signal

# ---------------------------------------------------------
# 1. 데이터 업데이트 워커
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
            'stock': "1. 원천 업데이트 (RAW)", 'kospi': "2. KOSPI 지수 갱신",
            'feature': "3. 피처 생성", 'db': "4. DB 생성"
        }
        total = len(self.task_list)
        try:
            for i, task in enumerate(self.task_list):
                script_path = script_map.get(task)
                display_name = task_names.get(task, task)

                if not script_path or not os.path.exists(script_path):
                    self.error_signal.emit(f"⚠ 스크립트 없음: {script_path}")
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
                    self.error_signal.emit(f"❌ [{display_name}] 실패 (code: {process.poll()})")
                time.sleep(0.5)

            self.progress_signal.emit(100)
            self.finished_signal.emit("요청한 모든 작업이 완료되었습니다.")
        except Exception as e:
            self.error_signal.emit(f"시스템 오류: {str(e)}")

# ---------------------------------------------------------
# 2. 학습 워커 (기존 로직 유지)
# ---------------------------------------------------------
class TrainingWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(dict)
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
            self.error_signal.emit(f"⚠ 학습 스크립트 없음: {script_path}")
            return

        log_dir = os.path.join(self.base_path, "HOJ_ENGINE", self.params['mode'].upper(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"Log_{self.params['mode'].upper()}_{now_str}.txt"
        log_path = os.path.join(log_dir, log_filename)

        start_msg = f"🚀 학습 프로세스 시작 (모드: {self.params['mode'].upper()})..."
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
                    if len(last_lines) > 20: last_lines.pop(0)

            if process.poll() == 0:
                result_package = {
                    "status": "success",
                    "message": f"학습 완료 ({self.params['mode']})",
                    "mode": self.params['mode'],
                    "log_path": log_path,
                    "last_lines": last_lines,
                    "params": self.params
                }
                self.finished_signal.emit(result_package)
                self.save_log(log_path, "[SUCCESS] Process Finished")
            else:
                self.error_signal.emit(f"학습 실패 (Exit Code: {process.poll()})")
                self.save_log(log_path, f"[ERROR] Exit Code {process.poll()}")

        except Exception as e:
            self.error_signal.emit(f"예외 발생: {str(e)}")
            self.save_log(log_path, f"[EXCEPTION] {str(e)}")

    def save_log(self, path, content):
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(content + "\n")
        except:
            pass

# ---------------------------------------------------------
# 3. 수동 다운로드 워커
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
                self.error_signal.emit(f"스크립트 없음: {self.script}")
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

# ---------------------------------------------------------
# 4. 예측 워커
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
            if not self.eng or not os.path.exists(self.eng):
                raise FileNotFoundError(f"엔진 파일을 찾을 수 없습니다: {self.eng}")

            with open(self.eng, "rb") as f:
                data = pickle.load(f)

            if not isinstance(data, dict):
                raise ValueError("엔진 포맷이 올바르지 않습니다.")

            meta = data.get("meta", {})
            required_features = data.get("features", [])
            model_reg = data.get("model_reg")
            model_cls = data.get("model_cls")

            version = meta.get("version", "V31")
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_DB"))

            # 엔진 파일명에서 YYMMDD 태그 추출 (예: ..._251126.pkl -> 251126)
            tag = None
            try:
                fname = os.path.basename(self.eng)
                tags = re.findall(r"(\d{6})", fname)
                if tags:
                    tag = tags[-1]
            except Exception:
                tag = None

            candidates = []
            if tag:
                candidates.append(os.path.join(base_dir, f"HOJ_DB_{version}_{tag}.parquet"))
            # 같은 버전의 최신 스냅샷 우선
            candidates.extend(sorted(glob.glob(os.path.join(base_dir, f"HOJ_DB_{version}_*.parquet")), reverse=True))
            # 기본 이름
            candidates.append(os.path.join(base_dir, f"HOJ_DB_{version}.parquet"))
            candidates.append(os.path.join(base_dir, "HOJ_DB.parquet"))

            db_path = None
            for c in candidates:
                if os.path.exists(c):
                    db_path = c
                    break
            if db_path is None:
                raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다 (version={version}, tag={tag})")

            df = pd.read_parquet(db_path)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            tgt_date = pd.to_datetime(self.date)
            daily_df = df[df["Date"] == tgt_date].copy()
            # 기준일 데이터가 없으면 최신 날짜로 대체
            if daily_df.empty:
                max_date = df["Date"].max()
                daily_df = df[df["Date"] == max_date].copy()
                tgt_date = max_date
                if daily_df.empty:
                    self.finished_signal.emit(None)
                    return

            if self.code:
                daily_df = daily_df[daily_df["Code"] == self.code]
                if daily_df.empty:
                    self.finished_signal.emit(None)
                    return

            missing = [c for c in required_features if c not in daily_df.columns]
            if missing:
                raise KeyError(f"필수 피처가 DB에 없습니다: {missing[:5]}...")

            X = daily_df[required_features]
            mask = X.notnull().all(axis=1)
            daily_df = daily_df[mask]
            X = X[mask]
            if daily_df.empty:
                self.finished_signal.emit(None)
                return

            pred_score = model_reg.predict(X) if model_reg is not None else pd.Series([0] * len(X))
            daily_df["score"] = pred_score
            if model_cls is not None:
                daily_df["prob"] = model_cls.predict_proba(X)[:, 1]
            else:
                daily_df["prob"] = 0.0

            daily_df = daily_df.sort_values("score", ascending=False)
            if not self.code:
                daily_df = daily_df.head(self.n)

            out = daily_df.rename(columns={"Code": "code", "Name": "name", "Close": "close"})
            out = out[["code", "name", "close", "score", "prob"]]
            out.reset_index(drop=True, inplace=True)
            self.finished_signal.emit(out)
        except Exception as e: 
            self.error_signal.emit(f"예측 실패: {str(e)}")
