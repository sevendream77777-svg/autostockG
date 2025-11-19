# ===========================================================
# full_update_pipeline.py (V32 — 데이터 최신 날짜 기반 백업)
# RAW → KOSPI → FEATURE → HOJ_DB(RESEARCH/REAL) → HOJ_ENGINE
# + SLE_DB_REAL / SLE_ENGINE_REAL 백업 (데이터 기준 날짜)
# ===========================================================

import os
import sys
import traceback
import subprocess
import shutil
import pandas as pd
from datetime import datetime, time

from config_paths import get_path
from version_utils import get_timestamp

import build_features
import build_HOJ_DB_RESEARCH
import build_HOJ_DB_REAL

try:
    import update_raw_data
except ImportError:
    update_raw_data = None

try:
    import build_kospi
except ImportError:
    build_kospi = None


# -----------------------------------------------------------
# 로그
# -----------------------------------------------------------

def log(msg: str, file=None):
    print(msg)
    if file:
        with open(file, "a", encoding="utf-8") as f:
            f.write(msg + "\n")


def ensure_log_dir(path):
    os.makedirs(path, exist_ok=True)


# -----------------------------------------------------------
# 실제 데이터 최신 날짜 추출
# -----------------------------------------------------------

def get_latest_date_from_file(path):
    try:
        df = pd.read_parquet(path)
    except:
        return None

    for col in ["Date", "날짜", "date"]:
        if col in df.columns:
            try:
                d = pd.to_datetime(df[col]).max()
                return d.strftime("%y%m%d")
            except:
                continue
    return None


# -----------------------------------------------------------
# 날짜 기반 백업 생성
# -----------------------------------------------------------

def save_with_version(dated_path: str):
    if not os.path.exists(dated_path):
        return dated_path

    root, ext = os.path.splitext(dated_path)
    idx = 1
    while True:
        cand = f"{root}_{idx}{ext}"
        if not os.path.exists(cand):
            return cand
        idx += 1


def backup_actual_date(src_path: str, logger):
    if not os.path.exists(src_path):
        logger(f"[BACKUP] 없음 → {src_path}")
        return

    latest_date = get_latest_date_from_file(src_path)

    if latest_date is None:
        latest_date = datetime.now().strftime("%y%m%d")

    base, ext = os.path.splitext(src_path)
    dated = f"{base}_{latest_date}{ext}"
    target = save_with_version(dated)

    try:
        shutil.copy2(src_path, target)
        logger(f"[BACKUP] {src_path} → {target}")
    except:
        logger(f"[ERROR] 백업 실패: {src_path}")
        logger(traceback.format_exc())


# -----------------------------------------------------------
# RAW 시간 체크
# -----------------------------------------------------------

def allow_raw_update():
    now = datetime.now()
    if now.weekday() >= 5:
        return True
    if now.time() >= time(16, 10):
        return True
    return False


# -----------------------------------------------------------
# 외부 스크립트 실행
# -----------------------------------------------------------

def run_subprocess(script_name: str, logger):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, script_name)

    if not os.path.exists(script_path):
        logger(f"[ERROR] 없음: {script_path}")
        raise FileNotFoundError(script_path)

    logger(f"[RUN] {script_path}")
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True, text=True,
        encoding="utf-8", errors="ignore"
    )

    if result.stdout:
        for line in result.stdout.splitlines():
            logger("[STDOUT] " + line)

    if result.stderr:
        for line in result.stderr.splitlines():
            logger("[STDERR] " + line)

    if result.returncode != 0:
        logger(f"[ERROR] 실패: {script_name}")
        raise RuntimeError(f"{script_name} failed")


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------

def main():
    LOG_DIR = get_path("LOG")
    ensure_log_dir(LOG_DIR)

    ts = get_timestamp()
    log_file = os.path.join(LOG_DIR, f"full_update_{ts}.log")
    open(log_file, "w").close()
    lg = lambda m: log(m, log_file)

    lg("==========================================")
    lg(f"[FULL UPDATE START] {ts}")
    lg("==========================================\n")

    # ---------------------------------------------------
    # (0) RAW
    # ---------------------------------------------------
    if update_raw_data:
        lg("[STEP 0] RAW 업데이트 검사")
        if allow_raw_update():
            try:
                lg("[STEP 0] RAW 업데이트 시작")
                update_raw_data.update_raw_data()
                lg("[STEP 0] RAW 업데이트 완료")
            except:
                lg("[ERROR] RAW 업데이트 실패")
                lg(traceback.format_exc())
        else:
            lg("[STEP 0] RAW 금지 구간 → 건너뜀")
    else:
        lg("[STEP 0] RAW 모듈 없음 → 건너뜀")

    raw_file = get_path("RAW", "all_stocks_cumulative.parquet")
    if os.path.exists(raw_file):
        backup_actual_date(raw_file, lg)

    # ---------------------------------------------------
    # (0-2) KOSPI
    # ---------------------------------------------------
    if build_kospi:
        try:
            lg("[STEP 0-2] KOSPI 생성 시작")
            build_kospi.build_kospi()
            lg("[STEP 0-2] KOSPI 생성 완료")
        except:
            lg("[ERROR] KOSPI 생성 실패")
            lg(traceback.format_exc())

    kospi_file = get_path("RAW", "kospi_index_10y.parquet")
    if os.path.exists(kospi_file):
        backup_actual_date(kospi_file, lg)

    # ---------------------------------------------------
    # (1) FEATURE
    # ---------------------------------------------------
    try:
        lg("[STEP 1] FEATURE 생성")
        build_features.build_features()
        lg("[STEP 1] FEATURE 완료")
    except:
        lg("[FATAL] FEATURE 실패")
        lg(traceback.format_exc())
        return

    features_file = get_path("FEATURE", "features_V31.parquet")
    if os.path.exists(features_file):
        backup_actual_date(features_file, lg)

    # ---------------------------------------------------
    # (2) HOJ_DB_RESEARCH
    # ---------------------------------------------------
    try:
        lg("[STEP 2] HOJ_DB_RESEARCH 생성")
        build_HOJ_DB_RESEARCH.build_hoj_research_db()
        lg("[STEP 2] 완료")
    except:
        lg("[ERROR] RESEARCH DB 실패")
        lg(traceback.format_exc())

    db_res = get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
    if os.path.exists(db_res):
        backup_actual_date(db_res, lg)

    # ---------------------------------------------------
    # (3) HOJ_DB_REAL
    # ---------------------------------------------------
    try:
        lg("[STEP 3] HOJ_DB_REAL 생성")
        build_HOJ_DB_REAL.build_hoj_real_db()
        lg("[STEP 3] 완료")
    except:
        lg("[ERROR] REAL DB 실패")
        lg(traceback.format_exc())

    db_real = get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")
    if os.path.exists(db_real):
        backup_actual_date(db_real, lg)

    # ---------------------------------------------------
    # (4) HOJ 엔진 (RESEARCH)
    # ---------------------------------------------------
    try:
        lg("[STEP 4] 연구 엔진 학습")
        run_subprocess("train_HOJ_ENGINE_RESEARCH.py", lg)
        lg("[STEP 4] 완료")
    except:
        lg("[ERROR] 연구 엔진 실패")
        lg(traceback.format_exc())

    eng_res = get_path("HOJ_ENGINE", "RESEARCH", "HOJ_ENGINE_RESEARCH_V31.pkl")
    if os.path.exists(eng_res):
        backup_actual_date(eng_res, lg)

    # ---------------------------------------------------
    # (5) HOJ 엔진 (REAL)
    # ---------------------------------------------------
    try:
        lg("[STEP 5] 실전 엔진 학습")
        run_subprocess("train_HOJ_ENGINE_REAL.py", lg)
        lg("[STEP 5] 완료")
    except:
        lg("[ERROR] REAL 엔진 실패")
        lg(traceback.format_exc())

    eng_real = get_path("HOJ_ENGINE", "REAL", "HOJ_ENGINE_REAL_V31.pkl")
    if os.path.exists(eng_real):
        backup_actual_date(eng_real, lg)

    # ---------------------------------------------------
    # (6) SLE_DB_REAL
    # ---------------------------------------------------
    sle_db = get_path("SLE_DB", "REAL", "SLE_DB_REAL_V21.parquet")
    if os.path.exists(sle_db):
        backup_actual_date(sle_db, lg)

    # ---------------------------------------------------
    # (7) SLE_ENGINE_REAL
    # ---------------------------------------------------
    sle_eng = get_path("SLE_ENGINE", "REAL", "SLE_ENGINE_REAL_V21.pkl")
    if os.path.exists(sle_eng):
        backup_actual_date(sle_eng, lg)

    # ---------------------------------------------------
    # END
    # ---------------------------------------------------
    lg("\n==========================================")
    lg("[FULL UPDATE END]")
    lg("==========================================")
    lg(f"📄 로그: {log_file}")


if __name__ == "__main__":
    main()
