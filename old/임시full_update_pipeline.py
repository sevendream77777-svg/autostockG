# full_update_pipeline.py (absolute path version)

import subprocess, sys, os
from version_utils import backup_file
from config_paths import *
from pipeline_utils import ensure_exists

# RAW / FEATURE 절대 경로
RAW_SCRIPT = r"F:\autostockG\MODELENGINE\RAW\update_raw_data.py"
FEATURE_SCRIPT = r"F:\autostockG\MODELENGINE\FEATURE\build_features.py"

# UTIL 내부 스크립트 (현재 폴더)
UTIL_DIR = os.path.dirname(os.path.abspath(__file__))
HOJ_RESEARCH = os.path.join(UTIL_DIR, "build_HOJ_DB_RESEARCH.py")
HOJ_REAL = os.path.join(UTIL_DIR, "build_HOJ_DB_REAL.py")
TRAIN_HOJ_R = os.path.join(UTIL_DIR, "train_HOJ_ENGINE_RESEARCH.py")
TRAIN_HOJ_REAL = os.path.join(UTIL_DIR, "train_HOJ_ENGINE_REAL.py")
TRAIN_SLE_REAL = os.path.join(UTIL_DIR, "train_SLE_ENGINE_REAL.py")

steps = [
    RAW_SCRIPT,
    FEATURE_SCRIPT,
    HOJ_RESEARCH,
    HOJ_REAL,
    TRAIN_HOJ_R,
    TRAIN_HOJ_REAL,
    TRAIN_SLE_REAL
]

for s in steps:
    print(f"\n[RUN] {s}")
    subprocess.run([sys.executable, s], check=True)

print("\n✅ full_update_pipeline 실행 완료!")
