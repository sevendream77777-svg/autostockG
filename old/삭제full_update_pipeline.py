# full_update_pipeline.py (CLEAN FINAL)

import sys
import subprocess

# MODELENGINE 루트 패스 등록
sys.path.append(r"F:\autostockG\MODELENGINE")

# 실행할 스크립트 절대경로
scripts = [
    r"F:\autostockG\MODELENGINE\RAW\update_raw_data.py",
    r"F:\autostockG\MODELENGINE\FEATURE\build_features.py",
    r"F:\autostockG\MODELENGINE\util\build_HOJ_DB_RESEARCH.py",
    r"F:\autostockG\MODELENGINE\util\build_HOJ_DB_REAL.py"
]

for s in scripts:
    print(f"[RUN] {s}")
    subprocess.run([sys.executable, s], check=True)

print("\n=== PIPELINE COMPLETE ===")
