from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
# rename_files_v25.py
# ---------------------------------------------
# 기존 파일을 V25 표준 파일명으로 자동 변경하는 스크립트
# ---------------------------------------------

import os
import shutil

BASE = r"F:\autostockG"

mapping = {
    "new_Hoj_DB_V25_FULL.parquet": HOJ_DB_RESEARCH,
    "new_Hoj_DB_V25_FULL_CLEAN.parquet": HOJ_DB_REAL,
    "new_Hoj_MODELENGINE_V25.pkl": HOJ_ENGINE_RESEARCH,
    "REAL_Hoj_MODELENGINE_V25.pkl": HOJ_ENGINE_REAL,
}

print("\n=== V25 파일명 표준화 실행 ===\n")

for old_name, new_name in mapping.items():
    old_path = os.path.join(BASE, old_name)
    new_path = os.path.join(BASE, new_name)

    if os.path.exists(old_path):
        # 이미 새 파일명이 있으면 건너뜀
        if os.path.exists(new_path):
            print(f"[SKIP] 이미 존재: {new_name}")
        else:
            shutil.move(old_path, new_path)
            print(f"[RENAMED] {old_name} → {new_name}")
    else:
        print(f"[NOT FOUND] {old_name}")

print("\n=== 완료! ===")
