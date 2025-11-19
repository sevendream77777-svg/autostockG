# ============================================
# create_modelengine_structure.py
# MODELENGINE 폴더 전체 구조 자동 생성
# ============================================

import os

BASE_DIR = r"F:\autostockG\MODELENGINE"

STRUCTURE = [
    "RAW",
    "FEATURE",
    "HOJ_DB/RESEARCH",
    "HOJ_DB/REAL",
    "HOJ_ENGINE/RESEARCH",
    "HOJ_ENGINE/REAL",
    "SLE_DB/RESEARCH",
    "SLE_DB/REAL",
    "SLE_ENGINE/RESEARCH",
    "SLE_ENGINE/REAL",
    "REPORT",
    "LOG"
]

def create_structure():
    print("📂 MODELENGINE 폴더 자동 생성 시작...\n")

    for path in STRUCTURE:
        full_path = os.path.join(BASE_DIR, path)
        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)
            print(f"  ✔ 생성됨: {full_path}")
        else:
            print(f"  - 이미 있음: {full_path}")

    print("\n🎉 MODELENGINE 전체 구조 생성 완료!")
    print(f"📍 루트 경로: {BASE_DIR}")

if __name__ == "__main__":
    create_structure()
