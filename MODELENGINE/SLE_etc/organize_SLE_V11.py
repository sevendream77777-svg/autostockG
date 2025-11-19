from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
import os
import shutil
from datetime import datetime

BASE = r"F:\autostockG"

# SLE 파일 매핑
FILES = {
    "SLE_DB_RESEARCH": SLE_DB_REAL,
    "SLE_DB_REAL": SLE_DB_REAL,
    "SLE_ENGINE_REAL": SLE_ENGINE_REAL
}

TARGET_DIRS = [
    r"SLE_DB\RESEARCH",
    r"SLE_DB\REAL",
    r"SLE_DB\OLD",
    r"SLE_ENGINE\RESEARCH",
    r"SLE_ENGINE\REAL",
    r"SLE_ENGINE\OLD",
]

# SLE 관련 임시 파일을 모아둘 폴더 (루트 기반 자동 탐색)
TEMP_KEYWORDS = ["sle", "SLE", "V11", "v11"]

def ensure_dirs():
    print("[STEP 1] SLE 폴더 생성 중...")
    for d in TARGET_DIRS:
        path = os.path.join(BASE, d)
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"  → 생성: {path}")
        else:
            print(f"  → 이미 존재: {path}")

def move_main_files():
    print("\n[STEP 2] SLE 정식 파일 이동 중...")

    for key, filename in FILES.items():
        src = os.path.join(BASE, filename)

        if not os.path.exists(src):
            print(f"  ⚠ 스킵 (없음): {filename}")
            continue

        # 대상 폴더
        if key == "SLE_DB_RESEARCH":
            dst = os.path.join(BASE, r"SLE_DB\RESEARCH", filename)
        elif key == "SLE_DB_REAL":
            dst = os.path.join(BASE, r"SLE_DB\REAL", filename)
        else:
            dst = os.path.join(BASE, r"SLE_ENGINE\REAL", filename)

        # 백업 생성
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = src + f".backup_{ts}"
        shutil.copy2(src, backup)
        print(f"  → 백업 생성: {backup}")

        # 이동
        shutil.move(src, dst)
        print(f"  → 이동 완료: {filename} → {dst}")

def move_temp_files():
    print("\n[STEP 3] 임시 SLE 관련 파일 OLD로 이동")

    old_db = os.path.join(BASE, r"SLE_DB\OLD")
    old_engine = os.path.join(BASE, r"SLE_ENGINE\OLD")

    # 루트 파일 순회
    for f in os.listdir(BASE):
        lower = f.lower()

        # db/engine 본체 제외
        if f in FILES.values():
            continue

        # SLE 관련 키워드 판단
        if any(k.lower() in lower for k in TEMP_KEYWORDS):

            src_path = os.path.join(BASE, f)

            # 파이썬 파일은 OLD/ENGINE으로
            if f.endswith(".pkl") or "engine" in lower:
                dst = os.path.join(old_engine, f)
            else:
                dst = os.path.join(old_db, f)

            if os.path.isfile(src_path):
                shutil.move(src_path, dst)
                print(f"  → 임시파일 이동: {f} → {dst}")

def main():
    print("\n==== SLE DB/ENGINE 자동 정리 시작 ====\n")

    ensure_dirs()
    move_main_files()
    move_temp_files()

    print("\n==== SLE 구조 정리 완료 ====\n")

if __name__ == "__main__":
    main()
