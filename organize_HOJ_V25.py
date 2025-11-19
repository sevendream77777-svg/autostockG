from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
import os
import shutil
from datetime import datetime

BASE = r"F:\autostockG"

# 정식 파일 정의
FILES = {
    "HOJ_DB_RESEARCH": HOJ_DB_RESEARCH,
    "HOJ_DB_REAL": HOJ_DB_REAL,
    "HOJ_ENGINE_RESEARCH": HOJ_ENGINE_RESEARCH,
    "HOJ_ENGINE_REAL": HOJ_ENGINE_REAL,
}

# 대상 폴더 구조
TARGET_DIRS = [
    r"HOJ_DB\RESEARCH",
    r"HOJ_DB\REAL",
    r"HOJ_DB\OLD",
    r"HOJ_ENGINE\RESEARCH",
    r"HOJ_ENGINE\REAL",
    r"HOJ_ENGINE\OLD",
]

# 임시/중간 파일 폴더
SOURCE_DIRS = {
    "db_temp": r"hoj_db",
    "engine_temp": r"hoj_modelengine",
}

# ------------------------------------------------
def ensure_dirs():
    print("[STEP 1] 폴더 생성 중...")
    for d in TARGET_DIRS:
        path = os.path.join(BASE, d)
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"  → 생성: {path}")
        else:
            print(f"  → 이미 존재: {path}")

# ------------------------------------------------
def move_main_files():
    print("\n[STEP 2] 정식 파일 이동 중...")

    for key, filename in FILES.items():
        src_path = os.path.join(BASE, filename)

        if not os.path.exists(src_path):
            print(f"  ⚠ 스킵: {filename} 없음")
            continue

        if key == "HOJ_DB_RESEARCH":
            dst = os.path.join(BASE, r"HOJ_DB\RESEARCH", filename)
        elif key == "HOJ_DB_REAL":
            dst = os.path.join(BASE, r"HOJ_DB\REAL", filename)
        elif key == "HOJ_ENGINE_RESEARCH":
            dst = os.path.join(BASE, r"HOJ_ENGINE\RESEARCH", filename)
        else:
            dst = os.path.join(BASE, r"HOJ_ENGINE\REAL", filename)

        # 백업 생성
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = src_path + f".backup_{ts}"
        shutil.copy2(src_path, backup)
        print(f"  → 백업 생성: {backup}")

        # 이동
        shutil.move(src_path, dst)
        print(f"  → 이동 완료: {filename} → {dst}")

# ------------------------------------------------
def move_temp_files():
    print("\n[STEP 3] 임시/중간 파일 OLD 폴더로 이동...")

    # 1) hoj_db → HOJ_DB\OLD\
    src_db = os.path.join(BASE, SOURCE_DIRS["db_temp"])
    dst_db = os.path.join(BASE, r"HOJ_DB\OLD")

    if os.path.exists(src_db):
        for f in os.listdir(src_db):
            src = os.path.join(src_db, f)
            dst = os.path.join(dst_db, f)
            if os.path.isfile(src):
                shutil.move(src, dst)
                print(f"  → DB 임시파일 이동: {f}")

    # 2) hoj_modelengine → HOJ_ENGINE\OLD\
    src_eng = os.path.join(BASE, SOURCE_DIRS["engine_temp"])
    dst_eng = os.path.join(BASE, r"HOJ_ENGINE\OLD")

    if os.path.exists(src_eng):
        for f in os.listdir(src_eng):
            src = os.path.join(src_eng, f)
            dst = os.path.join(dst_eng, f)
            if os.path.isfile(src):
                shutil.move(src, dst)
                print(f"  → 엔진 임시파일 이동: {f}")

# ------------------------------------------------
def main():
    print("\n==== HOJ V25 자동 구조 정리기 작동 시작 ====\n")

    ensure_dirs()
    move_main_files()
    move_temp_files()

    print("\n==== 완료! HOJ V25 구조가 정상적으로 정비되었습니다. ====\n")


if __name__ == "__main__":
    main()
