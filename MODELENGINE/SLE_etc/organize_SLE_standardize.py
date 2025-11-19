from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
import os
import shutil
from datetime import datetime

BASE = r"F:\autostockG"

# ---- 기존 SLE 파일 → 표준 파일명 매핑 ----
RENAME_MAP = {
    SLE_DB_REAL: "SLE_DB_RESEARCH_V11.parquet",
    SLE_DB_REAL: SLE_DB_REAL,
    SLE_ENGINE_REAL: SLE_ENGINE_REAL,
}

TARGET_DIRS = [
    r"SLE_DB\RESEARCH",
    r"SLE_DB\REAL",
    r"SLE_DB\OLD",
    r"SLE_ENGINE\RESEARCH",
    r"SLE_ENGINE\REAL",
    r"SLE_ENGINE\OLD",
]

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

def move_and_rename_main_files():
    print("\n[STEP 2] SLE 정식 파일 리네이밍 & 이동 중...")

    for old_name, new_name in RENAME_MAP.items():
        src = os.path.join(BASE, old_name)

        if not os.path.exists(src):
            print(f"  ⚠ 존재하지 않아 스킵: {old_name}")
            continue

        # 리네임된 이름을 보고 경로 자동 지정
        if "DB_RESEARCH" in new_name:
            dst_dir = os.path.join(BASE, r"SLE_DB\RESEARCH")
        elif "DB_REAL" in new_name:
            dst_dir = os.path.join(BASE, r"SLE_DB\REAL")
        elif "ENGINE_REAL" in new_name:
            dst_dir = os.path.join(BASE, r"SLE_ENGINE\REAL")
        else:
            dst_dir = os.path.join(BASE, r"SLE_ENGINE\OLD")

        dst = os.path.join(dst_dir, new_name)

        # 백업 생성
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = src + f".backup_{ts}"
        shutil.copy2(src, backup)
        print(f"  → 백업 생성: {backup}")

        # 이동 + 이름 변경
        shutil.move(src, dst)
        print(f"  → 이동 완료: {old_name} → {dst}")

def move_temp_files():
    print("\n[STEP 3] SLE 관련 임시파일 OLD로 이동 중...")

    old_db = os.path.join(BASE, r"SLE_DB\OLD")
    old_engine = os.path.join(BASE, r"SLE_ENGINE\OLD")

    for f in os.listdir(BASE):
        lower = f.lower()

        if f in RENAME_MAP.keys() or f in RENAME_MAP.values():
            continue

        if any(k.lower() in lower for k in TEMP_KEYWORDS):
            src_path = os.path.join(BASE, f)

            if f.endswith(".pkl") or "engine" in lower:
                dst = os.path.join(old_engine, f)
            else:
                dst = os.path.join(old_db, f)

            if os.path.isfile(src_path):
                shutil.move(src_path, dst)
                print(f"  → 임시 SLE 파일 이동: {f}")

def main():
    print("\n==== SLE 표준화 정리 시작 ====\n")

    ensure_dirs()
    move_and_rename_main_files()
    move_temp_files()

    print("\n==== SLE 표준화 완료! ====\n")

if __name__ == "__main__":
    main()
