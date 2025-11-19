import os
import re
import shutil

BASE = r"F:\autostockG"
BACKUP_DIR = os.path.join(BASE, "backup_auto_patch")

os.makedirs(BACKUP_DIR, exist_ok=True)

# 패치 패턴: "하드코딩된 경로" → "config_paths 변수"
REPLACE_PATTERNS = {
    # HOJ DB
    r'"HOJ_DB_RESEARCH_V25\.parquet"': 'HOJ_DB_RESEARCH',
    r'"HOJ_DB_REAL_V25\.parquet"': 'HOJ_DB_REAL',

    # HOJ ENGINE
    r'"HOJ_ENGINE_RESEARCH_V25\.pkl"': 'HOJ_ENGINE_RESEARCH',
    r'"HOJ_ENGINE_REAL_V25\.pkl"': 'HOJ_ENGINE_REAL',

    # SLE DB
    r'"SLE_DB_REAL_V11\.parquet"': 'SLE_DB_REAL',

    # SLE ENGINE
    r'"SLE_ENGINE_REAL_V32\.pkl"': 'SLE_ENGINE_REAL',

    # 옛날 이름들(혹시 남아 있을 경우)
    r'"V11_Merged_SLE_Base\.parquet"': 'SLE_DB_REAL',
    r'"V11_Database_Final\.parquet"': 'SLE_DB_REAL',
    r'"SLE_CHAMPION_MODEL_V32\.pkl"': 'SLE_ENGINE_REAL',
}

IMPORT_LINE = "from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL"

def ensure_import(lines):
    """이미 import 되어있는지 확인하고 없으면 추가."""
    for line in lines:
        if "from config_paths import" in line:
            return lines  # 이미 있음

    return [IMPORT_LINE + "\n"] + lines


def patch_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        original = f.read()

    patched = original
    changed = False

    # 패턴 반복 적용
    for pattern, repl_var in REPLACE_PATTERNS.items():
        if re.search(pattern, patched):
            patched = re.sub(pattern, repl_var, patched)
            changed = True

    # import 추가
    if changed:
        lines = patched.splitlines(True)
        lines = ensure_import(lines)
        patched = "".join(lines)

        # 백업 저장
        backup_path = os.path.join(BACKUP_DIR, os.path.basename(file_path))
        shutil.copy2(file_path, backup_path)

        # 원본 덮어쓰기
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(patched)

        print(f"[패치됨] {file_path}")
        return True

    return False


def main():
    print("=== 자동 패치 시작 (원본 덮어쓰기 버전) ===")

    patched_count = 0

    for root, dirs, files in os.walk(BASE):
        for name in files:
            if name.lower().endswith(".py") and "apply_auto_patch" not in name:
                fpath = os.path.join(root, name)
                if patch_file(fpath):
                    patched_count += 1

    print(f"\n=== 패치 완료: 총 {patched_count}개 파일 수정됨 ===")
    print(f"백업 폴더: {BACKUP_DIR}")


if __name__ == "__main__":
    main()
