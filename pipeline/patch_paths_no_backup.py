import os
import re

BASE = r"F:\autostockG"

# 이 디렉토리는 아예 스킵 (백업 폴더들)
SKIP_DIRS = {"backup_auto_patch", "backup_auto_patch_detect", "__pycache__"}

# 교체 규칙: "문자열 리터럴" → config_paths 변수명
REPLACE_PATTERNS = {
    r'"HOJ_DB_RESEARCH_V25\.parquet"': 'HOJ_DB_RESEARCH',
    r'"HOJ_DB_REAL_V25\.parquet"': 'HOJ_DB_REAL',

    r'"HOJ_ENGINE_RESEARCH_V25\.pkl"': 'HOJ_ENGINE_RESEARCH',
    r'"HOJ_ENGINE_REAL_V25\.pkl"': 'HOJ_ENGINE_REAL',

    r'"SLE_DB_REAL_V11\.parquet"': 'SLE_DB_REAL',
    r'"SLE_ENGINE_REAL_V32\.pkl"': 'SLE_ENGINE_REAL',

    # 혹시 옛날 이름이 남아있다면 → 새 규칙으로 맞춤
    r'"V11_Merged_SLE_Base\.parquet"': 'SLE_DB_REAL',
    r'"V11_Database_Final\.parquet"': 'SLE_DB_REAL',
    r'"SLE_CHAMPION_MODEL_V32\.pkl"': 'SLE_ENGINE_REAL',
}

IMPORT_LINE = "from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL"


def ensure_import(text: str) -> str:
    """이미 config_paths import가 있으면 그대로 두고, 없으면 맨 위에 한 줄 추가."""
    if "from config_paths import" in text:
        return text

    # 맨 위에 추가 (shebang, encoding 같은 거 없으니 단순히 제일 위에 붙여도 무방)
    return IMPORT_LINE + "\n" + text


def patch_file(file_path: str) -> bool:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            original = f.read()
    except UnicodeDecodeError:
        # cp949로 다시 시도
        try:
            with open(file_path, "r", encoding="cp949") as f:
                original = f.read()
        except Exception as e:
            print(f"[SKIP] {file_path} (읽기 실패: {e})")
            return False

    patched = original
    changed = False

    for pattern, var_name in REPLACE_PATTERNS.items():
        if re.search(pattern, patched):
            patched = re.sub(pattern, var_name, patched)
            changed = True

    if not changed:
        return False

    # import 추가
    patched = ensure_import(patched)

    # 백업 없이 바로 덮어쓰기
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(patched)
    except Exception as e:
        print(f"[ERROR] {file_path} 저장 실패: {e}")
        return False

    print(f"[패치됨] {file_path}")
    return True


def main():
    print("=== 경로 하드코딩 제거 패치 시작 (백업 없음, 직접 덮어쓰기) ===")

    patched_count = 0

    for root, dirs, files in os.walk(BASE):
        # 백업 폴더들 스킵
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        for name in files:
            if not name.lower().endswith(".py"):
                continue

            # 자기 자신, 스캐너, 디텍터는 스킵
            if name in {"patch_paths_no_backup.py", "scan_path_usage.py", "detect_locked_file.py"}:
                continue

            fpath = os.path.join(root, name)
            if patch_file(fpath):
                patched_count += 1

    print(f"\n=== 패치 완료: 총 {patched_count}개 파일 수정됨 ===")


if __name__ == "__main__":
    main()
