import os
import shutil

BASE = r"F:\autostockG"
BACKUP_DIR = os.path.join(BASE, "backup_auto_patch_detect")

os.makedirs(BACKUP_DIR, exist_ok=True)

def try_backup(file_path):
    dst = os.path.join(BACKUP_DIR, os.path.basename(file_path))
    try:
        shutil.copy2(file_path, dst)
        return True
    except Exception as e:
        print("\n=== 잠겨있는 파일 발견! ===")
        print(f"문제 파일: {file_path}")
        print(f"오류: {e}")
        return False

def main():
    print("=== 점유된 파일 자동 탐색 시작 ===")

    for root, dirs, files in os.walk(BASE):
        for name in files:
            if name.lower().endswith(".py"):
                fpath = os.path.join(root, name)
                ok = try_backup(fpath)
                if not ok:
                    print("\n⚠ 패치 엔진이 이 파일 때문에 매번 멈추고 있었음")
                    print("이 파일을 VSCode/메모장/탐색기 미리보기에서 닫고 다시 실행하세요.\n")
                    return

    print("모든 .py 파일을 정상 백업했습니다. 잠긴 파일은 없음.")

if __name__ == "__main__":
    main()
