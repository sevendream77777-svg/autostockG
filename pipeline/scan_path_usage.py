from config_paths import HOJ_DB_RESEARCH, HOJ_DB_REAL, HOJ_ENGINE_RESEARCH, HOJ_ENGINE_REAL, SLE_DB_REAL, SLE_ENGINE_REAL
import os

BASE = r"F:\autostockG"

# 찾을 문자열 패턴들 (필요하면 여기 계속 추가 가능)
TARGET_PATTERNS = [
    # HOJ DB/ENGINE 파일명
    HOJ_DB_RESEARCH,
    HOJ_DB_REAL,
    HOJ_ENGINE_RESEARCH,
    HOJ_ENGINE_REAL,

    # SLE 관련 옛 이름 / 새 이름
    SLE_DB_REAL,
    SLE_DB_REAL,
    SLE_ENGINE_REAL,
    SLE_DB_REAL,
    SLE_ENGINE_REAL,

    # 절대 경로 하드코딩 의심
    r"F:\\autostockG",
    r"F:/autostockG",
]

REPORT_FILE = os.path.join(BASE, "path_usage_report.txt")


def scan_py_files(base_dir):
    results = []  # (file_path, line_no, pattern, line_text)

    for root, dirs, files in os.walk(base_dir):
        for name in files:
            if not name.lower().endswith(".py"):
                continue

            fpath = os.path.join(root, name)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    lines = f.readlines()
            except UnicodeDecodeError:
                # 인코딩 문제 있으면 cp949로 재시도
                try:
                    with open(fpath, "r", encoding="cp949") as f:
                        lines = f.readlines()
                except Exception as e:
                    print(f"[SKIP] {fpath} (읽기 실패: {e})")
                    continue

            for i, line in enumerate(lines, start=1):
                for pat in TARGET_PATTERNS:
                    if pat in line:
                        results.append((fpath, i, pat, line.rstrip("\n")))
                        break  # 한 줄에 여러 패턴이 있어도 한 번만 기록

    return results


def main():
    print("=== 경로/파일명 하드코딩 스캐너 시작 ===")
    results = scan_py_files(BASE)

    if not results:
        print("✅ 하드코딩된 대상 패턴을 사용하는 .py 파일이 없습니다.")
        return

    print(f"🔎 총 {len(results)}개 위치에서 패턴 발견됨.")
    print(f"📄 상세 내역은 {REPORT_FILE} 에 저장됩니다.\n")

    with open(REPORT_FILE, "w", encoding="utf-8") as rf:
        current_file = None
        for fpath, line_no, pat, text in results:
            if fpath != current_file:
                rf.write("\n=== 파일: {} ===\n".format(fpath))
                current_file = fpath
            rf.write(f"  [줄 {line_no}] ({pat}) {text}\n")

    print("✅ 스캔 완료. path_usage_report.txt를 열어서 어떤 파일들이 대상인지 확인하세요.")


if __name__ == "__main__":
    main()
