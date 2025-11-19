# auto_describe_files.py - 자동 설명 생성기 (v1)
# 위대하신호정님 전용: 프로젝트 전체 .py 파일을 분석하여 descriptions.txt 생성

from pathlib import Path

def summarize_py(file_path: Path) -> str:
    try:
        text = file_path.read_text(encoding='utf-8', errors='ignore')
    except:
        return "내용을 불러올 수 없습니다."

    lines = text.splitlines()
    summary = []

    for line in lines[:120]:
        s = line.strip()

        if s.startswith("class "):
            summary.append(f"클래스 정의: {s}")
        elif s.startswith("def "):
            summary.append(f"함수 정의: {s}")
        elif s.startswith("import ") or s.startswith("from "):
            summary.append(f"의존 모듈: {s}")
        elif s.startswith("#"):
            summary.append(f"주석: {s}")

        if len(summary) >= 3:
            break

    if not summary:
        return "요약 없음 (주석/함수/class 부족)"

    return " | ".join(summary)

def main():
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent

    output_path = script_dir / "descriptions.txt"
    results = []

    for py_file in root_dir.rglob("*.py"):
        # 자신의 실행파일 포함된 pipeline 폴더 내부는 스킵
        if script_dir in py_file.resolve().parents:
            continue

        summary = summarize_py(py_file)
        results.append(f"{py_file.relative_to(root_dir)}\n    → {summary}\n")

    output_path.write_text("\n".join(results), encoding='utf-8')
    print(f"[SAVE] 설명 파일 생성 완료 → {output_path}")

if __name__ == "__main__":
    main()
