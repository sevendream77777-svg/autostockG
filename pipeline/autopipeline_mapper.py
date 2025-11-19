"""
autopipeline_mapper.py
F:/autostockG 프로젝트의 파이프라인(코드 ↔ 데이터 파일 ↔ 엔진 파일) 구조를
자동으로 스캔해서 정리해주는 도구.

실행:
    python autopipeline_mapper.py

결과:
    pipeline_map.txt      - 사람이 읽기 좋은 요약
    pipeline_graph.json   - 구조화된 그래프 데이터 (필요시 추가 활용)
"""

import os
import re
import json
from pathlib import Path
from collections import defaultdict

# --- 설정 ---
PROJECT_ROOT = Path(__file__).resolve().parent

# 분석 대상 확장자
CODE_EXT = [".py"]
DATA_EXT = [".parquet", ".csv", ".pkl"]  # DB / 모델 / 로그 파일 등

# 경로 패턴 (read_parquet, read_csv, joblib.load 등)
RE_READ_PARQUET = re.compile(r'read_parquet\(\s*[rRuU]?[\'"]([^\'"]+)[\'"]')
RE_READ_CSV = re.compile(r'read_csv\(\s*[rRuU]?[\'"]([^\'"]+)[\'"]')
RE_JOBLIB_LOAD = re.compile(r'joblib\.load\(\s*[rRuU]?[\'"]([^\'"]+)[\'"]')
RE_PICKLE_LOAD = re.compile(r'pickle\.load\([^)]*open\(\s*[rRuU]?[\'"]([^\'"]+)[\'"]')

# 문자열 안에 하드코딩된 경로 후보 (예: "F:/autostockG/HOJ_DB/REAL/...")
RE_PATH_STRING = re.compile(r'[rRuU]?[\'"]([^\'"]+\.(?:parquet|csv|pkl))[\'"]')

# import 패턴 (내부 의존성 분석용)
RE_IMPORT = re.compile(r'^\s*import\s+([a-zA-Z0-9_\.]+)', re.MULTILINE)
RE_FROM_IMPORT = re.compile(r'^\s*from\s+([a-zA-Z0-9_\.]+)\s+import\s+([a-zA-Z0-9_,\s\*]+)', re.MULTILINE)


def scan_project(root: Path):
    """프로젝트 전체 파일 스캔"""
    code_files = []
    data_files = []

    for dirpath, dirnames, filenames in os.walk(root):
        dirpath = Path(dirpath)
        # .git, __pycache__ 등은 스킵
        if ".git" in dirpath.parts or "__pycache__" in dirpath.parts:
            continue

        for fname in filenames:
            fpath = dirpath / fname
            ext = fpath.suffix.lower()

            if ext in CODE_EXT:
                code_files.append(fpath.relative_to(root))
            elif ext in DATA_EXT:
                data_files.append(fpath.relative_to(root))

    return code_files, data_files


def analyze_code_file(path: Path, root: Path):
    """단일 .py 파일에서 의존성/경로 추출"""
    full_path = root / path
    try:
        text = full_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            text = full_path.read_text(encoding="cp949")
        except Exception as e:
            print(f"[WARN] {path} 읽기 실패: {e}")
            return None

    info = {
        "file": str(path),
        "imports": [],
        "data_reads": [],
        "model_loads": [],
        "path_literals": [],
    }

    # --- import 분석 ---
    imports = []
    for m in RE_IMPORT.finditer(text):
        imports.append(m.group(1))
    for m in RE_FROM_IMPORT.finditer(text):
        imports.append(m.group(1))
    info["imports"] = sorted(set(imports))

    # --- 데이터/모델 경로 분석 ---
    reads = []
    for m in RE_READ_PARQUET.finditer(text):
        reads.append({"type": "read_parquet", "path": m.group(1)})
    for m in RE_READ_CSV.finditer(text):
        reads.append({"type": "read_csv", "path": m.group(1)})
    info["data_reads"] = reads

    loads = []
    for m in RE_JOBLIB_LOAD.finditer(text):
        loads.append({"type": "joblib.load", "path": m.group(1)})
    for m in RE_PICKLE_LOAD.finditer(text):
        loads.append({"type": "pickle.load", "path": m.group(1)})
    info["model_loads"] = loads

    # --- 문자열 안의 경로 후보 ---
    path_literals = []
    for m in RE_PATH_STRING.finditer(text):
        path_literals.append(m.group(1))
    info["path_literals"] = sorted(set(path_literals))

    return info


def build_dependency_graph(code_files, data_files, root: Path):
    """코드/데이터 파일 리스트를 기반으로 의존성 그래프 구성"""
    analysis = {}
    for cf in code_files:
        info = analyze_code_file(cf, root)
        if info:
            analysis[str(cf)] = info

    # 노드/엣지 구성
    nodes = set()
    edges = []

    data_set = set(str(p) for p in data_files)

    for fname, info in analysis.items():
        nodes.add(fname)

        # import 기반 코드-코드 연결
        for mod in info["imports"]:
            # 같은 프로젝트 내 모듈 추정: run_xxx → 파일명 xxx.py
            mod_base = mod.split(".")[0]
            candidate = f"{mod_base}.py"
            if any(str(cf).endswith(candidate) for cf in analysis.keys()):
                edges.append({"from": fname, "to": candidate, "type": "import"})

        # 데이터 파일 연결
        for r in info["data_reads"]:
            path = r["path"]
            # 상대/절대 경로 상관 없이 문자열로만 먼저 연결
            edges.append({"from": fname, "to": path, "type": r["type"]})
            nodes.add(path)

        # 모델 파일 연결
        for l in info["model_loads"]:
            path = l["path"]
            edges.append({"from": fname, "to": path, "type": l["type"]})
            nodes.add(path)

        # 하드코딩 경로 리터럴
        for p in info["path_literals"]:
            if p not in data_set:  # 이미 data_files에 있으면 중복
                edges.append({"from": fname, "to": p, "type": "literal"})
                nodes.add(p)

    graph = {
        "nodes": sorted(nodes),
        "edges": edges,
        "analysis": analysis,
        "root": str(root),
    }
    return graph


def classify_node(name: str):
    """노드의 용도 추정 (간단 태그)"""
    n = name.lower()
    tags = []

    if n.endswith(".py"):
        tags.append("code")
    if n.endswith(".parquet"):
        tags.append("parquet")
    if n.endswith(".csv"):
        tags.append("csv")
    if n.endswith(".pkl"):
        tags.append("model")

    # HOJ / SLE / UI / KIWOOM 등 태그
    if "hoj" in n:
        tags.append("hoj")
    if "sle" in n:
        tags.append("sle")
    if "ui" in n:
        tags.append("ui")
    if "kiwoom" in n or "rest" in n:
        tags.append("kiwoom")
    if "token" in n or "kakao" in n:
        tags.append("notifier")

    if "db" in n:
        tags.append("db")
    if "engine" in n:
        tags.append("engine")
    if "train" in n:
        tags.append("train")
    if "build" in n or "merge" in n or "process" in n:
        tags.append("pipeline")
    if "test" in n or "eval" in n or "check" in n:
        tags.append("test")

    return tags


def write_pipeline_map(graph, root: Path):
    """사람이 읽기 좋은 텍스트 요약 생성"""
    out_txt = root / "pipeline_map.txt"
    out_json = root / "pipeline_graph.json"

    # json 저장
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    # 코드 파일별 연결 요약
    by_file = defaultdict(list)
    for e in graph["edges"]:
        by_file[e["from"]].append(e)

    lines = []
    lines.append(f"=== AUTOSTOCKG PIPELINE MAP ===")
    lines.append(f"ROOT: {graph['root']}")
    lines.append("")

    # HOJ, SLE, 기타 순으로 정렬
    code_files = [n for n in graph["nodes"] if n.endswith(".py")]
    def sort_key(name):
        nl = name.lower()
        if "hoj" in nl:
            prefix = "0"
        elif "sle" in nl:
            prefix = "1"
        else:
            prefix = "2"
        return prefix + nl
    code_files = sorted(set(code_files), key=sort_key)

    for cf in code_files:
        info = graph["analysis"].get(cf)
        tags = classify_node(cf)
        lines.append(f"[CODE] {cf}   ({', '.join(tags)})" if tags else f"[CODE] {cf}")

        if info:
            if info["imports"]:
                lines.append(f"  - imports: {', '.join(info['imports'])}")
        if by_file.get(cf):
            for e in by_file[cf]:
                tgt = e["to"]
                etype = e["type"]
                ttags = classify_node(str(tgt))
                tag_str = f" ({', '.join(ttags)})" if ttags else ""
                lines.append(f"  - uses [{etype}]: {tgt}{tag_str}")
        lines.append("")

    # 데이터/모델 파일 목록 요약
    lines.append("\n=== DATA / MODEL FILES (탐지된 것) ===\n")
    data_like = [n for n in graph["nodes"] if not str(n).endswith(".py")]
    for n in sorted(set(data_like)):
        tags = classify_node(str(n))
        lines.append(f"- {n}   ({', '.join(tags)})")

    out_txt.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] pipeline_map.txt, pipeline_graph.json 생성 완료")


def main():
    print(f"[STEP 1] 프로젝트 스캔 시작: {PROJECT_ROOT}")
    code_files, data_files = scan_project(PROJECT_ROOT)
    print(f"  - 코드 파일 수: {len(code_files)}")
    print(f"  - 데이터/모델 파일 수: {len(data_files)}")

    print("[STEP 2] 의존성 분석 중...")
    graph = build_dependency_graph(code_files, data_files, PROJECT_ROOT)

    print("[STEP 3] 파이프라인 맵 생성...")
    write_pipeline_map(graph, PROJECT_ROOT)

    print("\n✅ 자동 파이프라인 분석 완료!")
    print("   - pipeline_map.txt 를 열어서 전체 흐름을 확인하세요.")
    print("   - pipeline_graph.json 은 추가 도구에서 재활용 가능 (예: 그래프 시각화 등).")


if __name__ == "__main__":
    main()
