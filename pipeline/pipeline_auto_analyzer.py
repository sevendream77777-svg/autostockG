"""
pipeline_auto_analyzer.py

F:\autostockG 전체를 스캔해서
- .py / .parquet / .pkl 파일 수집
- import 관계 분석
- 파일 I/O(읽기/쓰기) 추적
- 경로 문자열 감지 (특히 autostockG 관련)
- HOJ / SLE / UI / REST / 크롤러 / 전처리 / 기타 분류
- 삭제 가능 후보 자동 태깅
- NetworkX 그래프 생성 후 JSON으로 저장

출력 파일:
- pipeline_map.txt        : 상세 텍스트 리포트
- pipeline_graph.json     : 의존성 그래프 (노드/엣지)
- pipeline_tree.txt       : 트리 구조 (디렉터리/파일)
- pipeline_summary.txt    : 핵심 파이프라인 요약

필요 패키지:
    pip install networkx
"""

import os
import re
import json
import sys
import argparse
import datetime
from collections import defaultdict

try:
    import networkx as nx
except ImportError:
    print("[ERROR] networkx 패키지가 필요합니다. 먼저 설치해주세요:")
    print("    pip install networkx")
    sys.exit(1)


# --------- 기본 설정 ---------

DEFAULT_BASE_DIR = r"F:\autostockG"

# 텍스트 파일 출력 인코딩
ENCODING = "utf-8"

# 무시할 디렉터리들 (가상환경 등)
IGNORE_DIR_NAMES = {
    ".git", ".venv", "venv", "__pycache__", ".idea", ".vscode", ".mypy_cache",
}


# --------- 유틸 함수 ---------

def iter_files(base_dir):
    """base_dir 아래의 모든 파일 경로를 yield."""
    for root, dirs, files in os.walk(base_dir):
        # 무시할 디렉터리 필터
        dirs[:] = [d for d in dirs if d not in IGNORE_DIR_NAMES]
        for fname in files:
            yield os.path.join(root, fname)


def rel_path(path, base_dir):
    return os.path.relpath(path, base_dir).replace("\\", "/")


def get_module_name(py_rel_path):
    # 예: 'HOJ_DB/REAL/build_REAL_HOJ_V25.py' -> 'HOJ_DB.REAL.build_REAL_HOJ_V25'
    if py_rel_path.lower().endswith(".py"):
        return py_rel_path[:-3].replace("/", ".")
    return py_rel_path.replace("/", ".")


def read_text_file(path):
    try:
        with open(path, "r", encoding=ENCODING, errors="ignore") as f:
            return f.read()
    except Exception as e:
        print(f"[WARN] 텍스트 파일 읽기 실패: {path} ({e})")
        return ""


def get_file_metadata(path):
    try:
        st = os.stat(path)
        return {
            "size": st.st_size,
            "mtime": st.st_mtime,
            "mtime_iso": datetime.datetime.fromtimestamp(
                st.st_mtime
            ).isoformat(timespec="seconds"),
        }
    except Exception as e:
        print(f"[WARN] 파일 메타데이터 조회 실패: {path} ({e})")
        return {}


# --------- 코드 분석 (import / IO / 경로 감지) ---------

IMPORT_RE = re.compile(r"^\s*import\s+([a-zA-Z0-9_\.]+)", re.MULTILINE)
FROM_IMPORT_RE = re.compile(r"^\s*from\s+([a-zA-Z0-9_\.]+)\s+import\s+", re.MULTILINE)
PATH_STRING_RE = re.compile(
    r"['\"]([^'\"]+\.(?:csv|parquet|pkl|json|txt))['\"]"
)

def analyze_imports(text):
    imports = set(IMPORT_RE.findall(text))
    imports.update(FROM_IMPORT_RE.findall(text))
    return sorted(imports)


def analyze_io_usage(text):
    """
    open(), pandas read_/to_*, joblib/pickle load/dump 등
    간단히 읽기/쓰기 패턴을 추적.
    """
    lines = text.splitlines()
    io_info = {
        "open_read": [],
        "open_write": [],
        "open_unknown": [],
        "pandas_read": [],
        "pandas_write": [],
        "pickle_joblib": [],
    }

    for i, line in enumerate(lines, start=1):
        l = line.strip()
        if "open(" in l:
            mode = None
            # 간단한 mode 추출
            m = re.search(r"open\([^,]+,\s*['\"]([rwaxtb\+]+)['\"]", l)
            if m:
                mode = m.group(1)
            if mode is None:
                io_info["open_unknown"].append((i, l))
            elif any(ch in mode for ch in ["w", "a", "x"]):
                io_info["open_write"].append((i, l))
            else:
                io_info["open_read"].append((i, l))

        # pandas 계열
        if "read_csv" in l or "read_parquet" in l or "read_pickle" in l or "read_json" in l:
            io_info["pandas_read"].append((i, l))
        if "to_csv" in l or "to_parquet" in l or "to_pickle" in l or "to_json" in l:
            io_info["pandas_write"].append((i, l))

        # pickle / joblib
        if "pickle.load" in l or "pickle.dump" in l or "joblib.load" in l or "joblib.dump" in l:
            io_info["pickle_joblib"].append((i, l))

    return io_info


def detect_paths_in_text(text, base_dir):
    """
    코드 안에 등장하는 파일 경로 문자열 후보 추출.
    특히 autostockG 관련 경로는 별도로 표시.
    """
    paths = set(PATH_STRING_RE.findall(text))
    autostock_paths = set()

    bd_norm = os.path.normpath(base_dir).lower()
    for p in list(paths):
        norm = os.path.normpath(p).lower()
        if bd_norm in norm or "autostockg" in norm:
            autostock_paths.add(p)

    return sorted(paths), sorted(autostock_paths)


def has_main_guard(text):
    return "__main__" in text


# --------- 파일 분류 (HOJ/SLE/UI/REST/크롤러/전처리/기타) ---------

def classify_file_type(path, rel, text, is_py):
    """
    category: 'HOJ', 'SLE', 'UI', 'REST', 'crawler', 'preprocess', 'test', 'utility', 'data'
    data_subtype: 'HOJ_DB', 'SLE_DB', 'HOJ_ENGINE', 'SLE_ENGINE', 'MODEL', 'OTHER_DATA'
    """
    name = os.path.basename(path)
    name_lower = name.lower()
    rel_lower = rel.lower()
    text_lower = (text or "").lower()

    if not is_py:
        ext = os.path.splitext(name)[1].lower()
        if ext in (".parquet", ".pkl"):
            # 데이터 타입 분류
            if "hoj_db" in name_lower or "/hoj_db/" in rel_lower or "\\hoj_db\\" in path.lower():
                return "data", "HOJ_DB"
            if "sle_db" in name_lower or "/sle_db/" in rel_lower or "\\sle_db\\" in path.lower():
                return "data", "SLE_DB"
            if "hoj_engine" in name_lower or "hoj_model" in name_lower:
                return "data", "HOJ_ENGINE"
            if "sle_engine" in name_lower or "sle_champion" in name_lower or "sle_model" in name_lower:
                return "data", "SLE_ENGINE"
            if "engine" in name_lower or "model" in name_lower:
                return "data", "MODEL"
            return "data", "OTHER_DATA"
        else:
            return "other", None

    # --- 여기서부터 .py 파일 분류 ---
    # HOJ / SLE 우선
    if "hoj" in name_lower or "/hoj_" in rel_lower or "hoj_" in name_lower:
        return "HOJ", None
    if "sle" in name_lower or "/sle_" in rel_lower or "sle_" in name_lower:
        return "SLE", None

    # UI
    if any(kw in name_lower for kw in ["ui", "viewer", "dashboard", "window", "qt", "tk", "wx"]):
        return "UI", None
    if any(kw in text_lower for kw in ["streamlit", "gradio", "flask", "django", "fastapi"]):
        return "UI", None

    # REST / API
    if "rest" in name_lower or "api" in name_lower or "uapi" in name_lower:
        return "REST", None
    if "kiwoom" in text_lower and "requests" in text_lower:
        return "REST", None

    # 크롤러
    if any(kw in name_lower for kw in ["crawler", "crawl", "scrap", "scraper", "spider"]):
        return "crawler", None
    if "beautifulsoup" in text_lower or "bs4" in text_lower:
        return "crawler", None

    # 전처리/DB 빌드
    if any(kw in name_lower for kw in [
        "build_db", "build_database", "build_", "merge_", "preprocess", "feature", "clean", "prepare"
    ]):
        return "preprocess", None

    # 테스트/실험
    if any(kw in name_lower for kw in ["test", "check", "trial", "debug", "sample", "experiment", "exp_"]):
        return "test", None

    # 기본은 utility
    return "utility", None


def is_deletion_candidate(fname, rel, category, has_main, graph_degree_in, graph_degree_out):
    """
    삭제 가능 후보를 대략적으로 판단.
    - 명백한 temp/backup/old/test/experiment 이름
    - 그래프 의존성이 거의 없고 main도 없는 유틸리티성 파일
    """
    name_lower = os.path.basename(fname).lower()
    rel_lower = rel.lower()

    # 강한 삭제 후보 키워드
    delete_keywords = [
        "temp", "tmp", "backup", "bak", "old", "deprecated", "copy", "unused",
        "sandbox", "draft", "test", "sample", "trial", "experiment", "exp_"
    ]
    if any(kw in name_lower for kw in delete_keywords):
        return True, "filename_keyword"

    # SLE 관련인데, HOJ-only 환경이라면 나중에 참고용 -> 여기서는 '후보'로만 태깅
    if category == "SLE":
        # 완전 삭제가 아니라, '후보' 정도 느낌으로 태깅
        return True, "sle_related"

    # 그래프 의존성이 거의 없는 경우
    if category in ("test", "utility"):
        if graph_degree_in == 0 and graph_degree_out == 0 and not has_main:
            return True, "isolated_utility"

    # 기본: 삭제 후보 아님
    return False, None


# --------- 메인 분석 ---------

def main(base_dir):
    base_dir = os.path.abspath(base_dir)
    print(f"[INFO] 분석 시작: {base_dir}")

    py_files = []
    data_files = []

    for path in iter_files(base_dir):
        lower = path.lower()
        if lower.endswith(".py"):
            py_files.append(path)
        elif lower.endswith(".parquet") or lower.endswith(".pkl"):
            data_files.append(path)

    print(f"[INFO] .py 파일 수: {len(py_files)}")
    print(f"[INFO] .parquet/.pkl 파일 수: {len(data_files)}")

    # --- 1차 스캔: Python 파일 정보 수집 ---
    py_infos = {}
    module_map = {}  # module_name -> 파일 경로

    for path in py_files:
        rel = rel_path(path, base_dir)
        text = read_text_file(path)
        imports = analyze_imports(text)
        io_info = analyze_io_usage(text)
        paths_in_text, autostock_paths = detect_paths_in_text(text, base_dir)
        main_flag = has_main_guard(text)
        category, data_subtype = classify_file_type(path, rel, text, is_py=True)

        info = {
            "path": path,
            "rel": rel,
            "module_name": get_module_name(rel),
            "imports_raw": imports,
            "internal_imports": [],  # 나중에 채움
            "external_imports": [],
            "io_info": io_info,
            "paths_in_text": paths_in_text,
            "autostock_paths": autostock_paths,
            "has_main": main_flag,
            "category": category,
            "data_subtype": data_subtype,  # 거의 None
            "metadata": get_file_metadata(path),
            "deletion_candidate": False,
            "deletion_reason": None,
        }
        py_infos[path] = info
        module_map[info["module_name"]] = path

    # --- 2차 스캔: 데이터 파일 정보 수집 ---
    data_infos = {}
    for path in data_files:
        rel = rel_path(path, base_dir)
        # 데이터 파일은 내용은 열지 않고 메타데이터만
        category, data_subtype = classify_file_type(path, rel, text=None, is_py=False)
        info = {
            "path": path,
            "rel": rel,
            "category": category,      # 'data'
            "data_subtype": data_subtype,
            "metadata": get_file_metadata(path),
        }
        data_infos[path] = info

    # --- 3. 의존성 그래프 구성 ---
    G = nx.DiGraph()

    # 노드 추가 (python 파일 + 데이터 파일)
    for path, info in py_infos.items():
        G.add_node(
            path,
            rel=info["rel"],
            type="py",
            category=info["category"],
            data_subtype=info["data_subtype"],
            has_main=info["has_main"],
            **{"size": info["metadata"].get("size", None),
               "mtime": info["metadata"].get("mtime", None)}
        )

    for path, info in data_infos.items():
        G.add_node(
            path,
            rel=info["rel"],
            type="data",
            category=info["category"],
            data_subtype=info["data_subtype"],
            **{"size": info["metadata"].get("size", None),
               "mtime": info["metadata"].get("mtime", None)}
        )

    # module_name -> path 를 이용해 internal/external import 분리
    for path, info in py_infos.items():
        internal = []
        external = []
        for mod in info["imports_raw"]:
            # 정확히 일치하는 모듈명
            if mod in module_map:
                internal.append(module_map[mod])
            else:
                # prefix로 매칭 시도 (예: import HOJ_DB.REAL)
                candidates = [p for m, p in module_map.items() if m.startswith(mod + ".")]
                if candidates:
                    internal.extend(candidates)
                else:
                    external.append(mod)

        # 중복 제거
        internal = sorted(set(internal))
        external = sorted(set(external))

        info["internal_imports"] = internal
        info["external_imports"] = external

        # 그래프에 import edge 추가 (py -> py)
        for tgt_path in internal:
            if tgt_path in G.nodes:
                G.add_edge(path, tgt_path, type="import")

    # --- 4. 데이터 파일 접근 관계 추출 ---
    # 텍스트 안의 경로 문자열과 IO 함수 정보를 조합해서
    # 대략적인 read/write/접속 관계를 잡는다.
    data_paths_set = set(data_files)

    def resolve_data_path(ref_str):
        """
        코드에 등장하는 문자열(ref_str)을 실제 파일 경로로 추정.
        - 절대경로면 그대로
        - 상대경로면 base_dir 기준
        """
        # 환경에 따라 다른 케이스가 있을 수 있지만, 최대한 단순하게 처리
        # 1) 이미 절대경로처럼 보이는 경우 (드라이브 문자 포함 등)
        if ":" in ref_str or ref_str.startswith(os.sep) or ref_str.startswith("/"):
            cand = os.path.normpath(ref_str)
            if os.path.exists(cand):
                return cand
        # 2) base_dir 기준 상대경로
        cand = os.path.normpath(os.path.join(base_dir, ref_str))
        if os.path.exists(cand):
            return cand
        return None

    # 라인별로 "to_parquet('xxx.parquet')" 같은 패턴에서 파일명 추출
    SIMPLE_FILE_IN_LINE = re.compile(
        r"['\"]([^'\"]+\.(?:csv|parquet|pkl|json|txt))['\"]"
    )

    # py 파일 반복
    for path, info in py_infos.items():
        text = read_text_file(path)
        lines = text.splitlines()
        rel = info["rel"]

        # data_access_map: 파일경로 -> {"read":bool, "write":bool}
        data_access_map = defaultdict(lambda: {"read": False, "write": False, "raw_refs": set()})

        # 4-1) paths_in_text 기반으로 raw_refs에 먼저 넣기
        for s in info["paths_in_text"]:
            data_access_map[s]["raw_refs"].add("text")

        # 4-2) 라인 스캔하며 read/write 분류
        for i, line in enumerate(lines, start=1):
            l = line.strip()
            # 라인 안의 파일명 후보들
            candidates = SIMPLE_FILE_IN_LINE.findall(l)
            if not candidates:
                continue

            # 읽기 함수들
            is_read = any(kw in l for kw in [
                "read_csv", "read_parquet", "read_pickle", "read_json",
                "open("  # open은 모드 보고 추정
            ])
            # 쓰기 함수들
            is_write = any(kw in l for kw in [
                "to_csv", "to_parquet", "to_pickle", "to_json",
                ".save(", "dump(", "dump(", "save_model", "save"
            ])

            for ref_str in candidates:
                data_access_map[ref_str]["raw_refs"].add(f"line:{i}")
                if "open(" in l:
                    # open의 모드 판단을 위해 다시 한번
                    m = re.search(r"open\([^,]+,\s*['\"]([rwaxtb\+]+)['\"]", l)
                    if m:
                        mode = m.group(1)
                        if any(ch in mode for ch in ["w", "a", "x"]):
                            is_write = True
                        else:
                            is_read = True
                if is_read:
                    data_access_map[ref_str]["read"] = True
                if is_write:
                    data_access_map[ref_str]["write"] = True

        # 4-3) 실제 파일 경로로 resolve 후 그래프에 edge 추가
        for ref_str, flags in data_access_map.items():
            resolved = resolve_data_path(ref_str)
            if not resolved:
                # 실제 존재하지 않더라도 autostock 경로면 노드로 추가할 수도 있음
                continue
            if resolved not in data_paths_set:
                # 데이터 파일 목록에 없으면 스킵
                continue

            # read/write 구분
            if flags["read"] and not flags["write"]:
                edge_type = "data_read"
            elif flags["write"] and not flags["read"]:
                edge_type = "data_write"
            elif flags["read"] and flags["write"]:
                edge_type = "data_read_write"
            else:
                edge_type = "data_access"

            G.add_edge(path, resolved, type=edge_type)

    # --- 5. 삭제 후보 태깅 ---
    for path, info in py_infos.items():
        deg_in = G.in_degree(path)
        deg_out = G.out_degree(path)
        category = info["category"]

        candidate, reason = is_deletion_candidate(
            fname=path,
            rel=info["rel"],
            category=category,
            has_main=info["has_main"],
            graph_degree_in=deg_in,
            graph_degree_out=deg_out,
        )
        info["deletion_candidate"] = candidate
        info["deletion_reason"] = reason
        G.nodes[path]["deletion_candidate"] = candidate
        G.nodes[path]["deletion_reason"] = reason

    # --------- 6. 출력: pipeline_tree.txt ---------
    tree_lines = []

    # 디렉터리 트리를 재구성하기 위해 (root-relative path -> list)
    all_paths = list(py_files) + list(data_files)
    all_rel_paths = sorted(rel_path(p, base_dir) for p in all_paths)

    def add_tree_entry(rel):
        parts = rel.split("/")
        for depth in range(len(parts)):
            sub = "/".join(parts[: depth + 1])
            tree_entries.add(sub)

    tree_entries = set()
    for r in all_rel_paths:
        add_tree_entry(r)

    # 디렉터리 먼저, 그 다음 파일
    dir_entries = [e for e in tree_entries if not any(e.endswith(ext) for ext in [".py", ".parquet", ".pkl"])]
    file_entries = [e for e in tree_entries if any(e.endswith(ext) for ext in [".py", ".parquet", ".pkl"])]

    dir_entries = sorted(dir_entries)
    file_entries = sorted(file_entries)

    tree_lines.append(f"Base directory: {base_dir}")
    tree_lines.append("=" * 70)
    tree_lines.append("")

    # 디렉터리/파일 함께 정렬된 순서로 트리 표현
    all_entries = sorted(tree_entries, key=lambda x: (x.count("/"), x))
    for entry in all_entries:
        depth = entry.count("/")
        indent = "  " * depth
        if any(entry.endswith(ext) for ext in [".py", ".parquet", ".pkl"]):
            # 파일
            full_path = os.path.join(base_dir, entry.replace("/", os.sep))
            tag = ""
            if full_path in py_infos:
                info = py_infos[full_path]
                tag = f"[{info['category']}]"
                if info["deletion_candidate"]:
                    tag += "(삭제후보)"
            elif full_path in data_infos:
                info = data_infos[full_path]
                tag = f"[data/{info['data_subtype']}]"
            tree_lines.append(f"{indent}- {entry} {tag}")
        else:
            # 디렉터리
            tree_lines.append(f"{indent}{entry}/")

    with open(os.path.join(base_dir, "pipeline_tree.txt"), "w", encoding=ENCODING) as f:
        f.write("\n".join(tree_lines))

    print("[OK] pipeline_tree.txt 생성 완료.")

    # --------- 7. 출력: pipeline_map.txt (상세 리포트) ---------
    map_lines = []
    map_lines.append(f"Base directory: {base_dir}")
    map_lines.append(f"Generated at: {datetime.datetime.now().isoformat(timespec='seconds')}")
    map_lines.append("=" * 80)
    map_lines.append("")

    # 7-1) Python 파일별 상세
    for path in sorted(py_infos, key=lambda p: py_infos[p]["rel"]):
        info = py_infos[path]
        rel = info["rel"]
        map_lines.append(f"=== PYTHON FILE: {rel} ===")
        map_lines.append(f"Absolute : {path}")
        map_lines.append(f"Category : {info['category']}")
        if info["data_subtype"]:
            map_lines.append(f"Data Subtype : {info['data_subtype']}")
        map_lines.append(f"Has __main__ : {info['has_main']}")
        map_lines.append(f"Size/mtime   : {info['metadata'].get('size')} bytes, {info['metadata'].get('mtime_iso')}")
        if info["deletion_candidate"]:
            map_lines.append(f"삭제 후보    : YES (reason={info['deletion_reason']})")
        else:
            map_lines.append(f"삭제 후보    : NO")

        map_lines.append("")
        map_lines.append(f"Internal imports ({len(info['internal_imports'])}):")
        for tgt in info["internal_imports"]:
            map_lines.append(f"  - {rel_path(tgt, base_dir)}")
        map_lines.append("")
        map_lines.append(f"External imports ({len(info['external_imports'])}):")
        for mod in info["external_imports"]:
            map_lines.append(f"  - {mod}")

        # 이 파일이 접근하는 데이터 노드
        out_edges = G.out_edges(path, data=True)
        data_edges = [e for e in out_edges if G.nodes[e[1]].get("type") == "data"]
        if data_edges:
            map_lines.append("")
            map_lines.append(f"Data access ({len(data_edges)}):")
            for src, tgt, ed in data_edges:
                data_info = data_infos[tgt]
                map_lines.append(
                    f"  - {data_info['rel']} [{data_info['data_subtype']}] (type={ed.get('type')})"
                )

        # IO usage 요약
        map_lines.append("")
        map_lines.append("I/O usage summary:")
        io = info["io_info"]
        for key in ["open_read", "open_write", "open_unknown", "pandas_read", "pandas_write", "pickle_joblib"]:
            entries = io.get(key, [])
            if entries:
                map_lines.append(f"  {key} ({len(entries)} lines)")
        if info["autostock_paths"]:
            map_lines.append("")
            map_lines.append("autostockG 관련 경로 문자열:")
            for s in info["autostock_paths"]:
                map_lines.append(f"  - {s}")

        map_lines.append("")
        map_lines.append("-" * 80)
        map_lines.append("")

    # 7-2) 데이터 파일별 사용처
    map_lines.append("")
    map_lines.append("=" * 80)
    map_lines.append("DATA FILES")
    map_lines.append("=" * 80)
    map_lines.append("")
    for path in sorted(data_infos, key=lambda p: data_infos[p]["rel"]):
        info = data_infos[path]
        rel = info["rel"]
        map_lines.append(f"=== DATA FILE: {rel} ===")
        map_lines.append(f"Absolute : {path}")
        map_lines.append(f"Category : {info['category']}")
        map_lines.append(f"Subtype  : {info['data_subtype']}")
        map_lines.append(f"Size/mtime: {info['metadata'].get('size')} bytes, {info['metadata'].get('mtime_iso')}")

        in_edges = G.in_edges(path, data=True)
        if in_edges:
            map_lines.append("")
            map_lines.append("Used by:")
            for src, tgt, ed in in_edges:
                src_rel = rel_path(src, base_dir)
                map_lines.append(f"  - {src_rel} (type={ed.get('type')})")
        else:
            map_lines.append("")
            map_lines.append("Used by: (no direct reference detected)")

        map_lines.append("")
        map_lines.append("-" * 80)
        map_lines.append("")

    with open(os.path.join(base_dir, "pipeline_map.txt"), "w", encoding=ENCODING) as f:
        f.write("\n".join(map_lines))

    print("[OK] pipeline_map.txt 생성 완료.")

    # --------- 8. 출력: pipeline_graph.json ---------
    graph_dict = {
        "base_dir": base_dir,
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "nodes": [],
        "edges": [],
    }

    for node, attrs in G.nodes(data=True):
        graph_dict["nodes"].append({
            "id": rel_path(node, base_dir),
            "abs_path": node,
            "type": attrs.get("type"),
            "category": attrs.get("category"),
            "data_subtype": attrs.get("data_subtype"),
            "deletion_candidate": attrs.get("deletion_candidate", False),
            "deletion_reason": attrs.get("deletion_reason", None),
            "size": attrs.get("size"),
            "mtime": attrs.get("mtime"),
        })

    for src, tgt, attrs in G.edges(data=True):
        graph_dict["edges"].append({
            "source": rel_path(src, base_dir),
            "target": rel_path(tgt, base_dir),
            "type": attrs.get("type"),
        })

    with open(os.path.join(base_dir, "pipeline_graph.json"), "w", encoding=ENCODING) as f:
        json.dump(graph_dict, f, ensure_ascii=False, indent=2)

    print("[OK] pipeline_graph.json 생성 완료.")

    # --------- 9. 출력: pipeline_summary.txt (핵심 파이프라인 요약) ---------
    summary_lines = []
    summary_lines.append(f"Base directory: {base_dir}")
    summary_lines.append(f"Generated at: {datetime.datetime.now().isoformat(timespec='seconds')}")
    summary_lines.append("=" * 80)
    summary_lines.append("핵심 파이프라인 요약 (자동 추출)")
    summary_lines.append("=" * 80)
    summary_lines.append("")

    # 9-1. 모델 파일(.pkl) 기준으로 파이프라인 요약
    model_nodes = [p for p in data_infos if p.lower().endswith(".pkl")]
    if model_nodes:
        summary_lines.append("1) 모델(.pkl) 기준 파이프라인")
        summary_lines.append("-" * 80)
        for model_path in sorted(model_nodes, key=lambda p: data_infos[p]["rel"]):
            info = data_infos[model_path]
            rel = info["rel"]
            engine_type = "HOJ" if "hoj" in rel.lower() else ("SLE" if "sle" in rel.lower() else "OTHER")
            summary_lines.append(f"[{engine_type}] 모델: {rel}")

            in_edges = G.in_edges(model_path, data=True)
            trainers = []
            users = []
            for src, tgt, ed in in_edges:
                if not src.lower().endswith(".py"):
                    continue
                etype = ed.get("type")
                if etype in ("data_write", "data_read_write"):
                    trainers.append(src)
                else:
                    users.append(src)

            # 학습 스크립트
            if trainers:
                summary_lines.append("  - 학습 스크립트:")
                for t in sorted(set(trainers)):
                    t_rel = rel_path(t, base_dir)
                    # 이 스크립트가 읽는 DB 추출
                    out_edges = G.out_edges(t, data=True)
                    db_inputs = []
                    for s2, tgt2, ed2 in out_edges:
                        if tgt2 in data_infos and tgt2.lower().endswith(".parquet"):
                            db_inputs.append(rel_path(tgt2, base_dir))
                    if db_inputs:
                        summary_lines.append(f"      · {t_rel}  (입력 DB: {', '.join(sorted(set(db_inputs)))})")
                    else:
                        summary_lines.append(f"      · {t_rel}")
            else:
                summary_lines.append("  - 학습 스크립트: (명확히 탐지되지 않음)")

            # 사용 스크립트
            if users:
                summary_lines.append("  - 모델 사용 스크립트:")
                for u in sorted(set(users)):
                    summary_lines.append(f"      · {rel_path(u, base_dir)}")
            else:
                summary_lines.append("  - 모델 사용 스크립트: (직접 참조 없음)")

            summary_lines.append("")
        summary_lines.append("")

    # 9-2. 핵심 DB(.parquet) 기준 파이프라인 요약
    db_nodes = [p for p in data_infos if p.lower().endswith(".parquet")]
    if db_nodes:
        summary_lines.append("2) DB(.parquet) 기준 파이프라인")
        summary_lines.append("-" * 80)
        for db_path in sorted(db_nodes, key=lambda p: data_infos[p]["rel"]):
            info = data_infos[db_path]
            rel = info["rel"]
            subtype = info["data_subtype"]
            summary_lines.append(f"[{subtype}] DB: {rel}")

            in_edges = G.in_edges(db_path, data=True)   # 이 DB를 쓰는/생성하는 코드
            out_edges = G.out_edges(db_path, data=True) # 이 DB로부터 다른 데이터로 가는 연결 (거의 없음)

            writers = []
            readers = []
            for src, tgt, ed in in_edges:
                if not src.lower().endswith(".py"):
                    continue
                etype = ed.get("type")
                if etype in ("data_write", "data_read_write"):
                    writers.append(src)
                else:
                    readers.append(src)

            if writers:
                summary_lines.append("  - DB 생성/갱신 스크립트:")
                for w in sorted(set(writers)):
                    summary_lines.append(f"      · {rel_path(w, base_dir)}")
            else:
                summary_lines.append("  - DB 생성/갱신 스크립트: (명확히 탐지되지 않음)")

            if readers:
                summary_lines.append("  - DB를 읽는 스크립트:")
                for r in sorted(set(readers)):
                    summary_lines.append(f"      · {rel_path(r, base_dir)}")
            else:
                summary_lines.append("  - DB를 읽는 스크립트: (직접 참조 없음)")

            summary_lines.append("")
        summary_lines.append("")

    # 9-3. 삭제 후보 파일 요약
    summary_lines.append("3) 삭제 가능 후보(.py) 리스트 (자동 추정)")
    summary_lines.append("-" * 80)
    any_del = False
    for path, info in sorted(py_infos.items(), key=lambda kv: kv[1]["rel"]):
        if info["deletion_candidate"]:
            any_del = True
            summary_lines.append(
                f"- {info['rel']}  (category={info['category']}, reason={info['deletion_reason']})"
            )
    if not any_del:
        summary_lines.append("(삭제 후보로 추정되는 .py 파일 없음)")
    summary_lines.append("")

    with open(os.path.join(base_dir, "pipeline_summary.txt"), "w", encoding=ENCODING) as f:
        f.write("\n".join(summary_lines))

    print("[OK] pipeline_summary.txt 생성 완료.")
    print("[DONE] 전체 파이프라인 자동 분석 완료.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="autostockG 파이프라인 자동 분석기")
    parser.add_argument(
        "--base-dir",
        type=str,
        default=DEFAULT_BASE_DIR,
        help=f"분석할 루트 디렉터리 (기본값: {DEFAULT_BASE_DIR})",
    )
    args = parser.parse_args()
    main(args.base_dir)
