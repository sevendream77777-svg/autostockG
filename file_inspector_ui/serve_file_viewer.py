"""Feature-rich HTTP UI to browse repository files, pipelines, and summaries."""

from __future__ import annotations

import ast
import html
import json
import os
import re
import webbrowser
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote, urlparse

PORT = 8000
REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_URL = f"http://127.0.0.1:{PORT}"
SEARCH_LIMIT = 64
PIPELINE_MATCH_LIMIT = 4


@dataclass(frozen=True)
class RepoEntry:
    path: Path
    rel: str
    rel_lower: str
    name: str
    size: int
    mtime: float


def build_line_list(content: str) -> Iterable[str]:
    """Yield escaped lines with line numbers."""
    for idx, line in enumerate(content.splitlines(), start=1):
        escaped = html.escape(line, quote=False)
        yield (
            f'<span class="line-number">{idx:04d}</span>'
            f'<span class="line-body">{escaped}</span>'
        )


def _human_size(size: int) -> str:
    """Convert size in bytes to a friendly unit."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:0.1f} {unit}"
        value /= 1024
    return f"{value:0.1f} PB"


def _format_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _highlight_term(text: str, term: str) -> str:
    if not term:
        return html.escape(text)
    emitter = []
    last = 0
    pattern = re.compile(re.escape(term), re.IGNORECASE)
    for match in pattern.finditer(text):
        emitter.append(html.escape(text[last : match.start()]))
        emitter.append(f"<mark>{html.escape(match.group(0))}</mark>")
        last = match.end()
    emitter.append(html.escape(text[last:]))
    return "".join(emitter)


def _load_repo_index() -> list[RepoEntry]:
    entries: list[RepoEntry] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        rel = path.relative_to(REPO_ROOT)
        rel_posix = rel.as_posix()
        entries.append(
            RepoEntry(
                path=path,
                rel=rel_posix,
                rel_lower=rel_posix.lower(),
                name=path.name,
                size=stat.st_size,
                mtime=stat.st_mtime,
            )
        )
    entries.sort(key=lambda entry: entry.rel_lower)
    return entries


def _load_file_descriptions() -> dict[str, str]:
    source = REPO_ROOT / "pipeline" / "descriptions.txt"
    if not source.exists():
        return {}
    descriptions: dict[str, str] = {}
    current_key = ""
    parts: list[str] = []
    for raw in source.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.rstrip()
        if not line:
            if current_key and parts:
                descriptions.setdefault(current_key, " ".join(parts).strip())
            current_key = ""
            parts = []
            continue
        if line.startswith(" ") or line.startswith("\t"):
            if current_key:
                parts.append(line.strip())
        else:
            if current_key and parts:
                descriptions.setdefault(current_key, " ".join(parts).strip())
            current_key = os.path.basename(line.strip())
            parts = []
    if current_key and parts:
        descriptions.setdefault(current_key, " ".join(parts).strip())
    return descriptions


def _load_custom_descriptions() -> dict[str, str]:
    """Load user-provided descriptions from file_inspector_ui/custom_descriptions.json."""
    source = REPO_ROOT / "file_inspector_ui" / "custom_descriptions.json"
    if not source.exists():
        return {}
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    descriptions: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        descriptions[key] = value.strip()
    return descriptions


def _guess_inline_summary(content: str) -> str:
    for raw in content.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            return stripped.lstrip("# ").strip()
        if stripped.startswith('"""') or stripped.startswith("'''"):
            guess = stripped.strip('"\' ')
            if guess:
                return guess
        if stripped.startswith(("import ", "from ")):
            continue
        break
    return ""


def _read_summary_from_path(path: Path) -> str:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""
    return _guess_inline_summary(content)


def _build_python_summaries(entries: Iterable[RepoEntry]) -> list[dict[str, str]]:
    summaries: list[dict[str, str]] = []
    for entry in entries:
        if not entry.rel_lower.endswith(".py"):
            continue
        summary = FILE_DESCRIPTIONS.get(entry.name) or _read_summary_from_path(entry.path)
        summaries.append(
            {"rel": entry.rel, "name": entry.name, "summary": summary or "설명 없음"}
        )
    return sorted(summaries, key=lambda record: record["rel"])


def _module_from_rel(rel: str) -> str:
    path = Path(rel)
    if path.name == "__init__.py":
        path = path.parent
    else:
        path = path.with_suffix("")
    parts = [part for part in path.parts if part and part != "."]
    return ".".join(parts)


def _build_module_mapping(entries: Iterable[RepoEntry]) -> dict[str, RepoEntry]:
    mapping: dict[str, RepoEntry] = {}
    for entry in entries:
        if not entry.rel_lower.endswith(".py"):
            continue
        module = _module_from_rel(entry.rel)
        if not module:
            continue
        parts = module.split(".")
        # full module path (예: MODELENGINE.RAW.make_kospi_index_10y)
        mapping.setdefault(module, entry)
        # 루트 디렉터리 제거한 경로 (예: RAW.make_kospi_index_10y, UTIL.config_paths)
        if len(parts) > 1:
            sans_root = ".".join(parts[1:])
            mapping.setdefault(sans_root, entry)
        # 파일명만 (예: make_kospi_index_10y)
        if parts:
            mapping.setdefault(parts[-1], entry)
    return mapping


def _resolve_relative_module(current_module: str, module: str | None, level: int) -> str:
    base_parts = current_module.split(".") if current_module else []
    if level:
        base_parts = base_parts[: max(0, len(base_parts) - level)]
    if module:
        base_parts = base_parts + module.split(".")
    return ".".join(part for part in base_parts if part)


def _match_module(module_name: str, module_map: dict[str, RepoEntry]) -> RepoEntry | None:
    parts = module_name.split(".")
    for i in range(len(parts), 0, -1):
        candidate = ".".join(parts[:i])
        entry = module_map.get(candidate)
        if entry:
            return entry
    return None


def _gather_import_targets(entry: RepoEntry, module_map: dict[str, RepoEntry]) -> set[str]:
    targets: set[str] = set()
    try:
        source = entry.path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return targets
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return targets
    current_module = _module_from_rel(entry.rel)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if not alias.name:
                    continue
                matched = _match_module(alias.name, module_map)
                if matched and matched.rel != entry.rel:
                    targets.add(matched.rel)
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_relative_module(current_module, node.module, node.level)
            if base:
                matched = _match_module(base, module_map)
                if matched and matched.rel != entry.rel:
                    targets.add(matched.rel)
            for alias in node.names:
                if alias.name == "*":
                    continue
                candidate = base
                if candidate:
                    candidate = f"{candidate}.{alias.name}"
                else:
                    candidate = _resolve_relative_module(current_module, alias.name, node.level)
                if not candidate:
                    continue
                matched = _match_module(candidate, module_map)
                if matched and matched.rel != entry.rel:
                    targets.add(matched.rel)
    return targets


def _coerce_join_literal(call: ast.Call) -> str | None:
    if not isinstance(call.func, ast.Attribute) or call.func.attr != "join":
        return None
    parts: list[str] = []
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            parts.append(arg.value)
        else:
            return None
    if not parts:
        return None
    return os.path.join(*parts)


def _resolve_literal_to_rel(literal: str) -> str | None:
    raw = literal.strip().replace("\\", "/")
    if not raw:
        return None
    if raw.lower().startswith(str(REPO_ROOT).lower().replace("\\", "/")):
        try:
            rel = Path(raw).resolve().relative_to(REPO_ROOT)
            rel_posix = rel.as_posix()
            if rel_posix in REL_TO_ENTRY:
                return rel_posix
        except (OSError, ValueError):
            pass
    candidate_path = Path(raw)
    if candidate_path.suffix == ".py":
        rel_candidate = candidate_path.as_posix()
        if rel_candidate in REL_TO_ENTRY:
            return rel_candidate
        rel_lower = rel_candidate.lower()
        matches = [e.rel for e in FILE_ENTRIES if e.rel_lower.endswith(rel_lower)]
        if matches:
            return sorted(matches, key=len)[0]
    base = candidate_path.name or raw
    base_lower = base.lower()
    if base_lower.endswith(".py") and base_lower in _BASENAME_MAP:
        entries = _BASENAME_MAP[base_lower]
        if entries:
            return sorted(entries, key=lambda e: len(e.rel))[0].rel
    rel_lower = raw.lower()
    matches = [e.rel for e in FILE_ENTRIES if rel_lower in e.rel_lower]
    if matches:
        return sorted(matches, key=len)[0]
    return None


def _gather_execution_targets(entry: RepoEntry) -> set[str]:
    targets: set[str] = set()
    try:
        source = entry.path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return targets
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return targets

    literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if ".py" in node.value:
                literals.add(node.value)
        elif isinstance(node, ast.Str):
            if ".py" in node.s:
                literals.add(node.s)
        elif isinstance(node, ast.Call):
            join_literal = _coerce_join_literal(node)
            if join_literal and ".py" in join_literal:
                literals.add(join_literal)

    for literal in literals:
        resolved = _resolve_literal_to_rel(literal)
        if resolved and resolved != entry.rel:
            targets.add(resolved)
    return targets


def _build_dependency_graph(
    entries: Iterable[RepoEntry], module_map: dict[str, RepoEntry]
) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for entry in entries:
        if not entry.rel_lower.endswith(".py"):
            continue
        imports = _gather_import_targets(entry, module_map)
        execs = _gather_execution_targets(entry)
        deps = imports | execs
        deps.discard(entry.rel)
        graph[entry.rel] = deps
    return graph


def _invert_graph(graph: dict[str, set[str]]) -> dict[str, set[str]]:
    reverse: dict[str, set[str]] = {}
    for source, children in graph.items():
        reverse.setdefault(source, set())
        for child in children:
            reverse.setdefault(child, set()).add(source)
    return reverse


def _build_indexes() -> None:
    global FILE_ENTRIES, FILE_DESCRIPTIONS, CUSTOM_DESCRIPTIONS
    global PIPELINE_CATALOG, PIPELINE_MAP, RECENT_FILES
    global PYTHON_SUMMARIES, PYTHON_SUMMARY_MAP
    global _BASENAME_MAP, MODULE_MAP, REL_TO_ENTRY
    global DEPENDENCY_GRAPH, REVERSE_DEPENDENCIES

    FILE_ENTRIES = _load_repo_index()
    FILE_DESCRIPTIONS = _load_file_descriptions()
    CUSTOM_DESCRIPTIONS = _load_custom_descriptions()

    PYTHON_SUMMARIES = _build_python_summaries(FILE_ENTRIES)
    PYTHON_SUMMARY_MAP = {record["rel"]: record["summary"] for record in PYTHON_SUMMARIES}

    _BASENAME_MAP = {}
    for entry in FILE_ENTRIES:
        _BASENAME_MAP.setdefault(entry.name.lower(), []).append(entry)

    PIPELINE_CATALOG = _build_fs_pipeline_catalog(FILE_ENTRIES)
    PIPELINE_MAP = {entry["id"]: entry for entry in PIPELINE_CATALOG}

    MODULE_MAP = _build_module_mapping(FILE_ENTRIES)
    REL_TO_ENTRY = {
        entry.rel: entry for entry in FILE_ENTRIES if entry.rel_lower.endswith(".py")
    }
    DEPENDENCY_GRAPH = _build_dependency_graph(FILE_ENTRIES, MODULE_MAP)
    REVERSE_DEPENDENCIES = _invert_graph(DEPENDENCY_GRAPH)
    RECENT_FILES = sorted(FILE_ENTRIES, key=lambda entry: entry.mtime, reverse=True)[:8]
def _search_files(query: str, limit: int = SEARCH_LIMIT) -> tuple[list[RepoEntry], int]:
    needle = query.lower()
    matches = [entry for entry in FILE_ENTRIES if needle in entry.rel_lower]
    return matches[:limit], len(matches)


def _collect_pipeline_matches(
    pipeline_id: str, limit: int = PIPELINE_MATCH_LIMIT
) -> list[dict[str, object]]:
    pipeline = PIPELINE_MAP.get(pipeline_id)
    if not pipeline:
        return []
    groups: list[dict[str, object]] = []
    for candidate in pipeline["candidates"]:
        needle = candidate.replace("\\", "/").lower()
        matched = [entry for entry in FILE_ENTRIES if needle in entry.rel_lower]
        groups.append(
            {
                "label": candidate,
                "matches": matched[:limit],
                "total": len(matched),
                "extra": max(0, len(matched) - limit),
            }
        )
    return groups


def _summary_for_entry(entry: RepoEntry) -> str:
    return (
        CUSTOM_DESCRIPTIONS.get(entry.rel)
        or CUSTOM_DESCRIPTIONS.get(entry.name)
        or FILE_DESCRIPTIONS.get(entry.name)
        or PYTHON_SUMMARY_MAP.get(entry.rel)
        or _read_summary_from_path(entry.path)
        or "설명 없음 - 파일 상단 주석/문자열로 역할을 추가하거나 custom_descriptions.json에 기록하세요."
    )


def _build_fs_pipeline_catalog(entries: Iterable[RepoEntry]) -> list[dict[str, object]]:
    """Build pipeline catalog by scanning all .py files."""
    catalog: list[dict[str, object]] = []
    py_entries = [e for e in entries if e.rel_lower.endswith(".py")]
    py_entries.sort(key=lambda e: e.rel_lower)
    for entry in py_entries:
        summary = _summary_for_entry(entry)
        catalog.append(
            {
                "id": entry.rel,  # relative path as id
                "name": entry.rel,
                "summary": summary,
                "candidates": [entry.rel],
            }
        )
    return catalog


_build_indexes()


def _resolve_literal_to_rel(literal: str) -> str | None:
    raw = literal.strip().replace("\\", "/")
    if not raw:
        return None
    if raw.lower().startswith(str(REPO_ROOT).lower().replace("\\", "/")):
        try:
            rel = Path(raw).resolve().relative_to(REPO_ROOT)
            rel_posix = rel.as_posix()
            if rel_posix in REL_TO_ENTRY:
                return rel_posix
        except (OSError, ValueError):
            pass
    # direct relative path
    candidate_path = Path(raw)
    if candidate_path.suffix == ".py":
        rel_candidate = candidate_path.as_posix()
        if rel_candidate in REL_TO_ENTRY:
            return rel_candidate
        # search by suffix match
        rel_lower = rel_candidate.lower()
        matches = [e.rel for e in FILE_ENTRIES if e.rel_lower.endswith(rel_lower)]
        if matches:
            return sorted(matches, key=len)[0]
    # basename search
    base = candidate_path.name or raw
    base_lower = base.lower()
    if base_lower.endswith(".py") and base_lower in _BASENAME_MAP:
        entries = _BASENAME_MAP[base_lower]
        if entries:
            return sorted(entries, key=lambda e: len(e.rel))[0].rel
    # substring search as last resort
    rel_lower = raw.lower()
    matches = [e.rel for e in FILE_ENTRIES if rel_lower in e.rel_lower]
    if matches:
        return sorted(matches, key=len)[0]
    return None
class FileViewerHandler(BaseHTTPRequestHandler):
    _current_pipeline_id = ""
    _current_search_query = ""
    _current_python_filter = ""

    def _render(self, body: str, title: str = "파일 인스펙터") -> bytes:
        html_page = """
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="utf-8">
            <title>{title}</title>
            <style>
                :root {
                    --bg: #0c0c12;
                    --panel: #140f1d;
                    --border: #2c2e3a;
                    --muted: #9ea5ba;
                    --accent: #3d7bff;
                }
                * {
                    box-sizing: border-box;
                }
                body {
                    margin: 0;
                    min-height: 100vh;
                    background: linear-gradient(135deg, #08080d, #1b1325);
                    color: #f5f5f8;
                    font-family: "Segoe UI", system-ui, sans-serif;
                }
                .page-shell {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 24px;
                }
                header {
                    margin-bottom: 18px;
                }
                header h1 {
                    margin: 0;
                    font-size: 2rem;
                }
                header p {
                    margin: 6px 0 12px;
                    color: var(--muted);
                }
                form {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 8px;
                    align-items: center;
                }
                label {
                    font-size: 0.85rem;
                    color: var(--muted);
                    margin-right: 4px;
                }
                input[type=text] {
                    flex: 1;
                    min-width: 200px;
                    padding: 8px 12px;
                    border-radius: 8px;
                    border: 1px solid #2c2e3a;
                    background: #0f0f17;
                    color: #f5f5f8;
                }
                button {
                    border: none;
                    background: var(--accent);
                    color: white;
                    font-weight: 600;
                    padding: 10px 18px;
                    border-radius: 10px;
                    cursor: pointer;
                }
                .layout {
                    display: grid;
                    grid-template-columns: 280px 1fr;
                    gap: 16px;
                }
                .sidebar {
                    background: var(--panel);
                    border: 1px solid var(--border);
                    border-radius: 16px;
                    padding: 16px;
                    height: calc(100vh - 128px);
                    min-height: 420px;
                    overflow-y: auto;
                }
                .main-pane {
                    display: flex;
                    flex-direction: column;
                    gap: 16px;
                }
                .panel {
                    background: var(--panel);
                    border: 1px solid var(--border);
                    border-radius: 16px;
                    padding: 16px;
                    box-shadow: 0 10px 35px rgba(0, 0, 0, 0.2);
                }
                pre {
                    background: #0a0a10;
                    padding: 12px;
                    border-radius: 12px;
                    overflow: auto;
                    max-height: 60vh;
                    font-family: "JetBrains Mono", "Source Code Pro", monospace;
                    line-height: 1.5;
                }
                .line-number {
                    display: inline-block;
                    width: 52px;
                    text-align: right;
                    margin-right: 8px;
                    color: #6d738a;
                }
                .line-body {
                    white-space: pre;
                }
                .panel h2 {
                    margin: 0 0 8px;
                    font-size: 1.35rem;
                }
                .panel p {
                    margin: 0 0 8px;
                    color: #dcdfe9;
                }
                .pipeline-menu {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    margin-bottom: 16px;
                }
                .pipeline-menu a {
                    display: flex;
                    justify-content: space-between;
                    text-decoration: none;
                    padding: 10px 12px;
                    border-radius: 10px;
                    border: 1px solid var(--border);
                    color: #f5f5f8;
                    background: #12131d;
                }
                .pipeline-menu a.active {
                    background: rgba(61, 123, 255, 0.15);
                    border-color: var(--accent);
                }
                .muted-note {
                    color: var(--muted);
                    font-size: 0.9rem;
                }
                .result-list {
                    list-style: none;
                    margin: 0;
                    padding: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }
                .result-item {
                    padding-bottom: 12px;
                    border-bottom: 1px solid #2b2d38;
                }
                .result-item:last-child {
                    border-bottom: none;
                    padding-bottom: 0;
                }
                .result-path {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 12px;
                }
                .result-path a {
                    color: #9bdcff;
                    text-decoration: none;
                    font-weight: 600;
                }
                .result-meta {
                    color: var(--muted);
                    font-size: 0.85rem;
                }
                mark {
                    background: #ffd166;
                    color: #1c1d1f;
                    border-radius: 4px;
                }
                .pipeline-candidate {
                    border-top: 1px solid #2b2d38;
                    padding-top: 10px;
                    margin-top: 10px;
                }
                .pipeline-candidate:first-child {
                    border-top: none;
                    padding-top: 0;
                    margin-top: 0;
                }
                .candidate-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    font-weight: 600;
                }
                .candidate-items {
                    list-style: none;
                    margin: 6px 0 0;
                    padding: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }
                .candidate-items li {
                    padding: 8px;
                    border-radius: 10px;
                    background: #0e0f18;
                    border: 1px solid #1e1f28;
                }
                .candidate-items li a {
                    color: #8ef0ff;
                    text-decoration: none;
                    font-weight: 600;
                }
                .candidate-label {
                    font-size: 0.95rem;
                    color: #d4d8e6;
                }
                .recent-panel {
                    margin-top: 16px;
                    padding-top: 12px;
                    border-top: 1px solid #2c2e3a;
                }
                .recent-panel ul {
                    list-style: none;
                    margin: 8px 0 0;
                    padding: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }
                .recent-panel li {
                    display: flex;
                    justify-content: space-between;
                    font-size: 0.85rem;
                    color: var(--muted);
                }
                .recent-panel li a {
                    color: #f5f5f8;
                    text-decoration: none;
                }
                .python-overview {
                    max-height: 240px;
                    overflow: auto;
                    border: 1px solid #2b2d38;
                    border-radius: 10px;
                    padding: 8px;
                    background: #07080f;
                    margin-top: 12px;
                }
                .python-overview ul {
                    list-style: none;
                    margin: 0;
                    padding: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }
                .python-overview li {
                    padding-bottom: 6px;
                    border-bottom: 1px solid #1c1f2a;
                }
                .python-overview li:last-child {
                    border-bottom: none;
                }
                .python-overview a {
                    color: #9bdcff;
                    text-decoration: none;
                    font-weight: 600;
                }
                .dependency-panel {
                    margin-top: 16px;
                }
                .dependency-tree {
                    list-style: none;
                    margin: 0;
                    padding: 0;
                }
                .dependency-tree li {
                    margin-bottom: 10px;
                }
                .dependency-tree ul {
                    margin: 8px 0 0 16px;
                    padding: 0;
                }
                .dependency-tree a {
                    color: #8ef0ff;
                    text-decoration: none;
                }
                .dependency-meta {
                    margin-bottom: 12px;
                }
                .dependency-meta strong {
                    display: block;
                    margin-top: 8px;
                }
                .dependency-meta ul {
                    list-style: none;
                    margin: 6px 0 0;
                    padding-left: 16px;
                }
                .instructions ul {
                    margin: 10px 0 0;
                    padding-left: 18px;
                    color: var(--muted);
                }
                @media (max-width: 1000px) {
                    .layout {
                        grid-template-columns: 1fr;
                    }
                    .sidebar {
                        height: auto;
                        order: 2;
                    }
                }
            </style>
        </head>
        <body>
            {body}
        </body>
        </html>
        """
        html_page = html_page.replace("{title}", html.escape(title))
        html_page = html_page.replace("{body}", body)
        return html_page.encode("utf-8")

    def _safe_path(self, rel_path: str) -> Path | None:
        candidate = (REPO_ROOT / rel_path).resolve()
        if REPO_ROOT in candidate.parents or candidate == REPO_ROOT:
            return candidate
        return None

    def _render_pipeline_menu(self, pipeline_id: str, search_query: str) -> str:
        menu_items = []
        query_part = f"&query={quote(search_query)}" if search_query else ""
        for entry in PIPELINE_CATALOG:
            is_active = entry["id"] == pipeline_id
            href = f"?pipeline={quote(entry['id'])}{query_part}"
            active_class = " active" if is_active else ""
            menu_items.append(
                f'<a class="pipeline-entry{active_class}" href="{href}">'
                f'<span>#{entry["id"]} {html.escape(entry["name"])}</span>'
                f'<span class="muted-note">{len(entry["candidates"])}개 항목</span>'
                "</a>"
            )
        if not menu_items:
            return "<p class='muted-note'>파이프라인 메타 정보를 로딩할 수 없습니다.</p>"
        clear_href = f"?query={quote(search_query)}" if search_query else "/"
        refresh_href = f"?refresh=1{query_part}"
        return (
            f"<div class='pipeline-menu'>"
            f'<a href="{clear_href}" class="muted-note">전체 보기</a>'
            f'<a href="{refresh_href}" class="muted-note">⟳ 새로고침 (repo 스캔)</a>'
            + "".join(menu_items)
            + "</div>"
        )

    def _make_path_link(self, rel_path: str) -> str:
        params = [f"path={quote(rel_path)}"]
        if self._current_search_query:
            params.append(f"query={quote(self._current_search_query)}")
        if self._current_pipeline_id:
            params.append(f"pipeline={quote(self._current_pipeline_id)}")
        if self._current_python_filter:
            params.append(f"python_filter={quote(self._current_python_filter)}")
        return f"?{'&'.join(params)}"

    def _build_search_section(
        self, search_query: str, matches: list[RepoEntry], total: int
    ) -> str:
        if not search_query:
            return ""
        intro = f"검색 결과 {len(matches)} / {total}건"
        rows = []
        for entry in matches:
            desc = _summary_for_entry(entry)
            rows.append(
                "<li class='result-item'>"
                f"<div class='result-path'>"
                f"<a href=\"{self._make_path_link(entry.rel)}\">{_highlight_term(entry.rel, search_query)}</a>"
                f"<span class='result-meta'>{_human_size(entry.size)}</span>"
                "</div>"
                f"<p class='result-meta'>{html.escape(desc)}</p>"
                f"<div class='result-meta'>updated {_format_mtime(entry.mtime)}</div>"
                "</li>"
            )
        more_note = ""
        if total > len(matches):
            more_note = (
                f"<p class='muted-note'>전체 {total}건 중 상위 {len(matches)}건만 표시 중입니다.</p>"
            )
        return (
            "<section class='panel'>"
            f"<h2>검색: {_highlight_term(search_query, search_query)}</h2>"
            f"<p class='muted-note'>{intro}</p>"
            f"<ul class='result-list'>{''.join(rows)}</ul>"
            f"{more_note}"
            "</section>"
        )

    def _build_pipeline_section(
        self, pipeline: dict[str, object], groups: list[dict[str, object]]
    ) -> str:
        name = html.escape(pipeline["name"])
        summary = pipeline.get("summary", "")
        summary_html = f"<p class='muted-note'>{html.escape(summary)}</p>" if summary else ""
        body = [
            "<section class='panel'>",
            f"<h2>파이프라인: {name}</h2>",
            summary_html,
        ]
        if not groups:
            body.append("<p class='muted-note'>등록된 파일이 없습니다.</p>")
        else:
            for group in groups:
                label = html.escape(group["label"])
                total = group["total"]
                group_header = (
                    f"<div class='candidate-header'>"
                    f"<span class='candidate-label'>{label}</span>"
                    f"<span class='muted-note'>{total}개 매칭</span>"
                    f"</div>"
                )
                items = []
                for entry in group["matches"]:
                    desc = _summary_for_entry(entry)
                    items.append(
                        "<li>"
                        f"<a href='{self._make_path_link(entry.rel)}'>{html.escape(entry.rel)}</a>"
                        f"<div class='result-meta'>{html.escape(desc)}</div>"
                        f"<div class='result-meta'>size {_human_size(entry.size)} · {_format_mtime(entry.mtime)}</div>"
                        "</li>"
                    )
                extra = ""
                if group["extra"]:
                    extra = f"<p class='muted-note'>+{group['extra']}개 더 일치</p>"
                body.extend(
                    [
                        "<div class='pipeline-candidate'>",
                        group_header,
                        f"<ul class='candidate-items'>{''.join(items)}</ul>",
                        extra,
                        "</div>",
                    ]
                )
        body.append("</section>")
        return "".join(body)

    def _render_dependency_branch(
        self, rel: str, visited: set[str], depth: int
    ) -> str:
        if depth > 6:
            return "<li class='muted-note'>더 깊은 수준은 생략되었습니다.</li>"
        if rel in visited:
            return (
                "<li>"
                f"<span class='muted-note'>{html.escape(rel)} (순환 참조)</span>"
                "</li>"
            )
        visited = visited | {rel}
        entry = REL_TO_ENTRY.get(rel)
        summary = _summary_for_entry(entry) if entry else "설명 없음"
        children = sorted(DEPENDENCY_GRAPH.get(rel, []))
        child_html = ""
        if children:
            child_html = (
                "<ul>"
                + "".join(
                    self._render_dependency_branch(child, visited, depth + 1)
                    for child in children
                )
                + "</ul>"
            )
        return (
            "<li>"
            f"<a href='{self._make_path_link(rel)}'>{html.escape(rel)}</a>"
            f"<p class='muted-note'>{html.escape(summary)}</p>"
            f"{child_html}"
            "</li>"
        )

    def _build_dependency_tree_panel(self, root_rel: str | None) -> str:
        if not root_rel or root_rel not in REL_TO_ENTRY:
            message = "<p class='muted-note'>파일을 검색하거나 목록에서 클릭하면 연계 트리가 나타납니다.</p>"
            return (
                "<section class='panel dependency-panel'>"
                "<h3>의존성 트리</h3>"
                f"{message}"
                "</section>"
            )
        tree_html = (
            "<ul class='dependency-tree'>"
            + self._render_dependency_branch(root_rel, set(), 0)
            + "</ul>"
        )
        return (
            "<section class='panel dependency-panel'>"
            "<h3>의존성 트리</h3>"
            f"{tree_html}"
            "</section>"
        )

    def _build_file_panel(self, rel_path: str) -> tuple[str, HTTPStatus]:
        if not rel_path:
            return "", HTTPStatus.OK
        safe_path = self._safe_path(rel_path)
        if safe_path is None or not safe_path.exists():
            msg = (
                f"<p class='muted-note'>존재하지 않거나 접근할 수 없는 경로입니다: {html.escape(rel_path)}</p>"
            )
            return (
                "<section class='panel'>"
                "<h2>파일 열기</h2>"
                f"{msg}"
                "</section>",
                HTTPStatus.NOT_FOUND,
            )
        if safe_path.is_dir():
            msg = f"<p class='muted-note'>디렉터리를 선택하셨습니다: {html.escape(rel_path)}</p>"
            return (
                "<section class='panel'>"
                "<h2>파일 열기</h2>"
                f"{msg}"
                "</section>",
                HTTPStatus.BAD_REQUEST,
            )
        try:
            content = safe_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return (
                "<section class='panel'>"
                "<h2>파일 열기</h2>"
                "<p class='muted-note'>UTF-8 텍스트가 아닙니다.</p>"
                "</section>",
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            )
        stat = safe_path.stat()
        summary = FILE_DESCRIPTIONS.get(safe_path.name) or _guess_inline_summary(content)
        line_html = "\n".join(build_line_list(content))
        rel = safe_path.relative_to(REPO_ROOT).as_posix()
        dependencies = sorted(DEPENDENCY_GRAPH.get(rel, []))
        dependents = sorted(REVERSE_DEPENDENCIES.get(rel, []))
        dep_count = len(dependencies)
        rev_count = len(dependents)

        def _render_dependency_links(items: list[str]) -> str:
            if not items:
                return "<span class='muted-note'>연결 없음</span>"
            links = []
            for item in items:
                links.append(
                    f"<li><a href='{self._make_path_link(item)}'>{html.escape(item)}</a></li>"
                )
            return "<ul>" + "".join(links) + "</ul>"

        info = (
            f"<div class='file-info'>"
            f"<strong>경로:</strong> {html.escape(str(safe_path.relative_to(REPO_ROOT)))}<br>"
            f"<strong>설명:</strong> {html.escape(summary or '설명 없음')}<br>"
            f"<strong>크기:</strong> {_human_size(stat.st_size)} · "
            f"<strong>수정:</strong> {_format_mtime(stat.st_mtime)} · "
            f"<strong>참조:</strong> {dep_count}개 · "
            f"<strong>역참조:</strong> {rev_count}개"
            f"</div>"
        )
        dependency_section = (
            "<div class='dependency-meta'>"
            "<strong>이 파일이 참조하는 파일</strong>"
            f"{_render_dependency_links(dependencies)}"
            "<strong>이 파일을 참조하는 파일</strong>"
            f"{_render_dependency_links(dependents)}"
            "</div>"
        )
        return (
            "<section class='panel'>"
            "<h2>파일 내용</h2>"
            f"{info}"
            f"{dependency_section}"
            f"<pre>{line_html}</pre>"
            "</section>",
            HTTPStatus.OK,
        )

    def _build_instructions(self) -> str:
        return (
            "<section class='panel instructions'>"
            "<h2>시작하기</h2>"
            "<p>검색창에는 partial/full 경로를 입력하면 바로 결과가 뜨고, 경로 입력란에는 정확한 경로를 넣어 열어볼 수 있습니다.</p>"
            "<ul>"
            "<li>파이프라인 목록을 클릭하면 연관 스크립트와 설명을 함께 확인할 수 있습니다.</li>"
            "<li>검색 결과에서 중복이 있을 경우 리스트에서 선택합니다.</li>"
            "<li>최근 수정 목록은 사이드바에서 빠르게 다시 열 수 있습니다.</li>"
            "</ul>"
            "</section>"
        )

    def _build_recent_panel(self) -> str:
        if not RECENT_FILES:
            return "<div class='recent-panel muted-note'>최근 수정된 파일이 없습니다.</div>"
        items = []
        for entry in RECENT_FILES[:6]:
            desc = _summary_for_entry(entry)
            desc_html = ""
            if desc and desc != "설명 없음":
                desc_html = f" · {html.escape(desc)}"
            items.append(
                "<li>"
                f"<a href=\"{self._make_path_link(entry.rel)}\">{html.escape(entry.rel)}</a>"
                f"<span>{_format_mtime(entry.mtime)}{desc_html}</span>"
                "</li>"
            )
        return (
            "<div class='recent-panel'>"
            "<h3>최근 수정 파일</h3>"
            "<ul>"
            + "".join(items)
            + "</ul>"
            + "</div>"
        )

    def _build_python_overview_section(self, python_filter: str) -> str:
        query = python_filter.lower().strip()
        filtered = [
            record
            for record in PYTHON_SUMMARIES
            if not query or query in record["rel"].lower()
        ]
        if not filtered:
            return (
                "<section class='panel'>"
                "<h2>Python 파일 개요</h2>"
                "<p class='muted-note'>조건에 맞는 Python 파일이 없습니다.</p>"
                "</section>"
            )
        rows = []
        for record in filtered:
            summary = record["summary"]
            rows.append(
                "<li>"
                f"<a href=\"{self._make_path_link(record['rel'])}\">{html.escape(record['rel'])}</a>"
                f"<p class='muted-note'>{html.escape(summary)}</p>"
                "</li>"
            )
        return (
            "<section class='panel'>"
            "<h2>Python 파일 개요</h2>"
            f"<p class='muted-note'>총 {len(filtered)}개 - 검색어가 없으면 전체 목록이 표시됩니다.</p>"
            f"<div class='python-overview'><ul>{''.join(rows)}</ul></div>"
            "</section>"
        )

    def _build_page_body(
        self,
        search_query: str,
        path_query: str,
        pipeline_id: str,
        pipeline_menu: str,
        sections: list[str],
        python_filter: str,
        tree_root: str | None,
    ) -> str:
        search_value = html.escape(search_query)
        path_value = html.escape(path_query)
        python_value = html.escape(python_filter)
        main_sections = "".join(sections)
        recent_panel = self._build_recent_panel()
        python_panel = self._build_python_overview_section(python_filter)
        dependency_panel = self._build_dependency_tree_panel(tree_root)
        return (
            "<div class='page-shell'>"
            "<header>"
            "<h1>파일 인스펙터</h1>"
            "<p>상단 검색으로 이름 또는 경로를 검색하고, 사이드바에서 파이프라인 맵을 탐색해보세요.</p>"
            "<form method='get'>"
            "<label for='query'>파일 검색</label>"
            f"<input id='query' name='query' value='{search_value}' placeholder='partial/full 경로 입력'>"
            "<label for='path'>직접 열기</label>"
            f"<input id='path' name='path' value='{path_value}' placeholder='예: MODELENGINE/UTIL/daily_recommender.py'>"
            "<label for='python-filter'>Python 필터</label>"
            f"<input id='python-filter' name='python_filter' value='{python_value}' placeholder='예: MODELENGINE/UTIL'>"
            f"<input type='hidden' name='pipeline' value='{html.escape(pipeline_id)}'>"
            "<button type='submit'>열기 / 검색</button>"
            "</form>"
            "</header>"
            "<div class='layout'>"
            "<aside class='sidebar'>"
            "<h2>파이프라인 목록</h2>"
            f"{pipeline_menu}"
            f"{dependency_panel}"
            f"{recent_panel}"
            "</aside>"
            "<main class='main-pane'>"
            f"{main_sections}"
            f"{python_panel}"
            "</main>"
            "</div>"
            "</div>"
        )

    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        if query.get("refresh", ["0"])[0] == "1":
            _build_indexes()
        search_query = query.get("query", [""])[0].strip()
        path_query = query.get("path", [""])[0].strip()
        pipeline_id = query.get("pipeline", [""])[0].strip()
        python_filter = query.get("python_filter", [""])[0].strip()
        self._current_search_query = search_query
        self._current_pipeline_id = pipeline_id
        self._current_python_filter = python_filter
        status = HTTPStatus.OK
        sections: list[str] = []
        search_matches: list[RepoEntry] = []
        total_matches = 0
        if search_query:
            search_matches, total_matches = _search_files(search_query)
            sections.append(self._build_search_section(search_query, search_matches, total_matches))
        pipeline_groups = _collect_pipeline_matches(pipeline_id)
        pipeline_info = PIPELINE_MAP.get(pipeline_id)
        if pipeline_info:
            sections.append(self._build_pipeline_section(pipeline_info, pipeline_groups))
        file_panel, file_status = self._build_file_panel(path_query)
        if file_panel:
            sections.append(file_panel)
        if path_query and file_status != HTTPStatus.OK:
            status = file_status
        if not sections:
            sections.append(self._build_instructions())
        pipeline_menu = self._render_pipeline_menu(pipeline_id, search_query)
        tree_root: str | None = None
        if path_query and path_query in REL_TO_ENTRY:
            tree_root = path_query
        elif search_matches:
            tree_root = search_matches[0].rel
        else:
            for group in pipeline_groups:
                if group["matches"]:
                    tree_root = group["matches"][0].rel
                    break
        if tree_root is None and RECENT_FILES:
            tree_root = RECENT_FILES[0].rel
        body = self._build_page_body(
            search_query,
            path_query,
            pipeline_id,
            pipeline_menu,
            sections,
            python_filter,
            tree_root,
        )
        title_context = path_query or (pipeline_info["name"] if pipeline_info else "파일 인스펙터")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(self._render(body, title_context))

    def log_message(self, format: str, *args) -> None:
        pass


def main() -> None:
    server = HTTPServer(("127.0.0.1", PORT), FileViewerHandler)
    print(f"Serving file inspector at {SERVER_URL}")
    print("Ctrl+C로 중단하세요.")
    try:
        webbrowser.open_new(SERVER_URL)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버를 종료합니다.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
