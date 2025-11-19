import os
import json
import re
from pathlib import Path
import networkx as nx

ROOT = Path("F:/autostockG")

# 스캔 대상 확장자
CODE_EXT = {".py"}
DATA_EXT = {".parquet", ".pkl", ".csv"}
GRAPH_JSON = ROOT / "pipeline_graph.json"
GRAPH_PNG = ROOT / "pipeline_graph.png"


def list_all_files():
    files = []
    for root, _, filenames in os.walk(ROOT):
        for f in filenames:
            full = Path(root) / f
            files.append(full)
    return files


def scan_dependencies(file_path, content):
    deps = []

    # parquet / pkl / csv 경로 감지
    parquet_paths = re.findall(r"[A-Za-z]:[\\/][^\s\"']+\.parquet", content)
    pkl_paths = re.findall(r"[A-Za-z]:[\\/][^\s\"']+\.pkl", content)
    csv_paths = re.findall(r"[A-Za-z]:[\\/][^\s\"']+\.csv", content)

    deps += parquet_paths
    deps += pkl_paths
    deps += csv_paths

    return deps


def build_graph():
    files = list_all_files()
    graph = nx.DiGraph()

    for f in files:
        graph.add_node(str(f))

    for f in files:
        if f.suffix.lower() in CODE_EXT:
            try:
                content = f.read_text(encoding="utf-8")
            except:
                continue
            deps = scan_dependencies(f, content)

            for d in deps:
                d_path = Path(d)
                if d_path.exists():
                    graph.add_edge(str(f), str(d_path))

    return graph


def save_graph(graph):
    # JSON 출력
    data = nx.node_link_data(graph)
    with open(GRAPH_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # PNG 출력
    try:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(25, 20))
        pos = nx.spring_layout(graph, k=0.4)
        nx.draw(
            graph,
            pos,
            with_labels=False,
            node_size=50,
            edge_color="#999999",
            alpha=0.7,
        )
        plt.savefig(GRAPH_PNG, dpi=300)
        plt.close()
    except ImportError:
        print("⚠ matplotlib 미설치 → PNG 출력 생략됨")


def main():
    print("=== Pipeline Graph Builder 시작 ===")
    graph = build_graph()
    print(f"[1] 그래프 노드 수  : {len(graph.nodes())}")
    print(f"[2] 그래프 엣지 수  : {len(graph.edges())}")

    save_graph(graph)
    print(f"[OK] JSON 저장   : {GRAPH_JSON}")
    print(f"[OK] PNG 저장    : {GRAPH_PNG}")
    print("=== 완료 ===")


if __name__ == "__main__":
    main()
