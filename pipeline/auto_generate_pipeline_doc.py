"""
auto_generate_pipeline_doc.py - v3 (위대하신호정님 전용)

기능 요약
---------
- F:\autostockG 전체를 스캔해서:
  - HOJ / SLE / REST / pipeline / test / utility / 데이터(.parquet, .pkl) 분류
  - 핵심 DB / 핵심 모델 파일 별도 정리
  - 각 .py 파일 내용 일부를 자동 분석해서 "한 줄 설명" 생성
  - 특정 핵심 파일은 수동 설명(STATIC_DESC)로 더 보기 좋게 보정

- 출력 파일:
  F:\autostockG\pipeline\pipeline_YYYYMMDD.txt
  (같은 날짜에 여러 번 실행 시 -1, -2 번호 자동 증가)
"""

from pathlib import Path
from datetime import datetime
from typing import Dict, List


# ------------------------------------------------------------
# 1. 출력 파일명 생성 (pipeline_YYYYMMDD[-N].txt)
# ------------------------------------------------------------

def get_output_path(pipeline_dir: Path) -> Path:
    today = datetime.now().strftime("%Y%m%d")
    base = pipeline_dir / f"pipeline_{today}.txt"
    if not base.exists():
        return base

    idx = 1
    while True:
        cand = pipeline_dir / f"pipeline_{today}-{idx}.txt"
        if not cand.exists():
            return cand
        idx += 1


# ------------------------------------------------------------
# 2. 카테고리 분류 로직
# ------------------------------------------------------------

def categorize_py(path: Path, root_dir: Path) -> str:
    """파일 경로/이름을 보고 대략적인 카테고리 추론."""
    name = path.name.lower()
    parts = [p.lower() for p in path.relative_to(root_dir).parts]

    # 우선순위: SLE -> HOJ -> REST -> pipeline -> test -> utility
    if "sle" in name or any("sle" in p for p in parts):
        return "SLE"
    if "hoj" in name or any("hoj" in p for p in parts):
        return "HOJ"
    if any(k in name for k in ["rest", "kiwoom", "token", "kakao"]):
        return "REST"
    if any(k in name for k in ["build_", "train_", "update_", "run_"]):
        return "pipeline"
    if any(k in name for k in ["test", "backtest", "check_", "grid_search", "quantile"]):
        return "test"
    return "utility"


def scan_project(root_dir: Path, pipeline_dir: Path) -> Dict[str, List[Path]]:
    """
    루트 폴더 전체를 스캔해서 카테고리별로 py 파일을 정리하고,
    데이터 파일(.parquet, .pkl)도 별도로 모은다.
    """
    cats: Dict[str, List[Path]] = {
        "HOJ": [],
        "SLE": [],
        "REST": [],
        "pipeline": [],
        "test": [],
        "utility": [],
        "data_parquet": [],
        "data_model": [],
    }

    # .py 파일 스캔
    for path in root_dir.rglob("*.py"):
        try:
            if pipeline_dir in path.resolve().parents:
                # pipeline 폴더 안의 분석/문서용 스크립트는 제외
                continue
        except Exception:
            pass

        cat = categorize_py(path, root_dir)
        cats.setdefault(cat, []).append(path)

    # 데이터 파일 스캔 (.parquet / .pkl)
    for ext in ("*.parquet", "*.pkl"):
        for path in root_dir.rglob(ext):
            try:
                if pipeline_dir in path.resolve().parents:
                    continue
            except Exception:
                pass

            if path.suffix.lower() == ".parquet":
                cats["data_parquet"].append(path)
            elif path.suffix.lower() == ".pkl":
                cats["data_model"].append(path)

    return cats


# ------------------------------------------------------------
# 3. 핵심 DB / 모델 필터링
# ------------------------------------------------------------

CORE_DB_KEYWORDS = [
    "hoj_db_real",
    "hoj_db_research",
    "sle_db_real",
    "sle_db_research",
    "all_stocks_cumulative",
    "all_features_cumulative",
    "v25",  # V25 계열 DB
    "v11_merged_sle_base",
    "v11_database_final",
    "kospi_index_10y",
    "ticker_map",
]

CORE_MODEL_KEYWORDS = [
    "hoj_engine_real",
    "hoj_engine_research",
    "sle_engine_real",
    "real_hoj_modelengine",
    "real_champion_model",
    "champion_model_",
    "engine_v25",
    "engine_v32",
]


def filter_core_files(paths: List[Path], keywords: List[str]) -> List[Path]:
    lowered = [k.lower() for k in keywords]
    result: List[Path] = []
    for p in paths:
        name = p.name.lower()
        if any(k in name for k in lowered):
            result.append(p)
    return result


# ------------------------------------------------------------
# 4. 경로 & 섹션 렌더링 유틸
# ------------------------------------------------------------

def rel(path: Path, root_dir: Path) -> str:
    try:
        return str(path.relative_to(root_dir))
    except ValueError:
        return str(path)


def render_section(title: str, lines: List[str]) -> str:
    underline = "-" * len(title)
    if not lines:
        body = "  (해당 없음)\n"
    else:
        body = "".join(f"  - {line}\n" for line in lines)
    return f"{title}\n{underline}\n{body}\n"


# ------------------------------------------------------------
# 5. 설명 자동 생성 (요약기) + 수동 보정
# ------------------------------------------------------------

# 특정 핵심 파일은 수동설명 우선 적용
STATIC_FILE_DESCRIPTIONS: Dict[str, str] = {
    "merge_pbr_per_V17.py": "PER/PBR 병합기 (보조). SLE DB 생성 파이프라인 중간 단계.",
    "train_sle_engine_V31.py": "SLE 엔진 학습 코드 (보조).",
    "sle_sample_runner_multithread.py": "SLE 멀티스레드 러너 (보조).",
    "train_REAL_Hoj_ENGINE_V25.py": "HOJ 실전 엔진 학습 코드 (핵심).",
    "build_FULL_Hoj_DB_V25.py": "HOJ FULL DB 생성기 (핵심).",
    "naver_pbr_per_crawler_V16.py": "네이버 PBR/PER 크롤러 (보조).",
    "merge_hoj_sle_V30.py": "HOJ+SLE 하이브리드 병합기 (보조).",
    "daily_recommender.py": "HOJ 종목 추천 엔진 (실전).",
    "run_hybrid_test_V34.py": "하이브리드 백테스트 실행기 (연구).",
}


def summarize_py(file_path: Path) -> str:
    """
    .py 파일 내용을 가볍게 훑어서
    - class/def
    - import/from
    - 주석
    기준으로 짧은 설명을 만든다.
    """
    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return "내용을 읽지 못했습니다."

    lines = text.splitlines()
    summary_parts: List[str] = []

    for line in lines[:120]:
        s = line.strip()
        if not s:
            continue

        if s.startswith("class "):
            summary_parts.append(f"클래스 정의: {s}")
        elif s.startswith("def "):
            summary_parts.append(f"함수 정의: {s}")
        elif s.startswith("import ") or s.startswith("from "):
            summary_parts.append(f"의존 모듈: {s}")
        elif s.startswith("#"):
            summary_parts.append(f"주석: {s}")

        if len(summary_parts) >= 3:
            break

    if not summary_parts:
        return "요약 없음 (주석/함수/class 정보 부족)"

    return " | ".join(summary_parts)


def build_descriptions(
    categories: Dict[str, List[Path]],
    root_dir: Path,
) -> Dict[str, str]:
    """
    카테고리에 들어있는 모든 .py 파일에 대해
    - STATIC_FILE_DESCRIPTIONS 우선 적용
    - 없으면 summarize_py()로 자동 생성
    결과를 rel 경로를 key로 하는 dict로 반환.
    """
    desc_map: Dict[str, str] = {}

    for cat in ["HOJ", "SLE", "REST", "pipeline", "test", "utility"]:
        for p in categories.get(cat, []):
            rel_path = rel(p, root_dir)
            # 1순위: 수동 고정 설명
            if p.name in STATIC_FILE_DESCRIPTIONS:
                desc_map[rel_path] = STATIC_FILE_DESCRIPTIONS[p.name]
                continue

            # 2순위: 자동 요약
            auto_summary = summarize_py(p)
            desc_map[rel_path] = auto_summary

    return desc_map


def make_described(path: Path, root_dir: Path, desc_map: Dict[str, str]) -> str:
    base = rel(path, root_dir)
    desc = desc_map.get(base, "")
    if desc:
        return f"{base}  ← {desc}"
    return base


# ------------------------------------------------------------
# 6. 리포트 생성 함수 (build_report) - 설명 포함
# ------------------------------------------------------------

def build_report(
    root_dir: Path,
    pipeline_dir: Path,
    categories: Dict[str, List[Path]],
    desc_map: Dict[str, str],
    output_filename: str,
) -> str:
    now = datetime.now()
    header = f"""F:\\autostockG Pipeline Report
====================================

생성 시각 : {now.strftime("%Y-%m-%d %H:%M:%S")}
루트 폴더 : {root_dir}
출력 파일 : {output_filename}

이 리포트는 실행 시점 기준으로 F:\\autostockG 전체 구조를 스캔하여,
HOJ / SLE / SLE / REST / pipeline / test / utility / 데이터 파일을 분류한 요약본입니다.

※ 실제 프로그램 로직(학습/백테스트/매매)은 이 리포트를 기반으로
   별도 상세 문서에서 설명할 수 있습니다.
"""

    # 1. 개수 요약
    counts_lines = [
        f"HOJ 관련 코드      : {len(categories['HOJ'])}개",
        f"SLE 관련 코드      : {len(categories['SLE'])}개",
        f"REST / 키움 연동   : {len(categories['REST'])}개",
        f"파이프라인 스크립트: {len(categories['pipeline'])}개",
        f"테스트/실험 코드   : {len(categories['test'])}개",
        f"유틸리티 코드      : {len(categories['utility'])}개",
        f"데이터(.parquet)   : {len(categories['data_parquet'])}개",
        f"모델(.pkl)         : {len(categories['data_model'])}개",
    ]
    summary_section = render_section("1. 전체 카테고리 요약", counts_lines)

    # 2. 코어 DB / 모델
    core_db = filter_core_files(categories["data_parquet"], CORE_DB_KEYWORDS)
    core_models = filter_core_files(categories["data_model"], CORE_MODEL_KEYWORDS)

    core_db_lines = [make_described(p, root_dir, desc_map) for p in sorted(core_db)]
    core_model_lines = [make_described(p, root_dir, desc_map) for p in sorted(core_models)]

    core_section = ""
    core_section += render_section("2. 핵심 DB 파일 (Core DB)", core_db_lines)
    core_section += render_section("3. 핵심 모델 파일 (Core Models)", core_model_lines)

    # 3. 카테고리별 목록
    def sort_with_desc(paths: List[Path]) -> List[str]:
        return [make_described(p, root_dir, desc_map) for p in sorted(paths)]

    hoj_section = render_section("4. HOJ 관련 코드", sort_with_desc(categories["HOJ"]))
    sle_section = render_section("5. SLE 관련 코드", sort_with_desc(categories["SLE"]))
    rest_section = render_section("6. REST / 키움 연동 코드", sort_with_desc(categories["REST"]))
    pipe_section = render_section("7. 파이프라인 실행/생성 스크립트", sort_with_desc(categories["pipeline"]))
    test_section = render_section("8. 테스트 / 백테스트 / 실험 코드", sort_with_desc(categories["test"]))
    util_section = render_section("9. 유틸리티 / 보조 스크립트", sort_with_desc(categories["utility"]))

    data_parquet_section = render_section(
        "10. 데이터 파일 목록 (.parquet)",
        [make_described(p, root_dir, desc_map) for p in sorted(categories["data_parquet"])],
    )
    data_model_section = render_section(
        "11. 모델 파일 목록 (.pkl)",
        [make_described(p, root_dir, desc_map) for p in sorted(categories["data_model"])],
    )

    footer = """끝.
이 파일은 F:\\autostockG\\pipeline 내에서 언제든 다시 생성할 수 있습니다.

- 생성 스크립트 : auto_generate_pipeline_doc.py
- 위치          : F:\\autostockG\\pipeline\\auto_generate_pipeline_doc.py
"""

    return (
        header
        + "\n"
        + summary_section
        + core_section
        + hoj_section
        + sle_section
        + rest_section
        + pipe_section
        + test_section
        + util_section
        + data_parquet_section
        + data_model_section
        + footer
    )


# ------------------------------------------------------------
# 7. 메인 엔트리
# ------------------------------------------------------------

def main() -> None:
    script_path = Path(__file__).resolve()
    pipeline_dir = script_path.parent
    root_dir = pipeline_dir.parent

    if not root_dir.exists():
        raise SystemExit(f"[오류] 루트 폴더를 찾을 수 없습니다: {root_dir}")

    output_path = get_output_path(pipeline_dir)

    print(f"[INFO] 루트 폴더 스캔 시작: {root_dir}")
    categories = scan_project(root_dir, pipeline_dir)
    print("[INFO] 스캔 완료, 파일 설명 자동 생성 중...")

    desc_map = build_descriptions(categories, root_dir)

    print("[INFO] 리포트 생성 중...")
    report_text = build_report(root_dir, pipeline_dir, categories, desc_map, output_path.name)

    output_path.write_text(report_text, encoding="utf-8")
    print(f"[SAVE] 파이프라인 리포트 저장 완료 → {output_path}")


if __name__ == "__main__":
    main()
