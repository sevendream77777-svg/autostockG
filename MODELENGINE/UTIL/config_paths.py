# ================================================================
# config_paths.py (FINAL / UNIFIED VERSION)
# 모든 파이프라인이 공통으로 사용하는 경로 시스템
# HOJ / SLE / RAW / FEATURE / DB / ENGINE / LOG 등에 100% 호환
# ================================================================

import os

# MODELENGINE 루트 경로 (절대경로 고정)
BASE = r"F:\autostockG\MODELENGINE"


# ---------------------------------------------------------------
# 핵심: 가변 인자를 받아 경로를 유연하게 조합하는 get_path
# 예: get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
#     get_path("FEATURE", "HOJ", "features_V21C.parquet")
#     get_path("RAW", "all_stocks_cumulative.parquet")
#     get_path("LOG")
# ---------------------------------------------------------------
def get_path(*parts):
    """
    *parts: ("FOlder", "Subfolder", "File") 자유 조합
    BASE 아래에서 parts를 순서대로 이어 붙여 최종 경로 생성
    """
    # parts가 비었으면 BASE 자체 반환
    if not parts:
        return BASE

    # 문자열 변환 + 공백 제거
    clean_parts = [str(p).strip() for p in parts if str(p).strip()]

    # BASE + 모든 parts 결합
    return os.path.join(BASE, *clean_parts)


# ---------------------------------------------------------------
# 백업 파일명 생성용 (옵션)
# 예: "file.parquet" → "file_250117.parquet"
# ---------------------------------------------------------------
def versioned_filename(path):
    base, ext = os.path.splitext(path)
    from datetime import datetime
    today = datetime.now().strftime("%y%m%d")
    return f"{base}_{today}{ext}"


# ---------------------------------------------------------------
# LOG 디렉토리 보조 함수 (선택적으로 사용)
# ---------------------------------------------------------------
def get_log_path(filename=None):
    log_dir = get_path("LOG")
    if filename:
        return os.path.join(log_dir, filename)
    return log_dir
