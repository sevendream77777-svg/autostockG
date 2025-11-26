# ================================================================
# config_paths.py (FINAL / UNIFIED VERSION)
# 모든 파이프라인이 공통으로 사용하는 경로 시스템
# HOJ / SLE / RAW / FEATURE / DB / ENGINE / LOG 등에 100% 호환
# ================================================================

import os
from datetime import datetime

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
    """
    기존 파일 내용에 기반해 날짜 태그(YYMMDD)를 추출하고,
    동일한 이름이 존재하면 _1, _2 를 붙여 유일한 백업 경로를 반환한다.
    """
    base, ext = os.path.splitext(path)
    date_tag = _infer_data_tag(path, ext)
    candidate = f"{base}_{date_tag}{ext}"
    counter = 1

    while os.path.exists(candidate):
        candidate = f"{base}_{date_tag}_{counter}{ext}"
        counter += 1

    return candidate


def _infer_data_tag(path: str, ext: str) -> str:
    """
    파일 내 Date 컬럼(Parquet)을 우선 사용하고, 실패하면 수정 시각이나 현재 날짜를 반환.
    """
    if os.path.exists(path):
        if ext.lower() == ".parquet":
            try:
                import pandas as pd

                df = pd.read_parquet(path, columns=["Date"])
                if "Date" in df.columns:
                    latest = pd.to_datetime(df["Date"], errors="coerce").max()
                    if pd.notnull(latest):
                        return latest.strftime("%y%m%d")
            except Exception:
                pass

        try:
            ts = datetime.fromtimestamp(os.path.getmtime(path))
            return ts.strftime("%y%m%d")
        except Exception:
            pass

    return datetime.now().strftime("%y%m%d")


# ---------------------------------------------------------------
# LOG 디렉토리 보조 함수 (선택적으로 사용)
# ---------------------------------------------------------------
def get_log_path(filename=None):
    log_dir = get_path("LOG")
    if filename:
        return os.path.join(log_dir, filename)
    return log_dir
