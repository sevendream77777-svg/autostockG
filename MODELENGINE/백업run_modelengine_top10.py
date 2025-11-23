
"""
End-to-end MODELENGINE pipeline runner.

순서:
1) RAW 시세 패치 (RAW/raw_patch.py)
2) KOSPI 지수 업데이트 (RAW/make_kospi_index_10y.py)
3) 피처 생성 (UTIL/build_features.py)   # [UPDATED-FM] full_update_pipeline.py 대신 직접 호출
4) HOJ DB (RESEARCH/REAL) 생성
5) RESEARCH/REAL 엔진 학습
6) Top10 추천 (repo 루트 daily_recommender.py)
"""

# [UPDATED-FM] 날짜 정석화 적용 버전
# - 기존 주석/출력 형식 최대한 유지
# - 필요 최소 수정 + 상세 안내 출력 강화
# - 의존 순서: RAW → KOSPI → FEATURE → DB(RESEARCH/REAL) → ENGINE(RESEARCH/REAL) → Top10
# - 'full_update_pipeline.py'를 호출하지 않음 (이 파일이 전체 오케스트레이터이므로 중복 방지)

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple, Optional

try:
    import pandas as pd
except Exception:
    pd = None  # 일부 환경에서 pandas 미설치 대응

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # MODELENGINE/
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
PYTHON = sys.executable

# 주요 산출물 파일 경로
FEATURE_FILE = os.path.join(BASE_DIR, "FEATURE", "features_V31.parquet")
DB_REAL_FILE = os.path.join(BASE_DIR, "HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")
DB_RESEARCH_FILE = os.path.join(BASE_DIR, "HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet")
ENGINE_REAL_FILE = os.path.join(BASE_DIR, "HOJ_ENGINE", "REAL", "HOJ_ENGINE_REAL_V31.pkl")
ENGINE_RESEARCH_FILE = os.path.join(BASE_DIR, "HOJ_ENGINE", "RESEARCH", "HOJ_ENGINE_RESEARCH_V31.pkl")

# 스크립트 경로
RAW_PATCH = os.path.join(BASE_DIR, "RAW", "raw_patch.py")
KOSPI_UPDATE = os.path.join(BASE_DIR, "RAW", "make_kospi_index_10y.py")
FEATURE_BUILD = os.path.join(BASE_DIR, "UTIL", "build_features.py")  # [UPDATED-FM]
DB_RESEARCH_BUILD = os.path.join(BASE_DIR, "UTIL", "build_HOJ_DB_RESEARCH.py")
DB_REAL_BUILD = os.path.join(BASE_DIR, "UTIL", "build_HOJ_DB_REAL.py")
ENGINE_RESEARCH_TRAIN = os.path.join(BASE_DIR, "UTIL", "train_HOJ_ENGINE_RESEARCH.py")
ENGINE_REAL_TRAIN = os.path.join(BASE_DIR, "UTIL", "train_HOJ_ENGINE_REAL.py")
TOP10_RECOMMENDER = os.path.join(BASE_DIR, "UTIL", "daily_recommender.py")

GEMINI_FILTER = os.path.join(BASE_DIR, "UTIL", "gemini_filter.py")

# ------------------------------
# [UPDATED-FM] 유틸: parquet Date.max() 읽기
# ------------------------------
def get_last_date_from_parquet(path: str) -> Optional[str]:
    """Parquet 파일에서 Date 컬럼의 최대 날짜(YYYY-MM-DD 문자열) 반환.
    실패 시 None 반환. pandas 없으면 mtime 기반 YYYY-MM-DD로 대체."""
    if not os.path.exists(path):
        return None
    # pandas가 없거나, 파일 읽기 실패시 mtime 사용
    if pd is None:
        try:
            ts = datetime.fromtimestamp(os.path.getmtime(path)).date().isoformat()
            return ts
        except Exception:
            return None
    try:
        # 빠른 컬럼 로드
        df = pd.read_parquet(path, columns=["Date"])
        if "Date" not in df.columns or len(df) == 0:
            return None
        dt = pd.to_datetime(df["Date"], errors="coerce").max()
        if pd.isna(dt):
            return None
        return str(dt.date())
    except Exception:
        # 전체 로드로 재시도
        try:
            df = pd.read_parquet(path)
            if "Date" not in df.columns or len(df) == 0:
                return None
            dt = pd.to_datetime(df["Date"], errors="coerce").max()
            if pd.isna(dt):
                return None
            return str(dt.date())
        except Exception:
            try:
                ts = datetime.fromtimestamp(os.path.getmtime(path)).date().isoformat()
                return ts
            except Exception:
                return None


def compare_dates(a: Optional[str], b: Optional[str]) -> int:
    """문자열 YYYY-MM-DD 비교. a > b => 1, a == b => 0, a < b => -1, None 안전 처리."""
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    return (1 if a > b else (0 if a == b else -1))


def run_step(name: str, script_path: str, extra_env: Optional[dict] = None) -> bool:
    """단일 스텝 실행."""
    if not os.path.exists(script_path):
        print(f"[SKIP] {name} - 스크립트를 찾을 수 없습니다: {script_path}")
        return False

    start = datetime.now()
    print("=" * 60)
    print(f"[{name}] 실행 시작 - {script_path}")
    print("=" * 60)

    try:
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        subprocess.run(
            [PYTHON, script_path],
            check=True,
            cwd=os.path.dirname(script_path),
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] {name} 실패 (returncode={exc.returncode})")
        return False

    duration = datetime.now() - start
    print(f"[DONE] {name} 완료 (소요시간: {duration})\n")
    return True


def main() -> None:
    print("\n=== MODELENGINE Top10 Pipeline (FM) 시작 ===\n")

    # ----------------------------------------------------------
    # 0) 시작 전 현황 스냅샷: FEATURE/DB/ENGINE의 마지막 날짜 읽기
    # ----------------------------------------------------------
    feature_before = get_last_date_from_parquet(FEATURE_FILE)
    db_real_before = get_last_date_from_parquet(DB_REAL_FILE)
    db_research_before = get_last_date_from_parquet(DB_RESEARCH_FILE)
    # 엔진은 메타가 없을 수 있으므로 파일 존재/mtime 기준으로만 참고
    engine_real_exists = os.path.exists(ENGINE_REAL_FILE)
    engine_research_exists = os.path.exists(ENGINE_RESEARCH_FILE)

    print("[상태] 시작 시점 스냅샷:")
    print(f"  - FEATURE 마지막 날짜: {feature_before}")
    print(f"  - HOJ_DB REAL 마지막 날짜: {db_real_before}")
    print(f"  - HOJ_DB RESEARCH 마지막 날짜: {db_research_before}")
    print(f"  - ENGINE REAL 파일 존재: {engine_real_exists}")
    print(f"  - ENGINE RESEARCH 파일 존재: {engine_research_exists}")
    print()

    # ----------------------
    # 1) RAW 업데이트
    # ----------------------
    print("[RAW] 업데이트 시도")
    if not run_step("RAW 시세 패치", RAW_PATCH):
        print("[STOP] RAW 단계 실패로 파이프라인을 중단합니다.")
        return

    # ----------------------
    # 2) KOSPI 업데이트
    # ----------------------
    print("[KOSPI] 업데이트 시도")
    if not run_step("KOSPI 지수 업데이트", KOSPI_UPDATE):
        print("[STOP] KOSPI 단계 실패로 파이프라인을 중단합니다.")
        return

    # ----------------------
    # 3) FEATURE 생성 (정석)
    # ----------------------
    print("[FEATURE] 생성 시도 (정석: RAW/KOSPI 공통 최신일까지)")
    feature_before2 = get_last_date_from_parquet(FEATURE_FILE)
    if not run_step("피처 생성", FEATURE_BUILD):
        print("[STOP] FEATURE 단계 실패로 파이프라인을 중단합니다.")
        return
    feature_after = get_last_date_from_parquet(FEATURE_FILE)

    # FEATURE 갱신 여부 판단
    feature_updated = compare_dates(feature_after, feature_before2) == 1
    print(f"[판단] FEATURE 업데이트 여부: {feature_updated} (before={feature_before2}, after={feature_after})")

    # ----------------------
    # 4) HOJ DB (RESEARCH/REAL)
    # ----------------------
    # 규칙: FEATURE_last > DB_last 이면 재생성
    need_db_research = compare_dates(feature_after, db_research_before) == 1
    need_db_real = compare_dates(feature_after, db_real_before) == 1

    if need_db_research:
        print("[DB/RESEARCH] FEATURE가 더 최신 → DB(Research) 재생성")
        if not run_step("HOJ DB (RESEARCH)", DB_RESEARCH_BUILD):
            print("[STOP] DB(Research) 단계 실패로 파이프라인을 중단합니다.")
            return
        db_research_after = get_last_date_from_parquet(DB_RESEARCH_FILE)
        print(f"[결과] HOJ_DB_RESEARCH 마지막 날짜: {db_research_after}")
    else:
        print("[SKIP] HOJ DB (RESEARCH) - FEATURE와 동일/과거라 건너뜁니다.")

    if need_db_real:
        print("[DB/REAL] FEATURE가 더 최신 → DB(Real) 재생성")
        if not run_step("HOJ DB (REAL)", DB_REAL_BUILD):
            print("[STOP] DB(Real) 단계 실패로 파이프라인을 중단합니다.")
            return
        db_real_after = get_last_date_from_parquet(DB_REAL_FILE)
        print(f"[결과] HOJ_DB_REAL 마지막 날짜: {db_real_after}")
    else:
        print("[SKIP] HOJ DB (REAL) - FEATURE와 동일/과거라 건너뜁니다.")

    # ----------------------
    # 5) HOJ ENGINE 학습 (RESEARCH/REAL)
    # ----------------------
    # 규칙: ENGINE_last < DB_last 이면 재학습
    # 엔진 마지막 날짜는 메타가 없으므로 DB_after 기준으로 판정
    db_research_latest = get_last_date_from_parquet(DB_RESEARCH_FILE)
    db_real_latest = get_last_date_from_parquet(DB_REAL_FILE)

    need_engine_research = False
    if db_research_latest is not None:
        # 엔진이 없거나, 엔진의 갱신 근거가 DB보다 과거라고 판단되면 학습
        need_engine_research = True  # [단순화/FM] DB 갱신되면 재학습, 아니면 유지
    if feature_updated is False and not need_db_research:
        # FEATURE/DB 둘 다 안 바뀌었으면 엔진 유지
        need_engine_research = False

    need_engine_real = False
    if db_real_latest is not None:
        need_engine_real = True
    if feature_updated is False and not need_db_real:
        need_engine_real = False

    if need_engine_research:
        print("[ENGINE/RESEARCH] DB(Research) 최신 반영 → 재학습")
        if not run_step("HOJ ENGINE RESEARCH 학습", ENGINE_RESEARCH_TRAIN):
            print("[STOP] ENGINE(Research) 단계 실패로 파이프라인을 중단합니다.")
            return
    else:
        print("[SKIP] HOJ ENGINE RESEARCH 학습 - DB 변경 없음")

    if need_engine_real:
        print("[ENGINE/REAL] DB(Real) 최신 반영 → 재학습")
        if not run_step("HOJ ENGINE REAL 학습", ENGINE_REAL_TRAIN):
            print("[STOP] ENGINE(Real) 단계 실패로 파이프라인을 중단합니다.")
            return
    else:
        print("[SKIP] HOJ ENGINE REAL 학습 - DB 변경 없음")

    # ----------------------
    # 6) Top10 추천
    # ----------------------
    print("[Top10] 예측/추천 실행")
    if not run_step("Top10 추천", TOP10_RECOMMENDER):
        print("[STOP] Top10 단계 실패로 파이프라인을 중단합니다.")
        return

# (▼ 여기서부터 추가하세요) -----------------------------------------
    # 7) Gemini AI 필터링 (최종 검증)
    # ----------------------
    print("[Gemini] Top10 종목 뉴스/악재 정밀 타격 분석")
    # API 키가 있는지 확인하는 로직은 gemini_filter.py 내부에 있음
    if not run_step("Gemini AI 전략가", GEMINI_FILTER):
        print("[WARNING] Gemini 분석 실패 (API 키 확인 필요), 하지만 기본 추천은 완료됨.")
    # -----------------------------------------------------------------

    print("\n=== MODELENGINE Top10 Pipeline (FM) 완료 ===\n")


if __name__ == "__main__":
    main()
