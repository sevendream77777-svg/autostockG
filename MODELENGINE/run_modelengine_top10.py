"""
End-to-end MODELENGINE pipeline runner.

순서:
1) RAW 시세 패치 (RAW/raw_patch.py)
2) KOSPI 지수 업데이트 (RAW/make_kospi_index_10y.py)
3) 피처 생성 (UTIL/full_update_pipeline.py)
4) HOJ DB (RESEARCH/REAL) 생성
5) RESEARCH/REAL 엔진 학습
6) Top10 추천 (repo 루트 daily_recommender.py)
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # MODELENGINE/
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
PYTHON = sys.executable
FEATURE_FILE = os.path.join(BASE_DIR, "FEATURE", "features_V31.parquet")
FEATURE_STEP_NAME = "피처 생성"
DEPENDENT_STEPS = {
    "HOJ DB (RESEARCH)",
    "HOJ DB (REAL)",
    "HOJ ENGINE RESEARCH 학습",
    "HOJ ENGINE REAL 학습",
}


def build_steps() -> List[Tuple[str, str]]:
    """경로 목록을 정의."""
    return [
        ("RAW 시세 패치", os.path.join(BASE_DIR, "RAW", "raw_patch.py")),
        ("KOSPI 지수 업데이트", os.path.join(BASE_DIR, "RAW", "make_kospi_index_10y.py")),
        ("피처 생성", os.path.join(BASE_DIR, "UTIL", "full_update_pipeline.py")),
        ("HOJ DB (RESEARCH)", os.path.join(BASE_DIR, "UTIL", "build_HOJ_DB_RESEARCH.py")),
        ("HOJ DB (REAL)", os.path.join(BASE_DIR, "UTIL", "build_HOJ_DB_REAL.py")),
        ("HOJ ENGINE RESEARCH 학습", os.path.join(BASE_DIR, "UTIL", "train_HOJ_ENGINE_RESEARCH.py")),
        ("HOJ ENGINE REAL 학습", os.path.join(BASE_DIR, "UTIL", "train_HOJ_ENGINE_REAL.py")),
        ("Top10 추천", os.path.join(BASE_DIR, "UTIL", "daily_recommender.py")),
    ]


def run_step(name: str, script_path: str) -> bool:
    """단일 스텝 실행."""
    if not os.path.exists(script_path):
        print(f"[SKIP] {name} - 스크립트를 찾을 수 없습니다: {script_path}")
        return False

    start = datetime.now()
    print("=" * 60)
    print(f"[{name}] 실행 시작 - {script_path}")
    print("=" * 60)

    try:
        subprocess.run(
            [PYTHON, script_path],
            check=True,
            cwd=os.path.dirname(script_path),
        )
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] {name} 실패 (returncode={exc.returncode})")
        return False

    duration = datetime.now() - start
    print(f"[DONE] {name} 완료 (소요시간: {duration})\n")
    return True


def main() -> None:
    print("\n=== MODELENGINE Top10 Pipeline 시작 ===\n")
    features_updated = False

    for step_name, script in build_steps():
        if step_name in DEPENDENT_STEPS and not features_updated:
            print(f"[SKIP] {step_name} - 피처 업데이트가 없어 건너뜁니다.")
            continue

        before_mtime = None
        if step_name == FEATURE_STEP_NAME and os.path.exists(FEATURE_FILE):
            before_mtime = os.path.getmtime(FEATURE_FILE)

        ok = run_step(step_name, script)
        if not ok:
            print(f"\n[STOP] '{step_name}' 단계에서 중단되었습니다.\n")
            break

        if step_name == FEATURE_STEP_NAME:
            if os.path.exists(FEATURE_FILE):
                after_mtime = os.path.getmtime(FEATURE_FILE)
                features_updated = before_mtime is None or after_mtime != before_mtime
            else:
                features_updated = False
    else:
        print("\n=== MODELENGINE Top10 Pipeline 완료 ===\n")


if __name__ == "__main__":
    main()
