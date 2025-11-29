# ============================================================
# run_top10_unified_simple.py
# - 순차 1→2→3→4→5→6→7 실행
# - 날짜 태그/백업/중복 방지 등은 각 스크립트 내부에 위임
# - 필수 스크립트만 직렬 호출
# ============================================================

import os
import sys
import time
import subprocess

# ------------------------------------------------------------
# 경로 셋업: MODELENGINE 경로 확인 + get_path 보조
# ------------------------------------------------------------
CUR = os.path.abspath(os.path.dirname(__file__))        # 이 파일 위치(즉: MODELENGINE 폴더)
ROOT = os.path.dirname(CUR)                             # 프로젝트 루트( daily_recommender.py 가 여기에 있을 수도 있음 )

if CUR not in sys.path:
    sys.path.append(CUR)

try:
    # 기준: MODELENGINE/UTIL/config_paths.py
    from UTIL.config_paths import get_path
except Exception:
    # 예외: get_path 불러오기 실패 시, F 드라이브 기본값(로컬 환경)
    def get_path(*parts):
        base = r"F:\autostockG\MODELENGINE"
        clean = [str(p).strip() for p in parts if str(p).strip()]
        return os.path.join(base, *clean)

# ------------------------------------------------------------
# 공통 실행 함수
# ------------------------------------------------------------
def run_py(py_path, name):
    """주요 파이썬 스크립트 실행. 경로 없으면 SKIP."""
    if not py_path:
        print(f"[{name}] 경로 없음 → SKIP")
        return True
    if not os.path.exists(py_path):
        print(f"[{name}] 파일 없음 → SKIP  ({py_path})")
        return True

    print("\n" + "=" * 64)
    print(f"[RUN-{name}] {py_path}")
    print("=" * 64)
    start = time.time()
    try:
        # capture_output=False 로 설정해 입력 프롬프트/실시간 로그가 그대로 표시되도록 함
        res = subprocess.run([sys.executable, py_path], text=True)
        if res.returncode != 0:
            print(f"[{name}] 실패 (returncode={res.returncode})")
            return False
        print(f"[{name}] 완료 ✔ {time.time()-start:.2f}s")
        return True
    except Exception as e:
        print(f"[{name}] 예외 발생: {e}")
        return False

def pick_first_existing(candidates):
    """후보 경로 리스트에서 실제 존재하는 첫 번째를 반환. 없으면 None."""
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None

# ------------------------------------------------------------
# 단계별 스크립트 경로 후보
# ------------------------------------------------------------
# 1) RAW 수집
RAW_CAND = [
    get_path("RAW", "raw_patch.py"),
    os.path.join(CUR, "RAW", "raw_patch.py"),
]

# 2) KOSPI 지수갱신
KOSPI_CAND = [
    get_path("RAW", "make_kospi_index_10y.py"),
    os.path.join(CUR, "RAW", "make_kospi_index_10y.py"),
]

# 3) FEATURE 생성
FEATURE_CAND = [
    get_path("UTIL", "build_features.py"),
    os.path.join(CUR, "UTIL", "build_features.py"),
    os.path.join(CUR, "build_features.py"),
]

# 4) DB(unified) 생성
DB_CAND = [
    get_path("UTIL", "build_unified_db.py"),
    get_path("build_unified_db.py"),
    os.path.join(CUR, "UTIL", "build_unified_db.py"),
    os.path.join(CUR, "build_unified_db.py"),
]

# 5) 엔진(unified) 학습
ENGINE_CAND = [
    get_path("UTIL", "train_engine_unified.py"),
    get_path("train_engine_unified.py"),
    os.path.join(CUR, "UTIL", "train_engine_unified.py"),
    os.path.join(CUR, "train_engine_unified.py"),
]

# 6) Top10 생성 (daily_recommender.py가 루트/UTIL 등에 있을 수 있음)
TOP10_CAND = [
    os.path.join(ROOT, "UTIL", "daily_recommender.py"),
    get_path("UTIL", "daily_recommender.py"),
    os.path.join(CUR, "UTIL", "daily_recommender.py"),
]

# ------------------------------------------------------------
# 실행
# ------------------------------------------------------------
def main():
    print("=== MODELENGINE 통합 실행기 (SIMPLE) 시작 ===")
    print("※ 순서 고정 실행: 1→2→3→4→5→6 (지능형 로직 없음)")

    steps = [
        ("RAW",     pick_first_existing(RAW_CAND)),
        ("KOSPI",   pick_first_existing(KOSPI_CAND)),
        ("FEATURE", pick_first_existing(FEATURE_CAND)),
        ("DB",      pick_first_existing(DB_CAND)),
        ("ENGINE",  pick_first_existing(ENGINE_CAND)),
        ("TOP10",   pick_first_existing(TOP10_CAND)),
    ]

    ok = True
    for name, path in steps:
        if not run_py(path, name):
            ok = False
            # 필요 시 중단하려면 아래 주석 해제
            # break

    print("\n=== MODELENGINE 통합 실행기 (SIMPLE) 종료 ===")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
