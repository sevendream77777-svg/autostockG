# full_update_pipeline_FM.py
# FM version: date-based pipeline controller using pipeline_date_manager
# Keeps console style similar to existing pipeline; no modifications to original files.

import os, sys, subprocess, time
from datetime import datetime

CUR = os.path.dirname(__file__)
PARENT = os.path.dirname(CUR)
if PARENT not in sys.path:
    sys.path.append(PARENT)

from pipeline_date_manager import plan
try:
    from config_paths import get_path
except Exception:
    def get_path(*parts):
        base = r"F:\autostockG\MODELENGINE"
        clean = [str(p).strip() for p in parts if str(p).strip()]
        return os.path.join(base, *clean)

def run_py(path):
    print(f"[RUN] {path}")
    res = subprocess.run([sys.executable, path], capture_output=True, text=True)
    print(res.stdout, end="")
    if res.returncode != 0:
        print(res.stderr, end="")
        raise RuntimeError(f"Script failed: {path}")
    return True

def pretty(d):
    return d if d is not None else "None"

def main():
    print("=== MODELENGINE Top10 Pipeline (FM) 시작 ===")
    st = time.time()

    # 0. RAW/KOSPI 업데이트 단계는 기존 파이프라인 스크립트에 위임
    #    (필요 시 별도 수행; 여기서는 항상 실행해도 안전)
    raw_script = get_path("RAW", "raw_patch.py")
    if os.path.exists(raw_script):
        print("\n[RAW] 업데이트 시도")
        try:
            run_py(raw_script)
        except Exception as e:
            print(f"[RAW] 경고: {e}")

    kospi_script = get_path("RAW", "make_kospi_index_10y.py")
    if os.path.exists(kospi_script):
        print("\n[KOSPI] 업데이트 시도")
        try:
            run_py(kospi_script)
        except Exception as e:
            print(f"[KOSPI] 경고: {e}")

    # 1. 기준 날짜 판단
    state = plan()
    s = state["snapshot"]
    need = state["need"]

    print("\n============================================================")
    print("[FM] 날짜 스냅샷")
    print(f"  RAW 최신일    : {pretty(s['RAW_latest'])}")
    print(f"  KOSPI 최신일  : {pretty(s['KOSPI_latest'])}")
    print(f"  공통 최신일   : {pretty(s['COMMON_latest'])}")
    print(f"  FEATURE 최신일: {pretty(s['FEATURE_latest'])}")
    print(f"  DB_REAL 최신일: {pretty(s['DB_REAL_latest'])}")
    print(f"  DB_RESR 최신일: {pretty(s['DB_RESEARCH_latest'])}")
    print(f"  ENG_REAL 최신일: {pretty(s['ENGINE_REAL_latest'])}")
    print(f"  ENG_RESR 최신일: {pretty(s['ENGINE_RESEARCH_latest'])}")

    # 2. FEATURE
    feat_script = get_path("UTIL", "build_features.py")
    if need["FEATURE"]:
        print("\n[FEATURE] 업데이트 필요 → 실행")
        run_py(feat_script)
    else:
        print("\n[FEATURE] 최신 상태 → SKIP")

    # Recompute after feature step (DB/Engine decisions depend on feature date)
    state = plan()
    s = state["snapshot"]
    need = state["need"]

    # 3. DBs
    db_real_script = get_path("UTIL", "build_HOJ_DB_REAL.py")
    db_res_script = get_path("UTIL", "build_HOJ_DB_RESEARCH.py")
    if need["DB_REAL"]:
        print("\n[DB REAL] 업데이트 필요 → 실행")
        run_py(db_real_script)
    else:
        print("\n[DB REAL] 최신 상태 → SKIP")
    if need["DB_RESEARCH"]:
        print("\n[DB RESEARCH] 업데이트 필요 → 실행")
        run_py(db_res_script)
    else:
        print("\n[DB RESEARCH] 최신 상태 → SKIP")

    # Recompute after DB step
    state = plan()
    s = state["snapshot"]
    need = state["need"]

    # 4. ENGINES
    eng_real_script = get_path("UTIL", "train_HOJ_ENGINE_REAL.py")
    eng_res_script = get_path("UTIL", "train_HOJ_ENGINE_RESEARCH.py")
    if need["ENGINE_REAL"]:
        print("\n[ENGINE REAL] 업데이트 필요 → 실행")
        run_py(eng_real_script)
    else:
        print("\n[ENGINE REAL] 최신 상태 → SKIP")
    if need["ENGINE_RESEARCH"]:
        print("\n[ENGINE RESEARCH] 업데이트 필요 → 실행")
        run_py(eng_res_script)
    else:
        print("\n[ENGINE RESEARCH] 최신 상태 → SKIP")

    et = time.time() - st
    print(f"\n=== MODELENGINE Top10 Pipeline (FM) 완료 — 소요시간: {et:0.2f}s ===")

if __name__ == "__main__":
    main()