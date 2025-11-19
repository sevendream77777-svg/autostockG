import os
import glob
import json
from config_paths import BASE, HOJ_ENGINE_REAL

ENGINE_DIR = os.path.join(BASE, "HOJ_ENGINE", "REAL")
OVERRIDE_FILE = os.path.join(ENGINE_DIR, ".current_engine")


def get_engine_list():
    if not os.path.exists(ENGINE_DIR):
        return []
    files = glob.glob(os.path.join(ENGINE_DIR, "HOJ_ENGINE_REAL_*.pkl"))
    return sorted(files, key=os.path.getmtime, reverse=True)


def load_override_engine():
    if os.path.exists(OVERRIDE_FILE):
        try:
            with open(OVERRIDE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            eng = data.get("engine")
            if eng and os.path.exists(eng):
                return eng
        except Exception:
            pass
    return None


def get_current_engine():
    # 1) override 우선
    eng = load_override_engine()
    if eng:
        return eng
    # 2) config_paths 기본 엔진
    if HOJ_ENGINE_REAL and os.path.exists(HOJ_ENGINE_REAL):
        return HOJ_ENGINE_REAL
    # 3) 폴더 최신 엔진
    engines = get_engine_list()
    if engines:
        return engines[0]
    return None
