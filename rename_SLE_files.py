import os
import re

BASE = r"F:\autostockG"

def get_latest_versioned_file(folder, prefix):
    full_path = os.path.join(BASE, folder)
    if not os.path.exists(full_path):
        return None

    latest_ver = -1
    latest_file = None

    for f in os.listdir(full_path):
        if f.startswith(prefix):
            m = re.search(r"_V(\d+)", f)
            if m:
                ver = int(m.group(1))
                if ver > latest_ver:
                    latest_ver = ver
                    latest_file = os.path.join(full_path, f)

    return latest_file


# -----------------------------
# HOJ 엔진 (연구 + 실전)
# -----------------------------
HOJ_DB_RESEARCH = get_latest_versioned_file(r"HOJ_DB\RESEARCH", "HOJ_DB_RESEARCH")
HOJ_DB_REAL     = get_latest_versioned_file(r"HOJ_DB\REAL", "HOJ_DB_REAL")

HOJ_ENGINE_RESEARCH = get_latest_versioned_file(r"HOJ_ENGINE\RESEARCH", "HOJ_ENGINE_RESEARCH")
HOJ_ENGINE_REAL     = get_latest_versioned_file(r"HOJ_ENGINE\REAL", "HOJ_ENGINE_REAL")


# -----------------------------
# SLE 엔진 (실전 전용)
# -----------------------------
# 연구용은 없으므로 항상 None
SLE_DB_RESEARCH = None
SLE_ENGINE_RESEARCH = None

# 실전 DB / 실전 엔진만 자동 탐색
SLE_DB_REAL     = get_latest_versioned_file(r"SLE_DB\REAL", "SLE_DB_REAL")
SLE_ENGINE_REAL = get_latest_versioned_file(r"SLE_ENGINE\REAL", "SLE_ENGINE_REAL")


# -----------------------------
# 디버그 출력
# -----------------------------
if __name__ == "__main__":
    print("\n==== HOJ ====")
    print("HOJ_DB_RESEARCH:", HOJ_DB_RESEARCH)
    print("HOJ_DB_REAL    :", HOJ_DB_REAL)
    print("HOJ_ENGINE_RESEARCH:", HOJ_ENGINE_RESEARCH)
    print("HOJ_ENGINE_REAL    :", HOJ_ENGINE_REAL)

    print("\n==== SLE ====")
    print("SLE_DB_RESEARCH:", SLE_DB_RESEARCH)
    print("SLE_DB_REAL    :", SLE_DB_REAL)
    print("SLE_ENGINE_RESEARCH:", SLE_ENGINE_RESEARCH)
    print("SLE_ENGINE_REAL    :", SLE_ENGINE_REAL)
