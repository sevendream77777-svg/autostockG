import os
import glob

# ============================================
# 🔧 기본 BASE 경로 (프로젝트 루트)
# ============================================
BASE = r"F:\autostockG"


# ============================================
# 🔍 최신 파일 자동 탐색 함수
# ============================================
def find_latest(folder, pattern):
    files = glob.glob(os.path.join(folder, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


# ============================================
# 📁 HOJ DB (REAL)
# ============================================
HOJ_DB_REAL = find_latest(
    os.path.join(BASE, "HOJ_DB", "REAL"),
    "HOJ_DB_REAL_*.parquet"
)


# ============================================
# 🧠 HOJ 엔진 (REAL)
# ============================================
HOJ_ENGINE_REAL = find_latest(
    os.path.join(BASE, "HOJ_ENGINE", "REAL"),
    "HOJ_ENGINE_REAL_*.pkl"
)


# ============================================
# 📁 SLE DB (REAL)
# ============================================
SLE_DB_REAL = find_latest(
    os.path.join(BASE, "SLE_DB", "REAL"),
    "SLE_DB_REAL_*.parquet"
)


# ============================================
# 🧠 SLE 엔진 (REAL)
# ============================================
SLE_ENGINE_REAL = find_latest(
    os.path.join(BASE, "SLE_ENGINE", "REAL"),
    "*.pkl"
)


# ============================================
# 🔥 TOP10 파일 저장 경로
# ============================================
TOP10_DIR = os.path.join(BASE, "top10data")


# ============================================
# 📌 디버깅 용도 (직접 실행하면 경로 출력)
# ============================================
if __name__ == "__main__":
    print("BASE:", BASE)
    print("HOJ_DB_REAL:", HOJ_DB_REAL)
    print("HOJ_ENGINE_REAL:", HOJ_ENGINE_REAL)
    print("SLE_DB_REAL:", SLE_DB_REAL)
    print("SLE_ENGINE_REAL:", SLE_ENGINE_REAL)
    print("TOP10_DIR:", TOP10_DIR)
