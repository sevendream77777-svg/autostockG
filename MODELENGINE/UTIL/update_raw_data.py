# ============================================================
# update_raw_data.py (MODELENGINE RAW 관리 - 백업 & 버전생성)
# - 파일 위치: F:\autostockG\MODELENGINE\RAW\stocks\all_stocks_cumulative.parquet
# - 기능: 
#   1. 해당 경로에 파일이 있는지 확인
#   2. 작업 전 안전을 위해 타임스탬프 백업 생성 (YYYYMMDD_HHMMSS)
#   3. 날짜별 버전 관리 파일 생성 (YYMMDD)
#   4. 불필요한 루트 폴더 복사 방지 (stocks 폴더 단일 관리)
# ============================================================
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import shutil
import datetime
import pandas as pd

from config_paths import get_path, versioned_filename

# ★ 핵심 수정: SOURCE와 TARGET을 모두 'stocks' 폴더 내부로 통일
# 실제 데이터가 위치해야 할 단일 경로입니다.
TARGET_FILE = get_path("RAW", "stocks", "all_stocks_cumulative.parquet")
SOURCE_FILE = TARGET_FILE  # 외부에서 다운로드해 오는 게 아니라면, 현 위치를 원본으로 간주

def backup_timestamp(path: str) -> None:
    """파일을 타임스탬프(YYYYMMDD_HHMMSS) 이름으로 복사해 백업합니다."""
    if not os.path.exists(path):
        return
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir, name = os.path.split(path)
    root, ext = os.path.splitext(name)
    
    # 백업 파일명 예: all_stocks_cumulative_20251119_193000.parquet
    backup_name = f"{root}_{ts}{ext}"
    backup_path = os.path.join(base_dir, backup_name)
    
    shutil.copy2(path, backup_path)
    print(f"[RAW 백업] 안전 백업 생성 완료: {backup_name}")

def log_parquet_info(path: str, label: str = "RAW") -> None:
    """Parquet 파일의 행/열/기간 정보를 출력합니다."""
    if not os.path.exists(path):
        print(f"[ERR] 파일을 찾을 수 없습니다: {path}")
        return
        
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"[WARN] {label} 파일 로드 실패: {path} ({e})")
        return

    if "Date" not in df.columns or "Code" not in df.columns:
        print(f"[WARN] {label} 필수 컬럼(Date, Code) 누락")
        return

    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"])

    min_date = df["Date"].min()
    max_date = df["Date"].max()
    n_rows = len(df)
    n_codes = df["Code"].nunique()

    print(f"[{label}] 파일 정보 요약")
    print(f"  - 경로: {path}")
    print(f"  - 기간: {min_date.date()} ~ {max_date.date()}")
    print(f"  - 규모: {n_rows:,} 행 / {n_codes:,} 종목")

def main():
    print("="*60)
    print("[RAW 단계] 시세 데이터 버전 관리 및 점검 시작")
    print(f"  TARGET FILE: {TARGET_FILE}")
    print("="*60)

    if not os.path.exists(TARGET_FILE):
        # 파일 자체가 없다면 아무것도 할 수 없음
        raise FileNotFoundError(f"[ERR] 원본 파일을 찾을 수 없습니다: {TARGET_FILE}\n      경로를 확인하거나 데이터를 먼저 생성해주세요.")

    # 1. 작업 전 타임스탬프 백업 (덮어쓰기 사고 방지용)
    backup_timestamp(TARGET_FILE)

    # 2. 외부 소스 복사 로직 (필요 시 활성화)
    # 만약 '다운로드 폴더'에서 'stocks 폴더'로 가져오는 경우라면 여기에 copy 로직을 넣지만,
    # 지금은 이미 stocks 폴더에 파일이 있다고 가정하므로 패스합니다.
    if SOURCE_FILE != TARGET_FILE and os.path.exists(SOURCE_FILE):
        print(f"[INFO] 외부 소스에서 최신본 복사 중... ({SOURCE_FILE} -> {TARGET_FILE})")
        shutil.copy2(SOURCE_FILE, TARGET_FILE)
    else:
        print("[INFO] 현 위치의 파일을 기준으로 버전 관리를 수행합니다. (복사 생략)")

    # 3. 날짜(YYMMDD) 기반 버전 백업 (이력 관리용)
    # 예: all_stocks_cumulative_251119.parquet 생성
    version_path = versioned_filename(TARGET_FILE)
    
    # 오늘 날짜 버전이 이미 있어도, 내용이 바뀌었을 수 있으므로 덮어쓰기 복사 수행
    shutil.copy2(TARGET_FILE, version_path)
    print(f"[RAW 버전] 날짜별 버전 파일 생성/갱신: {os.path.basename(version_path)}")

    # 4. 최종 파일 정보 로그
    print("-" * 40)
    log_parquet_info(TARGET_FILE, label="STOCKS_RAW")
    print("-" * 40)
    print("[완료] RAW 데이터 점검 및 백업 종료.")

if __name__ == "__main__":
    main()