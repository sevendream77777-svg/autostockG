# ============================================
# version_utils.py
# 버전 파일 / 날짜 기반 셀렉터 (V32 스펙 업데이트)
# ============================================

import os
import shutil
import datetime
import glob
import pandas as pd  # 날짜 확인용

def get_timestamp():
    """현재 날짜 타임스탬프 (예: 251123)"""
    return datetime.datetime.now().strftime("%y%m%d")

# -----------------------------------------------------------
# [V32 추가] 최신 날짜 태그 기반 파일 검색
# -----------------------------------------------------------

def find_latest_file(directory, prefix, extension=".parquet"):
    """
    directory 내에서 'prefix_YYMMDD*.extension' 패턴을 가진
    파일 중 가장 최신(정렬 기준 마지막) 파일을 반환한다.
    예: features_V31_251123_1.parquet
    """
    search_pattern = os.path.join(directory, f"{prefix}_*{extension}")
    files = glob.glob(search_pattern)
    
    if not files:
        # 날짜 없는 레거시 무시 (패치)
        return None

    latest_file = sorted(files)[-1]
    return latest_file

# -----------------------------------------------------------
# [핵심] 날짜 정책 완전 패치
# FEATURE / HOJ_DB 파일 → 무조건 파일 내부 Date 마지막날짜로 저장
# 오늘 날짜 fallback, SKIP 로직 전부 제거
# -----------------------------------------------------------

def save_dataframe_with_date(df, base_dir, file_prefix, date_col="Date", extension=".parquet"):
    """
    DF 내부 최대 날짜(Max Date)를 읽어 파일명에 YYMMDD 태그로 저장.
    날짜 없는 파일 생성 금지.
    파일 중복 시 _1, _2 시퀀스 생성.
    """

    os.makedirs(base_dir, exist_ok=True)

    # 1. 날짜 태그 = 무조건 데이터 마지막 날짜
    if date_col not in df.columns or df.empty:
        raise ValueError(f"[ERROR] '{date_col}' 컬럼이 없어서 날짜 태그 생성 불가")

    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        temp_dates = pd.to_datetime(df[date_col], errors='coerce')
        max_date = temp_dates.max()
    else:
        max_date = df[date_col].max()

    if pd.isnull(max_date):
        raise ValueError("[ERROR] Date 컬럼에서 유효한 날짜를 찾을 수 없음")

    date_tag = max_date.strftime("%y%m%d")

    # 2. 파일명 생성
    base_filename = f"{file_prefix}_{date_tag}"
    save_path = os.path.join(base_dir, f"{base_filename}{extension}")

    # 3. 중복 처리 (_1, _2…)
    counter = 1
    while os.path.exists(save_path):
        save_path = os.path.join(base_dir, f"{base_filename}_{counter}{extension}")
        counter += 1
    
    # 4. 저장
    try:
        if extension == ".parquet":
            df.to_parquet(save_path, index=False)
        elif extension == ".csv":
            df.to_csv(save_path, index=False)

        print(f"  ▶ [저장 완료] {os.path.basename(save_path)} (Rows: {len(df):,}, Date: {date_tag})")
        return save_path
    except Exception as e:
        print(f"[ERROR] 저장 실패: {e}")
        return None

# -----------------------------------------------------------
# [유틸 함수] 기존 파일 백업
# -----------------------------------------------------------

def backup_existing_file(file_path, date_tag: str | None = None):
    """기존 파일이 있을 경우 백업 생성"""
    if not os.path.exists(file_path):
        return None

    dirname, filename = os.path.split(file_path)
    name, ext = os.path.splitext(filename)

    ts = (date_tag or _infer_parquet_date_tag(file_path) or get_timestamp())

    backup_name = f"{name}_{ts}{ext}"
    backup_path = os.path.join(dirname, backup_name)

    counter = 1
    while os.path.exists(backup_path):
        backup_name = f"{name}_{ts}_{counter}{ext}"
        backup_path = os.path.join(dirname, backup_name)
        counter += 1

    try:
        shutil.move(file_path, backup_path)
        print(f"  ▶ 백업 생성: {os.path.basename(backup_path)}")
    except Exception as e:
        print(f"  ※ 백업 이동 실패: {e}")
    return backup_path

def save_new_file(df, save_path):
    """(Deprecated) 신규 파일 저장 유틸 함수"""
    backup_existing_file(save_path)

    dirname = os.path.dirname(save_path)
    os.makedirs(dirname, exist_ok=True)

    df.to_parquet(save_path, index=False)
    print(f"  ▶ 새 파일 저장 완료: {save_path}")

def _infer_parquet_date_tag(file_path: str) -> str | None:
    """기존 parquet 파일에서 Date 컬럼을 읽어 YYMMDD 태그를 추출"""
    try:
        if os.path.exists(file_path) and file_path.lower().endswith((".parquet", ".pq")):
            try:
                df = pd.read_parquet(file_path, columns=["Date"])
            except:
                df = pd.read_parquet(file_path)
            
            if "Date" in df.columns and not df.empty:
                if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
                    dt_series = pd.to_datetime(df["Date"], errors="coerce")
                else:
                    dt_series = df["Date"]
                
                latest = dt_series.max()
                if pd.notnull(latest):
                    return latest.strftime("%y%m%d")
    except Exception:
        pass
    return None
