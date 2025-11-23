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
        # 1. 날짜 태그 없는 legacy 파일 있는지 확인
        legacy_path = os.path.join(directory, f"{prefix}{extension}")
        if os.path.exists(legacy_path):
            print(f"  ※ 최신 버전(prefix_YYMMDD) 없음. 기존 파일 사용: {os.path.basename(legacy_path)}")
            return legacy_path
        return None

    # 파일명 정렬 → 가장 마지막(최신)
    latest_file = sorted(files)[-1]
    return latest_file

def save_dataframe_with_date(df, base_dir, file_prefix, date_col="Date", extension=".parquet"):
    """
    DF 내부 최대 날짜(Max Date)를 읽어 파일명에 YYMMDD 태그로 저장.
    파일 중복 시 _1, _2 시퀀스 생성.
    """
    os.makedirs(base_dir, exist_ok=True)
    
    # 1. 기본 날짜 태그 = 오늘
    date_tag = get_timestamp()

    # DF에서 날짜 추출
    if date_col in df.columns and not df.empty:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                temp_dates = pd.to_datetime(df[date_col], errors='coerce')
                max_date = temp_dates.max()
            else:
                max_date = df[date_col].max()
            
            if pd.notnull(max_date):
                date_tag = max_date.strftime("%y%m%d")
        except Exception as e:
            print(f"  ※ 날짜 태그 생성 실패 (기본 날짜 사용): {e}")

    # 기존 동일 날짜 파일이 있는지 확인 → 있다면 SKIP
    try:
        existing = glob.glob(os.path.join(base_dir, f"{file_prefix}_{date_tag}*{extension}"))
        if existing:
            latest_existing = sorted(existing)[-1]
            try:
                df_prev = pd.read_parquet(latest_existing, columns=[date_col])
            except Exception:
                df_prev = pd.read_parquet(latest_existing)
            if date_col in df_prev.columns and not df_prev.empty:
                prev_date = pd.to_datetime(df_prev[date_col], errors="coerce").max()
                if pd.notnull(prev_date) and prev_date.strftime("%y%m%d") == date_tag:
                    print(f"  ▶ [SKIP] 동일 날짜({date_tag}) 파일 존재: {os.path.basename(latest_existing)}")
                    return latest_existing
    except Exception as e:
        print(f"  ※ 기존 파일 확인 오류: {e}")

    # 2. 파일명 기본 경로
    base_filename = f"{file_prefix}_{date_tag}"
    save_path = os.path.join(base_dir, f"{base_filename}{extension}")

    # 3. 중복 파일 처리 (_1, _2…)
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
        print(f"※ 저장 실패: {e}")
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

    # 중복 방지
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
