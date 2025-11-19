import os
import shutil
import datetime


# ------------------------------------------------------------
# 1) 기존 파일 백업 (기본 백업 기능)
# ------------------------------------------------------------
def backup_file(path):
    """path 파일이 존재하면 timestamp를 붙여 백업 생성"""
    if not os.path.exists(path):
        return None

    base, name = os.path.split(path)
    root, ext = os.path.splitext(name)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    new = os.path.join(base, f"{root}_{stamp}{ext}")
    shutil.copy2(path, new)
    return new


# ------------------------------------------------------------
# 2) 엔진 스크립트가 사용하는 백업 함수 (필수)
# ------------------------------------------------------------
def backup_existing_file(path: str):
    """
    train_HOJ / train_SLE 엔진에서 사용하는 백업 함수.
    기존 파일이 존재하면 timestamp 백업 생성.
    """
    if os.path.exists(path):
        try:
            return backup_file(path)
        except Exception:
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            fallback = f"{path}.{stamp}.bak"
            shutil.copy2(path, fallback)
            return fallback
    return None


# ------------------------------------------------------------
# 3) 날짜 버전 생성 함수 (필수)
#    예: original.parquet → original_251116.parquet
# ------------------------------------------------------------
def versioned_filename(original_path: str):
    """
    엔진 DB/모델 저장 시 날짜 태그 붙여 새 파일명을 만들어주는 함수.
    예: HOJ_DB_REAL.parquet → HOJ_DB_REAL_251116.parquet
    """
    base, name = os.path.split(original_path)
    root, ext = os.path.splitext(name)

    # YYMMDD 형식
    day_tag = datetime.datetime.now().strftime("%y%m%d")
    new_name = f"{root}_{day_tag}{ext}"

    return os.path.join(base, new_name)


# ------------------------------------------------------------
# 4) 신버전 파일 저장 함수 (기존 유지)
# ------------------------------------------------------------
def save_new_file(src_path: str, dst_path: str):
    """
    기존 dst_path가 있으면 backup_file()로 백업 후,
    src_path 파일을 새 버전으로 저장하는 함수.
    """
    # 목적지 폴더 생성
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    # 기존 파일 백업
    if os.path.exists(dst_path):
        try:
            backup_file(dst_path)
        except Exception:
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            shutil.copy2(dst_path, f"{dst_path}.{stamp}.bak")

    # 신규 파일 저장
    shutil.copy2(src_path, dst_path)
    print(f"[save_new_file] {src_path} → {dst_path} 저장 완료")
