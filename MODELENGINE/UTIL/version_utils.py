# ============================================
# version_utils.py
# 공통 백업 / 버전 관리 유틸
# ============================================

import os
import shutil
import datetime

def get_timestamp():
    """날짜 기반 타임스탬프 (예: 251116)"""
    return datetime.datetime.now().strftime("%y%m%d")

def backup_existing_file(file_path):
    """파일이 존재하면 백업본 생성"""
    if not os.path.exists(file_path):
        return None  # 백업할 필요 없음

    dirname, filename = os.path.split(file_path)
    name, ext = os.path.splitext(filename)
    ts = get_timestamp()

    # 새 백업 파일 이름
    backup_name = f"{name}_{ts}{ext}"
    backup_path = os.path.join(dirname, backup_name)

    counter = 1
    while os.path.exists(backup_path):
        backup_name = f"{name}_{ts}_{counter}{ext}"
        backup_path = os.path.join(dirname, backup_name)
        counter += 1

    shutil.move(file_path, backup_path)
    print(f"  🔄 기존파일 백업됨 → {backup_path}")
    return backup_path


def save_new_file(df, save_path):
    """새로운 파일 저장 시 백업 후 저장"""
    backup_existing_file(save_path)

    dirname = os.path.dirname(save_path)
    os.makedirs(dirname, exist_ok=True)

    df.to_parquet(save_path, index=False)
    print(f"  💾 새 파일 저장됨 → {save_path}")
