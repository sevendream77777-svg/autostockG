import os
import datetime

# MODELENGINE 루트
BASE = r"F:\\autostockG\\MODELENGINE"

def get_path(component, mode, filename):
    return os.path.join(BASE, component, mode, filename)

def get_raw_path(filename):
    return os.path.join(BASE, "RAW", filename)

def get_feature_path(filename):
    return os.path.join(BASE, "FEATURE", filename)

def get_util_path(filename):
    return os.path.join(BASE, "util", filename)

def versioned_filename(path):
    """파일명에 날짜 붙여 백업 경로 생성"""
    base, ext = os.path.splitext(path)
    today = datetime.datetime.now().strftime("%y%m%d")
    return f"{base}_{today}{ext}"
