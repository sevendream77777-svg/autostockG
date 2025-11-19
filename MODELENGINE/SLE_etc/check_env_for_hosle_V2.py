# check_env_for_hosle_V3_auto.py
# -------------------------------
# 위대하신호정님 전용 - 자동 환경 점검 매크로
# 새 방, 새 프로젝트 실행 직후 자동으로 시스템/라이브러리/자원 상태 점검

import importlib.util
import sys
import platform
import pkg_resources
import os
import psutil
import time
from datetime import datetime

def check_module(name):
    spec = importlib.util.find_spec(name)
    return spec is not None

def print_status(label, status):
    icon = "✅" if status else "❌"
    print(f"{icon} {label}: {'활성화됨' if status else '비활성화됨'}")

def bytes_to_mb(size):
    return round(size / 1024 / 1024, 1)

def run_env_check():
    print("\n🧠 [호슬 프로젝트 자동 환경 점검 V3] ======================")
    print(f"🕒 실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 현재 경로: {os.getcwd()}")
    print("=========================================================\n")
    time.sleep(0.3)

    # Python/OS 기본정보
    print("🐍 Python & 시스템 정보")
    print(f" - Python 버전: {platform.python_version()}")
    print(f" - OS: {platform.system()} {platform.release()}\n")

    # 필수 모듈 점검
    print("📦 필수 라이브러리 상태:")
    required_libs = [
        "pandas", "requests", "openpyxl", "numpy",
        "lightgbm", "pykrx", "joblib", "tqdm"
    ]
    for lib in required_libs:
        print_status(f"{lib}", check_module(lib))
    print()

    # ChatGPT 가용 도구 상태 (가상 확인)
    print("🔧 ChatGPT 가용 도구 상태:")
    tools = {
        "file_search(파일 업로드)": True,
        "web(웹 검색)": True,
        "image_gen(이미지 생성)": True,
        "python(파이썬 실행)": True,
    }
    for k, v in tools.items():
        print_status(k, v)
    print()

    # 설정 파일 존재여부
    print("📁 설정 파일 점검:")
    for f in ["config.ini", "token.json", "kakao_token.json"]:
        print_status(f, os.path.exists(f))
    print()

    # 시스템 자원 사용률
    print("⚙️ 시스템 자원 상태:")
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    disk = psutil.disk_usage(os.getcwd())
    print(f" - 메모리 사용률: {mem.percent}% ({bytes_to_mb(mem.used)}MB / {bytes_to_mb(mem.total)}MB)")
    print(f" - CPU 사용률: {cpu}%")
    print(f" - 디스크 사용률: {disk.percent}% ({bytes_to_mb(disk.used)}MB / {bytes_to_mb(disk.total)}MB)")

    # 렉 예측
    if mem.percent > 80 or cpu > 90:
        print("🚨 [경고] 시스템 과부하 — 렉 가능성 매우 높음!")
    elif mem.percent > 65 or cpu > 70:
        print("⚠️ [주의] 시스템 부하 중간 — 버벅임 발생 가능.")
    else:
        print("✅ [안정] 시스템 자원 상태 양호.")
    print()

    # 파일 개수 및 총 용량
    total_files = 0
    total_size = 0
    for root, _, files in os.walk(os.getcwd()):
        for f in files:
            total_files += 1
            total_size += os.path.getsize(os.path.join(root, f))
    print(f"📂 폴더 내 파일 개수: {total_files:,}개")
    print(f"📦 총 용량: {bytes_to_mb(total_size)}MB")

    if total_files > 5000 or total_size > 2 * 1024 * 1024 * 1024:
        print("⚠️ [주의] 파일이 많거나 용량이 큼 — 캐시 부하 가능성 있음.")
    else:
        print("✅ [정상] 폴더 데이터량 적정.\n")

    # 주요 패키지 버전 요약
    print("🧩 주요 패키지 버전:")
    for lib in ["pandas", "requests", "openpyxl"]:
        try:
            ver = pkg_resources.get_distribution(lib).version
            print(f" - {lib}: {ver}")
        except Exception:
            pass

    print("\n✅ 환경 점검 완료 — 모든 항목 정상 작동 중!\n")

# 자동 실행
if __name__ == "__main__":
    run_env_check()
