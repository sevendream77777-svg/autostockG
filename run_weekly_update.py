# run_weekly_update.py
# 'V34 (호슬) 프로젝트'의 '오프라인 연구소' 주간 자동 업데이트 마스터 스크립트
# UI의 [업데이트] 버튼 클릭 시 이 파일을 실행합니다.

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import subprocess
import sys
import os
from datetime import datetime

def get_python_executable():
    """현재 실행 중인 Python 인터프리터 경로를 반환합니다."""
    # 가상 환경(venv) 등을 사용하는 경우 'sys.executable'이 정확합니다.
    return sys.executable

def run_script(script_name: str, python_exec: str):
    """지정된 Python 스크립트를 실행하고 성공/실패를 반환합니다."""
    
    # -----------------------------------------------------------------
    # 중요: 모든 스크립트가 동일한 '기준 디렉토리'에서 실행되어야
    # .parquet, .pkl, .py 파일 경로를 올바르게 찾을 수 있습니다.
    # -----------------------------------------------------------------
    # 이 스크립트(run_weekly_update.py)가 있는 폴더를 기준 디렉토리로 설정합니다.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    script_path = os.path.join(base_dir, script_name)
    
    if not os.path.exists(script_path):
        print(f"[{script_name}] ❌ 실패: 파일을 찾을 수 없습니다. (경로: {script_path})")
        return False
        
    print(f"=================================================")
    print(f"[{script_name}] ▶️ 실행 시작...")
    print(f"=================================================")
    
    try:
        # subprocess.run을 사용하여 스크립트를 실행하고,
        # 출력을 실시간으로 스트리밍하며, 오류 발생 시 즉시 중단(check=True)합니다.
        # cwd=base_dir : 스크립트의 '작업 디렉토리'를 base_dir로 설정합니다.
        process = subprocess.run(
            [python_exec, script_path], 
            check=True, 
            cwd=base_dir,
            text=True, 
            encoding='utf-8',
            stdout=sys.stdout, # 표준 출력을 실시간으로 UI나 콘솔에 표시
            stderr=sys.stderr  # 표준 오류를 실시간으로 UI나 콘솔에 표시
        )
        
        print(f"=================================================")
        print(f"[{script_name}] ✅ 성공")
        print(f"=================================================\n")
        return True
        
    except subprocess.CalledProcessError as e:
        # 스크립트 실행 중 오류가 발생한 경우
        print(f"=================================================")
        print(f"[{script_name}] ❌ 실패: 스크립트 실행 중 오류 발생.")
        print(f"오류 코드: {e.returncode}")
        print(f"=================================================\n")
        return False
    except Exception as e:
        # 기타 예외 (파일 못찾음 등)
        print(f"=================================================")
        print(f"[{script_name}] ❌ 실패: 예상치 못한 오류 발생.")
        print(f"오류: {e}")
        print(f"=================================================\n")
        return False

def main_pipeline():
    """Hoj 엔진 주간 재학습 파이프라인을 순차적으로 실행합니다."""
    
    start_time = datetime.now()
    print(f"--- Hoj 엔진 주간 재학습 파이프라인 시작 --- ({start_time.strftime('%Y-%m-%d %H:%M:%S')})\n")
    
    python_exec = get_python_executable()
    print(f"사용될 Python 실행 파일: {python_exec}\n")

    # 파이프라인 스크립트 목록 (실행 순서가 매우 중요)
    pipeline_scripts = [
        # 1. 최신 시세 데이터를 다운로드하여 'all_stocks_cumulative.parquet'에 추가
        "update_data_incrementally.py",
        
        # 2. 'all_stocks...'를 읽어 12개 피처를 계산하고 'all_features_cumulative_V21_Hoj.parquet'로 저장
        "update_features_incrementally.py",
        
        # 3. 'V21 DB'와 'ticker_map'을 병합하여 'V25_Hoj_DB.parquet' 생성
        "build_database_V25.py",
        
        # 4. 'V25 DB'를 기반으로 Hoj AI '뇌'('REAL_CHAMPION_MODEL_V25.pkl')를 재학습
        "train_real_engine_V25.py",
        
        # 5. 재학습된 '뇌'와 'DB'를 사용하여 '오늘의 Top 10' (recommendation...csv) 생성
        "daily_recommender.py"
    ]
    
    for script in pipeline_scripts:
        success = run_script(script, python_exec)
        if not success:
            print(f"--- ❗ 파이프라인 중단: [{script}] 실행 실패 ---")
            break
    
    end_time = datetime.now()
    print(f"--- Hoj 엔진 주간 재학습 파이프라인 종료 --- ({end_time.strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"총 소요 시간: {end_time - start_time}")

if __name__ == "__main__":
    main_pipeline()