# auto_raw_updater_v4.py (통합 스크립트)

import os
import sys
from datetime import datetime, timedelta

import pandas as pd

# V3가 필요로 하는 핵심 함수들을 builder_v2에서 가져옵니다.
# V4는 V2, V3의 기능을 모두 수행합니다.
try:
    from safe_raw_builder_v2 import (
        RAW_MAIN, log,
        load_all_codes, fetch_ohlcv_multi_source
    )
    from safe_raw_patch_v3 import normalize_numeric_series, fetch_single_day_multi
    
except ImportError as e:
    # 경로 설정을 위해 sys.path를 조정합니다.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.abspath(os.path.join(current_dir, '..', 'RAW'))) 
    
    # 재시도 (수정 필요 시 이 부분 확인)
    from safe_raw_builder_v2 import (
        RAW_MAIN, log,
        load_all_codes, fetch_ohlcv_multi_source
    )
    from safe_raw_patch_v3 import normalize_numeric_series, fetch_single_day_multi
    
    log(f"⚠️ 임포트 경로 문제로 sys.path를 수정했습니다: {e}")
    
# ----------------------------------------------------------------------
# 보조 함수: RAW 파일의 가장 최신 날짜를 찾는 함수
# ----------------------------------------------------------------------
def get_latest_raw_date(raw_path: str) -> Optional[datetime.date]:
    """메인 RAW 파일에서 가장 최신 날짜를 추출합니다."""
    if not os.path.exists(raw_path):
        return None
    
    try:
        # Date 컬럼만 읽어 메모리 절약
        df = pd.read_parquet(raw_path, columns=["Date"])
        df["Date"] = pd.to_datetime(df["Date"])
        return df["Date"].max().date()
    except Exception as e:
        log(f"[ERROR] RAW 파일({raw_path}) 읽기 실패: {e}")
        return None


# ----------------------------------------------------------------------
# ⭐ 메인 자동 업데이트 및 병합 함수 (V4) ⭐
# ----------------------------------------------------------------------
def auto_update_raw():
    log("===== V4: 자동 RAW 업데이트 및 병합 시작 =====")
    
    if not os.path.exists(RAW_MAIN):
        log(f"[ERROR] 메인 RAW 파일 없음 ({RAW_MAIN}). 전체 구축(build_raw_all)이 먼저 필요합니다.")
        return

    # 1. 현재 RAW의 최신 날짜 확인
    latest_date = get_latest_raw_date(RAW_MAIN)
    if latest_date is None:
        log("[FATAL] RAW 파일이 비었거나 날짜를 찾을 수 없습니다.")
        return
        
    log(f"[INFO] 현재 RAW 최신 날짜: {latest_date}")

    # 2. 수집 시작 날짜 설정 (최신 날짜의 다음 날)
    start_date_to_fetch = latest_date + timedelta(days=1)
    today = datetime.now().date()
    
    # 수집할 날짜 목록 생성
    fetch_dates = []
    current_date = start_date_to_fetch
    while current_date < today:
        fetch_dates.append(current_date)
        current_date += timedelta(days=1)

    if not fetch_dates:
        log("[INFO] 업데이트할 새로운 날짜가 없습니다. 작업 종료.")
        return

    log(f"[INFO] 수집할 날짜 범위: {fetch_dates[0]} ~ {fetch_dates[-1]} ({len(fetch_dates)}일)")

    # 3. 데이터 수집 (V3 로직) 및 병합 준비
    codes = load_all_codes()
    all_new_data = []
    
    for date_obj in fetch_dates:
        date_str = date_obj.strftime("%Y-%m-%d")
        log(f"\n[FETCH] 날짜: {date_str} 데이터 수집 시작...")
        
        all_rows_for_day = []
        n_success = 0
        
        for code in codes:
            # fetch_single_day_multi 함수 사용 (V3 로직)
            df_day, status = fetch_single_day_multi(code, date_obj)
            
            if status == "success" and df_day is not None and not df_day.empty:
                all_rows_for_day.append(df_day)
                n_success += 1
        
        if n_success > 0 and all_rows_for_day:
            full_day_df = pd.concat(all_rows_for_day, ignore_index=True)
            log(f"[SUCCESS] {date_str}: {n_success}개 종목 데이터 수집 성공.")
            all_new_data.append(full_day_df)
        elif n_success == 0:
            log(f"[INFO] {date_str}: 거래일 아님 또는 수집 실패로 건너뜀.")

    if not all_new_data:
        log("[INFO] 수집된 새로운 데이터가 없습니다. 작업 종료.")
        return
        
    # 4. 기존 RAW 로드 및 새로운 데이터와 병합 (V2 로직 통합)
    df_main = pd.read_parquet(RAW_MAIN)
    frames = [df_main] + all_new_data
    
    merged = pd.concat(frames, ignore_index=True)
    
    # ⬇️⬇️ 1단계 해결: 중복 제거 로직 (중복된 날짜의 이전 데이터를 제거) ⬇️⬇️
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged["Code"] = merged["Code"].astype(str).str.zfill(6)
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep='last')
    
    # 5. 최종 정리 및 저장
    merged = merged.dropna(subset=["Date", "Code"])
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)

    # 기존 RAW 백업 (필요 시 backup_existing_raw 함수 호출 추가)
    
    merged.to_parquet(RAW_MAIN)
    log(f"\n🎉 [완료] RAW 최종 업데이트 완료.")
    log(f"       최신 날짜: {merged['Date'].max().date()}, 총 행수: {len(merged):,}")
    log("===== 자동 RAW 업데이트 완료 =====")


if __name__ == "__main__":
    auto_update_raw()