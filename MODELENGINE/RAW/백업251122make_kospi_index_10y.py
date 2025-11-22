# ============================================================
# make_kospi_index_10y.py — (Final Directory Fix)
# [기능]
# 1. KOSPI 지수 수집 (FDR -> Yahoo -> Pykrx 3중 백업)
# 2. [저장 경로] RAW/kospi_data/kospi_data.parquet (단일 경로 고정)
# 3. [오염 방지] 장 마감(16:00) 전에는 백업(Rename) 생략
# ============================================================

import sys
import os
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, time, timedelta

# ------------------------------------------------------------
# Path 설정
# ------------------------------------------------------------
current_script_path = os.path.abspath(__file__)
raw_dir_path = os.path.dirname(current_script_path)
modelengine_dir_path = os.path.dirname(raw_dir_path)

if modelengine_dir_path not in sys.path:
    sys.path.append(modelengine_dir_path)

try:
    from UTIL.config_paths import get_path, versioned_filename
except ImportError:
    def get_path(*args):
        return os.path.join(modelengine_dir_path, *args)
    def versioned_filename(path):
        base, ext = os.path.splitext(path)
        return f"{base}_backup{ext}"

# ------------------------------------------------------------
# 수집 함수 정의
# ------------------------------------------------------------
def fetch_by_fdr_naver(start_str, end_str):
    print(f"   [1순위] FinanceDataReader (Naver) 시도...")
    df = fdr.DataReader('KS11', start_str, end_str)
    if df is None or df.empty: raise Exception("FDR 데이터 없음")
    return df.reset_index()

def fetch_by_yfinance(start_str, end_str):
    print(f"   [2순위] yfinance 백업 시도 (^KS11)...")
    import yfinance as yf
    df = yf.download("^KS11", start=start_str, end=end_str, progress=False)
    if df is None or df.empty: raise Exception("yfinance 데이터 없음")
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
    if 'Adj Close' in df.columns: df = df.rename(columns={'Adj Close': 'Close'})
    return df

def fetch_by_pykrx(start_str, end_str):
    print(f"   [3순위] pykrx 예비 서버 시도 (1001)...")
    from pykrx import stock
    s_date = start_str.replace("-", "")
    e_date = end_str.replace("-", "")
    df = stock.get_index_ohlcv(s_date, e_date, "1001")
    if df is None or df.empty: raise Exception("pykrx 데이터 없음")
    df = df.reset_index()
    if '날짜' not in df.columns and 'Date' not in df.columns: df.columns.values[0] = 'Date'
    return df

# ------------------------------------------------------------
# 메인 실행 함수
# ------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("[KOSPI] 10년치 지수 수집 (kospi_data 폴더 저장)")
    print("=" * 60)

    # [경로 수정] 사장님 지시대로 'kospi_data' 폴더 안에만 저장
    target_dir = get_path("RAW", "kospi_data")
    os.makedirs(target_dir, exist_ok=True)
    target_path = os.path.join(target_dir, "kospi_data.parquet")

    # 2. 날짜 및 안내 메시지 로직
    now = datetime.now()
    market_close_time = time(16, 0) # 4시 기준
    is_market_open = now.time() < market_close_time
    
    target_end = now
    query_end = target_end + timedelta(days=1)
    start_date = target_end - timedelta(days=365 * 11)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = query_end.strftime('%Y-%m-%d')

    if is_market_open:
        standard_date = now - timedelta(days=1)
        print(f"🕒 현재 {now.strftime('%H:%M')} (장마감 전) -> '전일({standard_date.strftime('%Y-%m-%d')})' 기준 + 실시간 시세 수집")
    else:
        print(f"🕒 현재 {now.strftime('%H:%M')} (장마감 후) -> '금일({now.strftime('%Y-%m-%d')})' 마감 데이터 업데이트")

    # 기존 파일이 목표일 이상이면 스킵 (불필요한 재다운로드 방지)
    if os.path.exists(target_path):
        try:
            df_existing = pd.read_parquet(target_path, columns=["Date"])
            if "Date" in df_existing.columns and not df_existing.empty:
                latest_existing = pd.to_datetime(df_existing["Date"]).max().date()
                target_date = standard_date.date()
                if latest_existing >= target_date:
                    print(f"⚠️ 기존 kospi_data가 목표일({target_date})까지 포함 → 다운로드 건너뜁니다.")
                    return
        except Exception:
            pass  # 읽기 실패 시 그대로 진행

    # 3. 수집
    df_final = None
    try: df_final = fetch_by_fdr_naver(start_str, end_str)
    except Exception:
        try: df_final = fetch_by_yfinance(start_str, end_str)
        except Exception:
            try: df_final = fetch_by_pykrx(start_str, end_str)
            except Exception: pass

    if df_final is not None and not df_final.empty:
        rename_map = {'종가': 'Close', '날짜': 'Date'}
        df_final = df_final.rename(columns=rename_map)
        if 'Close' not in df_final.columns and '종가' in df_final.columns:
             df_final = df_final.rename(columns={'종가': 'Close'})
        if 'Date' in df_final.columns:
            df_final['Date'] = pd.to_datetime(df_final['Date'])
            df_final = df_final.sort_values('Date')
            if 'Close' in df_final.columns:
                df_final = df_final[['Date', 'Close']]

        last_date_obj = df_final['Date'].iloc[-1]
        last_date_str = last_date_obj.strftime('%Y-%m-%d')
        
        print("-" * 40)
        print(f"✅ 데이터 수집 완료 (최신: {last_date_str})")

        # 백업 여부 결정
        is_today_included = (last_date_obj.date() == now.date())
        do_backup = True
        
        if is_today_included and is_market_open:
            print(f"⚠️ [알림] 장 마감 전입니다. 데이터 오염 방지를 위해 백업(파일명 변경)은 생략합니다.")
            do_backup = False
        else:
            print(f"✅ 확정 데이터이므로 기존 파일을 백업합니다.")

        print("-" * 40)

        # (1) 백업 수행
        if do_backup and os.path.exists(target_path):
            try:
                backup_path = versioned_filename(target_path)
                os.rename(target_path, backup_path)
                print(f"📦 [백업] {os.path.basename(target_path)} -> {os.path.basename(backup_path)}")
            except Exception as e: print(f"⚠️ 백업 에러: {e}")

        # (2) 저장 (경로 대신 파일명만 출력)
        df_final.to_parquet(target_path, index=False)
        print(f"💾 [저장 완료] {os.path.basename(target_path)} (경로: RAW/kospi_data/)")
        
    else:
        print("\n❌ [실패] 모든 소스 수집 실패")

if __name__ == "__main__":
    main()
