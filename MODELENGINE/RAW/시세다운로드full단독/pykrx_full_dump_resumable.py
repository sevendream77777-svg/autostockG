# ================================================================
# pykrx_full_dump_resumable_v2.py
# ---------------------------------------------------------------
# 기능 요약:
# 1) 원하는 종목만 수집 가능 (코드 직접 입력 / txt / json)
# 2) 원하는 날짜만 수집 가능 (하루 / 특정일 / 날짜 범위)
# 3) 전체 RAW 를 pykrx 에서 가져와 parquet 로 저장
# 4) 원본 pykrx_full_dump_resumable.py 의 장점(재시도/로그/병합) 유지
# 5) 사용법:
#    예) 특정종목 하루:
#       python pykrx_full_dump_resumable_v2.py --codes 000020 --start 20250101 --end 20250101
#
#    예) txt 파일의 종목 리스트 범위 다운로드:
#       python pykrx_full_dump_resumable_v2.py --codes targets.txt --start 20240101 --end 20240131
#
#    예) json 파일의 종목 리스트 전체 기간:
#       python pykrx_full_dump_resumable_v2.py --codes targets.json
#
# ================================================================
# ================================================================
# pykrx_full_dump_resumable_v2.py (Modified)
# ---------------------------------------------------------------
# 기능 추가:
# - --columns 인자 지원 (원하는 컬럼만 저장)
# - 한글 컬럼 -> 영문 표준 컬럼명 자동 매핑
# ================================================================

# ================================================================
# pykrx_full_dump_resumable_v2.py (Modified V3)
# ---------------------------------------------------------------
# [추가 기능]
# - --codes 인자 생략 시, KOSPI/KOSDAQ 전체 종목 자동 다운로드
# ================================================================

import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import sys

# ---------------------------------------------------------------
# 컬럼 매핑 (한글 -> 영문)
# ---------------------------------------------------------------
COLUMN_MAP = {
    "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close",
    "거래량": "Volume", "거래대금": "Amount", "등락률": "Change",
    "날짜": "Date"
}

def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")

def load_codes_any(path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".txt":
        with open(path, encoding="utf-8") as f:
            return [x.strip() for x in f if x.strip()]
    if ext == ".json":
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    raise ValueError(f"지원하지 않는 파일 형식: {path}")

# ---------------------------------------------------------------
# [수정됨] 타겟 코드 결정 로직
# ---------------------------------------------------------------
def resolve_target_codes(codes_arg):
    # 1. codes_arg가 없으면 전체 종목 가져오기
    if not codes_arg:
        print("[INFO] 종목 코드가 지정되지 않아 KOSPI/KOSDAQ 전체 종목을 대상으로 합니다.")
        today = datetime.today().strftime("%Y%m%d")
        try:
            kospi = stock.get_market_ticker_list(today, market="KOSPI")
            kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
            all_tickers = sorted(list(set(kospi + kosdaq))) # 중복 제거 및 정렬
            print(f"[INFO] 총 {len(all_tickers)}개 종목 발견")
            return all_tickers
        except Exception as e:
            print(f"[ERROR] 전체 종목 리스트 조회 실패: {e}")
            sys.exit(1)

    # 2. 파일 경로인 경우 로드
    if len(codes_arg) == 1 and os.path.exists(codes_arg[0]):
        return load_codes_any(codes_arg[0])
    
    # 3. 코드 리스트인 경우 그대로 반환
    return codes_arg

def normalize_ohlcv(df: pd.DataFrame, code: str, target_columns: list = None) -> pd.DataFrame:
    if df is None or len(df) == 0: return pd.DataFrame()
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex):
        df.reset_index(inplace=True)
        if df.columns[0] == "날짜": df.rename(columns={"날짜": "Date"}, inplace=True)
        elif "index" in df.columns: df.rename(columns={"index": "Date"}, inplace=True)
        elif "Date" not in df.columns: df.rename(columns={df.columns[0]: "Date"}, inplace=True)
    if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.rename(columns=COLUMN_MAP, inplace=True)
    df["Code"] = code
    if target_columns:
        essential = ["Date", "Code"]
        cols_to_keep = essential + [c for c in target_columns if c in df.columns and c not in essential]
        df = df[cols_to_keep]
    df = df.sort_values(["Code", "Date"]).drop_duplicates(["Code", "Date"], keep="last")
    return df.reset_index(drop=True)

def generate_date_range(start: datetime, end: datetime):
    cur = start
    while cur <= end: yield cur; cur += timedelta(days=1)

def fetch_one_day(code: str, date: datetime):
    ymd = date.strftime("%Y%m%d")
    try: return stock.get_market_ohlcv(ymd, ymd, code)
    except Exception: return None

def download_codes(codes, start_date, end_date, output_dir, columns):
    os.makedirs(output_dir, exist_ok=True)
    all_results = []
    print(f"[CONFIG] 저장할 컬럼: {columns if columns else 'ALL (Original)'}")
    for i, code in enumerate(codes):
        print(f"\n[{i+1}/{len(codes)}] 다운로드 진행: {code}")
        merged = []
        for date in generate_date_range(start_date, end_date):
            df_raw = fetch_one_day(code, date)
            if df_raw is None or len(df_raw) == 0: continue
            df_norm = normalize_ohlcv(df_raw, code, columns)
            merged.append(df_norm)
        if not merged:
            print(f"  -> 데이터 없음, 건너뜀")
            continue
        df_final = pd.concat(merged, ignore_index=True)
        out_path = os.path.join(output_dir, f"{code}.parquet")
        df_final.to_parquet(out_path, index=False)
        print(f"  -> 저장 완료 ({len(df_final)}행): {os.path.basename(out_path)}")
        all_results.append(df_final)
    
    if all_results and len(codes) > 1:
        try:
            df_all = pd.concat(all_results, ignore_index=True)
            out_all = os.path.join(output_dir, "pykrx_raw_combined.parquet")
            df_all.to_parquet(out_all, index=False)
            print(f"\n[완료] 통합 파일 저장: {out_all}")
        except Exception as e:
             print(f"\n[경고] 통합 파일 생성 실패 (메모리 부족 등): {e}")

# ================================================================
# Main
# ================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # [수정] required=False로 변경 (미지정 시 전체 종목)
    parser.add_argument("--codes", nargs="+", required=False, help="종목코드 리스트 or 파일경로")
    parser.add_argument("--start", type=str, help="시작일 YYYYMMDD")
    parser.add_argument("--end", type=str, help="종료일 YYYYMMDD")
    parser.add_argument("--out", type=str, default="pykrx_selected", help="저장 경로")
    parser.add_argument("--columns", nargs="+", help="저장할 컬럼명 리스트")

    args = parser.parse_args()

    # 타겟 코드 결정
    codes = resolve_target_codes(args.codes)
    
    start_date = parse_date(args.start) if args.start else datetime(2020, 1, 1)
    end_date = parse_date(args.end) if args.end else datetime.today()

    print(f"=== [Custom Downloader V3] ===")
    print(f"대상 종목 수: {len(codes)}개")
    print(f"기간: {start_date.date()} ~ {end_date.date()}")
    
    download_codes(codes, start_date, end_date, args.out, args.columns)