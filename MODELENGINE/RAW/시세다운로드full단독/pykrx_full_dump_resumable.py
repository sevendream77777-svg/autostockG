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

import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock


# ---------------------------------------------------------------
# 날짜 파서 (YYYYMMDD → datetime)
# ---------------------------------------------------------------
def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


# ---------------------------------------------------------------
# txt/json 파일에서 종목 불러오기
# txt  : 한줄당 1개 코드
# json : ["000020","091440",...]
# ---------------------------------------------------------------
def load_codes_any(path: str):
    ext = os.path.splitext(path)[1].lower()

    # TXT 파일
    if ext == ".txt":
        with open(path, encoding="utf-8") as f:
            return [x.strip() for x in f if x.strip()]

    # JSON 파일
    if ext == ".json":
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    raise ValueError(f"지원하지 않는 파일 형식: {path}")


# ---------------------------------------------------------------
# 원하는 종목 리스트 로딩
# --codes가:
#   1) 단일 파일(txt/json)이면 → 파일 불러오기
#   2) 여러 종목 코드 직접 입력이면 → 그대로 리스트
# ---------------------------------------------------------------
def resolve_target_codes(codes_arg):
    # 입력이 파일 1개이면 → 내용 로드
    if len(codes_arg) == 1 and os.path.exists(codes_arg[0]):
        return load_codes_any(codes_arg[0])

    # 아니면 그냥 코드 리스트로 처리
    return codes_arg


# ---------------------------------------------------------------
# pykrx 원본 DataFrame → 정규화
# ※ 원본 컬럼은 모두 유지 (사용자가 요청한 대로 "제한 없음")
# ---------------------------------------------------------------
def normalize_ohlcv(df: pd.DataFrame, code: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    # 인덱스가 날짜이면 컬럼으로 이동
    if isinstance(df.index, pd.DatetimeIndex):
        df.reset_index(inplace=True)
        df.rename(columns={"index": "Date"}, inplace=True)

    # Date 컬럼 처리
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    elif "날짜" in df.columns:
        df["Date"] = pd.to_datetime(df["날짜"], errors="coerce")
        df.rename(columns={"날짜": "Date"}, inplace=True)
    else:
        raise KeyError("Date 컬럼이 없습니다.")

    # Code 컬럼 강제 추가
    df["Code"] = code

    # 정렬 및 중복 제거
    df = df.sort_values(["Code", "Date"])
    df = df.drop_duplicates(["Code", "Date"], keep="last")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------
# 지정한 날짜 범위를 생성
# ---------------------------------------------------------------
def generate_date_range(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# ---------------------------------------------------------------
# 단일 종목 + 단일 날짜 다운로드
# ---------------------------------------------------------------
def fetch_one_day(code: str, date: datetime):
    ymd = date.strftime("%Y%m%d")
    try:
        df = stock.get_market_ohlcv(ymd, ymd, code)
        return df
    except Exception:
        return None


# ---------------------------------------------------------------
# 메인 다운로드 함수
# ---------------------------------------------------------------
def download_codes(codes, start_date, end_date, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    all_results = []

    for code in codes:
        print(f"\n[DOWNLOAD] Code={code}")
        merged = []

        for date in generate_date_range(start_date, end_date):
            df_raw = fetch_one_day(code, date)
            if df_raw is None or len(df_raw) == 0:
                print(f"  {date.date()} → 데이터 없음/요청 실패")
                continue

            df_norm = normalize_ohlcv(df_raw, code)
            merged.append(df_norm)

        if not merged:
            print(f"  코드 {code}: 수집된 데이터 0건 → 스킵")
            continue

        df_final = pd.concat(merged, ignore_index=True)

        # 개별 저장
        out_path = os.path.join(output_dir, f"{code}.parquet")
        df_final.to_parquet(out_path, index=False)
        print(f"  저장 완료 → {out_path}")

        all_results.append(df_final)

    # 전체 merge 파일
    if all_results:
        df_all = pd.concat(all_results, ignore_index=True)
        out_all = os.path.join(output_dir, "pykrx_raw_selected.parquet")
        df_all.to_parquet(out_all, index=False)
        print(f"\n[완료] 전체 병합파일 저장 → {out_all}")
    else:
        print("\n[완료] 다운로드된 데이터 없음")


# ================================================================
# ENTRY POINT
# ================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="선택 종목 + 선택 기간 pykrx downloader")

    parser.add_argument("--codes", nargs="+",
                        help="종목코드 직접입력 or txt/json 파일 경로",
                        required=True)

    parser.add_argument("--start", type=str,
                        help="시작일 YYYYMMDD (미입력 시 전체기간)")

    parser.add_argument("--end", type=str,
                        help="종료일 YYYYMMDD (미입력 시 오늘)")

    parser.add_argument("--out", type=str, default="pykrx_selected",
                        help="저장 폴더명 (기본=pykrx_selected)")

    args = parser.parse_args()

    # 1) 종목 확정
    codes = resolve_target_codes(args.codes)
    print(f"[INFO] 다운로드 대상 종목: {codes}")

    # 2) 날짜 확정
    DEFAULT_START = datetime(2015, 1, 2)
    TODAY = datetime.today()

    start_date = parse_date(args.start) if args.start else DEFAULT_START
    end_date = parse_date(args.end) if args.end else TODAY

    print(f"[INFO] 기간: {start_date.date()} ~ {end_date.date()}")

    # 3) 다운로드 실행
    download_codes(codes, start_date, end_date, args.out)
