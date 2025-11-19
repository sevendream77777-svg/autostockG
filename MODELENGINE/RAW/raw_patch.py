# raw_patch.py  (V5)
# - KRX 일괄 수집(get_market_ohlcv_by_ticker)로 속도 개선
# - 부족분만 Naver Finance(siseJson)로 백업 수집
# - 시간대(16~20시) 경고 + 사용자의 Yes/No 인터랙션
# - RAW_MAIN 백업 규칙: all_stocks_cumulative_YYMMDD[_n].parquet
# - DAILY/LOGS 구조 유지

import os
import sys
import time
import math
import datetime as dt
from functools import lru_cache
from typing import Optional, Tuple, List, Iterable, Callable

import pandas as pd
from pykrx import stock
import requests


# ===== 경로 설정 =====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR = os.path.join(BASE_DIR, "stocks")
RAW_MAIN = os.path.join(STOCKS_DIR, "all_stocks_cumulative.parquet")
DAILY_DIR = os.path.join(STOCKS_DIR, "DAILY")
LOG_DIR = os.path.join(STOCKS_DIR, "LOGS")
OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# ===== 콘솔 UI 유틸 =====
def print_header():
    print("┌──────────────────────────────────────────────┐")
    print("│ 🎉 흰둥이와 함께하는 원본데이터 업데이트 (V5)│")
    print("└──────────────────────────────────────────────┘")
    print(f"[PATH] RAW_MAIN : {RAW_MAIN}")
    print(f"[PATH] DAILY    : {DAILY_DIR}")
    print(f"[PATH] LOGS     : {LOG_DIR}")
    print()


def log(msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def ask_yes_no(prompt: str) -> bool:
    while True:
        ans = input(f"{prompt} (y/n): ").strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("  → y 또는 n 으로만 입력해 주세요.")





def inline_progress(message: str):
    """한 줄에서 진행 상황을 갱신한다."""
    sys.stdout.write("\r" + message)
    sys.stdout.flush()


def end_inline_progress():
    sys.stdout.write("\n")
    sys.stdout.flush()


def _invalid_ohlcv_mask(df: pd.DataFrame) -> pd.Series:
    """OHLCV 중 NaN/0/음수를 포함한 row를 True로 반환합니다."""
    subset = df[OHLCV_COLS] if all(col in df.columns for col in OHLCV_COLS) else df
    return subset.isna().any(axis=1) | (subset <= 0).any(axis=1)


# ===== 날짜 관련 유틸 =====
def to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")



def parse_date(s: str) -> dt.date:
    # s: "YYYY-MM-DD" 또는 "YYYYMMDD"
    s = str(s)
    if "-" in s:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    return dt.datetime.strptime(s, "%Y%m%d").date()


@lru_cache(maxsize=512)
def _nearest_bizday_cached(date_ymd: str) -> str:
    """pykrx 거래일 API 결과를 캐시합니다."""
    return stock.get_nearest_business_day_in_a_week(date_ymd)


def is_trading_day(date: dt.date) -> bool:
    """pykrx 데이터 기준으로 영업일 여부를 판정합니다."""
    today = dt.date.today()

    # 당일은 장 종료 전까지 pykrx가 직전 영업일을 돌려주므로 주말만 제외하고 영업일로 본다
    if date == today:
        return date.weekday() < 5

    date_ymd = to_ymd(date)
    try:
        nearest = _nearest_bizday_cached(date_ymd)
    except Exception as e:
        log(f"[WARN] pykrx 날짜 확인 실패 - {e}. 주말 여부만으로 판정합니다.")
        return date.weekday() < 5
    return nearest == date_ymd


def get_next_bizdate(last_date: dt.date) -> dt.date:
    """주말/공휴일을 모두 건너뛰고 pykrx 영업일을 찾습니다."""
    d = last_date
    for _ in range(400):
        d = d + dt.timedelta(days=1)
        if is_trading_day(d):
            return d
    raise RuntimeError("다음 영업일을 1년 이내에서 찾지 못했습니다.")


def get_prev_bizdate(date: dt.date) -> dt.date:
    """주어진 날짜 이전의 가장 가까운 영업일."""
    d = date
    for _ in range(400):
        d = d - dt.timedelta(days=1)
        if is_trading_day(d):
            return d
    raise RuntimeError("이전 영업일을 1년 이내에서 찾지 못했습니다.")


def fetch_ohlcv_from_naver(code: str, date_ymd: str) -> Optional[dict]:
    """
    Naver Finance API (비공식)에서 해당 종목 하루 OHLCV 가져오기.
    https://api.finance.naver.com/siseJson.naver?symbol=005930&requestType=1&startTime=YYYYMMDD&endTime=YYYYMMDD&timeframe=day
    """
    url = (
        "https://api.finance.naver.com/siseJson.naver"
        f"?symbol={code}&requestType=1&startTime={date_ymd}&endTime={date_ymd}&timeframe=day"
    )
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://finance.naver.com/item/sise_day.nhn?code={code}",
    }

    try:
        r = requests.get(url, headers=headers, timeout=5)
        r.raise_for_status()
        text = r.text.strip()

        # 응답 포맷이 JS 배열 문자열이라 간단히 파싱
        # 예: [[날짜,시가,고가,저가,종가,거래량,외국인소진율], [...], ...]
        if not text or "[" not in text:
            return None

        # 맨 앞/뒤 대괄호 제거
        # 안전하게 eval 대신 pandas.read_json 등을 쓰고 싶지만,
        # 하루 한 줄만 필요하므로 간단 split 사용
        rows = text.split("],[")
        if len(rows) <= 1:
            return None

        # 첫 번째 실제 데이터는 rows[1] (rows[0]은 헤더)
        # 예: ["20190624", 45200, 45800, 45200, 45500, 6085066, 57.14
        data_part = rows[1]
        parts = data_part.replace("[", "").replace("]", "").split(",")

        date_str = parts[0].strip().replace('"', "").replace("'", "")
        open_p = float(parts[1])
        high_p = float(parts[2])
        low_p = float(parts[3])
        close_p = float(parts[4])
        volume = float(parts[5])

        return {
            "Date": dt.datetime.strptime(date_str, "%Y%m%d").date(),
            "Open": open_p,
            "High": high_p,
            "Low": low_p,
            "Close": close_p,
            "Volume": volume,
        }
    except Exception as e:
        log(f"[NAVER FAIL] {code} ({date_ymd}) - {e}")
        return None




FALLBACK_SOURCES: List[Tuple[str, Callable[[str, str], Optional[dict]]]] = [
    ("naver", fetch_ohlcv_from_naver),
]


# ===== RAW 메인 로딩/백업/병합 =====
def load_raw_main() -> pd.DataFrame:
    if not os.path.exists(RAW_MAIN):
        raise FileNotFoundError(f"RAW 메인 파일을 찾을 수 없습니다: {RAW_MAIN}")
    df = pd.read_parquet(RAW_MAIN)
    if "Date" not in df.columns:
        raise ValueError("RAW 파일에 'Date' 컬럼이 없습니다.")
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df



def backup_raw_main(raw_df: pd.DataFrame, today: dt.date) -> str:
    ymd_short = today.strftime("%y%m%d")  # 251119 형태
    base_name = f"all_stocks_cumulative_{ymd_short}.parquet"
    backup_path = os.path.join(STOCKS_DIR, base_name)

    # 겹치면 _1, _2 ...
    idx = 1
    final_path = backup_path
    while os.path.exists(final_path):
        final_path = os.path.join(
            STOCKS_DIR,
            f"all_stocks_cumulative_{ymd_short}_{idx}.parquet",
        )
        idx += 1

    raw_df.to_parquet(final_path)
    return final_path


def merge_daily_into_raw(raw_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
    # 단순 concat 후 (Date, Code) 기준 중복 제거 + 정렬
    merged = pd.concat([raw_df, daily_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep="last")
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)
    return merged


# ===== 메인 데이터 수집 로직 (V5 핵심) =====
def build_daily_from_pykrx(date: dt.date) -> Tuple[pd.DataFrame, List[str]]:
    """
    1) pykrx.get_market_ohlcv_by_ticker로 전체 시장 하루 OHLCV (초고속)
    2) '등락률'을 Change 컬럼으로 변환 (소수로)
    3) Code/Name/Date 컬럼 구성
    4) OHLCV NaN/0 의심 종목 리스트 리턴 (백업용)
    """
    date_ymd = to_ymd(date)
    log(f"[STEP] KRX 일괄 수집 시작: {date_ymd}")

    df = stock.get_market_ohlcv_by_ticker(date_ymd, market="ALL")
    if df is None or df.empty:
        raise RuntimeError(f"pykrx get_market_ohlcv_by_ticker 결과가 비었습니다. ({date_ymd})")

    # 컬럼 이름 통일
    rename_map = {
        "시가": "Open",
        "고가": "High",
        "저가": "Low",
        "종가": "Close",
        "거래량": "Volume",
        "등락률": "ChangePct",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # ChangePct가 있으면 Change(소수)로 변환
    if "ChangePct" in df.columns:
        df["Change"] = df["ChangePct"] / 100.0
    else:
        df["Change"] = 0.0

    # 인덱스(티커)를 Code로
    df = df.reset_index().rename(columns={"티커": "Code"})

    # Name 채우기 (한 번만 호출되므로 속도 괜찮음)
    def _get_name_safe(ticker: str) -> str:
        try:
            return stock.get_market_ticker_name(ticker)
        except Exception:
            return ""

    df["Name"] = df["Code"].map(_get_name_safe)

    # Date 고정
    df["Date"] = date

    # 필요한 컬럼만 정리
    keep_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Change", "Code", "Name"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[keep_cols]

    # 의심 종목 찾기: OHLCV 가 NaN이거나 0/음수이면 백업 후보
    mask_bad = _invalid_ohlcv_mask(df)
    suspicious_codes = df.loc[mask_bad, "Code"].tolist()

    log(
        f"[KRX] {date_ymd}: {len(df)}개 종목 수집 완료, "
        f"의심 종목(OHLCV NaN/0) {len(suspicious_codes)}개"
    )
    return df, suspicious_codes

def fill_missing_with_sources(
    daily_df: pd.DataFrame,
    date: dt.date,
    codes: List[str],
    sources: Optional[Iterable[Tuple[str, Callable[[str, str], Optional[dict]]]]] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """pykrx에서 빠진 티커를 여러 소스로 교차 검증해 보정한다."""
    if not codes:
        return daily_df, []

    if sources is None:
        sources = FALLBACK_SOURCES

    date_ymd = to_ymd(date)
    missing_log_path = os.path.join(LOG_DIR, f"missing_{date_ymd}.txt")
    unresolved = list(dict.fromkeys(codes))

    for source_name, fetcher in sources:
        if not unresolved:
            break

        log(f"[{source_name.upper()}] 결측 보정 시도 (잔여 {len(unresolved)}건)")
        next_round: List[str] = []
        progress_active = False

        for idx, code in enumerate(unresolved, start=1):
            progress_active = True
            inline_progress(f"    [{source_name.upper()}] {idx}/{len(unresolved)} {code}")
            info = fetcher(code, date_ymd)
            if not info:
                next_round.append(code)
                continue

            mask = daily_df["Code"] == code
            if not mask.any():
                row = {
                    "Date": info["Date"],
                    "Open": info["Open"],
                    "High": info["High"],
                    "Low": info["Low"],
                    "Close": info["Close"],
                    "Volume": info["Volume"],
                    "Change": 0.0,
                    "Code": code,
                    "Name": stock.get_market_ticker_name(code),
                }
                daily_df = pd.concat([daily_df, pd.DataFrame([row])], ignore_index=True)
                mask = daily_df["Code"] == code

            for col in OHLCV_COLS:
                series = daily_df.loc[mask, col]
                if series.isna().any() or (series <= 0).any():
                    daily_df.loc[mask, col] = info[col]

            if _invalid_ohlcv_mask(daily_df.loc[mask, OHLCV_COLS]).any():
                next_round.append(code)

        if progress_active:
            end_inline_progress()

        resolved_count = len(unresolved) - len(next_round)
        log(f"    -> {source_name} 보정 성공 {resolved_count}건, 잔여 {len(next_round)}건")
        unresolved = next_round

    if unresolved:
        with open(missing_log_path, "w", encoding="utf-8") as f:
            f.write("\n".join(unresolved))
        log(f"[WARN] 교차 검증에서도 실패한 종목 {len(unresolved)}건 - {missing_log_path} 기록")
    else:
        if os.path.exists(missing_log_path):
            os.remove(missing_log_path)
        log("[FALLBACK] 모든 결측 종목을 보정했습니다")

    return daily_df, unresolved


# ===== 메인 실행 =====
def main():
    print_header()

    # 1) RAW 파일 로드 + 기본 정보 출력
    raw_df = load_raw_main()
    last_date = max(raw_df["Date"])
    latest_mask = raw_df["Date"] == last_date
    latest_codes = raw_df.loc[latest_mask, "Code"].nunique()
    universe_codes = raw_df["Code"].nunique()

    print(f"[STEP 1] 현재 RAW 최신 날짜: {last_date}")
    print(f"         - 기준 종목 수(최신일 기준): {latest_codes}개")
    print(f"         - 전체 유니버스 종목 수   : {universe_codes}개")

    # 2) 업데이트 대상 범위 계산 (항상 '어제'까지)
    today = dt.datetime.now()
    today_date = today.date()
    cutoff_date = get_prev_bizdate(today_date)
    log(f"[INFO] 이번 실행의 수집 상한(어제 기준 영업일): {cutoff_date}")

    if last_date >= cutoff_date:
        log(
            f"[INFO] RAW 최신 날짜({last_date})가 수집 상한({cutoff_date}) 이상입니다. "
            "이번 실행에서 새로 받을 데이터가 없습니다."
        )
        return

    target_dates: List[dt.date] = []
    next_date = get_next_bizdate(last_date)
    while next_date <= cutoff_date:
        target_dates.append(next_date)
        next_date = get_next_bizdate(next_date)

    print()
    print(
        f"[STEP 2] 수집 대상 기간: {target_dates[0]} ~ {target_dates[-1]} "
        f"(총 {len(target_dates)}영업일)"
    )
    print()

    # 3) RAW 백업
    log("[STEP 3] RAW 백업 생성")
    backup_path = backup_raw_main(raw_df, today_date)
    log(f"[BACKUP] RAW 백업 생성: {backup_path}")

    # 4) 날짜별 패치 루프
    for idx, target_date in enumerate(target_dates, start=1):
        log(f"[STEP 4-{idx}] {target_date} 데이터 수집 시작")
        daily_df, suspicious_codes = build_daily_from_pykrx(target_date)
        daily_df, unresolved = fill_missing_with_sources(daily_df, target_date, suspicious_codes)

        target_ymd = to_ymd(target_date)
        daily_path = os.path.join(DAILY_DIR, f"{target_ymd}.parquet")
        daily_df.to_parquet(daily_path)
        log(f"[DAILY 저장] {daily_path}")

        if unresolved:
            log(
                f"[WARN] {target_ymd} 기준 교차 검증 실패 종목 {len(unresolved)}건 - "
                "LOGS 폴더를 확인하세요."
            )

        raw_df = merge_daily_into_raw(raw_df, daily_df)
        log(f"[STEP 4-{idx}] 병합 완료 - 누적 최신 날짜 {max(raw_df['Date'])}")

    # 5) RAW 저장
    raw_df.to_parquet(RAW_MAIN)

    log("🎉 [완료] RAW 패치 및 병합 완료")
    log(f"    - 최신 날짜 : {max(raw_df['Date'])}")
    log(f"    - 총 행 수   : {len(raw_df):,}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] 사용자가 강제 중단했습니다.")
    except Exception as e:
        print("\n[ERROR] 예외 발생:", e)
        sys.exit(1)
