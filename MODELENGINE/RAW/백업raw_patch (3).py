# raw_patch.py - V3 (최종 통합본)
# 역할:
#   1) 기존 RAW(all_stocks_cumulative.parquet)의 최신 날짜를 찾고
#   2) 그 다음 날짜부터 목표 날짜(기본: 어제)까지 하루 단위로 OHLCV를 수집한 뒤
#   3) 결측/누락 검증을 수행하고
#   4) DAILY 스냅샷 저장 + RAW 본체에 병합/백업까지 자동으로 처리한다.
#
# 데이터 소스 우선순위:
#   Yahoo → KRX(pykrx) → Naver → (stub) Kiwoom REST API

import os
import sys
import time
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd

import requests
import yfinance as yf

try:
    from pykrx import stock as krx_stock
    HAS_KRX = True
except Exception:
    HAS_KRX = False

# ---------------------------------------------------------
# 1. 경로 설정
# ---------------------------------------------------------

# RAW 메인 파일 (위대하신호정님 환경 고정)
RAW_MAIN = r"F:\autostockG\MODELENGINE\RAW\stocks\all_stocks_cumulative.parquet"

STOCKS_DIR = os.path.dirname(RAW_MAIN)
DAILY_DIR = os.path.join(STOCKS_DIR, "DAILY")   # 하루 스냅샷
LOG_DIR = os.path.join(STOCKS_DIR, "LOGS")      # 검증/에러 로그

os.makedirs(STOCKS_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ---------------------------------------------------------
# 2. 공통 유틸
# ---------------------------------------------------------

def log(msg: str) -> None:
    """콘솔 출력용 간단 로그"""
    print(msg, flush=True)


def backup_with_tag(path: str, tag: Optional[str] = None) -> Optional[str]:
    """
    기존 RAW 파일 백업.
    규칙: 파일명_YYMMDD.parquet, 중복 시 _YYMMDD_1, _YYMMDD_2 ...
    """
    if not os.path.exists(path):
        return None

    if tag is None:
        tag = datetime.today().strftime("%y%m%d")

    base_dir = os.path.dirname(path)
    base_name, ext = os.path.splitext(os.path.basename(path))

    candidate = os.path.join(base_dir, f"{base_name}_{tag}{ext}")
    idx = 1
    while os.path.exists(candidate):
        candidate = os.path.join(base_dir, f"{base_name}_{tag}_{idx}{ext}")
        idx += 1

    import shutil
    shutil.copy2(path, candidate)
    log(f"[BACKUP] RAW 백업 생성: {candidate}")
    return candidate


def get_raw_latest_date(raw_path: str) -> Optional[date]:
    """RAW_MAIN에서 Date 컬럼의 최댓값(최신 날짜)을 반환"""
    if not os.path.exists(raw_path):
        return None
    df = pd.read_parquet(raw_path, columns=["Date"])
    if df.empty:
        return None
    return pd.to_datetime(df["Date"]).max().date()


def generate_missing_dates(latest: date, end_date: date) -> List[date]:
    """
    latest+1일부터 end_date까지 중 주말을 제외한 날짜 후보 생성.
    (실제로 휴장일인 경우, 나중 단계에서 데이터가 비어 있으면 자동 건너뜀)
    """
    dates: List[date] = []
    curr = latest + timedelta(days=1)
    while curr <= end_date:
        if curr.weekday() < 5:  # 월(0)~금(4)
            dates.append(curr)
        curr += timedelta(days=1)
    return dates


def normalize_numeric_series(val):
    """
    숫자 컬럼 안정화용 헬퍼.
    Series/ndarray/스칼라 모두 안전하게 float 시리즈로 변환.
    """
    if val is None:
        return pd.Series([pd.NA])
    if isinstance(val, pd.Series):
        return pd.to_numeric(val, errors="coerce")
    if isinstance(val, np.ndarray):
        val = val.flatten()
    if isinstance(val, (int, float, str)):
        val = [val]
    if isinstance(val, (list, tuple)):
        return pd.to_numeric(pd.Series(val), errors="coerce")
    return pd.to_numeric(pd.Series([val]), errors="coerce")


# ---------------------------------------------------------
# 3. 원시 DF 정규화
# ---------------------------------------------------------

def normalize_raw_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """
    어떤 서버에서 오든 컬럼을 강제로 Date/Code/Open/High/Low/Close/Volume
    7개에 맞춰 정규화.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 컬럼 이름 평탄화 (MultiIndex/이상한 이름 방지)
    df.columns = [str(c).split(".")[-1] if isinstance(c, str) else str(c)
                  for c in df.columns]

    needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            return pd.DataFrame()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Code"] = str(code).zfill(6)

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Date"])
    df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

    return df


# ---------------------------------------------------------
# 4. 데이터 소스별 수집 함수
# ---------------------------------------------------------

def fetch_from_yahoo(code: str, start: str, end: str) -> pd.DataFrame:
    """1차: Yahoo Finance"""
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            if df is None or df.empty:
                continue
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "Date",
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume",
            })
            norm = normalize_raw_df(df, code)
            if not norm.empty:
                log(f"      [YAHOO] {code} ({ticker}) ✓ rows={len(norm)}")
                return norm
        except Exception as e:
            log(f"      [YAHOO] {code} ({ticker}) 예외: {e}")
    return pd.DataFrame()


def fetch_from_krx(code: str, start: str, end: str) -> pd.DataFrame:
    """2차: pykrx (KRX)"""
    if not HAS_KRX:
        return pd.DataFrame()

    try:
        s = start.replace("-", "")
        # pykrx는 end가 '포함'이 아니라 구간으로 동작하므로, end-1일 사용
        e = (pd.to_datetime(end) - pd.Timedelta(days=1)).strftime("%Y%m%d")

        df = krx_stock.get_market_ohlcv_by_date(s, e, code)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        df = df.rename(columns={
            "날짜": "Date",
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
        })
        norm = normalize_raw_df(df, code)
        if not norm.empty:
            log(f"      [KRX ] {code} ✓ rows={len(norm)}")
        return norm
    except Exception as e:
        log(f"      [KRX ] {code} 예외: {e}")
        return pd.DataFrame()


def fetch_from_naver(code: str) -> pd.DataFrame:
    """3차: Naver Stock API (전체 히스토리 받아서 나중에 날짜로 필터)"""
    url = f"https://api.stock.naver.com/stock/{code}/chart"
    params = {"period": "DAY", "count": "5000"}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            log(f"      [NAVER] {code} HTTP {r.status_code}")
            return pd.DataFrame()
        js = r.json()
        rows = js.get("chart", {}).get("result", [])
        if not rows:
            return pd.DataFrame()
        data = []
        for d in rows:
            dt = d.get("date")
            if isinstance(dt, str) and len(dt) == 8 and dt.isdigit():
                dt = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
            data.append([
                dt,
                code,
                d.get("open", 0),
                d.get("high", 0),
                d.get("low", 0),
                d.get("close", 0),
                d.get("volume", 0),
            ])
        df = pd.DataFrame(data, columns=["Date", "Code", "Open", "High",
                                         "Low", "Close", "Volume"])
        norm = normalize_raw_df(df, code)
        if not norm.empty:
            log(f"      [NAVER] {code} ✓ rows={len(norm)}")
        return norm
    except Exception as e:
        log(f"      [NAVER] {code} 예외: {e}")
        return pd.DataFrame()


def fetch_from_kiwoom_stub(code: str, start: str, end: str) -> pd.DataFrame:
    """
    4차: Kiwoom REST API (현재는 STUB)
    나중에 위대하신호정님이 kiwoom_api.py 의 일별 OHLCV 함수를 연결하면 됨.
    """
    # TODO: 필요 시 kiwoom_api.get_daily_ohlcv(code, start, end) 연결
    log(f"      [KIWOOM] {code} 아직 구현 안됨 (stub)")
    return pd.DataFrame()


def fetch_ohlcv_multi_source(code: str, start: str, end: str) -> pd.DataFrame:
    """
    데이터 소스 우선순위:
      1) Yahoo
      2) KRX(pykrx)
      3) Naver
      4) Kiwoom(Stub)
    """
    # 1) Yahoo
    df = fetch_from_yahoo(code, start, end)
    if df is not None and not df.empty:
        return df

    # 2) KRX
    df = fetch_from_krx(code, start, end)
    if df is not None and not df.empty:
        return df

    # 3) Naver
    df = fetch_from_naver(code)
    if df is not None and not df.empty:
        # Naver는 전체 히스토리이므로 날짜 범위로 한 번 더 필터링
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        mask = (df["Date"] >= start_dt) & (df["Date"] < end_dt)
        df = df.loc[mask]
        if not df.empty:
            return df

    # 4) Kiwoom stub
    df = fetch_from_kiwoom_stub(code, start, end)
    if df is not None and not df.empty:
        return df

    return pd.DataFrame()


# ---------------------------------------------------------
# 5. 하루치 데이터 수집 + 정규화
# ---------------------------------------------------------

def fetch_single_day_multi(code: str, date_obj: date) -> Tuple[Optional[pd.DataFrame], str]:
    """
    특정 종목/하루(date_obj)에 대한 OHLCV 1행짜리 DataFrame 수집.
    반환: (df, status)  where status in {"success", "empty"}
    """
    date_str = date_obj.strftime("%Y-%m-%d")
    start = date_str
    end = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    df_full = fetch_ohlcv_multi_source(code, start, end)
    if df_full is None or df_full.empty:
        return None, "empty"

    df_full["Date"] = pd.to_datetime(df_full["Date"])
    df_day = df_full[df_full["Date"].dt.date == date_obj].copy()
    if df_day.empty:
        return None, "empty"

    # 하루에 여러 행이 있을 경우 가장 마지막(또는 첫 행)을 사용
    df_day = df_day.sort_values("Date").tail(1)

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df_day.columns:
            df_day[col] = normalize_numeric_series(df_day[col]).values

    df_day = df_day.dropna(subset=["Open", "High", "Low", "Close"])
    if df_day.empty:
        return None, "empty"

    df_day["Code"] = str(code).zfill(6)
    df_day["Date"] = pd.to_datetime(df_day["Date"])

    df_day = df_day[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]
    return df_day, "success"


# ---------------------------------------------------------
# 6. 검증 로직 (누락 코드/NaN 체크)
# ---------------------------------------------------------

def verify_day_against_baseline(date_obj: date,
                                day_df: pd.DataFrame,
                                baseline_codes: List[str]) -> None:
    """
    - baseline_codes: 최신 RAW 날짜에 존재하던 종목 코드 목록
    - day_df: 새로 수집한 하루치 데이터
    """
    date_str = date_obj.strftime("%Y-%m-%d")
    new_codes = set(day_df["Code"].astype(str).str.zfill(6).unique())
    base_set = set(str(c).zfill(6) for c in baseline_codes)

    missing = sorted(base_set - new_codes)
    extra = sorted(new_codes - base_set)

    if missing:
        log(f"    ⚠ [WARN] {date_str}: 기존 종목 중 {len(missing)}개 누락")
        miss_path = os.path.join(LOG_DIR, f"missing_codes_{date_str}.txt")
        with open(miss_path, "w", encoding="utf-8") as f:
            f.write("\n".join(missing))
        log(f"       → 누락 코드 목록: {miss_path}")

    if extra:
        log(f"    ℹ [INFO] {date_str}: 신규 추정 종목 {len(extra)}개")
        extra_path = os.path.join(LOG_DIR, f"extra_codes_{date_str}.txt")
        with open(extra_path, "w", encoding="utf-8") as f:
            f.write("\n".join(extra))
        log(f"       → 신규 코드 목록: {extra_path}")

    # NaN 체크
    na_mask = day_df[["Open", "High", "Low", "Close", "Volume"]].isna().any(axis=1)
    if na_mask.any():
        n_nan = int(na_mask.sum())
        log(f"    ⚠ [WARN] {date_str}: OHLCV 결측 행 {n_nan}개 발견")
        nan_path = os.path.join(LOG_DIR, f"nan_rows_{date_str}.csv")
        day_df.loc[na_mask, ["Date", "Code", "Open", "High", "Low", "Close", "Volume"]].to_csv(
            nan_path, index=False, encoding="utf-8-sig"
        )
        log(f"       → 상세 목록: {nan_path}")


# ---------------------------------------------------------
# 7. 전체 RAW 자동 업데이트 메인 로직
# ---------------------------------------------------------

def auto_update_raw(target_end: Optional[str] = None) -> None:
    """
    RAW_MAIN 기준으로 최신 날짜 이후의 데이터를 자동 수집/병합.

    1) RAW_MAIN에서 최신 날짜 읽음
    2) 최신 날짜 + 1일 ~ target_end(또는 어제)까지 날짜 목록 생성
    3) 각 날짜별로 모든 종목에 대해 하루치 데이터 수집
    4) DAILY 스냅샷 저장 + 검증 + RAW_MAIN에 병합 & 백업
    """
    start_ts = time.time()

    log("")
    log("┌──────────────────────────────────────────────┐")
    log("│         RAW PATCH AUTO UPDATER (V3)         │")
    log("└──────────────────────────────────────────────┘")
    log(f"[PATH] RAW_MAIN : {RAW_MAIN}")
    log(f"[PATH] DAILY    : {DAILY_DIR}")
    log(f"[PATH] LOGS     : {LOG_DIR}")

    if not os.path.exists(RAW_MAIN):
        log(f"[FATAL] RAW_MAIN 파일이 없습니다: {RAW_MAIN}")
        return

    # 1. 기존 RAW 로드 및 최신 날짜/기준 종목 세트 확보
    df_raw = pd.read_parquet(RAW_MAIN)
    if df_raw.empty:
        log("[FATAL] RAW_MAIN이 비어 있습니다. (행 수 0)")
        return

    df_raw["Date"] = pd.to_datetime(df_raw["Date"])
    latest_date = df_raw["Date"].max().date()
    log(f"[STEP 1] 현재 RAW 최신 날짜: {latest_date}")

    latest_slice = df_raw[df_raw["Date"].dt.date == latest_date].copy()
    baseline_codes = sorted(latest_slice["Code"].astype(str).str.zfill(6).unique())
    universe_codes = sorted(df_raw["Code"].astype(str).str.zfill(6).unique())
    log(f"         - 기준 종목 수(최신일 기준): {len(baseline_codes):,}개")
    log(f"         - 전체 유니버스 종목 수   : {len(universe_codes):,}개")

    # 2. target_end 결정
    if target_end is None:
        today = date.today()
        target_end_date = today - timedelta(days=1)
    else:
        target_end_date = pd.to_datetime(target_end).date()

    if target_end_date <= latest_date:
        log(f"[INFO] 이미 최신입니다. (목표: {target_end_date}, 현재: {latest_date})")
        return

    fetch_dates = generate_missing_dates(latest_date, target_end_date)
    if not fetch_dates:
        log("[INFO] 수집할 신규 날짜가 없습니다.")
        return

    log(f"[STEP 2] 수집 대상 날짜: {fetch_dates[0]} ~ {fetch_dates[-1]} (총 {len(fetch_dates)}일)")

    all_new_data: List[pd.DataFrame] = []

    # 3. 날짜별 수집 루프
    for d_idx, date_obj in enumerate(fetch_dates, start=1):
        date_str = date_obj.strftime("%Y-%m-%d")
        log("")
        log(f"┌─ [STEP 3-{d_idx}] {date_str} 데이터 수집 시작 ──────────────────────┐")
        day_start_ts = time.time()

        day_rows: List[pd.DataFrame] = []
        n_success = 0

        for c_idx, code in enumerate(universe_codes, start=1):
            df_day, status = fetch_single_day_multi(code, date_obj)
            if status == "success" and df_day is not None and not df_day.empty:
                day_rows.append(df_day)
                n_success += 1

            if c_idx % 500 == 0:
                log(f"    ... 진행중: {c_idx}/{len(universe_codes)}개 코드 처리 완료")

        if n_success == 0:
            log(f"│   {date_str}: 유효한 데이터가 없어 건너뜀 (휴장일 또는 전체 실패) │")
            log("└────────────────────────────────────────────────────────────┘")
            continue

        day_df = pd.concat(day_rows, ignore_index=True)
        day_df["Code"] = day_df["Code"].astype(str).str.zfill(6)
        day_df["Date"] = pd.to_datetime(day_df["Date"])
        day_df = day_df.sort_values(["Date", "Code"]).reset_index(drop=True)

        log(f"│   {date_str}: {n_success:,}개 종목 수집 완료, 총 행 수 {len(day_df):,} │")

        # 3-1. 검증(누락/NaN) + 로그 저장
        verify_day_against_baseline(date_obj, day_df, baseline_codes)

        # 3-2. DAILY 스냅샷 저장
        daily_path = os.path.join(DAILY_DIR, f"{date_obj.strftime('%Y%m%d')}.parquet")
        day_df.to_parquet(daily_path)
        log(f"│   DAILY 저장: {daily_path} │")

        elapsed_day = time.time() - day_start_ts
        log(f"└─ [END   3-{d_idx}] {date_str} 처리 완료 ({elapsed_day:,.1f}초) ─────┘")

        all_new_data.append(day_df)

    if not all_new_data:
        log("[INFO] 새로 수집된 데이터가 없어 RAW 병합을 생략합니다.")
        return

    # 4. 병합 전 백업
    log("")
    log("[STEP 4] RAW_MAIN 백업 및 병합 시작")
    backup_with_tag(RAW_MAIN)

    new_block = pd.concat(all_new_data, ignore_index=True)
    new_block["Code"] = new_block["Code"].astype(str).str.zfill(6)
    new_block["Date"] = pd.to_datetime(new_block["Date"])

    merged = pd.concat([df_raw, new_block], ignore_index=True)
    merged = merged.dropna(subset=["Date", "Code"])
    merged["Code"] = merged["Code"].astype(str).str.zfill(6)
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep="last")
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)

    merged.to_parquet(RAW_MAIN)

    total_elapsed = time.time() - start_ts
    log("")
    log("🎉 [완료] RAW 패치 및 병합 완료")
    log(f"    - 최신 날짜 : {merged['Date'].max().date()}")
    log(f"    - 총 행 수   : {len(merged):,}")
    log(f"    - 전체 소요시간: {total_elapsed:,.1f}초")
    log("=============================================================")


if __name__ == "__main__":
    # 예시:
    #   python raw_patch.py               → RAW 최신 날짜 기준, 어제까지 자동 업데이트
    #   python raw_patch.py 2025-11-18    → 지정 날짜까지 업데이트
    if len(sys.argv) > 1:
        auto_update_raw(sys.argv[1])
    else:
        auto_update_raw()
