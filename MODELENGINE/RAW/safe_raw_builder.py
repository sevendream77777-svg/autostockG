# safe_raw_builder_v3.py
# RAW 3중 안정 수집 (Yahoo → Naver → KRX)
# + 강제 정규화(normalize_raw_df) 추가 버전

import os
import time
import datetime
import requests
import pandas as pd
import yfinance as yf

try:
    from pykrx import stock as krx_stock
    HAS_KRX = True
except:
    HAS_KRX = False

# ---------------------------------------------------------
# 설정
# ---------------------------------------------------------

BASE_DIR = r"F:\autostockG\MODELENGINE\RAW"
RAW_MAIN = os.path.join(BASE_DIR, "all_stocks_cumulative.parquet")
LOG_DIR = os.path.join(BASE_DIR, "LOGS")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

START_DATE = "2015-01-01"
END_DATE = datetime.date.today().strftime("%Y-%m-%d")
TAG = datetime.date.today().strftime("%y%m%d")

# ---------------------------------------------------------
#  공용 로그
# ---------------------------------------------------------

def log(msg: str):
    print(msg, flush=True)


# ---------------------------------------------------------
#  강제 정규화 함수 (핵심)
# ---------------------------------------------------------

def normalize_raw_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """
    어떤 서버에서 오든 컬럼을 강제로 Date/Code/O/H/L/C/V 7개에 맞춰 정규화.
    Series가 아닌 경우(DataFrame slice 등)도 안전하게 처리.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    # 컬럼 평탄화
    df.columns = [str(c).split(".")[-1] if isinstance(c, str) else str(c) for c in df.columns]

    # 필수 컬럼 존재 여부
    needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            return pd.DataFrame()

    # 날짜 변환
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Code 고정
    df["Code"] = str(code).zfill(6)

    # 숫자 컬럼 강제 변환 (Series 보장)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Date"])
    df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

    return df


# ---------------------------------------------------------
# 1단계: Yahoo
# ---------------------------------------------------------

def fetch_from_yahoo(code: str, start: str, end: str) -> pd.DataFrame:
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)

            # MultiIndex → 단일 컬럼
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Price.Open → Open
            df.columns = [c.split(".")[-1] for c in df.columns]

            df = df.reset_index()

            norm = normalize_raw_df(df, code)
            if not norm.empty:
                log(f"[YAHOO] {code} ✓ (ticker={ticker}, rows={len(norm)})")
                return norm

        except Exception as e:
            log(f"[YAHOO] {code} 예외: {e}")

    return pd.DataFrame()


# ---------------------------------------------------------
# 2단계: NAVER
# ---------------------------------------------------------

def fetch_from_naver(code: str) -> pd.DataFrame:
    url = f"https://api.stock.naver.com/stock/{code}/chart"
    params = {"period": "DAY", "count": "5000"}

    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            log(f"[NAVER] {code} HTTP {r.status_code}")
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
                dt, code,
                d.get("open", 0),
                d.get("high", 0),
                d.get("low", 0),
                d.get("close", 0),
                d.get("volume", 0)
            ])

        df = pd.DataFrame(data, columns=["Date", "Code", "Open", "High", "Low", "Close", "Volume"])
        norm = normalize_raw_df(df, code)

        if not norm.empty:
            log(f"[NAVER] {code} ✓ (rows={len(norm)})")
        return norm

    except Exception as e:
        log(f"[NAVER] {code} 예외: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# 3단계: KRX
# ---------------------------------------------------------

def fetch_from_krx(code: str, start: str, end: str) -> pd.DataFrame:
    if not HAS_KRX:
        return pd.DataFrame()

    try:
        s = start.replace("-", "")
        e = end.replace("-", "")

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
            "거래량": "Volume"
        })

        norm = normalize_raw_df(df, code)
        if not norm.empty:
            log(f"[KRX] {code} ✓ (rows={len(norm)})")
        return norm

    except Exception as e:
        log(f"[KRX] {code} 예외: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------
# 통합: 3단계 fallback
# ---------------------------------------------------------

def fetch_all(code: str) -> pd.DataFrame:
    df = fetch_from_yahoo(code, START_DATE, END_DATE)
    if not df.empty:
        return df

    df = fetch_from_naver(code)
    if not df.empty:
        return df

    df = fetch_from_krx(code, START_DATE, END_DATE)
    if not df.empty:
        return df

    log(f"[FAIL] {code} 3단계 실패")
    return pd.DataFrame()


# ---------------------------------------------------------
# 종목코드 수집
# ---------------------------------------------------------

def load_all_codes():
    codes = set()

    urls = [
        "https://api.stock.naver.com/marketindex/marketStock/KOSPI",
        "https://api.stock.naver.com/marketindex/marketStock/KOSDAQ"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=5)
            js = r.json()
            for s in js.get("stocks", []):
                c = str(s.get("code", "")).zfill(6)
                if c.isdigit():
                    codes.add(c)
        except:
            pass

    if HAS_KRX:
        try:
            kospi = krx_stock.get_market_ticker_list(market="KOSPI")
            kosdaq = krx_stock.get_market_ticker_list(market="KOSDAQ")
            for c in list(kospi) + list(kosdaq):
                codes.add(str(c).zfill(6))
        except:
            pass

    codes = sorted(list(codes))
    log(f"[INFO] 전체 종목코드: {len(codes)}개")
    return codes


# ---------------------------------------------------------
# RAW 구축 메인
# ---------------------------------------------------------

def build_raw_all():
    log("===== RAW 구축 시작 =====")

    codes = load_all_codes()
    all_dfs = []

    for idx, code in enumerate(codes, start=1):
        log(f"\n[CODE] ({idx}/{len(codes)}) {code}")
        df = fetch_all(code)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        log("[ERROR] 전체 df 비어있음")
        return

    full = pd.concat(all_dfs, ignore_index=True)
    full = full.dropna(subset=["Date", "Code"])
    full = full.sort_values(["Date", "Code"]).reset_index(drop=True)

    # 기존 RAW 백업
    if os.path.exists(RAW_MAIN):
        backup = RAW_MAIN.replace(".parquet", f"_{TAG}.parquet")
        if not os.path.exists(backup):
            os.rename(RAW_MAIN, backup)
            log(f"[BACKUP] → {backup}")

    full.to_parquet(RAW_MAIN)
    log(f"[SAVE] RAW 완료: {RAW_MAIN}")
    log("===== 종료 =====")


if __name__ == "__main__":
    build_raw_all()
