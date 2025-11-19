# safe_raw_builder_v2.py (경로 완전 통일본)
# RAW 안정형 구축: Yahoo → Naver → KRX 3단계 fallback
# 저장 위치: F:\autostockG\MODELENGINE\RAW\stocks\safe_raw_builder_v2.py

import os
import time
import datetime
from typing import List
import pandas as pd

import requests
import yfinance as yf

try:
    from pykrx import stock as krx_stock
    HAS_KRX = True
except Exception:
    HAS_KRX = False


# ---------------------------------------------------------
# ★ RAW 경로 완전 통일
# ---------------------------------------------------------
BASE_DIR = r"F:\autostockG\MODELENGINE\RAW\stocks"
RAW_MAIN = os.path.join(BASE_DIR, "all_stocks_cumulative.parquet")
DAILY_DIR = os.path.join(BASE_DIR, "DAILY")
LOG_DIR = os.path.join(BASE_DIR, "LOGS")

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

START_DATE = "2015-01-01"
END_DATE = datetime.date.today().strftime("%Y-%m-%d")

TODAY_TAG = datetime.date.today().strftime("%y%m%d")


def log(msg: str):
    print(msg, flush=True)


# ---------------------------------------------------------
# 1) 전체 종목 코드 수집
# ---------------------------------------------------------
def load_all_codes() -> List[str]:
    codes = set()
    urls = [
        "https://api.stock.naver.com/marketindex/marketStock/KOSPI",
        "https://api.stock.naver.com/marketindex/marketStock/KOSDAQ",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code != 200:
                log(f"[WARN] Naver 실패: {url} → {r.status_code}")
                continue
            js = r.json()
            for s in js.get("stocks", []):
                c = str(s.get("code", "")).strip()
                if len(c) == 6 and c.isdigit():
                    codes.add(c)
        except Exception as e:
            log(f"[WARN] Naver 코드 예외: {e}")

    if HAS_KRX:
        try:
            for c in list(krx_stock.get_market_ticker_list("KOSPI")) + \
                     list(krx_stock.get_market_ticker_list("KOSDAQ")):
                c = str(c).zfill(6)
                codes.add(c)
        except Exception as e:
            log(f"[WARN] KRX 보조 실패: {e}")

    codes = sorted(list(codes))
    log(f"[INFO] 전체 종목코드: {len(codes)}개")
    return codes


# ---------------------------------------------------------
# 2) Price 수집(Yahoo → Naver → KRX)
# ---------------------------------------------------------
def fetch_from_yahoo(code, start, end):
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            if df is not None and not df.empty:
                df = df.reset_index()
                df["Code"] = code
                df = df.rename(columns={
                    "Date":"Date","Open":"Open","High":"High",
                    "Low":"Low","Close":"Close","Volume":"Volume"
                })
                df = df[["Date","Code","Open","High","Low","Close","Volume"]]
                df["Date"] = pd.to_datetime(df["Date"])
                log(f"[YAHOO] {code} OK (rows={len(df)})")
                return df
        except Exception as e:
            log(f"[YAHOO] {code} 예외: {e}")

    return pd.DataFrame()


def fetch_from_naver(code, start, end):
    url = f"https://api.stock.naver.com/stock/{code}/chart"
    params = {"period":"DAY","count":"4000"}

    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return pd.DataFrame()

        js = r.json()
        rows = js.get("chart", {}).get("result", [])
        if not rows:
            return pd.DataFrame()

        data = []
        for d in rows:
            dt = d.get("date")
            if isinstance(dt, str) and len(dt)==8 and dt.isdigit():
                dt = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"

            data.append([
                dt,
                code,
                d.get("open",0),
                d.get("high",0),
                d.get("low",0),
                d.get("close",0),
                d.get("volume",0),
            ])

        df = pd.DataFrame(data, columns=["Date","Code","Open","High","Low","Close","Volume"])
        df["Date"] = pd.to_datetime(df["Date"])
        df = df[(df["Date"]>=pd.to_datetime(start)) & (df["Date"]<=pd.to_datetime(end))]

        if df.empty:
            return pd.DataFrame()

        log(f"[NAVER] {code} OK (rows={len(df)})")
        return df

    except:
        return pd.DataFrame()


def fetch_from_krx(code, start, end):
    if not HAS_KRX:
        return pd.DataFrame()

    try:
        s = start.replace("-","")
        e = end.replace("-","")
        df = krx_stock.get_market_ohlcv_by_date(s, e, code)
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        df["Code"] = code
        df = df.rename(columns={
            "날짜":"Date","시가":"Open","고가":"High",
            "저가":"Low","종가":"Close","거래량":"Volume"
        })
        df["Date"] = pd.to_datetime(df["Date"])
        df = df[["Date","Code","Open","High","Low","Close","Volume"]]
        log(f"[KRX] {code} OK (rows={len(df)})")
        return df
    except:
        return pd.DataFrame()


def fetch_ohlcv_multi_source(code, start, end,
                             fail_log, fallback_log, krx_log):
    df = fetch_from_yahoo(code, start, end)
    if not df.empty:
        return df

    fallback_log.append(code)
    df = fetch_from_naver(code, start, end)
    if not df.empty:
        return df

    krx_log.append(code)
    df = fetch_from_krx(code, start, end)
    if not df.empty:
        return df

    fail_log.append(code)
    log(f"[FAIL] {code} 3단계 실패")
    return pd.DataFrame()


# ---------------------------------------------------------
# 3) RAW 구축
# ---------------------------------------------------------
def backup_existing_raw():
    if not os.path.exists(RAW_MAIN):
        return

    base = os.path.join(BASE_DIR, f"all_stocks_cumulative_{TODAY_TAG}.parquet")
    backup_path = base
    idx = 1
    while os.path.exists(backup_path):
        backup_path = os.path.join(BASE_DIR, f"all_stocks_cumulative_{TODAY_TAG}_{idx}.parquet")
        idx += 1

    os.rename(RAW_MAIN, backup_path)
    log(f"[BACKUP] 기존 RAW → {backup_path}")


def build_raw_all():
    log("===== RAW 구축 시작 =====")
    log(f"[INFO] 기간: {START_DATE} ~ {END_DATE}")
    log(f"[INFO] 출력: {RAW_MAIN}")

    codes = load_all_codes()
    if not codes:
        log("[ERROR] 종목코드 없음 → 종료")
        return

    fail_log = []
    fallback_log = []
    krx_log = []

    all_dfs = []
    total = len(codes)

    for i, code in enumerate(codes, start=1):
        log(f"\n[CODE] ({i}/{total}) {code}")
        df = fetch_ohlcv_multi_source(code, START_DATE, END_DATE,
                                      fail_log, fallback_log, krx_log)
        if df.empty:
            continue

        df["Code"] = df["Code"].astype(str).zfill(6)
        for col in ["Open","High","Low","Close","Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        all_dfs.append(df)

    if not all_dfs:
        log("[ERROR] RAW 결과 없음(all_dfs empty)")
        return

    full = pd.concat(all_dfs, ignore_index=True)
    full = full.dropna(subset=["Date","Code"])
    full["Code"] = full["Code"].astype(str).zfill(6)
    full["Date"] = pd.to_datetime(full["Date"])
    full = full.sort_values(["Date","Code"]).reset_index(drop=True)

    log(f"[INFO] 최종 RAW 행수: {len(full)}")
    log(full.head())

    backup_existing_raw()

    full.to_parquet(RAW_MAIN)
    log(f"[SAVE] RAW 저장 완료: {RAW_MAIN}")

    with open(os.path.join(LOG_DIR, f"failed_codes_{TODAY_TAG}.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(fail_log))
    with open(os.path.join(LOG_DIR, f"fallback_used_{TODAY_TAG}.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(fallback_log))
    with open(os.path.join(LOG_DIR, f"krx_used_{TODAY_TAG}.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(krx_log))

    log("===== RAW 구축 완료 =====")


if __name__ == "__main__":
    build_raw_all()
