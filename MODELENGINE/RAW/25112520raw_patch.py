# raw_patch.py  (V6 - Kiwoom 1st Source Added)
# 수정사항:
# 1. build_daily_from_pykrx 등 누락된 함수 복구
# 2. 이미 최신 데이터(target_date == last_date)가 있으면 수집 SKIP 기능 추가
# 3. 파일 저장 시 날짜 태그 규칙 준수

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
import FinanceDataReader as fdr
import glob
import os as _os
import yfinance as yf
import os.path as _path

# ============================================================
#  KIWOOM REST API 경로/모듈 설정
# ============================================================
KIWOOM_REST_DIR = r"F:\autostockG"
if KIWOOM_REST_DIR not in sys.path:
    sys.path.append(KIWOOM_REST_DIR)

# REST API 전용 모듈 가져오기
try:
    from kiwoom_rest.token_manager import KiwoomTokenManager
    from kiwoom_rest.kiwoom_api import KiwoomRestApi
except ImportError:
    print("Warning: kiwoom_rest 모듈을 찾을 수 없습니다. 경로를 확인해주세요.")

# ======================
# 경로 설정
# ======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCKS_DIR = os.path.join(BASE_DIR, "stocks")
RAW_MAIN = os.path.join(STOCKS_DIR, "all_stocks_cumulative.parquet")
DAILY_DIR = os.path.join(STOCKS_DIR, "DAILY")
LOG_DIR = os.path.join(STOCKS_DIR, "LOGS")
OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]

os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# MODELENGINE 루트 경로를 sys.path에 추가하여 UTIL 모듈 사용
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from UTIL.config_paths import versioned_filename
from UTIL.version_utils import save_dataframe_with_date, find_latest_file

def print_header():
    print("┌──────────────────────────────────────────────┐")
    print("│ 🎉 흰둥이 원본데이터 업데이트 (V6)           │")
    print("└──────────────────────────────────────────────┘")
    print(f"[PATH] RAW_MAIN : {RAW_MAIN}")
    print()

def log(msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def _invalid_ohlcv_mask(df: pd.DataFrame) -> pd.Series:
    subset = df[OHLCV_COLS] if all(col in df.columns for col in OHLCV_COLS) else df
    return subset.isna().any(axis=1) | (subset <= 0).any(axis=1)

# ======================
# 날짜 유틸
# ======================
def to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def parse_date(s: str) -> dt.date:
    s = str(s)
    if "-" in s:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    return dt.datetime.strptime(s, "%Y%m%d").date()

@lru_cache(maxsize=512)
def _nearest_bizday_cached(date_ymd: str) -> str:
    return stock.get_nearest_business_day_in_a_week(date_ymd)

def is_trading_day(date: dt.date) -> bool:
    today = dt.date.today()
    if date == today:
        return date.weekday() < 5

    date_ymd = to_ymd(date)
    try:
        nearest = _nearest_bizday_cached(date_ymd)
        if nearest == date_ymd:
            return True
    except Exception as e:
        log(f"[WARN] pykrx 날짜 확인 실패 - {e}. 주말 여부로만 판정.")

    # pykrx 실패 시 FDR(코스피 지수)로 휴일 여부 보조 확인
    try:
        df_tmp = fdr.DataReader("KS11", date, date)
        if df_tmp is not None and not df_tmp.empty:
            return True
    except Exception as e:
        log(f"[WARN] FDR 휴일 확인 실패 - {e}. 요일만 사용.")

    return date.weekday() < 5

def get_next_bizdate(last_date: dt.date) -> dt.date:
    d = last_date
    for _ in range(400):
        d = d + dt.timedelta(days=1)
        if is_trading_day(d):
            return d
    raise RuntimeError("다음 영업일을 찾지 못했습니다.")

# ======================
# SAVE / LOAD
# ======================
def load_raw_main() -> pd.DataFrame:
    # find_latest_file을 사용하여 최신 날짜 태그 파일 로드
    latest_path = find_latest_file(STOCKS_DIR, "all_stocks_cumulative")
    
    if latest_path and os.path.exists(latest_path):
        log(f"[INFO] 최신 RAW 파일 사용: {os.path.basename(latest_path)}")
        df = pd.read_parquet(latest_path)
    else:
        # 태그 파일이 없으면 기존 레거시 파일 확인
        if os.path.exists(RAW_MAIN):
            log(f"[INFO] 태그 파일 없음. 기존 RAW_MAIN 사용: {os.path.basename(RAW_MAIN)}")
            df = pd.read_parquet(RAW_MAIN)
        else:
            raise FileNotFoundError(f"RAW 메인 파일 없음: {RAW_MAIN} 또는 all_stocks_cumulative_*.parquet")

    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df

def merge_daily_into_raw(raw_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([raw_df, daily_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["Date", "Code"], keep="last")
    merged = merged.sort_values(["Date", "Code"]).reset_index(drop=True)
    return merged

# =====================================================================================
# [복구] Kiwoom REST API 수집 함수
# =====================================================================================
def build_daily_from_kiwoom(date: dt.date, tickers: Optional[List[str]] = None) -> Tuple[pd.DataFrame, List[str]]:
    log(f"[STEP] KIWOOM 전체 일봉 수집 시작: {to_ymd(date)}")

    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(r"F:\autostockG\kiwoom_rest\config.ini")

    MODE = cfg["SETTINGS"]["MODE"].strip().lower()
    BASE_URL = cfg["SETTINGS"]["BASE_URL_PAPER"] if MODE == "paper" else cfg["SETTINGS"]["BASE_URL"]

    token_mgr = KiwoomTokenManager(
        config_file=r"F:\autostockG\kiwoom_rest\config.ini",
        token_file=r"F:\autostockG\kiwoom_rest\token.json"
    )
    token = token_mgr.get_access_token()

    if tickers is None:
        try:
            tickers = stock.get_market_ticker_list(date=to_ymd(date), market="ALL")
        except Exception:
            tickers = []
        if not tickers:
            try:
                tickers = load_raw_main()["Code"].unique().tolist()
            except Exception:
                tickers = []

    rows = []
    bad_codes = []
    target = to_ymd(date)
    timeout_sec = 2 

    for idx, code in enumerate(tickers, 1):
        if idx % 100 == 0:
            log(f"[KIWOOM] 진행 {idx}/{len(tickers)}")

        url = f"{BASE_URL}/api/dostk/chart"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10081",
            "authorization": f"Bearer {token}",
        }
        body = {
            "stk_cd": code,
            "base_dt": target,
            "upd_stkpc_tp": "0",
        }

        try:
            r = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
            r.raise_for_status()
            js = r.json()

            chart = js.get("stk_dt_pole_chart_qry") or js.get("chart") or []
            if not chart:
                bad_codes.append(code)
                continue
            matched = None
            for item in chart:
                if str(item.get("dt")) == target:
                    matched = item
                    break
            if matched is None:
                matched = chart[0]

            def _to_float(v):
                try:
                    return float(str(v).replace("+", "").replace(",", "").strip())
                except Exception:
                    return float("nan")

            rows.append({
                "Date": date,
                "Open": _to_float(matched.get("open_pric")),
                "High": _to_float(matched.get("high_pric")),
                "Low": _to_float(matched.get("low_pric")),
                "Close": _to_float(matched.get("cur_prc")),
                "Volume": _to_float(matched.get("trde_qty")),
                "Change": 0.0,
                "Code": code,
                "Name": "",
                "Market": ""
            })

        except Exception:
            bad_codes.append(code)
            continue

    df = pd.DataFrame(rows)
    log(f"[KIWOOM] {len(df)}개 종목 수집, 실패 {len(bad_codes)}개")
    return df, bad_codes

# =====================================================================================
# [복구] pykrx 수집 함수 (이게 없어서 에러났었음)
# =====================================================================================
def build_daily_from_pykrx(date: dt.date) -> Tuple[pd.DataFrame, List[str]]:
    for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        _os.environ[key] = ""
    date_ymd = to_ymd(date)
    log(f"[STEP] KRX 일괄 수집 시작: {date_ymd}")

    df = stock.get_market_ohlcv_by_ticker(date_ymd, market="ALL")
    if df is None or df.empty:
        raise RuntimeError(f"KRX 데이터 없음: {date_ymd}")

    eng_map = {"Open": "시가", "High": "고가", "Low": "저가", "Close": "종가", "Volume": "거래량"}
    if set(eng_map.keys()).issubset(df.columns):
        df = df.rename(columns=eng_map)

    rename_map = {
        "시가": "Open", "고가": "High", "저가": "Low",
        "종가": "Close", "거래량": "Volume", "등락률": "ChangePct"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "ChangePct" in df.columns:
        df["Change"] = df["ChangePct"] / 100.0
    else:
        df["Change"] = 0.0

    df = df.reset_index().rename(columns={"티커": "Code"})

    def _get_name_safe(ticker: str) -> str:
        try:
            return stock.get_market_ticker_name(ticker)
        except:
            return ""

    df["Name"] = df["Code"].map(_get_name_safe)
    df["Date"] = date

    keep_cols = ["Date","Open","High","Low","Close","Volume","Change","Code","Name"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[keep_cols]

    mask_bad = _invalid_ohlcv_mask(df)
    suspicious_codes = df.loc[mask_bad, "Code"].tolist()

    log(f"[KRX] {len(df)}개 종목 수집, 의심 {len(suspicious_codes)}개")
    return df, suspicious_codes

# =====================================================================================
# [복구] Fallback 소스 함수들
# =====================================================================================
def fetch_ohlcv_from_naver(ticker: str, yyyymmdd: str) -> Optional[dict]:
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={yyyymmdd}&endTime={yyyymmdd}"
    try:
        r = requests.get(url, timeout=5)
        arr = r.json()
        if not arr or len(arr) < 2:
            return None
        row = arr[1]
        return {
            "Open": float(row[1]), "High": float(row[2]),
            "Low": float(row[3]), "Close": float(row[4]),
            "Volume": float(row[5]),
        }
    except:
        return None

def fetch_ohlcv_from_fdr(ticker: str, yyyymmdd: str) -> Optional[dict]:
    try:
        start = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        end = start + dt.timedelta(days=1)
        df = fdr.DataReader(ticker, start, end)
        if df is None or df.empty:
            return None
        row = df.iloc[0]
        return {
            "Open": float(row["Open"]), "High": float(row["High"]),
            "Low": float(row["Low"]), "Close": float(row["Close"]),
            "Volume": float(row["Volume"]),
        }
    except:
        return None

def fetch_ohlcv_from_yahoo(ticker: str, yyyymmdd: str) -> Optional[dict]:
    try:
        dt_date = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date()
        start = dt_date.strftime("%Y-%m-%d")
        end = (dt_date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        for suffix in [".KS", ".KQ", ""]:
            df = yf.download(f"{ticker}{suffix}", start=start, end=end, progress=False)
            if df is None or df.empty:
                continue
            row = df.iloc[0]
            return {
                "Open": float(row["Open"]), "High": float(row["High"]),
                "Low": float(row["Low"]), "Close": float(row["Close"]),
                "Volume": float(row["Volume"]),
            }
        return None
    except:
        return None

FALLBACK_SOURCES: Iterable[Tuple[str, Callable[[str,str], Optional[dict]]]] = [
    ("fdr", fetch_ohlcv_from_fdr),
    ("yahoo", fetch_ohlcv_from_yahoo),
    ("naver", fetch_ohlcv_from_naver),
]

def fill_missing_with_sources(daily_df: pd.DataFrame, date: dt.date, codes: List[str],
                              sources: Optional[Iterable[Tuple[str,Callable[[str,str],Optional[dict]]]]] = None):
    if not codes:
        return daily_df, []
    if sources is None:
        sources = FALLBACK_SOURCES

    date_ymd = to_ymd(date)
    unresolved = list(dict.fromkeys(codes))

    for source_name, fetcher in sources:
        if not unresolved:
            break
        log(f"[{source_name.upper()}] 보조 수집 시작, {len(unresolved)}개 남음")

        still = []
        rows_update = {}

        for code in unresolved:
            o = fetcher(code, date_ymd)
            if not o:
                still.append(code)
                continue
            rows_update[code] = o

        for code, o in rows_update.items():
            idx = daily_df.index[daily_df["Code"] == code].tolist()
            if idx:
                i = idx[0]
                for col in ["Open","High","Low","Close","Volume"]:
                    daily_df.at[i, col] = o[col]
                daily_df.at[i, "Change"] = 0.0

        unresolved = still
        log(f"[{source_name.upper()}] 처리 후 남은 코드: {len(unresolved)}")

    return daily_df, unresolved


def build_daily_from_fallback_sources(date: dt.date, tickers: Iterable[str]) -> Tuple[pd.DataFrame, List[str]]:
    tickers = list(dict.fromkeys(tickers))
    base = pd.DataFrame({"Code": tickers})
    base["Date"] = date
    for col in ["Open","High","Low","Close","Volume","Change","Name"]:
        if col not in base.columns:
            base[col] = pd.NA

    daily_df, unresolved = fill_missing_with_sources(base, date, tickers, FALLBACK_SOURCES)
    mask_bad = _invalid_ohlcv_mask(daily_df)
    unresolved = list(dict.fromkeys(unresolved + daily_df.loc[mask_bad, "Code"].tolist()))
    log(f"[FALLBACK] 성공 {len(daily_df) - len(unresolved)}건, 실패 {len(unresolved)}건")
    return daily_df, unresolved


# =====================================================================================
# ⭐ 메인 실행부
# =====================================================================================
if __name__ == "__main__":
    print_header()

    now_dt = dt.datetime.now()
    today = now_dt.date()
    raw_df = load_raw_main()

    last_date = raw_df["Date"].max()
    log(f"[STEP 1] RAW 최신 날짜: {last_date}")

    # 16~18시는 오염 방지로 중단
    if dt.time(16, 0) <= now_dt.time() < dt.time(18, 0):
        log("[WARN] 16~18시는 오염 방지로 수집 중단")
        sys.exit(0)

    try:
        now_t = now_dt.time()
        is_biz = is_trading_day(today)

        # 1) 16:00 이전 → 무조건 전일(last_date)
        if now_t < dt.time(16, 0):
            target_date = last_date

        # 2) 16:00~18:00 → 위에서 차단됨 (pass)

        # 3) 18:00 이후
        else:
            if is_biz:
                target_date = today
            else:
                target_date = last_date

        # 4) 미래 날짜 방지
        if target_date > today:
            log("[SAFEGUARD] 미래 날짜로 점프 차단 → last_date로 조정")
            target_date = last_date

    except Exception as e:
        log(f"[ERROR] 날짜 판정 실패: {e}")
        sys.exit(1)

    log(f"[STEP 2] 수집 기간: {target_date} ~ {target_date}")
    date = target_date

    # -----------------------------------------------------------
    # [수정] 이미 해당 날짜 데이터가 존재하면 SKIP (단, 장중 업데이트는 고려 안함)
    # -----------------------------------------------------------
    if target_date == last_date:
        log(f"✅ [SKIP] 이미 최신 데이터({target_date})가 RAW 파일에 존재합니다.")
        log("   (추가 수집 없이 종료합니다.)")
        sys.exit(0)
    # -----------------------------------------------------------

    # ===========================
    # ⭐⭐ 1순위: pykrx
    # ===========================
    try:
        daily_df, bad_codes = build_daily_from_pykrx(date)
        log("[OK] KRX(pykrx) 데이터 사용")
    except Exception as e:
        log(f"[WARN] KRX(pykrx) 실패 → {e}")
        daily_df = None
        bad_codes = []

    # ===========================
    # ⭐⭐ 2순위: KIWOOM
    # ===========================
    if daily_df is None or daily_df.empty:
        try:
            daily_df, bad_codes = build_daily_from_kiwoom(date)
            log("[OK] KIWOOM 데이터 수집 성공. 2순위 소스로 사용.")
        except Exception as e:
            log(f"[WARN] KIWOOM 실패 → {e}")
            daily_df = None
            bad_codes = []

    # ⭐⭐ 3순위: FDR/Yahoo/Naver 전체 수집
    if daily_df is None or daily_df.empty:
        tickers = raw_df["Code"].unique().tolist()
        daily_df, bad_codes = build_daily_from_fallback_sources(date, tickers)
        if daily_df is None or daily_df.empty:
            log("[ERROR] FDR/Yahoo/Naver fallback 도 실패")
            sys.exit(1)
        else:
            log("[OK] FDR/Yahoo/Naver fallback 사용")

    # ===========================
    # ⭐⭐ 부족분 보조 수집
    # ===========================
    if bad_codes:
        try:
            # 자동 실행 환경에서는 y 입력 없이 자동 처리하거나, 기본값 사용
            # ans = input(...) -> 강제 진행
            pass 
        except Exception:
            pass

        # (1) Kiwoom으로 의심 코드 보완 시도
        try:
            kiw_df, _ = build_daily_from_kiwoom(date, tickers=bad_codes)
            if kiw_df is not None and not kiw_df.empty:
                kiw_sub = kiw_df[kiw_df["Code"].isin(bad_codes)]
                if not kiw_sub.empty:
                    daily_df = merge_daily_into_raw(daily_df, kiw_sub)
                    log(f"[KIWOOM] 보조 수집으로 {len(kiw_sub)}개 덮어씀")
        except Exception as e:
            log(f"[WARN] KIWOOM 보조 수집 실패 → {e}")

    # ===========================
    # SAVE
    # ===========================
    out_path = os.path.join(DAILY_DIR, f"daily_{to_ymd(date)}.parquet")
    daily_df.to_parquet(out_path)
    log(f"[SAVE] DAILY 저장 완료: {out_path}")

    merged = merge_daily_into_raw(raw_df, daily_df)
    
    # [수정] 날짜 태그 저장 로직 사용 (RAW_MAIN 덮어쓰기 대신)
    saved_path = save_dataframe_with_date(merged, STOCKS_DIR, "all_stocks_cumulative", date_col="Date")
    if saved_path:
        log(f"[SAVE] RAW 최신본 저장: {os.path.basename(saved_path)}")
    else:
        log("[SKIP] RAW 최신본 저장 건너뜀 (동일 날짜 파일 존재)")

    log("[DONE] RAW 업데이트 끝.")