# -*- coding: utf-8 -*-
# ============================================================
# [update_data_incrementally.py] V40 - KRX 일별 시세 증분 업데이트
#   - 기존 all_stocks_cumulative.parquet 에 이어붙이기(append)
#   - 기존 파일 삭제/rename 없음 (절대 날리지 않음)
#   - 10년치가 이미 있으면, 마지막 날짜+1일부터 오늘까지 추가
#   - FinanceDataReader 기반
# ============================================================

import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd

# ------------------------------------------------------------
# 경로 설정
#   - 이 파일이 있는 폴더 기준으로 all_stocks_cumulative.parquet 관리
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "all_stocks_cumulative.parquet")
BACKUP_DIR = os.path.join(BASE_DIR, "backup_raw")
PARTIAL_FILE = os.path.join(BASE_DIR, "all_stocks_cumulative_partial.parquet")

os.makedirs(BACKUP_DIR, exist_ok=True)

print("=================================================")
print("[update_data_incrementally.py] ▶ 실행 시작... (V40)")
print("BASE_DIR :", BASE_DIR)
print("DATA_FILE:", DATA_FILE)
print("=================================================")


# ------------------------------------------------------------
# 유틸 함수
# ------------------------------------------------------------
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backup_existing_file(path: str):
    """기존 원본을 백업 폴더에 타임스탬프 붙여 복사"""
    if not os.path.exists(path):
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(path)
    backup_name = f"{os.path.splitext(base)[0]}_{ts}.parquet"
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    try:
        print(f"[{now_str()}] 🔄 기존 파일 백업 생성: {backup_path}")
        pd.read_parquet(path).to_parquet(backup_path)
    except Exception as e:
        print(f"[{now_str()}] ⚠ 백업 중 오류 (계속 진행): {e}")


def load_existing_data():
    """기존 all_stocks_cumulative.parquet 로드"""
    if not os.path.exists(DATA_FILE):
        print(f"[{now_str()}] ℹ 기존 데이터 파일이 없습니다. (처음 실행)")
        return None

    try:
        df = pd.read_parquet(DATA_FILE)
        if "Date" not in df.columns or "Code" not in df.columns:
            raise ValueError("기존 파일에 Date/Code 컬럼이 없습니다.")
        df["Date"] = pd.to_datetime(df["Date"])
        print(
            f"[{now_str()}] 📥 기존 데이터 로드 완료: {len(df):,}행 "
            f"(최초일자={df['Date'].min().date()}, 최신일자={df['Date'].max().date()})"
        )
        return df
    except Exception as e:
        print(f"[{now_str()}] ❌ 기존 데이터 로드 실패: {e}")
        raise


def get_update_range(existing_df: pd.DataFrame | None):
    """증분 업데이트 시작일, 종료일 계산"""
    today = datetime.today().date()

    # 주말/장전 고려해서 '최대 수집 종료일'을 살짝 과거로 잡아도 됨.
    # 일단 최대 today 기준으로 두되, 실제 데이터가 없으면 FDR이 비어있는 DF를 줄 것.
    end_date = today

    if existing_df is None:
        # 처음 실행인 경우: 2015-01-01부터 시작
        start_date = datetime(2015, 1, 1).date()
    else:
        last_date = existing_df["Date"].max().date()
        start_date = last_date + timedelta(days=1)

    if start_date > end_date:
        print(
            f"[{now_str()}] ✅ 이미 최신입니다. (기존 최신일자={existing_df['Date'].max().date() if existing_df is not None else 'N/A'})"
        )
        return None, None

    print(
        f"[{now_str()}] 📅 업데이트 범위: {start_date} ~ {end_date} "
        f"({(end_date - start_date).days + 1}일)"
    )
    return start_date, end_date


def get_krx_tickers():
    """FinanceDataReader로 KRX 전체 종목 리스트 로드"""
    print(f"[{now_str()}] 🧾 KRX 종목 리스트 로드 중...")
    tickers = fdr.StockListing("KRX")
    cols = tickers.columns.tolist()
    print(f"[{now_str()}] ✅ KRX 리스트 컬럼: {cols}")

    # 코드 컬럼 자동 감지
    code_col = None
    for cand in ["Symbol", "Code"]:
        if cand in tickers.columns:
            code_col = cand
            break
    if code_col is None:
        raise KeyError("❌ 'Symbol' 또는 'Code' 컬럼을 찾을 수 없습니다. (FDR 버전 확인 필요)")

    name_col = "Name" if "Name" in tickers.columns else None
    market_col = None
    for cand in ["Market", "시장구분"]:
        if cand in tickers.columns:
            market_col = cand
            break

    tickers = tickers[[c for c in [code_col, name_col, market_col] if c is not None]].copy()
    tickers.rename(
        columns={code_col: "Code", name_col: "Name" if name_col else "Name", market_col: "Market" if market_col else "Market"},
        inplace=True,
    )

    if "Name" not in tickers.columns:
        tickers["Name"] = ""

    if "Market" not in tickers.columns:
        tickers["Market"] = ""

    tickers["Code"] = tickers["Code"].astype(str).str.zfill(6)

    print(f"[{now_str()}] ✅ KRX 종목 수: {len(tickers):,}개")
    return tickers


def fetch_price_one(code: str, start: datetime.date, end: datetime.date, name: str, market: str, max_retry: int = 3, sleep_sec: float = 0.4):
    """단일 종목 시세 데이터 수집 (FinanceDataReader, 재시도 포함)"""
    for attempt in range(1, max_retry + 1):
        try:
            df = fdr.DataReader(code, start, end)
            if df is None or df.empty:
                return None

            df = df.copy()
            if not isinstance(df.index, pd.DatetimeIndex):
                df = df.set_index(pd.to_datetime(df.index, errors="coerce"))

            df = df.reset_index().rename(columns={"index": "Date"})
            if "Date" not in df.columns:
                # FDR 버전 문제 시 방어
                if df.index.name is not None:
                    df = df.reset_index().rename(columns={df.columns[0]: "Date"})
                else:
                    return None

            df["Date"] = pd.to_datetime(df["Date"])
            df["Code"] = code
            df["Name"] = name
            df["Market"] = market
            return df
        except Exception as e:
            print(f"[{now_str()}] ⚠ {code} 시세 수집 실패 (시도 {attempt}/{max_retry}) → {e}")
            if attempt < max_retry:
                time.sleep(sleep_sec)
            else:
                return None


# ------------------------------------------------------------
# 메인 로직
# ------------------------------------------------------------
def main():
    print(f"[{now_str()}] ▶ 증분 업데이트 시작 (V40)")

    # 1) 기존 데이터 로드
    existing_df = load_existing_data()

    # 2) 증분 범위 계산
    start_date, end_date = get_update_range(existing_df)
    if start_date is None:
        print("=================================================")
        print("[update_data_incrementally.py] ✅ 이미 최신 상태입니다. 종료.")
        print("=================================================")
        return

    # 3) KRX 전체 종목 리스트
    tickers = get_krx_tickers()

    # 4) 수집 루프
    all_new = []
    success_count = 0
    fail_count = 0

    total = len(tickers)
    print(f"[{now_str()}] 🚀 시세 수집 시작: {total:,} 종목 대상")

    for idx, row in tickers.iterrows():
        code = str(row["Code"]).zfill(6)
        name = row.get("Name", "")
        market = row.get("Market", "")

        df_new = fetch_price_one(code, start_date, end_date, name, market)
        if df_new is None or df_new.empty:
            fail_count += 1
        else:
            all_new.append(df_new)
            success_count += 1

        # 진행 상황 출력 (간단)
        if (idx + 1) % 100 == 0 or (idx + 1) == total:
            print(
                f"[{now_str()}] ▶ 진행: {idx + 1}/{total} "
                f"(성공={success_count}, 실패={fail_count})"
            )

        # 500 종목 단위 임시 저장
        if success_count > 0 and success_count % 500 == 0:
            temp = pd.concat(all_new, ignore_index=True)
            temp.to_parquet(PARTIAL_FILE)
            print(
                f"[{now_str()}] 💾 임시 저장: {PARTIAL_FILE} "
                f"({len(temp):,}행, 성공종목={success_count})"
            )

    if not all_new:
        print(f"[{now_str()}] ⚠ 새로 추가된 데이터가 없습니다. (성공한 종목 없음)")
        print("=================================================")
        return

    new_df = pd.concat(all_new, ignore_index=True)
    new_df["Date"] = pd.to_datetime(new_df["Date"])
    new_df.sort_values(["Date", "Code"], inplace=True)
    print(
        f"[{now_str()}] 📊 신규 수집 결과: {len(new_df):,}행 "
        f"(기간={new_df['Date'].min().date()} ~ {new_df['Date'].max().date()})"
    )

    # 5) 기존 데이터와 병합 (append + 중복 제거)
    if existing_df is not None:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    # 중복 제거
    key_cols = ["Date", "Code"]
    combined.drop_duplicates(subset=key_cols, keep="last", inplace=True)
    combined.sort_values(["Date", "Code"], inplace=True).reset_index(drop=True, inplace=True)

    print(
        f"[{now_str()}] 📦 병합 후 전체 행수: {len(combined):,}행 "
        f"(최초일={combined['Date'].min().date()}, 최신일={combined['Date'].max().date()})"
    )

    # 6) 최종 저장 전 백업
    backup_existing_file(DATA_FILE)

    # 7) 최종 저장
    combined.to_parquet(DATA_FILE, index=False)
    print(f"[{now_str()}] 💾 최종 저장 완료 → {DATA_FILE}")
    print("=================================================")
    print("[update_data_incrementally.py] ✅ 증분 업데이트 완료 (V40)")
    print("=================================================")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[{now_str()}] ⏹ 사용자 중단 (KeyboardInterrupt)")
        print("현재까지 저장된 데이터는 all_stocks_cumulative.parquet / partial 에 반영된 수준입니다.")
    except Exception as e:
        print("=================================================")
        print(f"[{now_str()}] ❌ 치명적 오류 발생: {e}")
        traceback.print_exc()
        print("=================================================")
        sys.exit(1)
