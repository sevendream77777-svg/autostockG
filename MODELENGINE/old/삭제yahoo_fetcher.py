import pandas as pd
import yfinance as yf
from datetime import datetime

# 로그 함수가 정의되어 있지 않으므로 임시로 정의합니다.
def log(message):
    print(message)

# --- 여기에 fetch_from_yahoo 함수 전체 코드를 붙여넣습니다. ---
def fetch_from_yahoo(code: str, start: str, end: str) -> pd.DataFrame:
    """
    1차: Yahoo Finance에서 전체 구간 OHLCV 수집.
    MultiIndex 컬럼 구조를 평탄화하여 pandas 오류 방지.
    """
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)

            # --- 중요: MultiIndex → 1단계 컬럼 평탄화 ---
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df is not None and not df.empty:
                df = df.reset_index()

                # 날짜 변환 실패 방지
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"])

                df["Code"] = code

                df = df.rename(columns={
                    "Open": "Open",
                    "High": "High",
                    "Low": "Low",
                    "Close": "Close",
                    "Volume": "Volume"
                })

                # 필요한 컬럼만
                df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

                # 타입 정규화
                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                log(f"[YAHOO] {code} 전체 히스토리 ✓ (ticker={ticker}, rows={len(df)})")
                return df

            else:
                log(f"[YAHOO] {code} 빈 데이터 (ticker={ticker})")

        except Exception as e:
            log(f"[YAHOO] {code} 예외 발생 (ticker={ticker}): {e}")

    return pd.DataFrame()
# -------------------------------------------------------------

# 함수 호출 예시 (삼성전자)
if __name__ == "__main__":
    # 참고: yfinance는 한국 종목에 ".KS" 또는 ".KQ" 접미사가 필요합니다.
    data = fetch_from_yahoo("005930", "2023-01-01", datetime.now().strftime("%Y-%m-%d"))
    if not data.empty:
        print("\n--- 수집된 데이터 ---")
        print(data.head())
    else:
        print("\n데이터 수집 실패")