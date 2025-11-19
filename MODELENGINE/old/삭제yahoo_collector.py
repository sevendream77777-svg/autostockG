import pandas as pd
import yfinance as yf
from datetime import datetime

# --- 필수 정의: 로그 함수 ---
# 실제 환경에 맞게 메시지를 콘솔에 출력하는 임시 함수를 정의합니다.
def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# --- 사용자 제공 함수: fetch_from_yahoo ---
def fetch_from_yahoo(code: str, start: str, end: str) -> pd.DataFrame:
    """
    Yahoo Finance OHLCV 전체 구간 수집 (MultiIndex / Price.* 구조 자동 교정 포함)
    """
    for suffix in [".KS", ".KQ"]:
        ticker = f"{code}{suffix}"
        try:
            # 데이터 다운로드 (yfinance 사용)
            df = yf.download(ticker, start=start, end=end, progress=False)

            # --- 중요: MultiIndex → 평탄화 ---
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # --- Yahoo Price.* 자동 정리 ---
            # 예) Price.Open → Open
            clean_cols = []
            for c in df.columns:
                if isinstance(c, str) and "." in c:
                    clean_cols.append(c.split(".")[-1])  # Price.Open → Open
                else:
                    clean_cols.append(c)
            df.columns = clean_cols

            # 실제 필요한 컬럼이 있는지 확인
            needed = {"Open", "High", "Low", "Close", "Volume"}
            if not needed.issubset(set(df.columns)): # set(df.columns)로 명확히 비교
                log(f"[YAHOO] {code} 컬럼 부족 (ticker={ticker}) → 스킵")
                continue

            if df is not None and not df.empty:
                df = df.reset_index()

                # 날짜 변환 및 정리
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"])

                # 코드 컬럼 추가
                df["Code"] = code

                # 최종 확정 컬럼 순서에 맞게 정리
                df = df[["Date", "Code", "Open", "High", "Low", "Close", "Volume"]]

                # float 변환 및 float64 강제 타입 지정
                for col in ["Open", "High", "Low", "Close", "Volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

                log(f"[YAHOO] {code} 전체 히스토리 ✓ (ticker={ticker}, rows={len(df)})")
                return df

            else:
                log(f"[YAHOO] {code} 빈 데이터 (ticker={ticker})")

        except Exception as e:
            log(f"[YAHOO] {code} 예외 발생 (ticker={ticker}): {e}")

    return pd.DataFrame()


# -----------------------------------------------
# 실행 부분: 함수를 호출하여 데이터를 가져옵니다.
# -----------------------------------------------
if __name__ == "__main__":
    # 1. 수집할 종목 코드와 기간을 설정합니다. (예시: 삼성전자)
    STOCK_CODE = "005930"
    START_DATE = "2020-01-01"
    END_DATE = datetime.now().strftime("%Y-%m-%d")

    # 2. 함수 호출
    result_df = fetch_from_yahoo(STOCK_CODE, START_DATE, END_DATE)

    # 3. 결과 출력
    if not result_df.empty:
        print("\n--- 수집 완료된 데이터 (상위 5개 행) ---")
        print(result_df.head().to_string())
        print(f"\n[INFO] 최종 수집 행: {len(result_df)}개")
    else:
        print("\n[INFO] 데이터 수집 실패 또는 데이터 없음.")