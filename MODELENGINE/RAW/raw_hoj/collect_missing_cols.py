import pandas as pd
import yfinance as yf
from pykrx import stock
import requests
import datetime
import os
from pathlib import Path

# fredapi optional
try:
    from fredapi import Fred
except ImportError:
    Fred = None

ROOT = Path(__file__).resolve().parent

# ==========================================
# 키 로더: env → 로컬 파일 → 공유 경로 (사용자 파일 기반 복원)
# ==========================================
def _load_key(env_name: str, paths: list[Path], default: str = "") -> str:
    val = os.environ.get(env_name)
    if val:
        return val.strip()
    for p in paths:
        if p.exists():
            try:
                return p.read_text(encoding="utf-8").strip()
            except Exception:
                continue
    return default

FRED_API_KEY = _load_key("FRED_API_KEY", [ROOT / "fred_apikey.txt"], default="")
ECOS_API_KEY = _load_key(
    "ECOS_API_KEY",
    [
        ROOT / "ecos_apikey.txt",
        Path(r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\ecos_open_api.txt"),
    ],
    default="",
)

# ==========================================
# 체크 함수들
# ==========================================

def check_yfinance():
    print("\n" + "="*50)
    print("[1] yfinance (Yahoo Finance) 연결 테스트")
    print("="*50)
    
    try:
        # 애플(AAPL)
        apple = yf.Ticker("AAPL")
        hist = apple.history(period="5d")
        
        if not hist.empty:
            print(f"✅ 성공: Apple(AAPL) 최근 5일 데이터")
            print(hist[['Open', 'Close']].tail(2))
        else:
            print("❌ 실패: 데이터가 비어있습니다.")

        # 삼성전자(005930.KS)
        samsung = yf.Ticker("005930.KS")
        hist_s = samsung.history(period="5d")
        
        if not hist_s.empty:
            print(f"✅ 성공: 삼성전자(005930.KS) 데이터 수신 완료")
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

def check_pykrx():
    print("\n" + "="*50)
    print("[2] PyKRX (한국거래소) 연결 테스트")
    print("="*50)
    
    try:
        today = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y%m%d")
        
        df = stock.get_market_ohlcv(start_date, today, "005930")
        
        if not df.empty:
            print(f"✅ 성공: PyKRX 삼성전자 데이터 ({start_date} ~ {today})")
            print(df.tail(2))
        else:
            print("❌ 실패: 데이터를 가져오지 못했습니다.")
            
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

def check_naver_finance():
    print("\n" + "="*50)
    print("[3] 네이버 금융 (크롤링) 테스트")
    print("="*50)
    
    try:
        url = "https://finance.naver.com/item/sise_day.naver?code=005930&page=1"
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            try:
                dfs = pd.read_html(response.text)
                price_df = dfs[0].dropna()
                
                if not price_df.empty:
                    print("✅ 성공: 네이버 금융 일별 시세")
                    print(price_df.head(2))
                else:
                    print("❌ 실패: 데이터 없음")
            except ValueError:
                print("❌ 실패: HTML 테이블 파싱 오류")
        else:
            print(f"❌ 실패: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

def check_fred():
    """
    FRED 키가 없거나 유효하지 않으면 yfinance로 자동 대체
    """
    print("\n" + "="*50)
    print("[4] FRED (미국 경제 데이터) 연결 테스트")
    print("="*50)
    
    use_fallback = False
    
    # 키가 없으면 Fallback 사용
    if not FRED_API_KEY:
        print("ℹ️ 알림: FRED API Key가 감지되지 않았습니다.")
        use_fallback = True
    else:
        # 키가 있어도 실제 호출 시 에러가 나면 Fallback으로 전환 시도
        try:
            if Fred is None:
                raise ImportError("fredapi 모듈이 설치되지 않음")
                
            fred = Fred(api_key=FRED_API_KEY)
            data = fred.get_series('GDP')
            if not data.empty:
                print("✅ 성공: FRED GDP 데이터 수신 완료")
                print(data.tail(2))
                return # 성공 시 종료
        except Exception as e:
            print(f"⚠️ FRED API 호출 실패 (키 문제 또는 라이브러리 미설치): {e}")
            use_fallback = True

    if use_fallback:
        print("👉 [대체 로직 실행] yfinance를 통해 미국 10년물 국채 금리(^TNX)를 조회합니다.")
        try:
            treasury = yf.Ticker("^TNX")
            data = treasury.history(period="5d")
            
            if not data.empty:
                print("✅ 성공: (대체) 미국 10년물 국채 금리 수신 완료")
                print(data[['Close']].tail(2))
            else:
                print("❌ 실패: 대체 데이터(^TNX)도 가져오지 못했습니다.")
        except Exception as e:
            print(f"❌ 에러 발생: {e}")

def check_ecos():
    print("\n" + "="*50)
    print("[5] ECOS (한국은행) 연결 테스트")
    print("="*50)
    
    if not ECOS_API_KEY:
        print("⚠️ 경고: ECOS API Key를 찾을 수 없습니다. (지정된 경로 확인 필요)")
        return

    try:
        # 기준금리 조회 (722Y001)
        url = f"http://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/10/722Y001/D/20230101/20230131/0101000"
        
        response = requests.get(url)
        data = response.json()
        
        if 'StatisticSearch' in data:
            print("✅ 성공: ECOS 기준금리 데이터 수신 완료")
            row = data['StatisticSearch']['row'][0]
            print(f"날짜: {row['TIME']}, 금리: {row['DATA_VALUE']}")
        else:
            msg = data.get('RESULT', {}).get('MESSAGE', '알 수 없는 오류')
            print(f"❌ 실패: {msg}")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")

if __name__ == "__main__":
    print(">>> 금융 데이터 소스 통합 테스트 (DART 제외) <<<\n")
    print(f"✅ 키 로딩 확인 - ECOS: {'Found' if ECOS_API_KEY else 'Not Found'}, FRED: {'Found' if FRED_API_KEY else 'Not Found'}\n")

    check_yfinance()
    check_pykrx()
    check_naver_finance()
    check_fred()   # 키 문제 발생 시 자동 대체
    check_ecos()
    
    print("\n>>> 테스트 종료 <<<")