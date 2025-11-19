import requests
import json
import time # 지수 백오프 및 강제 대기 시간을 위해 time 모듈 추가
from datetime import datetime, timedelta
import re # Naver JSON 파싱을 위해 정규표현식(re) 모듈 추가

# --- 테스트 설정 ---
# 요청하신 날짜를 유지했습니다. 2023/11/18, 19일은 주말, 2025년 날짜는 미래이므로 데이터가 없습니다.
TARGET_DATES = ['20231117', '20231118', '20231119', '20251117', '20251118', '20251119']
# 과거 날짜를 포함하기 위해 최근 30일치 데이터를 요청합니다.
FETCH_COUNT = 30
# 테스트 종목을 상위 3개로 축소했습니다.
STOCKS_TO_TEST = [
    {'name': '삼성전자', 'symbol_naver': '005930', 'ticker_yahoo': '005930.KS'},
    {'name': 'SK하이닉스', 'symbol_naver': '000660', 'ticker_yahoo': '000660.KS'},
    {'name': 'LG에너지솔루션', 'symbol_naver': '373220', 'ticker_yahoo': '373220.KS'},
]

print(f"--- 주식 시세 데이터 테스트 스크립트 실행 ({len(STOCKS_TO_TEST)} 종목 고정) ---")
print(f"목표 날짜: {TARGET_DATES} (2023년 17일 데이터는 있어야 합니다. 18, 19일은 휴장일/미래)")
print(f"현재 날짜 기준: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# --- 헬퍼 함수 ---

def request_with_retry(url, headers=None, max_retries=5):
    """지수 백오프를 사용하여 API 요청을 재시도하는 함수입니다."""
    if headers is None:
        headers = {}
        
    for attempt in range(max_retries):
        try:
            # 모든 요청에 대해 User-Agent를 사용하여 봇 감지를 완화합니다.
            if 'User-Agent' not in headers:
                 headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                 
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            # 429, 500, 503 오류 발생 시 재시도
            if attempt < max_retries - 1 and e.response is not None and e.response.status_code in (429, 500, 503):
                wait_time = 2 ** attempt  # 1초, 2초, 4초, 8초 대기
                print(f"   - 요청 오류 ({e.response.status_code}): {url}. {wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                raise e # 재시도 횟수를 초과하거나 복구 불가능한 오류 (404 등) 시 예외 발생
    return None

def safe_parse_naver_list(raw_text):
    """
    Naver siseJson의 독특한 문자열 형태 (예: ' [["날짜",...][...]] ')를 안전하게 JSON/List로 변환합니다.
    """
    try:
        # 텍스트에서 유효한 JSON 배열 (대괄호 두 쌍으로 시작하는) 부분만 추출
        match = re.search(r'\[\[.*?\]\]', raw_text, re.DOTALL)
        if match:
            # 추출된 문자열을 파싱
            return json.loads(match.group(0))
        return None
    except Exception as e:
        return None

# filter_and_display_data 함수는 Naver SiseJson의 raw output 출력을 위해 전역으로 유지
data_list = None
def filter_and_display_data(stock_data, api_name, symbol_name, target_dates, key_map):
    """요청된 데이터에서 목표 날짜의 데이터를 찾아 출력합니다."""
    global data_list
    found_count = 0
    
    if not stock_data:
        print(f"   - [{api_name}] 필터링할 데이터가 없습니다.")
        return

    target_date_set = set(target_dates)
    past_dates = [d for d in target_dates if d.startswith('2023')]
    future_dates = [d for d in target_dates if d.startswith('2025')]
    
    for item in stock_data:
        try:
            date_str = None
            
            if isinstance(item, list) and len(item) > key_map.get('date_index', -1):
                # Naver siseJson API의 경우 (헤더 제외)
                date_str = str(item[key_map['date_index']])
            elif isinstance(item, int) and api_name == "Yahoo Finance Timestamps":
                 # Yahoo Finance의 Unix timestamp의 경우
                date_str_full = datetime.fromtimestamp(item).strftime('%Y%m%d')
                date_str = date_str_full

            if date_str in target_date_set:
                found_count += 1
                
                output = f"   - [✓] 날짜: {date_str}"
                
                if api_name == "Yahoo Finance Timestamps":
                    # Yahoo Finance는 별도의 함수에서 처리했으므로 여기서는 건너뜁니다.
                    continue
                elif isinstance(item, list): # Naver SiseJson
                    # 리스트에서 인덱스를 사용하여 종가와 거래량을 가져옵니다.
                    close_price = item[key_map['close_index']] if len(item) > key_map['close_index'] else 'N/A'
                    volume = item[key_map['volume_index']] if len(item) > key_map['volume_index'] else 'N/A'
                    output += f" | 종가: {close_price}, 거래량: {volume}"

                print(output)
        except Exception as e:
            pass
            
    if found_count == 0:
        print(f"   - [!] 요청한 날짜({', '.join(target_date_set)})에 해당하는 데이터를 찾을 수 없습니다. (미래 날짜, 휴장일 또는 데이터 부재)")
        
        # 2025년 날짜에 대한 명확한 설명 추가
        if future_dates:
             print(f"   - [i] 참고: {', '.join(future_dates)}는 미래 날짜이므로 데이터가 존재하지 않습니다.")
        
        # 2023년 18일, 19일은 주말이므로 데이터가 없는 것이 정상임을 설명
        if any(d in ['20231118', '20231119'] for d in target_dates):
             print(f"   - [i] 참고: 20231118(토), 20231119(일)은 주말 휴장일이므로 데이터가 없습니다.")


    # Naver SiseJson은 리스트 형태이므로, 딕셔너리 출력을 위해 첫 부분만 문자열로 변환하여 출력
    if api_name != "Yahoo Finance Timestamps" and data_list:
        print(f"   - 모든 데이터 (원문 JSON의 첫 500자): {json.dumps(data_list, ensure_ascii=False)[:500]}...")
    print("-" * 50)


# --- API 요청 함수 ---


def fetch_yahoo_finance(ticker, name, target_dates):
    """
    Yahoo Finance API를 사용하여 특정 종목의 차트 데이터를 가져옵니다.
    range=30d를 사용하여 최근 30일 데이터를 요청하고, 목표 날짜를 필터링합니다.
    """
    print(f"1. Yahoo Finance (query1.finance.yahoo.com) 데이터 요청 (종목: {name} / {ticker})...")
    # range를 30일로 늘렸습니다.
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=30d" 
    
    data = None 
    
    try:
        # User-Agent 헤더를 추가하여 429 오류 완화 시도
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; MyStockBot/1.0)'}
        response = request_with_retry(url, headers=headers)

        data = response.json()
        print("   => 요청 성공. 데이터 구조 확인:")

        if data['chart']['result']:
            result = data['chart']['result'][0]
            indicators = result['indicators']['quote'][0]
            timestamps = result['timestamp']

            if timestamps and indicators['close']:
                found_count = 0
                for i, timestamp in enumerate(timestamps):
                    date_str = datetime.fromtimestamp(timestamp).strftime('%Y%m%d')
                    if date_str in target_dates:
                        found_count += 1
                        close_price = indicators['close'][i]
                        volume = indicators['volume'][i]
                        print(f"   - [✓] 날짜: {date_str} | 종가: {close_price}, 거래량: {volume}")
                
                if found_count == 0:
                    print(f"   - [!] 요청한 날짜({', '.join(target_dates)})에 해당하는 데이터를 찾을 수 없습니다. (데이터 부재 또는 휴장일)")
                    
                    future_dates = [d for d in target_dates if d.startswith('2025')]
                    if future_dates:
                         print(f"   - [i] 참고: {', '.join(future_dates)}는 미래 날짜이므로 데이터가 존재하지 않습니다.")

            else:
                print("   - 데이터가 비어 있습니다.")
        else:
            print("   - Yahoo Finance 응답에 결과(result)가 없습니다.")

    except requests.exceptions.RequestException as e:
        print(f"   !!! Yahoo Finance 요청 중 오류 발생: {e}")
    except Exception as e:
        print(f"   !!! 데이터 처리 중 오류 발생: {e}")
    
    if data:
        print(f"   - 모든 데이터 (원문 JSON의 첫 500자): {json.dumps(data, ensure_ascii=False)[:500]}...")
    print("-" * 50)


def fetch_naver_sise_json(symbol, name, target_dates, fetch_count):
    """
    Naver Finance siseJson API를 사용하여 특정 종목의 일별 시세를 가져옵니다.
    count를 늘려 여러 날짜를 가져옵니다.
    """
    global data_list
    print(f"2. Naver Finance SiseJson (api.finance.naver.com/siseJson.naver) 데이터 요청 (종목: {name} / {symbol})...")
    # 시작일자를 명시적으로 넣어 과거 데이터를 더 확실히 가져오도록 개선 (Naver API 특성)
    today = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=FETCH_COUNT)).strftime('%Y%m%d')
    url = f"https://api.finance.naver.com/siseJson.naver?symbol={symbol}&requestType=1&timeframe=day&count={fetch_count}&startTime={start_date}&endTime={today}"
    
    data_list = None

    try:
        response = request_with_retry(url)
        raw_text = response.text.strip()
        
        # 정규표현식을 사용한 강력한 파싱 시도
        data_list = safe_parse_naver_list(raw_text)

        print("   => 요청 성공. 데이터 구조 확인:")
        
        if data_list and len(data_list) > 1:
            header = data_list[0]
            data_candles = data_list[1:]

            # 헤더에서 인덱스 추출
            try:
                key_map = {
                    'date_index': header.index("날짜"),
                    'close_index': header.index("종가"),
                    'volume_index': header.index("거래량")
                }
                
                # 필터링 및 출력
                filter_and_display_data(data_candles, "Naver SiseJson", name, target_dates, key_map)
                
            except ValueError:
                print("   - 데이터 헤더에 필요한 필드('날짜', '종가', '거래량')가 없습니다.")
        else:
            print("   - 데이터가 비어 있거나 형식이 예상과 다릅니다. (파싱 실패 가능성)")

    except requests.exceptions.RequestException as e:
        print(f"   !!! Naver Finance SiseJson 요청 중 오류 발생: {e}")
    except Exception as e:
        print(f"   !!! 데이터 처리 중 오류 발생: {e}")

    data_list = None # 다음 종목을 위해 전역 변수 초기화


def krx_api_note():
    """
    KRX 및 공공데이터포털 API 참고 사항
    """
    print("3. KRX 및 공공 데이터 포털 API 참고 사항...")
    print("   - 기존 KRX API (api.krx.co.kr)는 OTP와 복잡한 POST 파라미터가 필요해 간단한 테스트 불가.")
    print("   - [⭐ 안정적인 대체 API (공식): 공공데이터포털 - 금융위원회_주식시세정보]")
    print("     - **URL:** apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService")
    print("     - **특징:** 공공기관(한국거래소)에서 제공하는 공식 데이터이며, 가장 안정적이지만,")
    print("     - **요구 사항:** 별도의 **인증키(API Key)**를 발급받아야 하며, 응답 형식이 **XML**이라 파싱이 더 복잡합니다.")
    print("     - 상업적 용도로 사용하기 위해서는 라이선스 확인이 필요합니다.")
    print("-" * 50)


if __name__ == '__main__':
    
    print("-" * 50)

    # 각 종목별로 모든 API 테스트 함수를 실행합니다.
    for stock in STOCKS_TO_TEST:
        print(f"\n==================================================")
        print(f"테스트 종목: {stock['name']} ({stock['symbol_naver']})")
        print(f"==================================================")
        
        # 1. Yahoo Finance 테스트 (User-Agent 추가)
        fetch_yahoo_finance(stock['ticker_yahoo'], stock['name'], TARGET_DATES)
        time.sleep(2) # Yahoo API Rate Limit 방지를 위해 요청 후 2초 대기 (강화)

        # 2. Naver siseJson 테스트 (파싱 로직 강화)
        fetch_naver_sise_json(stock['symbol_naver'], stock['name'], TARGET_DATES, FETCH_COUNT)
        time.sleep(1) # 요청 후 1초 대기 (안정성 확보)

    # 3. KRX API 참고 사항 (공공데이터포털 정보 포함)
    krx_api_note()