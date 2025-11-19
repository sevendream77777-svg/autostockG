# --- 코드 버전: V10 (Naver Filter) ---
import pandas as pd
import pandas_ta as ta
import numpy as np
import lightgbm as lgb
import joblib 
import pykrx
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import os 
import requests # (★★★ V10: 실시간 스크래핑용 ★★★)
from bs4 import BeautifulSoup # (★★★ V10: 실시간 스크래핑용 ★★★)

# --- 1. 설정 ---
MODEL_FILE = "champion_model_60_5.pkl" 
FEATURE_FILE = "all_features_cumulative.parquet" # (V5 12개 피처 DB)
TOP_N_STOCKS = 10  # 1차 필터링 (Top 10)
FINAL_N_STOCKS = 5 # 2차 필터링 (V10 최종 5개)

# V5 피처 리스트 (모델 학습 때와 동일)
feature_columns_v5 = [
    'SMA_20', 'SMA_60', 'RSI_14', 'VOL_SMA_20', 'MACD', 'MACD_Sig',
    'BBP_20', 'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
]

# (★★★ V10 신규 함수: 네이버 금융 실시간 스크래핑 ★★★)
def get_v10_filter_data(ticker):
    """
    네이버 금융에서 PBR, PER, 외국인/기관 수급을 실시간 스크래핑.
    데이터가 없거나 에러 발생 시 9999 (패널티) 반환.
    """
    pbr, per, for_net, ins_net = 9999, 9999, -999999, -999999 # (기본값 = 패널티)
    
    try:
        # 1. PBR/PER 수집 (네이버 금융 메인)
        url_main = f"https://finance.naver.com/item/main.naver?code={ticker}"
        headers = {'User-Agent': 'Mozilla/5.0'} # (차단 방지용 헤더)
        resp_main = requests.get(url_main, headers=headers, timeout=3)
        soup_main = BeautifulSoup(resp_main.text, 'lxml')
        
        # PER/PBR 파싱
        per_tag = soup_main.select_one('em#_per')
        pbr_tag = soup_main.select_one('em#_pbr')
        
        if per_tag and per_tag.text not in ['N/A', '0.00']:
            per = float(per_tag.text.replace(',', ''))
        if pbr_tag and pbr_tag.text not in ['N/A', '0.00']:
            pbr = float(pbr_tag.text.replace(',', ''))
            
        # 2. 수급 수집 (네이버 금융 투자자별 매매동향)
        url_supply = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        resp_supply = requests.get(url_supply, headers=headers, timeout=3)
        df_supply = pd.read_html(resp_supply.text, encoding='euc-kr')[0]
        
        # (최근 5일치 데이터 중 '순매매량' 행을 찾음)
        df_supply = df_supply.iloc[[-4, -3]].set_index('날짜') 
        
        if '기관' in df_supply.columns and '외국인' in df_supply.columns:
            ins_net = float(df_supply.loc['순매매량']['기관'].replace(',', ''))
            for_net = float(df_supply.loc['순매매량']['외국인'].replace(',', ''))
            
    except Exception as e:
        print(f"  > V10 필터 ({ticker}) 스크래핑 실패: {e}")
        pass # (실패 시 기본값(패널티) 사용)
        
    return pbr, per, for_net, ins_net


def get_today_prediction_list(model):
    print(f"[1] '{FEATURE_FILE}' (최종 피처 데이터) 로드 중...")
    
    if not os.path.exists(FEATURE_FILE):
        print(f"  > 오류: '{FEATURE_FILE}' 파일이 없습니다."); return None, None
        
    try:
        df_processed = pd.read_parquet(FEATURE_FILE)
        print("  > 로드 성공. (1초)")
    except Exception as e:
        print(f"  > 오류: {FEATURE_FILE} 파일 로드 실패. ({e})"); return None, None
        
    # 2. 유효한 '오늘의 예측 기준 날짜' 데이터만 추출
    latest_date = df_processed['날짜'].max()
    today_data = df_processed[df_processed['날짜'] == latest_date]
    
    if today_data.empty:
        print("  > 오류: '오늘' 날짜의 데이터가 없습니다."); return None, None

    # 3. '오늘' 데이터로 '예측'
    print(f"  > '{latest_date.strftime('%Y-%m-%d')}' 종가 기준 '예상 수익률' 예측 중...")
    X_today = today_data[feature_columns_v5]
    X_today.columns = ["".join (c if c.isalnum() else "_" for c in str(col)) for col in X_today.columns]
    
    today_predictions = model.predict(X_today)
    
    # 4. '오늘'의 Top 10 리스트 생성 및 필터링
    df_today_result = pd.DataFrame({
        '종목코드': today_data['종목코드'],
        '종목명': [pykrx.stock.get_market_ticker_name(t) for t in today_data['종목코드']],
        '현재가': today_data['종가'],
        '예상수익률': today_predictions
    })
    
    # (필터링: 동전주만 제거)
    df_today_result = df_today_result[(df_today_result['현재가'] >= 1000)] # 1000원 미만 제외

    df_today_result.sort_values(by='예상수익률', ascending=False, inplace=True)
    
    # 1단계: Top 10 (패턴) 리스트 추출
    top_10_list = df_today_result.head(TOP_N_STOCKS).copy()
    
    if top_10_list.empty:
        print("  > AI 예측: Top 10 리스트가 비어있습니다.")
        return None, None
        
    # (★★★ V10 업그레이드: 'V10 필터' 적용 ★★★)
    print("\n[V10 필터] Top 10 대상 PBR/PER/수급 데이터 '실시간 스크래핑' 시작...")
    
    v10_data = []
    for ticker in tqdm(top_10_list['종목코드'], desc="Scraping V10 Data"):
        pbr, per, for_net, ins_net = get_v10_filter_data(ticker)
        v10_data.append([ticker, pbr, per, for_net, ins_net])
        time.sleep(0.1) # (네이버 차단 방지 딜레이)
    
    df_v10 = pd.DataFrame(v10_data, columns=['종목코드', 'PBR', 'PER', '외국인순매수', '기관순매수'])
    
    # AI 추천 리스트(Top 10)와 V10 필터(PBR/수급) 병합
    final_list = pd.merge(top_10_list, df_v10, on='종목코드', how='left')
    
    # V10 점수 계산 (PBR/PER는 낮을수록, 수급은 높을수록 좋음)
    final_list['V10_Score'] = (
        final_list['PBR'].rank(ascending=True) +       
        final_list['PER'].rank(ascending=True) +       
        final_list['외국인순매수'].rank(ascending=False) + 
        final_list['기관순매수'].rank(ascending=False)   
    )
    
    # V10 점수가 가장 좋은 (숫자가 낮은) 5개 선별
    final_list.sort_values(by='V10_Score', ascending=True, inplace=True)
    final_list_top5 = final_list.head(FINAL_N_STOCKS)

    return final_list_top5, latest_date.strftime("%Y%m%d") # 예측 날짜와 리스트 반환


# --- 3. 메인 실행 ---
if __name__ == "__main__":
    
    # 1. 모델 파일 로드
    try:
        print(f"[0] 챔피언 모델('{MODEL_FILE}') 로드 중...")
        model = joblib.load(MODEL_FILE) 
        print("  > 모델 로드 성공.")
    except Exception as e:
        print(f"  > 오류: '{MODEL_FILE}' 모델 파일이 없습니다. ({e})"); exit()
        
    # 2. '오늘의 Top' 리스트 계산
    target_list, target_date_str = get_today_prediction_list(model)
    
    if target_list is not None and not target_list.empty:
        # 3. 결과 출력 및 파일 저장
        
        timestamp = datetime.now().strftime("%H%M%S")
        output_filename = f"recommendation_V10_{target_date_str}_{timestamp}.csv"
        
        target_list['예상수익률(%)'] = (target_list['예상수익률'] * 100).round(2)
        
        final_cols = ['종목명', '종목코드', '현재가', '예상수익률(%)', 'PBR', 'PER', '외국인순매수', '기관순매수', 'V10_Score']
        
        target_list[final_cols].to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*80)
        print(f"★★★ '{target_date_str}' V10 최종 추천 Top {FINAL_N_STOCKS} (패턴+가치+수급) ★★★")
        print("="*80)
        print(target_list[final_cols].to_string(index=False))
        print("="*80)
        print(f"  > 추천 결과가 '{output_filename}' 파일로 저장되었습니다.")
        
    else:
        print("\n'오늘의 Top 10' 추천 종목이 없습니다. (데이터 부족 또는 필터링에 모두 걸림)")