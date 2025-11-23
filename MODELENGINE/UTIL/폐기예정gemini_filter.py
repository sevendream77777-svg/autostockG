import os
import glob
import pandas as pd
import google.generativeai as genai
from datetime import datetime

# ==========================================
# [설정] 사용자 API 키 (따옴표 안에 넣어야 함!)
# ==========================================
API_KEY = "AIzaSyBG_Q5-c2H3JgLssHxot-iPM69AJ9kzXdU" 

# ==========================================

def get_latest_recommendation_file(base_dir):
    search_pattern = os.path.join(base_dir, "recommendation_HOJ_*.csv")
    list_of_files = glob.glob(search_pattern)
    if not list_of_files: return None
    return max(list_of_files, key=os.path.getctime)

def run_gemini_filter():
    print("\n" + "="*60)
    print("[Gemini Filter] 🤖 AI 전략가 가동 시작...")
    print("="*60)

    # 1. API 키 설정 (이미 위에서 입력했으므로 바로 설정)
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-1.5-pro', tools=[{"google_search": {}}])
    except Exception as e:
        print(f"[오류] API 키 설정 실패: {e}")
        return

    # ... (이후 코드는 동일) ...
    # 아래 코드는 그대로 두셔도 됩니다.
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    latest_csv = get_latest_recommendation_file(current_dir)

    if not latest_csv:
        print("[경고] 분석할 추천 파일이 없습니다.")
        return

    print(f"[입력] 파일 로드: {os.path.basename(latest_csv)}")
    df = pd.read_csv(latest_csv)
    
    # 상위 10개 추출
    if len(df.columns) >= 2:
        targets = df.iloc[:10, :2]
        targets.columns = ['code', 'name']
    else:
        targets = df[['code', 'name']].head(10)
    
    target_list_str = targets.to_string(index=False)
    print(f"\n[분석 대상]\n{target_list_str}\n")

    prompt = f"""
    너는 20년 경력의 펀드매니저다. 
    아래 종목들의 오늘자 최신 뉴스, 악재, 테마 부합 여부를 검색해서
    가장 상승 확률 높은 3개(Best 3)와 절대 사면 안 되는 종목(Worst)을 뽑아줘.
    
    [종목 리스트]
    {target_list_str}
    
    결과는 '1. [종목명]: 이유', '2. [종목명]: 이유' 형식으로 간단명료하게 출력해.
    """

    print("[진행] Gemini가 분석 중입니다... (잠시만 기다려주세요)")
    try:
        response = model.generate_content(prompt)
        print("\n" + "-"*60)
        print(response.text)
        print("-"*60)
    except Exception as e:
        print(f"[통신 오류] {e}")

if __name__ == "__main__":
    run_gemini_filter()