import OpenDartReader
import pandas as pd
import time
import os

# 1. API 키 파일 경로
KEY_FILE_PATH = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\7109kitchen109naver_dart.txt"

def get_api_key(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except Exception as e:
        print(f"키 파일 읽기 실패: {e}")
        return None

def test_collect_1month():
    api_key = get_api_key(KEY_FILE_PATH)
    if not api_key: return
    
    dart = OpenDartReader(api_key)

    # 테스트를 위해 '2010년 1월' 한 달만 설정
    start_date = '20100101'
    end_date = '20100131'

    print(f"\n>>> 테스트 시작: {start_date} ~ {end_date} 데이터 수집 중...", flush=True)

    try:
        # 1월 한달치 요청
        df = dart.list(start=start_date, end=end_date)
        
        if df is not None and not df.empty:
            print(f"✅ 성공! 데이터를 가져왔습니다. (총 {len(df)}건)", flush=True)
            
            # 저장 테스트
            filename = "dart_test_2010_jan.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✅ 파일 저장 완료: {os.path.abspath(filename)}")
            
            # 일부 출력
            print("\n[수집 데이터 샘플]")
            print(df[['rcept_dt', 'corp_name', 'report_nm']].head())
        else:
            print("❌ 해당 기간에 데이터가 없거나, 가져오지 못했습니다.")

    except Exception as e:
        print(f"\n❌ 에러 발생: {e}")
        print("API 키가 올바른지, 인터넷 연결이 되어있는지 확인해주세요.")

if __name__ == "__main__":
    test_collect_1month()