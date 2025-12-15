import OpenDartReader
import pandas as pd
import time
from datetime import datetime, timedelta

# API 키 설정
api_key = 'YOUR_API_KEY_HERE'
dart = OpenDartReader(api_key)

start_date_str = '20160101'
end_date_str = datetime.now().strftime('%Y%m%d')

print("데이터 수집 시작... (무결성 확보 모드)")

all_dfs = []
curr = datetime.strptime(start_date_str, "%Y%m%d")
end = datetime.strptime(end_date_str, "%Y%m%d")

while curr <= end:
    next_date = curr + timedelta(days=90) # 3개월 단위
    if next_date > end: next_date = end
    
    s = curr.strftime("%Y%m%d")
    e = next_date.strftime("%Y%m%d")
    
    try:
        # pblntf_ty='A' (정기공시)
        df = dart.list(start=s, end=e, pblntf_ty='A', kind='A')
        if df is not None and not df.empty:
            all_dfs.append(df)
            print(f"[{s} ~ {e}] 수집 성공: {len(df)}건")
        else:
            print(f"[{s} ~ {e}] 데이터 없음")
            
    except Exception as ex:
        print(f"[{s} ~ {e}] 에러 발생 (재시도 필요): {ex}")
        # 실전에서는 여기서 while 루프를 멈추거나 재시도 로직을 넣을 수 있음
        
    curr = next_date + timedelta(days=1)
    time.sleep(0.5)

if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # [중요] 무결성을 위한 중복 제거 (접수번호 rcept_no 기준)
    # 동일한 보고서가 중복 수집되는 것을 100% 차단합니다.
    before_len = len(final_df)
    final_df.drop_duplicates(subset=['rcept_no'], keep='first', inplace=True)
    after_len = len(final_df)
    
    # 필요한 컬럼만 깔끔하게 정제
    # rcept_dt -> announce_date
    final_df = final_df[['rcept_dt', 'corp_code', 'corp_name', 'report_nm', 'rcept_no']]
    final_df.rename(columns={'rcept_dt': 'announce_date'}, inplace=True)
    final_df.sort_values(by='announce_date', inplace=True)
    
    print("="*40)
    print(f"수집 완료. 중복 제거: {before_len - after_len}건")
    print(f"최종 데이터 개수: {len(final_df)}건")
    
    final_df.to_csv('dart_announce_date_clean.csv', index=False, encoding='utf-8-sig')
    print("파일 저장 완료: dart_announce_date_clean.csv")
else:
    print("수집된 데이터가 없습니다.")