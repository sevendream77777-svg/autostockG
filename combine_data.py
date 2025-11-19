import pandas as pd
import glob
import os
from tqdm import tqdm # (진행 상황을 보여주는 라이브러리, pip install tqdm)

# 1. 원본 데이터 폴더
input_dir = "stock_data_10y_ALL"

# 2. 모든 CSV 파일의 경로를 리스트로 가져오기
#    ( F:\autostock\stock_data_10y_ALL\*.csv )
csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

print(f"총 {len(csv_files)}개의 CSV 파일을 하나로 합칩니다...")

# 모든 데이터를 담을 빈 리스트
all_data_frames = []

for file in tqdm(csv_files): # tqdm()으로 감싸면 진행 막대가 보임
    try:
        # 파일명에서 종목 코드(ticker) 추출 (예: 005930)
        ticker = os.path.basename(file).split('.')[0] 
        
        df = pd.read_csv(file)
        
        # '날짜' 컬럼을 datetime 형식으로 변환 (중요)
        df['날짜'] = pd.to_datetime(df['날짜'])
        
        # '종목코드' 컬럼을 추가 (어떤 종목의 데이터인지 구별해야 함)
        df['종목코드'] = ticker
        
        all_data_frames.append(df)
        
    except pd.errors.EmptyDataError:
        # 빈 파일은 건너뜀
        print(f"{file} 파일이 비어있습니다. 건너뜁니다.")
    except Exception as e:
        print(f"{file} 처리 중 오류: {e}")

# 3. 모든 DataFrame을 하나로 합치기 (Concatenate)
if not all_data_frames:
    print("합칠 데이터가 없습니다. CSV 파일 경로를 확인하세요.")
else:
    df_combined = pd.concat(all_data_frames, ignore_index=True)

    # 4. (중요) 날짜와 종목코드로 정렬
    #    데이터를 다루기 편하도록 날짜순 -> 종목코드순으로 정렬
    df_combined.sort_values(by=['날짜', '종목코드'], inplace=True)
    
    # 5. 합친 데이터를 효율적인 Parquet 포맷으로 저장
    #    (CSV보다 훨씬 빠르고 용량도 작습니다)
    output_file = "stock_data_10y_combined.parquet"
    df_combined.to_parquet(output_file, index=False)

    print("-" * 30)
    print(f"데이터 통합 완료! (총 {len(df_combined)} 행)")
    print(f"'{output_file}' 파일로 저장되었습니다.")
    print("\n통합된 데이터 샘플 5줄:")
    print(df_combined.head())