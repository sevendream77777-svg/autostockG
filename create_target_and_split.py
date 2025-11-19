import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm

# --- 1. 설정 ---
# (★★★ V5 수정 사항 ★★★)
# V4 데이터를 그대로 사용
input_file = "stock_data_10y_processed_v4.parquet"

# (★★★ V5 수정 사항 ★★★)
TARGET_DAYS = 5      # 20영업일 (X) -> 5영업일 (O)
TARGET_RETURN = 0.05 # 5% (5일 만에 5%는 매우 어려운 목표입니다)

# 학습/검증 분리 기준 (최근 1년)
TEST_DURATION_DAYS = 365 

# --- 2. 데이터 불러오기 ---
print(f"'{input_file}' (V4 데이터) 파일을 불러오는 중...")
try:
    df = pd.read_parquet(input_file)
except Exception as e:
    print(f"파일 읽기 오류: {e}")
    print("이전 단계(process_data_v4.py)가 성공적으로 완료되었는지 확인하세요.")
    exit()

print(f"파일 불러오기 완료. (데이터 행 수: {len(df)})")
print("날짜 컬럼을 datetime으로 변환합니다...")
df['날짜'] = pd.to_datetime(df['날짜'])


# --- 3. (핵심) '정답(Target)' 컬럼 생성 (V5) ---
print(f"'정답({TARGET_DAYS}일 뒤 {TARGET_RETURN*100}% 상승)' 컬럼을 생성합니다...")

def create_target(group_df):
    """한 종목(group_df)을 받아서 '정답' 컬럼 계산"""
    
    # 5영업일 '미래'의 종가 가져오기
    future_price = group_df['종가'].shift(-TARGET_DAYS)
    
    # (미래 종가 - 현재 종가) / 현재 종가 = 미래 수익률
    future_return = (future_price - group_df['종가']) / group_df['종가']
    
    # 목표 수익률(5%)을 넘었으면 1, 아니면 0
    group_df['Target'] = np.where(future_return >= TARGET_RETURN, 1, 0)
    
    return group_df

# groupby('종목코드')로 2800개 종목을 각각 분리하여 create_target 함수 적용
tqdm.pandas(desc="Creating Target (Y)")
df_target = df.groupby('종목코드', group_keys=False).progress_apply(create_target)

# (필수!) 결측치 제거
original_rows = len(df_target)
df_target.dropna(subset=['Target'], inplace=True)
cleaned_rows = len(df_target)
print(f"정답(Target) 계산 불가능 행 (각 종목의 마지막 {TARGET_DAYS}일) {original_rows - cleaned_rows}개 제거 완료.")


# --- 4. (핵심) 9년(학습) / 1년(검증) 데이터 분리 ---
print("데이터를 9년(학습) / 1년(검증)으로 분리합니다...")

# 1년 전 날짜 계산
split_date = datetime.now() - timedelta(days=TEST_DURATION_DAYS)
split_date_str = split_date.strftime("%Y-%m-%d")
print(f"분리 기준 날짜: {split_date_str}")

# 분리 기준 날짜보다 '이전' -> 9년 학습용
train_df = df_target[df_target['날짜'] < split_date]

# 분리 기준 날짜 '이후' -> 1년 검증용
test_df = df_target[df_target['날짜'] >= split_date]


# --- 5. 최종 파일 저장 ---
# (V4와 동일한 파일명으로 덮어씁니다)
output_train_file = "train_data.parquet"
output_test_file = "test_data.parquet"

train_df.to_parquet(output_train_file, index=False)
test_df.to_parquet(output_test_file, index=False)

print("-" * 30)
print("V5 '정답' 데이터 생성 완료!")
print(f"학습용(9년) 데이터: {output_train_file} (행 수: {len(train_df)})")
print(f"검증용(1년) 데이터: {output_test_file} (행 수: {len(test_df)})")