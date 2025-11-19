# verify_data_integrity.py
# [진단용 스크립트 2]
# 1단계(원본 재료)와 2단계(가공 피처) 파일들의 '무결성'을 검증합니다.
# (데이터가 비어있는지, 의미 없는 값인지 등을 확인)

import pandas as pd
import sys
import os

# --- 검증할 파일 목록 ---
# 1단계 '원본 재료' (나무)
FILE_1A = "all_stocks_cumulative.parquet"
# 1단계 '원본 재료' (숲)
FILE_1B = "kospi_index_10y.parquet"
# 2단계 '가공 피처' (V21 DB)
FILE_2 = "all_features_cumulative_V21_Hoj.parquet"

# 검증할 '원본 재료' 컬럼 (7~8개)
ORIGINAL_COLS = ['날짜', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Code']
# 검증할 '핵심 가공 피처' (23개 중 일부)
FEATURE_COLS = ['MA20', 'RSI', 'MACD', 'Market_Adjusted_Return']

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 50)

def print_break():
    print("\n" + "="*80 + "\n")

def check_file(file_path, cols_to_check, is_feature_db=False):
    """파일을 로드하고 무결성을 검사합니다."""
    print(f"--- 검증 시작: {file_path} ---")
    if not os.path.exists(file_path):
        print(f"❌ [검증 실패] 파일을 찾을 수 없습니다: {file_path}")
        return False

    try:
        df = pd.read_parquet(file_path)
        
        # 1. .info() : 컬럼명, 데이터 타입, Null(NaN) 개수 확인
        print("\n[1. 데이터 .info() (컬럼, 개수, Null 여부)]")
        print(df.info())
        
        # 2. .describe() : 통계 요약 (숫자 데이터가 '의미 없는 값'인지 확인)
        print("\n[2. 숫자 데이터 .describe() (통계 요약)]")
        # 너무 많으니 핵심 컬럼만 요약
        check_cols = [col for col in cols_to_check if col in df.columns]
        if check_cols:
            print(df[check_cols].describe())
        else:
            print(f"경고: {cols_to_check} 컬럼이 파일에 없습니다.")

        # 3. .tail(3) : '최근 데이터'가 제대로 입력됐는지 확인
        print("\n[3. 최신 데이터 .tail(3) (실제 값 확인)]")
        print(df.tail(3))
        
        if is_feature_db:
            # 4. 23개 피처 목록 전체 확인
            print("\n[4. '23개 피처' 목록 전체 (V21 DB)]")
            all_feature_cols = [
                'Market_Adjusted_Return', 'MA5', 'MA20', 'MA60', 
                'BollingerUpper', 'BollingerLower', 'MACD', 'MACD_Signal', 'RSI',
                'KOSPI_수익률', 'SMA_20', 'SMA_40', 'SMA_60', 'SMA_90', 
                'RSI_14', 'VOL_SMA_20', 'MACD_Sig', 'BBP_20', 
                'ATR_14', 'STOCH_K', 'STOCH_D', 'CCI_20', 'ALPHA_SMA_20'
            ]
            all_feature_cols = list(set(all_feature_cols)) # 중복 제거
            
            missing_features = [f for f in all_feature_cols if f not in df.columns]
            if missing_features:
                print(f"❌ [검증 실패] 23개 피처 중 다음 {len(missing_features)}개가 V21 DB에 없습니다:")
                print(missing_features)
            else:
                print("✅ [검증 성공] '23개 피처'가 V21 DB에 모두 존재합니다.")

        print(f"\n--- 검증 완료: {file_path} ---")
        return True

    except Exception as e:
        print(f"❌ [검증 실패] {file_path} 파일 처리 중 오류 발생: {e}")
        return False

# --- 스크립트 실행 ---
print(">>> Hoj 엔진 데이터 무결성 검증 시작 (1단계, 2단계 파일)")
print("    '의미 없는 데이터'나 'NaN' 값이 있는지 확인합니다.")
print_break()

# 1단계 '원본 재료' 검증 (나무)
check_file(FILE_1A, ORIGINAL_COLS)
print_break()

# 1단계 '원본 재료' 검증 (숲)
check_file(FILE_1B, ORIGINAL_COLS)
print_break()

# 2단계 '가공 피처' 검증 (V21 DB)
check_file(FILE_2, FEATURE_COLS, is_feature_db=True)
print_break()

print(">>> 모든 검증이 완료되었습니다.")