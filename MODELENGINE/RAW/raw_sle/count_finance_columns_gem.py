# -*- coding: utf-8 -*-
import os
import zipfile
import pandas as pd

# ==========================================
# [설정] 파일 경로 (사용자 경로 유지)
# ==========================================
TARGET_FILES = [
    r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\finance_data\2015_4Q_PL_20230503040205.zip",
    r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\finance_data\2015_4Q_BS_20230503040109.zip",
    r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\finance_data\2015_4Q_CE_20230503040337.zip",
    r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\finance_data\2015_4Q_CF_20230503040246.zip"
]

OUT_FILENAME = "2015_Finance_Fixed_Columns.csv"

def get_file_type(filename):
    if "_BS_" in filename: return "재무상태표(BS)"
    if "_PL_" in filename or "_IS_" in filename: return "손익계산서(PL)"
    if "_CIS_" in filename: return "포괄손익계산서(PL)"
    if "_CF_" in filename: return "현금흐름표(CF)"
    if "_CE_" in filename: return "자본변동표(CE)"
    return "기타"

def extract_columns_fixed(zip_path):
    results = []
    if not os.path.exists(zip_path):
        print(f"❌ 경로 없음: {zip_path}")
        return []

    file_name = os.path.basename(zip_path)
    report_type = get_file_type(file_name)
    print(f"📂 분석 중... [{report_type}] {file_name}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # zip 안의 '모든' txt 파일을 순회 (기존에는 첫번째만 봤음)
            txt_files = [f for f in z.namelist() if f.endswith('.txt')]
            
            for target_file in txt_files:
                # 파일명으로 업종 유추 (연결/별도, 금융/일반 등)
                sub_type = "일반"
                if "은행" in target_file: sub_type = "은행"
                elif "보험" in target_file: sub_type = "보험"
                elif "증권" in target_file: sub_type = "증권"
                
                # 파일 읽기 (100줄만)
                try:
                    with z.open(target_file) as f:
                        try:
                            df = pd.read_csv(f, sep='\t', encoding='cp949', nrows=100, dtype=str)
                        except UnicodeDecodeError:
                            f = z.open(target_file)
                            df = pd.read_csv(f, sep='\t', encoding='utf-8', nrows=100, dtype=str)
                except Exception as e:
                    continue # 파일 읽기 실패시 다음 파일로

                # -------------------------------------------------------
                # 구조 1: 리스트 형태 (행 단위) -> PL, BS, CF, 금융업CE
                # -------------------------------------------------------
                if '항목코드' in df.columns and '항목명' in df.columns:
                    value_cols = [c for c in df.columns if '당기' in c or 'Current' in c]
                    target_val_col = value_cols[0] if value_cols else df.columns[-1]
                    
                    # 데이터 추출
                    unique_items = df[['항목코드', '항목명']].drop_duplicates()
                    for _, row in unique_items.iterrows():
                        # 예시값 찾기
                        sample_val = ""
                        try:
                            sample_row = df[df['항목코드'] == row['항목코드']]
                            if not sample_row.empty:
                                sample_val = sample_row.iloc[0][target_val_col]
                        except: pass

                        results.append({
                            '파일명': file_name,
                            '내부파일명': target_file,
                            '재무제표_종류': report_type,
                            '구조': f'리스트({sub_type})',
                            '항목코드(Key)': row['항목코드'],
                            '항목명(Name)': row['항목명'],
                            '예시_데이터': sample_val
                        })

                # -------------------------------------------------------
                # 구조 2: 매트릭스 형태 (열 단위) -> 일반업종 자본변동표(CE)
                # -------------------------------------------------------
                else:
                    # CE 파일은 헤더가 중요함. 헤더에서 '자본' 관련 컬럼 찾기
                    start_idx = 0
                    for i, col in enumerate(df.columns):
                        # 자본변동표 매트릭스 특징: ifrs 코드나 '자본' 단어가 10번째 이후에 등장
                        if i > 5 and ('ifrs' in col or 'dart' in col or '자본' in col):
                            start_idx = i
                            break
                    
                    if start_idx > 0: # 유효한 매트릭스 구조 발견
                        target_cols = df.columns[start_idx:]
                        for col in target_cols:
                            sample_val = df.iloc[0][col] if not df.empty else ""
                            results.append({
                                '파일명': file_name,
                                '내부파일명': target_file,
                                '재무제표_종류': report_type,
                                '구조': '매트릭스(일반)',
                                '항목코드(Key)': col, # 헤더 자체가 코드
                                '항목명(Name)': col,  # 헤더 자체가 이름
                                '예시_데이터': sample_val
                            })

    except Exception as e:
        print(f"   -> 오류 발생: {e}")

    return results

def main():
    print(f"--- 2015년 금융 데이터(자본변동표 포함) 재추출 시작 ---")
    all_data = []
    for zip_path in TARGET_FILES:
        data = extract_columns_fixed(zip_path)
        all_data.extend(data)
        
    if all_data:
        df_res = pd.DataFrame(all_data)
        df_res.to_csv(OUT_FILENAME, index=False, encoding='utf-8-sig')
        print(f"\n✅ 완료! {OUT_FILENAME} 저장됨.")
        print(f"총 추출 항목: {len(df_res)}개")
        
        # 자본변동표 확인
        ce_count = len(df_res[df_res['재무제표_종류'] == '자본변동표(CE)'])
        print(f"-> 자본변동표(CE) 항목 수: {ce_count}개 (이제 0이 아니어야 함)")
    else:
        print("\n⚠️ 데이터 추출 실패")

if __name__ == "__main__":
    main()