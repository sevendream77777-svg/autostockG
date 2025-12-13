import os
import pandas as pd
import zipfile
import glob
from io import BytesIO

# =========================================================
# [설정] ZIP 파일들이 들어있는 폴더 경로
# Open DART에서 다운받은 모든 ZIP 파일들을 이 폴더에 넣으세요.
# !!! 요청하신 경로로 수정 완료 !!!
SOURCE_DIR = r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_stable" 

# 저장할 최종 파일명 (하나의 거대한 CSV 파일)
# 최종 파일은 SOURCE_DIR과 같은 위치에 생성됩니다.
OUTPUT_FILE = os.path.join(SOURCE_DIR, "dart_all_data_merged.csv")
# =========================================================

def process_bulk_data():
    all_dataframes = []
    
    # 폴더 내의 모든 .zip 파일 찾기
    zip_files = glob.glob(os.path.join(SOURCE_DIR, "*.zip"))
    
    if not zip_files:
        print(f"[경고] ZIP 파일을 찾을 수 없습니다. '{SOURCE_DIR}' 경로에 파일을 넣었는지 확인해주세요.")
        return

    print(f"총 {len(zip_files)}개의 ZIP 파일을 찾았습니다.")
    print("병합 작업을 시작합니다... (데이터 양에 따라 시간이 걸릴 수 있습니다)")

    for i, zip_path in enumerate(zip_files):
        print(f"[{i+1}/{len(zip_files)}] 처리 중: {os.path.basename(zip_path)}")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # 압축 파일 내부의 .txt 파일들을 순회
                for filename in zf.namelist():
                    # 필요한 파일 필터링: .txt 파일만 (대소문자 무시)
                    if filename.lower().endswith(".txt"):
                        try:
                            # 텍스트 파일 읽기 (인코딩: euc-kr 또는 cp949)
                            with zf.open(filename) as f:
                                # DART 대용량 파일은 탭(\t)으로 구분되어 있음
                                # 종목코드는 문자열로 강제 지정 (앞 0 손실 방지)
                                df = pd.read_csv(f, sep='\t', encoding='cp949', dtype={'종목코드': str})
                                
                                # 데이터 출처(파일명) 기록 (어느 분기 데이터인지 알기 위함)
                                df['source_file'] = filename
                                
                                all_dataframes.append(df)
                                
                        except Exception as e:
                            # 개별 파일 내부 오류 발생 시에도 전체 루프 중단 방지
                            print(f"  -> 내부 파일 읽기 실패 ({filename}): {e}")
                            
        except Exception as e:
            # ZIP 파일 자체 오류 발생 시 다음 파일로 넘어감
            print(f"  -> ZIP 파일 처리 실패: {e}")

    # 데이터 합치기
    if all_dataframes:
        print("데이터 병합 중...")
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        # ------------------- 데이터 클리닝 및 정리 -------------------
        # 1. 종목코드 대괄호 제거 ([005930] -> 005930)
        if '종목코드' in final_df.columns:
             final_df['종목코드'] = final_df['종목코드'].str.replace(r'[\[\]]', '', regex=True)
             
        # 2. 모든 컬럼 이름에서 공백 제거 (DB에 넣을 때 편함)
        final_df.columns = final_df.columns.str.strip()
        
        # 3. 항목명 컬럼의 공백 제거 (분석시 오류 방지)
        if '항목명' in final_df.columns:
            final_df['항목명'] = final_df['항목명'].str.strip()
        # -------------------------------------------------------------

        # 결과 저장
        # encoding='utf-8-sig'는 한글 깨짐 없이 엑셀에서 바로 열리게 합니다.
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print("=" * 50)
        print(f"[완료] 모든 데이터가 '{OUTPUT_FILE}'에 저장되었습니다.")
        print(f"총 데이터 행 수: {len(final_df):,}개")
        print("=" * 50)
    else:
        print("[경고] 합칠 데이터가 없습니다. 폴더 경로를 확인해주세요.")

if __name__ == "__main__":
    # pandas가 없다면 설치 필요: pip install pandas
    try:
        # NOTE: 이 코드는 실행 전에 'pip install pandas'가 필수입니다.
        if not os.path.exists(SOURCE_DIR):
            print(f"경로를 찾을 수 없습니다: {SOURCE_DIR}")
            os.makedirs(SOURCE_DIR, exist_ok=True)
            print(f"폴더를 생성했습니다. 여기에 DART에서 다운받은 ZIP 파일을 넣고 다시 실행하세요.")
        else:
            process_bulk_data()
    except NameError:
        print("\n[오류] pandas 라이브러리를 찾을 수 없습니다.")
        print("명령 프롬프트/파워쉘에서 'pip install pandas'를 입력하여 설치하세요.")