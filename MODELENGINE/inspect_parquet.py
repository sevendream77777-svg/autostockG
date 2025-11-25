import pandas as pd
import os
import sys

def inspect_parquet(file_path):
    if not os.path.exists(file_path):
        print(f"\n❌ 오류: 파일을 찾을 수 없습니다 -> {file_path}")
        return

    try:
        print(f"\n🔎 [파일 정밀 검사] 대상: {os.path.basename(file_path)}")
        print("="*60)
        
        # 파일 읽기
        df = pd.read_parquet(file_path)
        
        # 1. 기본 정보
        print(f"📂 총 데이터 행(Row) 수 : {len(df):,} 개")
        print(f"📊 총 컬럼(Column) 개수 : {len(df.columns)} 개")
        
        # 2. 컬럼 목록 출력
        print("-" * 60)
        print(f"📜 [컬럼 전체 목록]:")
        col_list = df.columns.tolist()
        print(col_list)
        
        # 3. 데이터 미리보기 (헤드)
        print("-" * 60)
        print("👀 [데이터 미리보기 (상위 3줄)]:")
        # 컬럼이 많으면 다 안 보일 수 있으니 주요 컬럼만 보거나 전체 출력 설정
        pd.set_option('display.max_columns', None) 
        print(df.head(3))

        # 4. 결측치(NaN) 체크
        print("-" * 60)
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls == 0:
            print("✅ 결측치(NaN) 없음. 데이터가 아주 깨끗합니다!")
        else:
            print("⚠️ [주의] 결측치(NaN)가 발견되었습니다 (상위 5개):")
            print(null_counts[null_counts > 0].sort_values(ascending=False).head(5))

        print("="*60)

    except Exception as e:
        print(f"❌ 읽기 실패: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python inspect_parquet.py [파일경로]")
    else:
        inspect_parquet(sys.argv[1])
