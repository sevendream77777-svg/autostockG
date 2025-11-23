import pandas as pd
import os

# ==============================================================================
# [사용자 설정] 피처 파일이 있는 폴더 경로를 입력하세요.
# 만약 실행이 안 되면, 아래 r"..." 안에 전체 경로(예: F:\autostockG\...)를 넣으세요.
# ==============================================================================
# 기본적으로 현재 폴더의 하위 경로를 탐색합니다.
possible_paths = [
    r"MODELENGINE/FEATURE/features_V31.parquet",
    r"MODELENGINE/FEATURE/features.parquet",
    r"F:\autostockG\MODELENGINE\FEATURE\features_V31.parquet",  # 사용자님 PC 경로 추정
    r"F:/autostockG/MODELENGINE/FEATURE/features_V31.parquet"
]

target_path = None
for path in possible_paths:
    if os.path.exists(path):
        target_path = path
        break

print("=" * 60)
if target_path:
    print(f"📂 파일 발견! 읽기 시작합니다: {target_path}")
else:
    print("❌ [오류] 'features_V31.parquet' 파일을 찾을 수 없습니다.")
    print("   👉 코드를 실행하는 위치가 'autostockG' 폴더인지 확인해주세요.")
    print("   👉 혹은 코드 상단의 'possible_paths' 리스트에 파일 경로를 직접 추가해주세요.")
    exit() # 파일 없으면 종료

try:
    # 1. 파일 로드
    df = pd.read_parquet(target_path)
    
    # 2. 컬럼 목록 가져오기
    columns = df.columns.tolist()
    columns.sort() # 보기 좋게 정렬

    print(f"\n✅ 로드 성공! (데이터 크기: {len(df):,} 행)")
    print("-" * 60)
    print("📋 [현재 포함된 모든 항목(Columns)]")
    
    sma_cols = []
    for col in columns:
        print(f"  - {col}")
        if "SMA" in col:
            sma_cols.append(col)
            
    print("-" * 60)
    print("🔍 [이동평균선(SMA) 포함 여부 확인 결과]")
    print(f"   👉 현재 있는 SMA 목록: {sma_cols}")
    
    # 3. 핵심 확인
    if "SMA_40" in columns:
        print("   ✅ SMA_40: 있음 (정상)")
    else:
        print("   ❌ SMA_40: 없음 (재계산 필요!)")
        
    if "SMA_90" in columns:
        print("   ✅ SMA_90: 있음 (정상)")
    else:
        print("   ❌ SMA_90: 없음 (재계산 필요!)")

except Exception as e:
    print(f"❌ 에러 발생: {e}")

print("=" * 60)