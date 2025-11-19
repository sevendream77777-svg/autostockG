import os
import pandas as pd
from datetime import datetime

# === 설정 영역 ===
# 변환할 엑셀 파일들이 들어있는 폴더 경로
INPUT_FOLDER = r"F:\autostockG"

# 변환된 CSV를 저장할 폴더 경로 (자동 생성)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FOLDER = os.path.join(INPUT_FOLDER, f"csv_export_{timestamp}")
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# === 함수 정의 ===
def sanitize_filename(name: str) -> str:
    """파일명에 쓸 수 없는 문자 제거"""
    for ch in r'\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip()

def convert_excel_to_csv(file_path: str, output_dir: str):
    try:
        excel = pd.ExcelFile(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"\n📘 {base_name} ({len(excel.sheet_names)} 시트) 변환 시작...")

        for sheet_name in excel.sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                safe_sheet = sanitize_filename(sheet_name)
                csv_name = f"{base_name}_{safe_sheet}.csv"
                save_path = os.path.join(output_dir, csv_name)
                df.to_csv(save_path, index=False, encoding="utf-8-sig")
                print(f"   ✅ {sheet_name} → {csv_name}")
            except Exception as e:
                print(f"   ❌ {sheet_name} 변환 실패: {e}")

    except Exception as e:
        print(f"❌ 파일 {file_path} 처리 중 오류: {e}")

# === 실행 영역 ===
if __name__ == "__main__":
    print("🚀 엑셀 → CSV 일괄 변환 시작")
    print("입력 폴더: F:\\autostockG")
    print(f"출력 폴더: {OUTPUT_FOLDER}")

    excel_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(".xlsx")]

    if not excel_files:
        print("❌ 엑셀 파일이 없습니다. INPUT_FOLDER 경로를 확인하세요.")
    else:
        for file in excel_files:
            full_path = os.path.join(INPUT_FOLDER, file)
            convert_excel_to_csv(full_path, OUTPUT_FOLDER)

    print("\n🎯 모든 변환 완료!")
    print(f"📂 CSV 저장 위치: {OUTPUT_FOLDER}")
