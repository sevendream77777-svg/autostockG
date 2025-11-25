import pickle
import sys
import os

def check_meta(file_path):
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        if 'meta' in data:
            print(f"\n🔎 [메타 정보 확인] {os.path.basename(file_path)}")
            print("="*60)
            for k, v in data['meta'].items():
                print(f" - {k}: {v}")
            print("="*60)
        else:
            print("⚠️ 이 파일에는 'meta' 기록이 없습니다.")

    except Exception as e:
        print(f"❌ 오류: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_meta(sys.argv[1])
