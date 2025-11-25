import pickle
import sys
import os

def inspect_engine(file_path):
    if not os.path.exists(file_path):
        print(f"❌ 오류: 파일을 찾을 수 없습니다 -> {file_path}")
        return

    print(f"\n🔎 [엔진 정밀 분석] 대상: {os.path.basename(file_path)}")
    print("="*60)
    
    try:
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        # 1. 저장된 키(Keys) 확인
        keys = list(data.keys()) if isinstance(data, dict) else "Not a dict"
        print(f"📌 데이터 구조(Keys): {keys}")

        # 2. 피처(컬럼) 개수 및 목록 확인
        if isinstance(data, dict) and 'features' in data:
            feats = data['features']
            print(f"\n🎯 [중요] 학습에 사용된 피처 개수: {len(feats)}개")
            print(f"📜 피처 목록:\n{feats}")
        else:
            print("\n⚠️ 'features' 키를 찾을 수 없습니다. (구조가 다른 버전일 수 있음)")

        # 3. 모델 객체 확인 (LightGBM)
        if isinstance(data, dict) and 'model_reg' in data:
            model = data['model_reg']
            print(f"\n🤖 모델 타입: {type(model)}")
            try:
                # 트리 개수 확인
                print(f"🌲 트리(Estimators) 개수: {model.num_trees()}")
            except:
                pass
                
    except Exception as e:
        print(f"❌ 읽기 실패: {e}")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python inspect_engine.py [파일경로]")
    else:
        inspect_engine(sys.argv[1])
