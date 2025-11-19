import os
import re
from typing import Dict, List

# ----------------------------------------------------
# 📌 설정: 분석할 폴더 경로 (수정)
# ----------------------------------------------------
# 스크립트 실행 위치: F:\autostockG\MODELENGINE\UTIL
# 분석 대상 최상위 폴더: F:\autostockG

current_dir = os.path.dirname(os.path.abspath(__file__))

# 상위 폴더로 2번 이동해야 F:\autostockG 에 도달합니다.
# '..', '..' 2번 사용
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..', '..'))

print(f"✅ 분석 대상 최상위 폴더 설정됨: {PROJECT_ROOT}")
# ----------------------------------------------------

# ... (나머지 analyze_dependencies 함수와 __main__ 코드는 동일합니다)
def analyze_dependencies(base_dir: str) -> Dict[str, List[str]]:
    """
    주어진 폴더 내의 .py 파일들을 분석하여 의존성(import 관계)을 딕셔너리로 반환합니다.
    """
    dependencies = {}
    
    if not os.path.isdir(base_dir):
        print(f"오류: 지정된 경로를 찾을 수 없거나 폴더가 아닙니다: {base_dir}")
        return dependencies

    # 1. 모든 .py 파일의 모듈 이름 목록을 미리 확보 (상대 경로 기준)
    all_py_modules = {} # {모듈 이름: 전체 경로} 딕셔너리
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.py'):
                relative_path = os.path.relpath(os.path.join(root, file), base_dir)
                # 확장자를 제거하고 os.sep(경로 구분자)를 '.'으로 변경하여 모듈 이름 형식 생성
                module_name = relative_path[:-3].replace(os.sep, '.') 
                all_py_modules[module_name] = os.path.join(root, file)
    
    # 2. 각 파일을 순회하며 import 문 분석
    for current_module_name, current_file_path in all_py_modules.items():
        imported_modules = set()
        
        # 스크립트 파일 자체는 분석 대상에서 제외
        if os.path.abspath(current_file_path) == os.path.abspath(__file__):
             continue

        try:
            with open(current_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # 정규 표현식을 사용하여 import 문 패턴 찾기
                
                # 패턴 1: 'import module', 'import package.sub_module'
                matches_import = re.findall(r'(?:^|\n)\s*import\s+([\w\.]+)', content)
                for match in matches_import:
                    imported_modules.add(match.split('.')[0])
                    
                # 패턴 2: 'from package import module', 'from . import module' (상대경로 임포트 제외)
                matches_from = re.findall(r'(?:^|\n)\s*from\s+([\w\.]+)\s+import\s+', content)
                for match in matches_from:
                    imported_modules.add(match.split('.')[0])
                    
                # 3. 로컬 프로젝트 내 모듈과의 연결 확인
                local_dependencies = []
                for imp_mod in imported_modules:
                    is_local = False
                    for existing_module in all_py_modules.keys():
                        # 임포트 모듈 이름이 존재하는 모듈 이름과 일치하거나 시작하는 경우
                        if existing_module.startswith(imp_mod + '.') or existing_module == imp_mod:
                            is_local = True
                            break
                            
                    # 로컬 모듈이면서 자기 자신 임포트가 아닌 경우에만 추가
                    if is_local and imp_mod != current_module_name.split('.')[0]: 
                        local_dependencies.append(imp_mod)
                        
                if local_dependencies:
                    dependencies[current_module_name] = sorted(list(set(local_dependencies)))
                    
        except Exception as e:
            print(f"⚠️ 파일 분석 중 오류 발생 ({current_file_path}): {e}")
            
    return dependencies

# ----------------------------------------------------
# 🚀 스크립트 실행
# ----------------------------------------------------

if __name__ == "__main__":
    print(f"\n🚀 **폴더 파이프라인 분석 시작:** {PROJECT_ROOT}\n")
    
    dependency_map = analyze_dependencies(PROJECT_ROOT)

    print("--- 📊 분석 결과: 파이프라인/의존성 구조 ---")
    if dependency_map:
        for source_module, target_modules in dependency_map.items():
            # source_module이 target_modules를 사용(import)하는 연결
            print(f"**{source_module}.py** ➡️ 사용(Import) ➡️ {', '.join([t + '.py' for t in target_modules])}")
        
        print("\n--- 분석 완료 ---")
    else:
        print("프로젝트 내에서 유효한 로컬 모듈 import 관계를 찾을 수 없습니다.")