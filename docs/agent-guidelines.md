# Agent Guidelines — MODELENGINE

요약: 이 파일은 AI 에이전트(및 협업자)가 이 저장소에서 즉시 참고해야 하는 핵심 규칙·실행법·주의사항을 정리합니다.

- 저장 위치: 프로젝트 루트의 `docs/agent-guidelines.md` (영구 보관). Copilot 전용 우선위치는 `/.github/copilot-instructions.md`입니다.

빠른 실행
- 피처 생성(수동):
```
python UTIL/full_update_pipeline.py
```
- 자동/비대화형 실행(권장):
```
python UTIL/full_update_pipeline.py --non-interactive
```
(해당 옵션이 없으면 대화형 `input()`이 실행되어 자동화가 중단될 수 있습니다.)

핵심 파일
- `UTIL/full_update_pipeline.py` — RAW + KOSPI → `FEATURE/features_V32.parquet` 생성. (주의: 파일 헤더와 실제 파일명이 불일치할 수 있음)
- `UTIL/config_paths.py` — 저장 경로, 버전화 함수(`versioned_filename`) 관리
- `UTIL/version_utils.py` — 버전 관리 관련 유틸
- `RAW/all_stocks_cumulative.parquet`, `RAW/kospi_index_10y.parquet` — 입력 데이터
- `FEATURE/features_V32.parquet` — 출력 데이터

프로젝트 규칙/관행
- 하드코딩된 컬럼명(`Date`, `Code`, `KOSPI_종가`)이 종종 쓰입니다. 데이터 포맷이 다르면 실패하므로 파일 구조를 먼저 확인하세요.
- 대용량 Parquet 파일을 전체 메모리로 읽으므로 메모리 부족 가능. 필요한 컬럼만 읽거나 청크 처리 권장.
- 자동화 환경에서는 `input()`을 피하거나 `--non-interactive` 플래그로 대체하세요.

통합 포인트 및 외부 의존
- 외부 데이터는 로컬 Parquet 파일로 관리(원격 API 사용 시 별도 스크립트 존재).
- 민감 정보(API 키 등)는 리포지토리에 저장하지 말고 환경변수/시크릿 매니저 사용.

문제 발생 시
- 로그와 스택트레이스 확인 후 `UTIL` 폴더의 관련 스크립트(`raw_checker.py`, `pipeline_utils.py`)를 먼저 열람하세요.
- 변경 시에는 백업 또는 Git 커밋을 먼저 하세요. 데이터 덮어쓰기는 주의.

원본·권장 변경 사항(요약)
- `input()` 대화형 확인은 자동화에 방해됩니다. `--non-interactive` 플래그를 도입하고 기본 동작을 non-interactive로 처리 권장.
- Parquet 읽기에서 필요한 컬럼만 선택적으로 읽도록 수정 권장.

파일 유지와 사용 방법
- 이 파일을 한 번 리포지토리에 넣어두면 AI 에이전트가 새 세션에서 읽어 참고할 수 있습니다. 파일 자체는 사용자의 로컬/원격 리포지토리에 저장됩니다.
- 팀과 공유하려면 Git 커밋 후 원격에 푸시하세요.

보안 주의
- 절대 비밀번호·시크릿을 이 파일에 저장하지 마세요.

추가 요청
- 이 지침을 `.github/copilot-instructions.md`로 복사하길 원하면 알려주세요.
