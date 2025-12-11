
==============================
v48 2단계 분리형 파이프라인 설계도
==============================

목표
- DART(분기/연간) 때문에 전체 파이프라인이 지연되지 않도록 분리 실행
- RAW(가격/수급/매크로/메타)는 즉시 완주 → 검증/EDA/모델링 선행
- DART는 별도 파이프라인에서 5개 키로 병렬 수집 → 나중에 안전 병합
- 언제 끊겨도 “데이터 오염 없이” 재시작 가능, 진행상황은 같은 폴더의 status.json으로만 확인

구성
1) run_dart_only_v48.py
   - DART만 수집(annual/full)
   - per-code Parquet (out_dart/finance/XXXXXX.parquet)
   - checkpoint.pkl / quality_dart.jsonl / fail_queue.jsonl / status.json
   - 재시작: 동일 범위/모드/워커 유지 시 완료/성공 파일 자동 skip
   - 다음 조건이면 새로 받음: 기간/모드 변경, (옵션) 워커 변경 시 --reset-on-worker-change, 로그는 100MB 단위 롤오버

2) merge_raw_dart_v48.py
   - RAW(일별) + DART(분기/연간)를 일별로 확장 후 LEFT JOIN
   - 스키마 강제(V48_COLS), 키(date,code) 1:1 보장
   - 코드 단위 tmp 저장(tmp_by_code/*.parquet) → 재시작/추가 병합 가능
   - status.json 로 진행률 확인
   - RAW가 갱신됐거나 강제 재병합하려면 --force-remerge 사용(또는 tmp보다 RAW가 최신이면 자동 재병합)

실행 순서
A. RAW 먼저(기존 파이프라인)
   - 예: raw_v48.parquet 생성(또는 날짜별 CSV 통합 후 단일 Parquet로 준비)

B. DART 분리 수집
   PS> python run_dart_only_v48.py --codes stocks.txt --start-date 20150102 --end-date 20251208 ^
        --out-dir .\out_dart --workers 8 --dart-mode full

   - 진행상황: .\out_dart\status.json
   - 실패 큐 재시도:
     PS> python run_dart_only_v48.py --codes stocks.txt --retry-failed --out-dir .\out_dart

C. 병합
   PS> python merge_raw_dart_v48.py --raw raw_v48.parquet --dart-dir .\out_dart\finance --out final_v48.parquet

   - 진행상황: final_v48.parquet와 동일 폴더의 status.json
   - 부분 병합(특정 종목만):
     --codes-file codes_subset.txt

안전장치 요약
- 중단/재시작: per-code 파일/체크포인트로 자동 이어받기
- 오염 방지: 결과는 항상 새 파일에 기록, 기존 파일 덮어쓰지 않음
- 진행상황: status.json에만 기록(제자리 확인)
- 스키마: V48_COLS 강제, 날짜 YYYYMMDD 문자열로 통일
- 병합: RAW의 (date,code) 달력을 기준으로만 LEFT JOIN
- 결측: 재무는 일별로 확장 후 forward-fill

주의
- earnings_date는 현재 외부 소스 필요(NA 허용)
- 매크로 구멍은 RAW 단계에서 ffill/bfill 했다는 가정
- 재무 값의 분기 경계 보정이나 연간/분기 혼합 규칙이 필요하면 merge 단계에서 후처리 룰을 추가 가능

튜닝 팁
- DART 키 5개 기준: --workers 8~16 권장
- 네트워크 품질에 따라 --max-tasks로 단계적 테스트 후 확장
- 병합 시 --codes-file로 부분 병합/검증 후 전체 수행
