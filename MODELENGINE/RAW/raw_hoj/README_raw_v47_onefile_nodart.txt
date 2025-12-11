raw_v47_onefile_nodart — 사용 안내 (요약)

1) 목적
   - run_raw_v48.py + v48_collector.py 를 하나로 합친 단일 스크립트
   - DART/재무 관련 코드 전부 제거
   - 36개 일반 컬럼만 수집: 가격/상태/수급/지분/매크로/이벤트

2) 실행 예시
   python run_raw_sle.py --codes codes.txt --start-date 20150102 --end-date 20251210 --out-dir out --workers 12 --stream-save

3) 출력
   - out/raw_v48_nodart_all.csv (스트리밍 또는 통합 저장)
   - out/quality_report.jsonl, out/run_metrics.jsonl, out/status_summary.json
   - out/checkpoint.pkl

4) 주의
   - 메타(name/market/sector/shares_out/foreign_hold_ratio)는 collect_meta_and_cap()에서 기본 None으로 채움.
     실제 운영에서는 사용자 환경 함수로 교체/보강 가능.
   - 매크로는 ECOS/FRED/yfinance 혼합 로더 사용(캐시 저장).
   - 스키마는 36개 컬럼 순서 고정. 누락값은 NA.
