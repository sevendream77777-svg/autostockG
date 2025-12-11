# raw_v48 Vectorized 수집기

대량 히스토리컬 데이터 구축을 위한 Vectorized 수집기입니다.

## 주요 특징

- **Vectorized 수집**: 종목별 11년치 데이터를 일괄 수집 (Point-wise → Vectorized)
- **PyKRX 기간 일괄 조회**: 일자별 반복 호출 대신 기간 단위 일괄 조회
- **DART 분기별 수집**: 연도별 × 분기별 수집 후 Merge & Forward Fill
- **매크로 전범위 캐시**: ECOS/FRED/yfinance 데이터를 Parquet 캐시로 저장
- **병렬화 지원**: 종목 단위 병렬 처리로 대량 수집 속도 향상
- **체크포인트 지원**: 중단 시 이어서 수집 가능

## 성능 개선

### 기존 방식 (legacy point-wise)
- 종목 × 일자 단위 순차 수집
- 1행당 10~15회 네트워크 호출
- 2,500종목 × 11년 ≈ 4,000~8,000시간 (166~333일)

### Vectorized 방식 (raw_v48)
- 종목별 11년치 일괄 수집
- 종목당 약 50~100회 네트워크 호출 (기간 일괄 조회)
- 2,500종목 × 11년 ≈ 1~3일 (병렬화 적용 시)

## 설치

필수 패키지:
```bash
pip install pandas pykrx yfinance requests
```

## 사용법

### 1. 종목 리스트 준비

```bash
python get_all_stocks.py --output stocks.txt
```

### 2. 데이터 수집

```bash
# 기본 사용
python run_raw_v48.py --codes stocks.txt --start-date 20150102 --end-date 20251205

# 병렬 워커 수 지정
python run_raw_v48.py --codes stocks.txt --start-date 20150102 --end-date 20251205 --workers 32

# 프록시 사용
python run_raw_v48.py --codes stocks.txt --start-date 20150102 --end-date 20251205 --proxy "http://proxy.example.com:8080"

# 프록시 파일 사용
python run_raw_v48.py --codes stocks.txt --start-date 20150102 --end-date 20251205 --proxy-file proxy.txt
```

### 3. 체크포인트

수집 중단 시 `out/checkpoint.pkl`에 진행 상황이 저장됩니다.
다시 실행하면 완료된 종목은 자동으로 스킵됩니다.

## 출력 파일

- `out/raw_v48_all.csv`: 모든 종목 통합 파일
- `out/raw_v48_YYYYMMDD.csv`: 날짜별 파일
- `out/checkpoint.pkl`: 진행 상황 체크포인트

## 수집 컬럼 (48개)

### 기본 정보
- date, code, name, market, listing_status, sector_code, sector_name

### 가격 & 유동성
- open, high, low, close, volume, amount, adj_factor, vwap, market_cap, shares_out

### 수급
- frgn_net_amt, inst_net_amt, nps_net_amt, tust_net_amt, dealer_net_amt
- frgn_net_qty, inst_net_qty, nps_net_qty

### 재무
- announce_date, revenue, op_income, net_income, total_equity, total_assets
- cash_flow_op, cash_flow_inv, cash_flow_fin, div_amount, eps, roe

### 매크로 & 이벤트
- usdkrw, us10y_yield, kr10y_yield, wti, dxy, cnykrw, gold, vix
- earnings_date, bps, debt_ratio

## 캐시 디렉토리

`cache/` 디렉토리에 다음 파일들이 저장됩니다:
- `yf_*.parquet`: yfinance 매크로 데이터
- `ecos_*.parquet`: ECOS 매크로 데이터
- `fred_*.parquet`: FRED 매크로 데이터
- `dart_corp_list.xml`: DART 종목 매핑 (선택적)

## 주의사항

1. **DART API 제한**: 일일 10,000건 제한 (종목당 약 44회 호출)
2. **네트워크 안정성**: 대량 수집 시 프록시 사용 권장
3. **메모리 사용량**: 종목당 11년치 데이터를 메모리에 로드하므로 충분한 메모리 필요
4. **API 키**: ECOS/DART API 키가 필요합니다 (자동 로드)

## 문제 해결

### 수집 속도가 느린 경우
- `--workers` 옵션으로 워커 수 증가 (기본: CPU*2)
- 프록시 사용으로 IP 차단 방지

### 일부 종목 실패
- 체크포인트에서 실패한 종목 확인
- 실패한 종목만 재수집 가능 (향후 기능)

### 메모리 부족
- 워커 수 감소 (`--workers` 옵션)
- 종목 리스트를 여러 파일로 분할하여 수집

현재까지 상황 요약 (근거: 코드·로그 직접 확인)
목표: DART 재무 14개 컬럼 수집/저장 후 RAW 34개와 병합해 최종 48개 완성.
DART 전용 수집기(run_dart_only_v48.py) 의도: 14컬럼(announce_date, revenue, op_income, net_income, total_equity, total_assets, cash_flow_op, cash_flow_inv, cash_flow_fin, div_amount, eps, roe, bps, debt_ratio).
실제: out_dart_test/finance_csv/005930.csv 컬럼 12개만 존재. 빠진 4개는 div_amount, roe, bps, debt_ratio.
empty_finance/020 이슈:
한도 초과(status=020) → 키를 바로 소진 처리하고 다음 키로 재시도하도록 수정했으나, 일부 종목에서 여전히 empty_finance(응답 list 없음) 발생.
일부 종목·연도에서 데이터 없음(status=013)도 로그에 등장.
최근 로그(quality_dart.jsonl):
14:29~14:37 사이 000020~000140 등 일부 PASS(행수 9~40) → 키 로테이션은 동작.
14:46 이후 000150~000230 empty_finance 반복.
.dart_usage.json는 삭제 후 재실행 완료.
코드 변경 사항 (근거)
키 로테이션/한도 처리:
_mark_key_exhausted로 020 키를 한도 처리 후 다음 키로 재시도.
_collect_dart_finance 요청 루프: status=020이면 즉시 다른 키로 재시도(키 수의 2배까지), 모두 소진 시 DartQuotaExhausted.
배당 매핑 추가: account_nm에 “배당” 포함 시 div_amount.
병합 시 파생 계산: roe, bps, debt_ratio (병합 단계에서 계산).
남은 문제 (핵심)
DART-only 저장 시 12컬럼만 저장됨: div_amount, roe, bps, debt_ratio가 DF에 없어서 필터링됨.
해결 필요: DART-only 저장 직전에 해당 4개 컬럼을 강제 생성(가능 시 계산, 아니면 NA라도)해 14컬럼을 항상 저장하도록 보강.
현재 상태로 파악한 원인
020 한도는 로테이션으로 일부 해소됐으나, 여전히 응답이 비는 경우 존재(데이터 없음/계정 매핑 미포함 등).
파생/배당 컬럼이 DF에 없으면 저장 시 제외 → 12컬럼 유지.
필요한 후속 조치 (제안)
collect_dart_only 저장 직전에 div_amount/roe/bps/debt_ratio 컬럼을 강제 생성(NA라도)해서 파켓/CSV에 14컬럼을 맞춤.
status별 로깅을 더 남기고, 013(데이터 없음) 종목은 제외 리스트로 분리.
한도(020) 재발 시: 새 키 추가 또는 다음날 리셋 후 재시도.
이 상태를 다른 작업자에게 넘기면:
남은 수정 포인트: DART-only 저장 시 4컬럼 강제 생성/계산 로직 추가.
키 로테이션/한도 처리는 이미 들어가 있음(020 시 다음 키 재시도).

현재 작업중인 상황이고 
F:\autostockG\MODELENGINE\RAW\raw_v48\dart_only
이경로에 있는 파일들이 dart쪽 14개 컬럼을 34개 컬럼에 앞서 수집하는 프로그램이야



✔ 왜 announce_date가 안 나왔는가?

지금 쓰는 DART API (fnlttSinglAcntAll.json)는 원래 announce_date(공시일자)를 절대 포함하지 않는 API다.
즉, 호출을 잘못한 게 아니라 API 자체가 announce_date를 안 주는 구조다.

✔ announce_date를 어떻게 뽑냐?

DART에서 공시일을 얻으려면 완전히 다른 API를 같이 호출해야 한다.

정답은 아래 둘 중 하나:

① list.json (기업공시 목록 API)

corp_code + bsns_year + reprt_code로 검색

rcept_dt = 공시 제출일(announce_date)

재무재표 API와 반드시 병합해야 함

② fnlttSinglAcnt.json (단일 계정 API)

일부 경우에 rcept_no, rcept_dt 포함

하지만 안정성은 list.json이 더 좋음

✔ 결론

announce_date = 별도 API 필요

지금 소스에는 announce_date 로직 자체가 없음 → 100% 당연히 안 나옴

해결하려면 DART API 1개 더 추가로 호출해야 한다 (list.json 추천)

announce_date 패치는 “짧은 패치”가 절대 불가능함.
새 API 호출 + json merge + 에러/키로테이션 처리 + 중복 공시 필터링까지 들어가서
반드시 전체 파일 재생성이 맞다.

요청대로 완성본(run_dart_standalone_v48.py) 전체 파일 새로 만들어서 아래에 바로 준다.
(announce_date 확보 + 진행상황 print 강화 포함)

✔ 변경점 요약

list.json 공시목록 API 추가 호출
→ rcept_dt = announce_date 로 매핑

reprt_code & year 조합별 가장 근접한 공시일을 연결

기존 재무 데이터와 merge

print 상태 출력 강화

현재 코드

현재 연도

reprt_code

DART 키

API 호출 시작/완료

저장 완료 메시지

