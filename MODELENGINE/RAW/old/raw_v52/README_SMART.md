# 스마트 수집기 사용 가이드

## 주요 기능

✅ **하루 단위 순차 수집**: 날짜별로 차곡차곡 수집  
✅ **중단 후 이어서 진행**: 체크포인트 저장/복구  
✅ **부분 재수집**: 누락/실패 데이터만 재수집  
✅ **자동 검증**: 데이터 품질 자동 검사  
✅ **오염 데이터 필터링**: 잘못된 데이터 자동 감지 및 재수집  

---

## 기본 사용법

### 1. 전체 수집 (하루 단위로 순차 진행)

```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --workers 10
```

- 날짜별로 순차 수집
- 중단되면 체크포인트에서 이어서 진행
- 완료된 작업은 자동 스킵

### 2. 검증만 수행

```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --verify-only
```

- 수집 없이 데이터 검증만 수행
- 문제 데이터를 `issues.json`에 저장

### 3. 실패한 작업만 재수집

```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --recollect-failed
```

- 이전에 실패한 작업만 재수집

### 4. 검증 실패 데이터만 재수집

```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --recollect-invalid
```

- 검증에서 문제가 발견된 데이터만 재수집

---

## 워크플로우 예시

### 초기 수집
```bash
# 1. 전체 수집 시작
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --workers 20

# 중단되면 그냥 다시 실행하면 이어서 진행됨
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --workers 20
```

### 수집 후 검증 및 재수집
```bash
# 2. 검증 수행
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --verify-only

# 3. 문제 데이터 재수집
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --recollect-invalid \
  --workers 20
```

---

## 검증 기준

### 자동 검증 항목

1. **필수 필드**: date, code 확인
2. **가격 데이터**: close 가격 존재 및 양수
3. **가격 일관성**: high >= low, high >= open, high >= close 등
4. **NaN 비율**: 50% 이상 NaN이면 실패

### 문제 데이터 분류

- **누락 (missing)**: 해당 종목/날짜 데이터가 없음
- **잘못됨 (invalid)**: 검증 실패 (가격 일관성 등)
- **NaN 많음 (nan_heavy)**: NaN이 50% 이상

---

## 체크포인트

- 위치: `out/checkpoint.pkl`
- 내용: 완료된 작업 목록, 실패한 작업 목록
- 자동 저장: 100건마다 자동 저장
- 수동 삭제: 처음부터 다시 시작하려면 삭제

```bash
# 체크포인트 삭제 (처음부터 다시 시작)
rm out/checkpoint.pkl
```

---

## 출력 파일

### CSV 파일
- `out/raw_v52_YYYYMMDD.csv`: 날짜별 데이터
- 기존 파일이 있으면 자동 병합 (중복 제거)

### 체크포인트
- `out/checkpoint.pkl`: 진행 상황 저장

### 검증 결과
- `out/issues.json`: 문제 데이터 목록 (--verify-only 실행 시)

---

## 성능 최적화

### 워커 수 조정
- **소량 (1-10종목)**: 4-10개 워커
- **중량 (100-1000종목)**: 10-50개 워커
- **대량 (2000+종목)**: 50-200개 워커

### 안전한 설정
```bash
# 안전한 설정 (차단 위험 낮음)
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --workers 20
```

### 빠른 설정 (주의)
```bash
# 빠른 설정 (차단 위험 높음)
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20251205 \
  --workers 100
```

---

## 문제 해결

### 중단 후 재시작
- 그냥 같은 명령 다시 실행하면 자동으로 이어서 진행

### 일부 데이터 누락
```bash
# 검증 후 재수집
python run_raw_v52_smart.py --verify-only ...
python run_raw_v52_smart.py --recollect-invalid ...
```

### 특정 날짜만 재수집
```bash
# 특정 날짜 범위만 지정
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150105:20150105 \
  --workers 20
```

---

## 주의사항

1. **API 제한**: DART 10,000건/일, ECOS 30,000건/일
2. **메모리**: 워커 수가 많을수록 메모리 사용량 증가
3. **네트워크**: 안정적인 인터넷 연결 필요
4. **체크포인트**: 정기적으로 백업 권장

---

## 예상 소요 시간

- **2,772종목 × 2,750일 = 7,623,000건**
- **20개 워커**: 약 1,058일 (약 3년)
- **50개 워커**: 약 423일 (약 1.2년)
- **100개 워커**: 약 212일 (약 7개월)

*실제 시간은 API 제한, 네트워크 속도에 따라 달라질 수 있습니다.*




