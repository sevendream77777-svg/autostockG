# 프록시/IP 변경 사용 가이드

## 현재 실행 파일

### 메인 실행 파일
- **`run_raw_v52_smart.py`**: 스마트 수집기 (권장)
  - 하루 단위 순차 수집
  - 중단 후 이어서 진행
  - 검증 및 재수집 기능
  - 프록시/IP 변경 지원

### 연결된 파일들

#### 핵심 수집 로직
- **`v52_collector.py`**: 실제 데이터 수집 로직
  - PyKRX, DART, 네이버, ECOS, FRED 등 모든 소스 수집
  - 프록시 지원 추가됨

#### 보조 파일들
- **`get_all_stocks.py`**: 전체 종목 리스트 가져오기
- **`verify_47_columns.py`**: 데이터 검증
- **`check_all_columns.py`**: 컬럼 확보 현황 확인

#### 기타 파일들
- **`run_raw_v52.py`**: 기본 수집기 (레거시)
- **`run_raw_v52_parallel.py`**: 병렬 수집기 (레거시)

---

## 프록시 설정 방법

### 방법 1: 명령줄에서 직접 지정

```bash
# HTTP 프록시
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20150102 \
  --days 5 \
  --workers 10 \
  --proxy http://proxy.example.com:8080

# SOCKS5 프록시
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20150102 \
  --days 5 \
  --workers 10 \
  --proxy socks5://127.0.0.1:1080
```

### 방법 2: 프록시 파일 사용

1. 프록시 설정 파일 생성:
```bash
# proxy.txt 파일 생성
echo "http://proxy.example.com:8080" > proxy.txt
# 또는
echo "socks5://127.0.0.1:1080" > proxy.txt
```

2. 실행:
```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20150102 \
  --days 5 \
  --workers 10 \
  --proxy-file proxy.txt
```

### 방법 3: 자동 로드 (기본 위치)

프록시 파일을 다음 위치에 두면 자동으로 로드됩니다:
- `MODELENGINE/RAW/raw_v52/proxy.txt`
- `~/.proxy_config.txt`

---

## 공유기 아이피타임 사용

### 공유기 아이피타임과 프록시의 관계

**공유기 아이피타임은 프록시와 별개입니다.**

- **공유기 아이피타임**: 공유기 자체의 IP 주소 (외부 IP)
- **프록시**: 요청을 중계하는 서버

### 사용 시나리오

1. **공유기 IP만 사용 (프록시 없음)**
   - 현재 공유기의 외부 IP로 직접 요청
   - IP 차단 시 공유기 재시작으로 IP 변경 가능 (아이피타임 설정)

2. **프록시 사용**
   - 프록시 서버를 통해 요청
   - 공유기 IP가 아닌 프록시 서버 IP로 요청
   - 공유기 재시작과 무관

### 권장 방법

**프록시 사용 권장:**
- 공유기 IP를 보호
- 차단 시 프록시만 변경하면 됨
- 공유기 재시작 불필요

---

## 프록시 서비스 추천

### 무료 프록시
- **프록시 리스트 사이트**: free-proxy-list.net 등
- **주의**: 불안정할 수 있음, 속도 느림

### 유료 프록시 (권장)
- **Bright Data (구 Luminati)**: 대용량 수집용
- **Smartproxy**: 안정적
- **Oxylabs**: 엔터프라이즈급

### VPN을 프록시로 사용
- **VPN 서비스**: NordVPN, ExpressVPN 등
- **로컬 프록시 설정**: VPN 클라이언트에서 SOCKS5 프록시 제공

---

## IP 변경 방법

### 방법 1: 프록시 로테이션
여러 프록시를 번갈아 사용:
```bash
# 프록시 리스트 파일
proxy1.txt: http://proxy1.com:8080
proxy2.txt: http://proxy2.com:8080
proxy3.txt: http://proxy3.com:8080

# 각각 다른 날짜에 사용
```

### 방법 2: 공유기 재시작 (아이피타임)
- 공유기 설정에서 IP 변경
- 또는 공유기 재시작 (동적 IP인 경우)
- **주의**: 공유기 IP는 프록시와 별개

### 방법 3: VPN 사용
- VPN 클라이언트에서 SOCKS5 프록시 제공
- VPN 서버 변경 = IP 변경

---

## 사용 예시

### 5영업일 수집 (프록시 사용)

```bash
# 1. 프록시 설정 파일 생성
echo "http://your-proxy.com:8080" > proxy.txt

# 2. 수집 실행
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20150102 \
  --days 5 \
  --workers 10 \
  --proxy-file proxy.txt
```

### 프록시 없이 진행 (공유기 IP만 사용)

```bash
python run_raw_v52_smart.py \
  --codes stocks.txt \
  --date-range 20150102:20150102 \
  --days 5 \
  --workers 10
```

**주의**: 차단 위험이 높으므로 워커 수를 줄이거나 요청 간격을 늘리는 것을 권장합니다.

---

## 프록시 테스트

프록시가 제대로 작동하는지 테스트:

```bash
# 프록시 없이
curl https://api.ipify.org

# 프록시 사용
curl --proxy http://proxy.example.com:8080 https://api.ipify.org
```

---

## 주의사항

1. **프록시 속도**: 프록시를 사용하면 속도가 느려질 수 있음
2. **프록시 안정성**: 무료 프록시는 불안정할 수 있음
3. **인증 필요**: 일부 프록시는 사용자명/비밀번호 필요
   - 형식: `http://username:password@proxy.com:8080`
4. **SOCKS5 vs HTTP**: SOCKS5가 더 안정적이지만 설정이 복잡할 수 있음

---

## 문제 해결

### 프록시 연결 실패
- 프록시 주소 확인
- 방화벽 설정 확인
- 프록시 서버 상태 확인

### 속도가 너무 느림
- 다른 프록시 서버 시도
- 프록시 없이 진행 (워커 수 감소)
- 프록시 서버 위치 확인 (가까운 서버 선택)

### 여전히 차단됨
- 프록시 서버 변경
- 요청 간격 증가
- 워커 수 감소




