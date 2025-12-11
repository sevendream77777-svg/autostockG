# p71 원천 컬럼 표준 정의서 (정식)

본 문서는 v53(원천 53개)에 추가 18개 확장 컬럼을 더한 **총 71개(p71)** 표준 스펙을 정의합니다.

모든 항목은 **정의/단위/데이터 타입/소스/포인트-인-타임(PIT) 처리/결측 보완/갱신 주기**를 명시합니다.

주의: 재무(DART)·이벤트는 **공시 접수일 기준 PIT**를 엄격 적용합니다.


## 가격(Price)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| date | 거래일(YYYYMMDD) | string |  | 거래일자(영업일). 포맷은 YYYYMMDD. | Kiwoom/PyKRX | — | EOD(T+0) | Source | 영업일만 취급. 휴장일은 최근 영업일로 백오프. |
| code | 종목코드(6자리) | string |  | KRX 6자리 종목코드. | Kiwoom/PyKRX | — | EOD(T+0) | Source | 선도/후행 없는 고정키. |
| name | 종목명 | string |  | 종목 명칭(한글). | Kiwoom/PyKRX | FnGuide | EOD(T+0) | Source | 공급원 불일치 시 PyKRX 우선. |
| market | 시장(KOSPI/KOSDAQ) | string |  | 상장 시장 구분. | PyKRX | 추론(code<100000→KOSPI) | EOD(T+0) | Derived | 소스 결측 시 코드 기반 추론. |
| open | 시가 | float | KRW | 해당 거래일 시가. | Kiwoom/PyKRX | — | EOD(T+0) | Source |  |
| high | 고가 | float | KRW | 해당 거래일 고가. | Kiwoom/PyKRX | — | EOD(T+0) | Source |  |
| low | 저가 | float | KRW | 해당 거래일 저가. | Kiwoom/PyKRX | — | EOD(T+0) | Source |  |
| close | 종가 | float | KRW | 해당 거래일 종가. | Kiwoom/PyKRX | — | EOD(T+0) | Source | 액면분할 등 과거 보정은 별도 컬럼 사용 권장. |
| volume | 거래량 | float | 주 | 당일 체결 주식수 총합. | Kiwoom/PyKRX | — | EOD(T+0) | Source |  |
| amount | 거래대금 | float | KRW | 당일 체결 금액 총합. | Kiwoom/PyKRX | Ticker Snapshot | EOD(T+0) | Source | PyKRX amount=None 시 티커 스냅샷으로 보완. |
| adj_factor | 수정계수 | float |  | 분할/배당 등 과거 보정계수. | PyKRX(가능 시) | — | EOD(T+0) | Source | 없으면 공란. |
| vwap | 체결가중평균 | float | KRW | 금액/거래량으로 계산한 당일 VWAP. | Calc(amount,volume) | — | EOD(T+0) | Derived | amount/volume. 스케일 이상치시 보정. |

## 수급(Flow)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| inst_net_qty | 기관 순매수 수량 | float | 주 | 기관 순매수(주). | PyKRX | — | EOD(T+0) | Source |  |
| inst_net_amt | 기관 순매수 금액 | float | KRW | 기관 순매수(원). | PyKRX | — | EOD(T+0) | Source |  |
| frgn_net_qty | 외국인 순매수 수량 | float | 주 | 외국인 순매수(주). | PyKRX | — | EOD(T+0) | Source |  |
| frgn_net_amt | 외국인 순매수 금액 | float | KRW | 외국인 순매수(원). | PyKRX | — | EOD(T+0) | Source |  |
| nps_net_qty | 연기금등 순매수 수량 | float | 주 | 연기금/기금 순매수(주). | PyKRX 확장 | KRX 원시 | EOD(T+0~1) | Source |  |
| nps_net_amt | 연기금등 순매수 금액 | float | KRW | 연기금/기금 순매수(원). | PyKRX 확장 | KRX 원시 | EOD(T+0~1) | Source |  |
| dealer_net_qty | 금융투자 순매수 수량 | float | 주 | 금융투자(딜러) 순매수(주). | PyKRX 확장 | KRX 원시 | EOD(T+0~1) | Source |  |
| dealer_net_amt | 금융투자 순매수 금액 | float | KRW | 금융투자(딜러) 순매수(원). | PyKRX 확장 | KRX 원시 | EOD(T+0~1) | Source |  |
| short_sell_qty | 공매도 거래량 | float | 주 | 당일 공매도 체결량. | PyKRX | KRX | EOD(T+0~1) | Source |  |
| short_sell_amt | 공매도 거래대금 | float | KRW | 당일 공매도 체결대금. | PyKRX | KRX | EOD(T+0~1) | Source |  |
| loan_balance_qty | 대차잔고 수량 | float | 주 | 대차잔고(주). | KRX(별도) | 크롤링 보조 | Daily(T+1~2) | Source | 공개 포맷 변동 위험. |
| loan_balance_amt | 대차잔고 금액 | float | KRW | 대차잔고(원). | KRX(별도) | 크롤링 보조 | Daily(T+1~2) | Source |  |

## 재무(Finance/DART)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| revenue | 매출액 | float | KRW | IFRS 연결 기준 매출(연/분기). | DART fnlttSinglAcntAll | FnGuide | PIT(공시접수일) | Source | 최신 확정/잠정 우선순위 규칙 적용. |
| op_income | 영업이익 | float | KRW | IFRS 연결 기준 영업이익. | DART | FnGuide | PIT(공시접수일) | Source |  |
| net_income | 당기순이익 | float | KRW | IFRS 연결 기준 당기순이익. | DART | FnGuide | PIT(공시접수일) | Source |  |
| eps | 주당순이익 | float | KRW | 기본 EPS. | DART | FnGuide | PIT(공시접수일) | Source |  |
| cash_flow_op | 영업현금흐름 | float | KRW | 현금흐름표 영업활동. | DART | FnGuide | PIT(공시접수일) | Source |  |
| cash_flow_inv | 투자현금흐름 | float | KRW | 현금흐름표 투자활동. | DART | FnGuide | PIT(공시접수일) | Source |  |
| cash_flow_fin | 재무현금흐름 | float | KRW | 현금흐름표 재무활동. | DART | FnGuide | PIT(공시접수일) | Source |  |

## 섹터·테마(Sector/Theme)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| sector_code | 업종코드(네이버) | string |  | 네이버 업종 코드(no=). | Naver | — | EOD(T+0~1) | Source | DOM 변동 시 셀렉터 업데이트. |
| sector_name | 업종명 | string |  | 업종(예: FICS 반도체 및 관련장비). | Naver/FnGuide | — | EOD(T+0~1) | Source |  |
| theme_code | 테마코드(네이버) | string |  | 네이버 테마 코드(no=). | Naver | — | EOD(T+0~1) | Source |  |
| theme_name | 테마명 | string |  | 대표 테마명. | Naver | — | EOD(T+0~1) | Source |  |
| sector_index_close | 업종지수 종가 | float | index | 업종 지수 현재가/종가. | Naver | KRX 업종지수 | EOD(T+0~1) | Source |  |

## 매크로(Macro)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| usdkrw | 원/달러 환율 | float | KRW/USD | 환율(달러). | FDR | Naver | EOD(T+0) | Source |  |
| cnykrw | 원/위안 환율 | float | KRW/CNY | 환율(위안). | FDR/Naver | — | EOD(T+0) | Source |  |
| dxy | 달러인덱스 | float | index | DXY. | FDR | — | EOD(T+0) | Source |  |
| us10y_yield | 미국 10Y 금리 | float | % | 미국 국채 10년 수익률. | FDR | — | EOD(T+0) | Source |  |
| kr10y_yield | 한국 10Y 금리 | float | % | 국고채 10년 수익률. | FDR | BOK/Naver | EOD(T+0) | Source | FDR 실패 시 BOK/네이버 보완. |
| wti | WTI | float | USD/bbl | 서부텍사스중질유. | FDR | — | EOD(T+0) | Source |  |
| gold | 국제 금 | float | USD/oz | 금 선물. | FDR | Naver | EOD(T+0) | Source |  |
| vix | VIX | float | index | 변동성 지수. | FDR | — | EOD(T+0) | Source |  |

## 이벤트(Event)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| earnings_announce_date | 실적발표일(접수) | string | YYYYMMDD | 잠정/확정 실적 공시 접수일. | DART list | — | PIT(접수일) | Source |  |
| earnings_effective_date | 실적 효력일 | string | YYYYMMDD | 효력 발생일(상세 본문 필요). | DART detail | — | PIT | Source | v4는 announce 우선. |
| ex_div_date | 배당락일(접수일 근사) | string | YYYYMMDD | 배당 공시일/락일. 우선 접수일 기록. | DART list | Naver | PIT | Source | 상세 파서 확장 시 정확도↑ |
| div_amount | 배당금(주당) | float | KRW | 현금배당 단위 금액. | DART detail | Naver | PIT | Source |  |
| split_announce_date | 분할 발표일 | string | YYYYMMDD | 주식분할 공시 접수일. | DART list | — | PIT | Source |  |
| split_effective_date | 분할 효력일 | string | YYYYMMDD | 분할 효력 발생일. | DART detail | — | PIT | Source |  |
| rights_issue_announce_date | 권리 공시일 | string | YYYYMMDD | 유/무상증자 등 권리 공시. | DART list | — | PIT | Source |  |
| rights_issue_effective_date | 권리 효력일 | string | YYYYMMDD | 권리 행사 효력일. | DART detail | — | PIT | Source |  |
| mna_announce_date | M&A 공시일 | string | YYYYMMDD | 합병/양수도 등 주요 M&A 공시. | DART list | — | PIT | Source |  |

## 확장(추가 18개)

| name | 설명 | datatype | unit | 정의 | primary_source | fallback_source | PIT/Refresh | calc_or_source | notes |
|---|---|---|---|---|---|---|---|---|---|
| mkt_cap | 시가총액 | float | KRW | 종가 × 상장주식수. | PyKRX/FnGuide | Naver | EOD(T+0) | Derived | 상장주식수는 분할/소각 반영. |
| float_mkt_cap | 유동 시가총액 | float | KRW | 유동주식수 × 종가. | FnGuide/Naver | 추정(상장-보호/자기주식) | Monthly/Quarterly | Derived | 데이터 가용성에 따라 월/분기 갱신. |
| turnover_rate_adj | 회전율(보정) | float | % | 거래량/유동주식수. | Calc(volume,free_float) | Calc(volume,listed) | EOD(T+0) | Derived | free-float 미보유 시 상장주식수 사용. |
| intraday_volatility | 일중 변동성 | float | % | (고가-저가)/종가. | Calc(high,low,close) | — | EOD(T+0) | Derived |  |
| overnight_return | 오버나이트 수익률 | float | % | (당일 시가 - 전일 종가)/전일 종가. | Calc(open,prev_close) | — | EOD(T+0) | Derived | 전일 종가 필요. |
| range_strength | 레인지 강도 | float | % | (고가-저가)/시가. | Calc(high,low,open) | — | EOD(T+0) | Derived |  |
| short_ratio | 공매도 비중(수량) | float | % | 공매도량/거래량. | Calc(short_sell_qty,volume) | — | EOD(T+0~1) | Derived |  |
| short_value_rate | 공매도 비중(금액) | float | % | 공매도대금/거래대금. | Calc(short_sell_amt,amount) | — | EOD(T+0~1) | Derived |  |
| loan_change | 대차잔고 증감 | float | 주 | 대차잔고 수량의 일간 증감. | Calc(loan_balance_qty - prev) | — | Daily(T+1~2) | Derived |  |
| loan_ratio | 대차잔고 비중 | float | % | 대차잔고/유동주식수. | Calc(loan_balance_qty/free_float) | Calc(…/listed) | Daily(T+1~2) | Derived |  |
| eps_ttm | EPS(TTM) | float | KRW | 최근 4분기 누적 EPS. | DART quarter | FnGuide | PIT(분기 공시) | Derived | 분기 공시 기준 롤링 합산. |
| bps_ttm | BPS(TTM) | float | KRW | 최근 4분기/연환산 BPS. | DART/FnGuide | — | PIT | Derived | 연환산/보간 방식 명시 필요. |
| oper_cf_ttm | 영업현금흐름(TTM) | float | KRW | 최근 4분기 누적 OCF. | DART | FnGuide | PIT | Derived |  |
| revenue_ttm | 매출액(TTM) | float | KRW | 최근 4분기 누적 매출. | DART | FnGuide | PIT | Derived |  |
| sector_perf_1d | 업종 1일 수익률 | float | % | 업종지수 전일대비 수익률. | Calc(sector_index) | — | EOD(T+0) | Derived |  |
| sector_perf_5d | 업종 5일 수익률 | float | % | 업종지수 5영업일 수익률. | Calc(sector_index) | — | EOD(T+0) | Derived |  |
| theme_perf_1d | 테마 1일 수익률 | float | % | 테마지수 전일대비 수익률. | Calc(theme_index) | — | EOD(T+0) | Derived |  |
| theme_perf_5d | 테마 5일 수익률 | float | % | 테마지수 5영업일 수익률. | Calc(theme_index) | — | EOD(T+0) | Derived |  |


---
## 데이터 품질 규칙(요약)
1) **키 무결성**: (date, code) 복합키 유일성 보장. 중복 발생 시 최신 수집시각 우선.
2) **단위 일관성**: 금액(KRW), 수량(주), 비율(%) 명확화. 입력 전 스케일(천/백만) 자동 교정.
3) **결측 처리**: 
   - `amount` 결측 시 티커 스냅샷으로 보완, 그래도 없으면 NaN 유지.
   - `kr10y_yield` FDR 실패 시 BOK/네이버 파싱으로 보완.
   - 섹터/테마 DOM 실패 시 FnGuide 업종명만 우선 기입, 코드/지수는 보류.
4) **PIT(포인트-인-타임)**:
   - 재무/이벤트는 **공시 접수일(rcp_dt)** 이후에만 값이 유효하다고 간주.
   - TTM 계산은 **최근 4개 분기 공시가 모두 PIT상 공개된 상태**에서만 산출.
5) **백오프**: 비영업일/결측 시 **최근 영업일 -3**까지 후퇴.
6) **검증**: (high ≥ max(open,close), low ≤ min(open,close)), amount≈close×volume ± 허용오차 등 기본 룰 체크.
7) **로그**: 소스별 확보/누락/사유를 notes에 누적 기록.

---
## 파일/스키마
- 파일명: `RAW/p71_YYYYMMDD.parquet` (또는 `csv`)
- 키: `date`(str, 8) + `code`(str, 6)
- 파티션: `year=YYYY/month=MM`
- 스키마는 동봉 CSV(`p71_standard_spec.csv`) 참조.
