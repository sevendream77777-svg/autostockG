# MASTER COLUMN SCHEMA — FINAL 52 COLUMNS (Optimized for Cursor AI)
# This file defines the complete set of 52 essential raw columns used across the autostockG data pipeline.
# No derived or calculated columns are included here.
# Cursor should use this file as the authoritative reference for column completeness, mapping, and auto-patching.

---

## 1. Basic Meta (7)
1.  date                # 일자 (YYYY-MM-DD)
2.  code                # 종목코드
3.  name                # 종목명
4.  market              # KOSPI/KOSDAQ
5.  listing_status      # 상장 상태 (Listed/Delisted)
6.  sector_code         # 업종 코드
7.  sector_name         # 업종명 (UI 표출용 보너스 컬럼)

---

## 2. Price & Liquidity (10)
8.   open               # 시가
9.   high               # 고가
10.  low                # 저가
11.  close              # 종가
12.  volume             # 거래량
13.  amount             # 거래대금
14.  adj_factor         # 수정계수
15.  vwap               # 거래량가중평균가
16.  market_cap         # 시가총액
17.  shares_out         # 상장주식수

---

## 3. Flow: 수급 (12)
18.  frgn_net_amt       # 외국인 순매수 금액
19.  inst_net_amt       # 기관 순매수 금액 (전체)
20.  nps_net_amt        # 연기금 순매수 금액
21.  tust_net_amt       # 투신 순매수 금액
22.  dealer_net_amt     # 금융투자(딜러) 순매수 금액

23.  frgn_net_qty       # 외국인 순매수 수량
24.  inst_net_qty       # 기관 순매수 수량
25.  nps_net_qty        # 연기금 순매수 수량

26.  short_sell_amt     # 공매도 거래대금
27.  short_sell_qty     # 공매도 거래수량

28.  loan_balance_amt   # 대차잔고 금액
29.  loan_balance_qty   # 대차잔고 수량 (보너스)

---

## 4. Finance: 재무 (12)
30.  announce_date      # 재무 공시일
31.  revenue            # 매출액
32.  op_income          # 영업이익
33.  net_income         # 당기순이익

34.  total_equity       # 자본총계
35.  total_assets       # 자산총계

36.  cash_flow_op       # 영업활동현금흐름
37.  cash_flow_inv      # 투자활동현금흐름
38.  cash_flow_fin      # 재무활동현금흐름

39.  div_amount         # 주당 배당금
40.  eps                # EPS
41.  roe                # ROE

---

## 5. Macro & Events (11)
42.  usdkrw             # 원/달러 환율
43.  us10y_yield        # 미국 10년물 금리
44.  kr10y_yield        # 한국 10년물 금리

45.  wti                # 국제유가
46.  dxy                # 달러 인덱스
47.  cnykrw             # 원/위안 환율
48.  gold               # 국제 금값

49.  ex_div_date        # 배당락일
50.  earnings_date      # 실적 발표 예정일

51.  bps                # BPS
52.  debt_ratio         # 부채비율

---

# END OF MASTER SCHEMA
# Cursor must ensure: 
# - These 52 columns exist in all final data outputs.
# - Missing columns must be auto-implemented.
# - Extra/derived columns must be excluded unless explicitly declared.
