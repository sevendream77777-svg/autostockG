#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""2800개 종목 × 11년 수집 계획 계산"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig
import time

print("=" * 80)
print("2800개 종목 × 11년 수집 계획 계산")
print("=" * 80)

# 기본 정보
NUM_STOCKS = 2800
YEARS = 11
TRADING_DAYS_PER_YEAR = 250  # 영업일 기준
TOTAL_TRADING_DAYS = YEARS * TRADING_DAYS_PER_YEAR
TOTAL_COMBINATIONS = NUM_STOCKS * TOTAL_TRADING_DAYS

print(f"\n[1] 기본 정보")
print("-" * 80)
print(f"종목 수: {NUM_STOCKS:,}개")
print(f"기간: {YEARS}년 ({TOTAL_TRADING_DAYS:,} 영업일)")
print(f"총 조합: {TOTAL_COMBINATIONS:,}건 (종목 × 날짜)")

# API 호출 분석
print(f"\n[2] 종목/날짜당 API 호출 분석")
print("-" * 80)

# 실제 테스트로 측정
print("테스트 중... (005930, 20251205)")
start = time.time()
collector = V52Collector(CollectConfig(code="005930", date="20251205"))
row = collector.run()
elapsed = time.time() - start

print(f"수집 시간: {elapsed:.2f}초")
print(f"수집된 컬럼: {sum(1 for v in row.values() if v is not None)}/52")

# API 호출 추정
api_calls_per_combination = {
    "PyKRX": 3,  # 가격, 거래량, 자금흐름
    "DART": 1,   # 재무제표 (분기별이므로 실제로는 더 적음)
    "네이버": 3,  # 스크래핑
    "ECOS/FRED": 0.1,  # 매크로는 날짜별로 캐시됨
}
total_api_per_combination = sum(api_calls_per_combination.values())

print(f"\n종목/날짜당 API 호출 추정:")
for source, count in api_calls_per_combination.items():
    print(f"  {source}: {count}회")
print(f"  총: {total_api_per_combination:.1f}회")

# 제한사항
print(f"\n[3] API 제한사항")
print("-" * 80)
limits = {
    "DART": 10000,  # 일일 제한
    "ECOS": 30000,  # 일일 제한
    "네이버": None,  # 명시적 제한 없음 (과도 시 차단)
    "PyKRX": None,  # 제한 없음
    "FRED": None,   # 제한 없음
}

for source, limit in limits.items():
    if limit:
        print(f"  {source}: {limit:,}건/일")
    else:
        print(f"  {source}: 제한 없음")

# 일일 수집 가능량 계산
print(f"\n[4] 일일 수집 가능량 계산")
print("-" * 80)

# DART가 가장 큰 제약
dart_limit = 10000
dart_calls_per_combination = 1  # 재무제표는 분기별이므로 실제로는 더 적음
max_combinations_per_day_by_dart = dart_limit // dart_calls_per_combination

# ECOS 제한
ecos_limit = 30000
ecos_calls_per_combination = 0.1  # 매크로는 날짜별로 캐시
max_combinations_per_day_by_ecos = int(ecos_limit / ecos_calls_per_combination)

# 실제 제약은 DART
max_combinations_per_day = min(max_combinations_per_day_by_dart, max_combinations_per_day_by_ecos)

print(f"DART 기준: {dart_limit:,}건/일 ÷ {dart_calls_per_combination}회 = {max_combinations_per_day_by_dart:,}개 조합/일")
print(f"ECOS 기준: {ecos_limit:,}건/일 ÷ {ecos_calls_per_combination}회 = {max_combinations_per_day_by_ecos:,}개 조합/일")
print(f"\n→ 실제 제약: DART")
print(f"→ 일일 최대 수집 가능: 약 {max_combinations_per_day:,}개 조합")

# 수집 기간 계산
print(f"\n[5] 전체 수집 기간 계산")
print("-" * 80)

days_needed = (TOTAL_COMBINATIONS + max_combinations_per_day - 1) // max_combinations_per_day
weeks_needed = (days_needed + 6) // 7
months_needed = (days_needed + 29) // 30

print(f"총 조합: {TOTAL_COMBINATIONS:,}건")
print(f"일일 수집량: {max_combinations_per_day:,}건")
print(f"필요 일수: {days_needed:,}일 ({weeks_needed:,}주, {months_needed:,}개월)")

# 최적화 방안
print(f"\n[6] 최적화 방안")
print("-" * 80)

# 재무제표는 분기별이므로 실제 DART 호출이 더 적음
# 분기당 1회 호출로 가정
quarters_per_year = 4
total_quarters = YEARS * quarters_per_year
dart_calls_optimized = NUM_STOCKS * total_quarters  # 종목 × 분기
dart_days_needed = (dart_calls_optimized + dart_limit - 1) // dart_limit

print(f"재무제표는 분기별이므로:")
print(f"  실제 DART 호출: {dart_calls_optimized:,}회 (종목 × 분기)")
print(f"  DART 수집 일수: {dart_days_needed:,}일")

# 매크로는 날짜별로 1회만 수집하면 됨
macro_calls = TOTAL_TRADING_DAYS * 3  # usdkrw, kr10y_yield, cnykrw 등
ecos_days_needed = (macro_calls + ecos_limit - 1) // ecos_limit

print(f"매크로는 날짜별로 1회만:")
print(f"  ECOS 호출: {macro_calls:,}회 (날짜 × 매크로 종류)")
print(f"  ECOS 수집 일수: {ecos_days_needed:,}일")

# 실제 제약은 일일 데이터 수집 속도
# PyKRX + 네이버 스크래핑 속도
estimated_time_per_combination = elapsed  # 테스트 결과
max_combinations_per_day_by_time = int((24 * 3600) / estimated_time_per_combination)  # 하루 24시간 기준

print(f"\n시간 기준:")
print(f"  종목/날짜당 소요 시간: {elapsed:.2f}초")
print(f"  하루 24시간 기준 최대: {max_combinations_per_day_by_time:,}개 조합/일")

# 최종 권장사항
print(f"\n[7] 최종 권장사항")
print("-" * 80)

# 보수적으로 하루 5,000개 조합으로 계산
conservative_daily = 5000
conservative_days = (TOTAL_COMBINATIONS + conservative_daily - 1) // conservative_daily

print(f"보수적 추정 (안전 마진 포함):")
print(f"  일일 수집량: {conservative_daily:,}개 조합")
print(f"  필요 일수: {conservative_days:,}일 ({conservative_days//7:,}주, {conservative_days//30:,}개월)")

print(f"\n권장 수집 전략:")
print(f"  1. 날짜별로 수집 (현재 방식 유지)")
print(f"  2. 하루에 약 {conservative_daily:,}개 조합 수집")
print(f"  3. 약 {conservative_days//30:,}개월에 걸쳐 수집")
print(f"  4. 매일 진행 시 약 {conservative_days:,}일 소요")

# 날짜 범위별 수집 예시
print(f"\n[8] 날짜 범위별 수집 예시")
print("-" * 80)

combinations_per_date = NUM_STOCKS  # 날짜당 종목 수
dates_per_batch = conservative_daily // combinations_per_date

print(f"날짜당 종목 수: {combinations_per_date:,}개")
print(f"하루 수집 가능 날짜 수: {dates_per_batch:,}일")
print(f"\n예시:")
print(f"  --date-range 20150102:20150131  # 1월치")
print(f"  --date-range 20150201:20150228  # 2월치")
print(f"  ... (매일 실행)")

print("\n" + "=" * 80)
print("결론: 하루 약 5,000개 조합, 약 1,500일(약 4년) 소요")
print("=" * 80)




