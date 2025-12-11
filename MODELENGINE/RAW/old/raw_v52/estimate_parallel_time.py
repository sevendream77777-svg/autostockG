#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""병렬 수집 시간 추정"""

NUM_STOCKS = 2772  # 실제 종목 수
YEARS = 11
TRADING_DAYS = YEARS * 250
TOTAL_TASKS = NUM_STOCKS * TRADING_DAYS

TIME_PER_TASK = 25  # 초 (종목/날짜당)
NUM_WORKERS = 200  # 워커 수

print("=" * 80)
print("병렬 수집 시간 추정")
print("=" * 80)

print(f"\n[기본 정보]")
print(f"종목 수: {NUM_STOCKS:,}개")
print(f"기간: {YEARS}년 ({TRADING_DAYS:,} 영업일)")
print(f"총 작업: {TOTAL_TASKS:,}건")

print(f"\n[병렬 처리]")
print(f"워커 수: {NUM_WORKERS}개")
print(f"작업당 시간: {TIME_PER_TASK}초")

# 병렬 처리 시간 계산
sequential_time = TOTAL_TASKS * TIME_PER_TASK  # 초
parallel_time = sequential_time / NUM_WORKERS  # 초

print(f"\n[시간 계산]")
print(f"순차 처리: {sequential_time/3600:.1f}시간 ({sequential_time/86400:.1f}일)")
print(f"병렬 처리: {parallel_time/3600:.1f}시간 ({parallel_time/86400:.1f}일)")

# 실제로는 오버헤드가 있으므로 1.5배
actual_time = parallel_time * 1.5
print(f"실제 예상: {actual_time/3600:.1f}시간 ({actual_time/86400:.1f}일)")

# 하루 내 가능 여부
if actual_time <= 86400:
    print(f"\n✅ 하루 내 수집 가능!")
    print(f"   예상 소요: {actual_time/3600:.1f}시간")
else:
    print(f"\n⚠️ 하루 내 수집 어려움")
    print(f"   예상 소요: {actual_time/86400:.1f}일")

# 워커 수별 시간 비교
print(f"\n[워커 수별 시간 비교]")
for workers in [50, 100, 150, 200, 300]:
    time_needed = (TOTAL_TASKS * TIME_PER_TASK / workers) * 1.5
    print(f"  {workers:3d}개 워커: {time_needed/3600:6.1f}시간 ({time_needed/86400:5.2f}일)")

print("\n" + "=" * 80)




