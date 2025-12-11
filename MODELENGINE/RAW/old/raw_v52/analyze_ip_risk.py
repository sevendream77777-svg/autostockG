#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""5영업일 수집 시 IP 차단 가능성 분석"""

NUM_STOCKS = 2772
TRADING_DAYS = 5
TOTAL_TASKS = NUM_STOCKS * TRADING_DAYS

print("=" * 80)
print("5영업일 수집 시 IP 차단 가능성 분석")
print("=" * 80)

print(f"\n[기본 정보]")
print(f"종목 수: {NUM_STOCKS:,}개")
print(f"영업일: {TRADING_DAYS}일")
print(f"총 작업: {TOTAL_TASKS:,}건")

# API 호출 분석
print(f"\n[API 호출 분석]")
api_calls = {
    "PyKRX": 3,  # 가격, 거래량, 자금흐름
    "DART": 1,   # 재무제표 (분기별이므로 실제로는 더 적음)
    "네이버": 3,  # 스크래핑
    "ECOS/FRED": 0.1,  # 매크로는 날짜별로 캐시
}
total_per_task = sum(api_calls.values())

print(f"종목/날짜당 API 호출:")
for source, count in api_calls.items():
    print(f"  {source}: {count}회")
print(f"  총: {total_per_task:.1f}회")

total_api_calls = TOTAL_TASKS * total_per_task
print(f"\n전체 API 호출: {total_api_calls:,.0f}회")

# 시간 분석
TIME_PER_TASK = 25  # 초
total_time = TOTAL_TASKS * TIME_PER_TASK
print(f"\n[시간 분석]")
print(f"작업당 시간: {TIME_PER_TASK}초")
print(f"순차 처리 시간: {total_time/3600:.1f}시간")
print(f"10개 워커 병렬: {total_time/10/3600:.1f}시간")

# 소스별 차단 위험도
print(f"\n[소스별 차단 위험도]")
risks = {
    "PyKRX": {
        "호출": TOTAL_TASKS * 3,
        "제한": "없음",
        "위험도": "❌ 매우 낮음",
        "이유": "공식 API, 정상 사용"
    },
    "DART": {
        "호출": TOTAL_TASKS * 1,
        "제한": "10,000건/일",
        "위험도": "✅ 안전",
        "이유": f"{TOTAL_TASKS * 1:,}건 < 10,000건 (일일 제한 내)"
    },
    "ECOS": {
        "호출": int(TRADING_DAYS * 3 * 0.1),  # 날짜별 매크로
        "제한": "30,000건/일",
        "위험도": "✅ 매우 안전",
        "이유": f"{int(TRADING_DAYS * 3 * 0.1)}건 << 30,000건"
    },
    "네이버 스크래핑": {
        "호출": TOTAL_TASKS * 3,
        "제한": "명시적 제한 없음",
        "위험도": "⚠️ 주의 필요",
        "이유": f"{TOTAL_TASKS * 3:,}건 스크래핑 - 과도한 요청 시 IP 차단 가능"
    }
}

for source, info in risks.items():
    print(f"\n{source}:")
    print(f"  호출: {info['호출']:,}건")
    print(f"  제한: {info['제한']}")
    print(f"  위험도: {info['위험도']}")
    print(f"  이유: {info['이유']}")

# 네이버 스크래핑 상세 분석
print(f"\n[네이버 스크래핑 상세 분석]")
naver_calls = TOTAL_TASKS * 3
print(f"총 스크래핑 요청: {naver_calls:,}건")

# 시간 분산
hours_10_workers = total_time / 10 / 3600
calls_per_hour = naver_calls / hours_10_workers if hours_10_workers > 0 else 0
calls_per_minute = calls_per_hour / 60

print(f"\n10개 워커 기준:")
print(f"  소요 시간: {hours_10_workers:.1f}시간")
print(f"  시간당 요청: {calls_per_hour:,.0f}건")
print(f"  분당 요청: {calls_per_minute:,.1f}건")

# 안전 기준
SAFE_CALLS_PER_MINUTE = 10  # 분당 10건 이하면 안전
SAFE_CALLS_PER_HOUR = 600   # 시간당 600건 이하면 안전

print(f"\n[안전 기준]")
print(f"  분당 {SAFE_CALLS_PER_MINUTE}건 이하: 안전")
print(f"  시간당 {SAFE_CALLS_PER_HOUR}건 이하: 안전")

if calls_per_minute <= SAFE_CALLS_PER_MINUTE:
    print(f"\n✅ 안전: 분당 {calls_per_minute:.1f}건 <= {SAFE_CALLS_PER_MINUTE}건")
elif calls_per_hour <= SAFE_CALLS_PER_HOUR:
    print(f"\n⚠️ 주의: 분당 {calls_per_minute:.1f}건 > {SAFE_CALLS_PER_MINUTE}건, 하지만 시간당 {calls_per_hour:,.0f}건 <= {SAFE_CALLS_PER_HOUR}건")
else:
    print(f"\n🔴 위험: 시간당 {calls_per_hour:,.0f}건 > {SAFE_CALLS_PER_HOUR}건")

# 권장사항
print(f"\n[권장사항]")
if calls_per_minute > SAFE_CALLS_PER_MINUTE:
    print("1. 워커 수 감소 (10개 → 5개)")
    print("2. 요청 간격 증가 (backoff_sleep 시간 증가)")
    print("3. 시간 분산 (하루에 나눠서 수집)")
    print("4. VPN/프록시 사용")
else:
    print("✅ 현재 설정으로 안전하게 수집 가능")

print("\n" + "=" * 80)




