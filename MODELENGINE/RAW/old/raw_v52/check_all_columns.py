#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""52개 컬럼 전체 확보 현황 확인"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig, V52_COLS

def check_columns(code: str = "005930", date: str = "20251205"):
    """52개 컬럼 확보 현황 확인"""
    print("=" * 70)
    print(f"52개 컬럼 확보 현황 확인")
    print(f"종목: {code}, 날짜: {date}")
    print("=" * 70)
    
    collector = V52Collector(CollectConfig(code=code, date=date))
    row = collector.run()
    
    # 확보된 컬럼
    secured = []
    missing = []
    
    for col in V52_COLS:
        val = row.get(col)
        if val is not None and val != "":
            secured.append(col)
        else:
            missing.append(col)
    
    print(f"\n✅ 확보된 컬럼: {len(secured)}/{len(V52_COLS)} ({len(secured)/len(V52_COLS)*100:.1f}%)")
    print(f"❌ 미확보 컬럼: {len(missing)}/{len(V52_COLS)} ({len(missing)/len(V52_COLS)*100:.1f}%)")
    
    print(f"\n📋 미확보 컬럼 목록:")
    for col in missing:
        print(f"  - {col}")
    
    print(f"\n📊 확보된 컬럼 샘플 (처음 10개):")
    for col in secured[:10]:
        val = row.get(col)
        if isinstance(val, float):
            print(f"  - {col}: {val:.2f}" if val else f"  - {col}: {val}")
        else:
            print(f"  - {col}: {val}")
    
    return row, secured, missing

if __name__ == "__main__":
    # 최근 날짜 테스트
    check_columns("005930", "20251205")
    
    print("\n" + "=" * 70)
    print("과거 날짜 테스트 (2015-01-05)")
    print("=" * 70)
    check_columns("005930", "20150105")




