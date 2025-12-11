#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cnykrw 2015년 ECOS 백업 테스트"""
import sys
sys.path.insert(0, r'F:\autostockG')
from MODELENGINE.RAW.raw_v52.v52_collector import V52Collector, CollectConfig

print('=' * 70)
print('cnykrw 확보 테스트 (yfinance → ECOS 백업)')
print('=' * 70)

for date in ['20251205', '20150105']:
    cfg = CollectConfig(code='005930', date=date)
    c = V52Collector(cfg)
    res = c.run()
    
    print(f'\n[{date}]')
    print(f'  cnykrw: {res.get("cnykrw")}')
    
    ok = sum(1 for v in res.values() if v is not None)
    print(f'  확보: {ok}/52 ({ok/52*100:.1f}%)')
    
    missing = [k for k, v in res.items() if v is None]
    if missing:
        print(f'  미확보: {missing}')





