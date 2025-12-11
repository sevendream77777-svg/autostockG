#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cnykrw 확보 테스트"""
import sys
sys.path.insert(0, r'F:\\autostockG')

from MODELENGINE.RAW.raw_v52.v52_collector import V52Collector, CollectConfig

for d in ["20251205", "20150105"]:
    cfg = CollectConfig(code="005930", date=d)
    c = V52Collector(cfg)
    res = c.run()
    print("\n---", d, "---")
    print("cnykrw:", res.get("cnykrw"))
    ok = sum(1 for v in res.values() if v is not None)
    print(f"확보: {ok}/52 ({ok/52*100:.1f}%)")
    missing = [k for k, v in res.items() if v is None]
    print("미확보:", missing)





