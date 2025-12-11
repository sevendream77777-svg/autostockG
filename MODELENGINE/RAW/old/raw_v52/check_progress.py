#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""수집 진행 상황 확인"""

import sys
import pickle
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

out_dir = Path(__file__).resolve().parent / "out"
checkpoint_file = out_dir / "checkpoint.pkl"

print("=" * 80)
print("수집 진행 상황 확인")
print("=" * 80)

# 체크포인트 확인
if checkpoint_file.exists():
    try:
        with open(checkpoint_file, "rb") as f:
            data = pickle.load(f)
            completed = data.get("completed", set())
            failed = data.get("failed", set())
            timestamp = data.get("timestamp", "알 수 없음")
        
        print(f"\n[체크포인트 정보]")
        print(f"  마지막 업데이트: {timestamp}")
        print(f"  완료된 작업: {len(completed):,}건")
        print(f"  실패한 작업: {len(failed):,}건")
        
        if completed:
            # 날짜별 통계
            by_date = {}
            for code, date in completed:
                by_date.setdefault(date, []).append(code)
            
            print(f"\n[날짜별 완료 현황]")
            for date in sorted(by_date.keys()):
                print(f"  {date}: {len(by_date[date]):,}개 종목")
    except Exception as e:
        print(f"[체크포인트 로드 실패] {e}")
else:
    print("\n[체크포인트 없음] 아직 시작하지 않았거나 진행 중입니다.")

# CSV 파일 확인
print(f"\n[저장된 CSV 파일]")
csv_files = sorted(out_dir.glob("raw_v52_*.csv"))
if csv_files:
    for csv_file in csv_files:
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            date = csv_file.stem.replace("raw_v52_", "")
            print(f"  {date}: {len(df):,}행")
        except:
            print(f"  {csv_file.name}: 읽기 실패")
else:
    print("  저장된 파일 없음")

print("\n" + "=" * 80)




