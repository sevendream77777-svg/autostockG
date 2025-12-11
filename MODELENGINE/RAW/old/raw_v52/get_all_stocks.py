#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""전체 상장 종목 리스트 가져오기"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from pykrx import stock

def get_all_stocks(date: str = None) -> list:
    """전체 상장 종목 리스트 가져오기"""
    if date is None:
        from datetime import datetime
        date = datetime.now().strftime("%Y%m%d")
    
    # KOSPI + KOSDAQ
    kospi = stock.get_market_ticker_list(date, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(date, market="KOSDAQ")
    
    all_stocks = sorted(set(kospi + kosdaq))
    return all_stocks

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="기준일 (YYYYMMDD)")
    parser.add_argument("--output", type=str, default="stocks.txt", help="출력 파일")
    args = parser.parse_args()
    
    print("전체 종목 리스트 가져오는 중...")
    stocks = get_all_stocks(args.date)
    
    output_path = Path(args.output)
    with open(output_path, "w", encoding="utf-8") as f:
        for code in stocks:
            f.write(f"{code}\n")
    
    print(f"총 {len(stocks):,}개 종목을 {output_path}에 저장했습니다.")




