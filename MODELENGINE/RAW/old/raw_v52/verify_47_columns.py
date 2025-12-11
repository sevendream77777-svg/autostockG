#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""47개 확보된 컬럼 데이터 정확성 검증"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "MODELENGINE" / "RAW" / "raw_v52"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

from v52_collector import V52Collector, CollectConfig, V52_COLS
from pykrx import stock
import requests
from bs4 import BeautifulSoup

# 미확보 컬럼 (검증 제외)
EXCLUDED = {"short_sell_amt", "short_sell_qty", "loan_balance_amt", "loan_balance_qty", "ex_div_date"}

def get_naver_finance_price(code: str, date: str) -> Optional[Dict]:
    """네이버 금융에서 가격 데이터 가져오기 (검증용)"""
    try:
        # 날짜를 YYYYMMDD에서 YYYY.MM.DD로 변환
        date_str = f"{date[:4]}.{date[4:6]}.{date[6:8]}"
        url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page=1"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 첫 번째 행에서 데이터 추출
        rows = soup.select("table.type_1 tr")
        for row in rows[1:6]:  # 헤더 제외
            tds = row.select("td")
            if len(tds) >= 6:
                try:
                    row_date = tds[0].text.strip().replace(".", "")
                    if row_date == date:
                        return {
                            "close": float(tds[1].text.replace(",", "")),
                            "change": tds[2].text.strip(),
                            "open": float(tds[3].text.replace(",", "")),
                            "high": float(tds[4].text.replace(",", "")),
                            "low": float(tds[5].text.replace(",", "")),
                            "volume": float(tds[6].text.replace(",", "")) if len(tds) > 6 else None,
                        }
                except:
                    continue
    except Exception as e:
        pass
    return None

def verify_basic(row: Dict, code: str, date: str) -> List[Tuple[str, str, bool]]:
    """Basic 컬럼 검증"""
    results = []
    
    # date
    results.append(("date", f"값: {row.get('date')}", row.get("date") == date))
    
    # code
    results.append(("code", f"값: {row.get('code')}", row.get("code") == code.zfill(6)))
    
    # name
    name = row.get("name")
    results.append(("name", f"값: {name}", name is not None and len(name) > 0))
    
    # market
    market = row.get("market")
    results.append(("market", f"값: {market}", market in ["KOSPI", "KOSDAQ", "KONEX"]))
    
    # listing_status
    status = row.get("listing_status")
    results.append(("listing_status", f"값: {status}", status == "Listed"))
    
    # sector_code
    sector_code = row.get("sector_code")
    results.append(("sector_code", f"값: {sector_code}", sector_code is not None and isinstance(sector_code, (int, str))))
    
    # sector_name
    sector_name = row.get("sector_name")
    results.append(("sector_name", f"값: {sector_name}", sector_name is not None and len(str(sector_name)) > 0))
    
    return results

def verify_price_liquidity(row: Dict, code: str, date: str) -> List[Tuple[str, str, bool]]:
    """Price & Liquidity 컬럼 검증"""
    results = []
    
    # PyKRX로 직접 확인
    try:
        df = stock.get_market_ohlcv_by_date(date, date, code)
        if not df.empty:
            pykrx_row = df.iloc[0]
            pykrx_close = float(pykrx_row["종가"])
            pykrx_open = float(pykrx_row["시가"])
            pykrx_high = float(pykrx_row["고가"])
            pykrx_low = float(pykrx_row["저가"])
            pykrx_volume = float(pykrx_row["거래량"])
            pykrx_amount = float(pykrx_row["거래대금"])
        else:
            pykrx_row = None
    except:
        pykrx_row = None
    
    # open
    open_val = row.get("open")
    if pykrx_row:
        results.append(("open", f"값: {open_val}, PyKRX: {pykrx_open}", abs(open_val - pykrx_open) < 0.01))
    else:
        results.append(("open", f"값: {open_val}", open_val is not None and open_val > 0))
    
    # high
    high_val = row.get("high")
    if pykrx_row:
        results.append(("high", f"값: {high_val}, PyKRX: {pykrx_high}", abs(high_val - pykrx_high) < 0.01))
    else:
        results.append(("high", f"값: {high_val}", high_val is not None and high_val > 0))
    
    # low
    low_val = row.get("low")
    if pykrx_row:
        results.append(("low", f"값: {low_val}, PyKRX: {pykrx_low}", abs(low_val - pykrx_low) < 0.01))
    else:
        results.append(("low", f"값: {low_val}", low_val is not None and low_val > 0))
    
    # close
    close_val = row.get("close")
    if pykrx_row:
        results.append(("close", f"값: {close_val}, PyKRX: {pykrx_close}", abs(close_val - pykrx_close) < 0.01))
    else:
        results.append(("close", f"값: {close_val}", close_val is not None and close_val > 0))
    
    # volume
    volume_val = row.get("volume")
    if pykrx_row:
        results.append(("volume", f"값: {volume_val}, PyKRX: {pykrx_volume}", abs(volume_val - pykrx_volume) < 100))
    else:
        results.append(("volume", f"값: {volume_val}", volume_val is not None and volume_val >= 0))
    
    # amount
    amount_val = row.get("amount")
    if pykrx_row:
        results.append(("amount", f"값: {amount_val}, PyKRX: {pykrx_amount}", abs(amount_val - pykrx_amount) < 1000))
    else:
        results.append(("amount", f"값: {amount_val}", amount_val is not None and amount_val >= 0))
    
    # adj_factor
    adj = row.get("adj_factor")
    results.append(("adj_factor", f"값: {adj}", adj is not None and adj > 0))
    
    # vwap (거래대금/거래량)
    vwap = row.get("vwap")
    if amount_val and volume_val and volume_val > 0:
        expected_vwap = amount_val / volume_val
        results.append(("vwap", f"값: {vwap}, 계산값: {expected_vwap:.2f}", abs(vwap - expected_vwap) < 0.01 if vwap else False))
    else:
        results.append(("vwap", f"값: {vwap}", vwap is None or vwap >= 0))
    
    # market_cap
    market_cap = row.get("market_cap")
    results.append(("market_cap", f"값: {market_cap}", market_cap is None or market_cap > 0))
    
    # shares_out
    shares_out = row.get("shares_out")
    results.append(("shares_out", f"값: {shares_out}", shares_out is None or shares_out > 0))
    
    # 가격 일관성 검증
    if open_val and high_val and low_val and close_val:
        results.append(("가격일관성", "high >= low, high >= open, high >= close, low <= open, low <= close",
                       high_val >= low_val and high_val >= open_val and high_val >= close_val and 
                       low_val <= open_val and low_val <= close_val))
    
    return results

def verify_flow(row: Dict, code: str, date: str) -> List[Tuple[str, str, bool]]:
    """Flow 컬럼 검증"""
    results = []
    
    # PyKRX로 직접 확인
    try:
        df = stock.get_market_trading_value_by_date(date, date, code)
        if not df.empty:
            pykrx_row = df.iloc[0]
            pykrx_frgn = float(pykrx_row["외국인"])
            pykrx_inst = float(pykrx_row["기관"])
        else:
            pykrx_row = None
    except:
        pykrx_row = None
    
    # frgn_net_amt
    frgn_amt = row.get("frgn_net_amt")
    if pykrx_row:
        results.append(("frgn_net_amt", f"값: {frgn_amt}, PyKRX: {pykrx_frgn}", abs(frgn_amt - pykrx_frgn) < 1000))
    else:
        results.append(("frgn_net_amt", f"값: {frgn_amt}", frgn_amt is None or isinstance(frgn_amt, (int, float))))
    
    # inst_net_amt
    inst_amt = row.get("inst_net_amt")
    if pykrx_row:
        results.append(("inst_net_amt", f"값: {inst_amt}, PyKRX: {pykrx_inst}", abs(inst_amt - pykrx_inst) < 1000))
    else:
        results.append(("inst_net_amt", f"값: {inst_amt}", inst_amt is None or isinstance(inst_amt, (int, float))))
    
    # nps_net_amt
    nps_amt = row.get("nps_net_amt")
    results.append(("nps_net_amt", f"값: {nps_amt}", nps_amt is None or isinstance(nps_amt, (int, float))))
    
    # tust_net_amt
    tust_amt = row.get("tust_net_amt")
    results.append(("tust_net_amt", f"값: {tust_amt}", tust_amt is None or isinstance(tust_amt, (int, float))))
    
    # dealer_net_amt
    dealer_amt = row.get("dealer_net_amt")
    results.append(("dealer_net_amt", f"값: {dealer_amt}", dealer_amt is None or isinstance(dealer_amt, (int, float))))
    
    # frgn_net_qty
    frgn_qty = row.get("frgn_net_qty")
    results.append(("frgn_net_qty", f"값: {frgn_qty}", frgn_qty is None or isinstance(frgn_qty, (int, float))))
    
    # inst_net_qty
    inst_qty = row.get("inst_net_qty")
    results.append(("inst_net_qty", f"값: {inst_qty}", inst_qty is None or isinstance(inst_qty, (int, float))))
    
    # nps_net_qty
    nps_qty = row.get("nps_net_qty")
    results.append(("nps_net_qty", f"값: {nps_qty}", nps_qty is None or isinstance(nps_qty, (int, float))))
    
    return results

def verify_finance(row: Dict, code: str, date: str) -> List[Tuple[str, str, bool]]:
    """Finance 컬럼 검증"""
    results = []
    
    # announce_date
    announce = row.get("announce_date")
    results.append(("announce_date", f"값: {announce}", announce is None or (isinstance(announce, str) and len(announce) == 8)))
    
    # revenue
    revenue = row.get("revenue")
    results.append(("revenue", f"값: {revenue}", revenue is None or (isinstance(revenue, (int, float)) and revenue >= 0)))
    
    # op_income
    op_income = row.get("op_income")
    results.append(("op_income", f"값: {op_income}", op_income is None or isinstance(op_income, (int, float))))
    
    # net_income
    net_income = row.get("net_income")
    results.append(("net_income", f"값: {net_income}", net_income is None or isinstance(net_income, (int, float))))
    
    # total_equity
    equity = row.get("total_equity")
    results.append(("total_equity", f"값: {equity}", equity is None or (isinstance(equity, (int, float)) and equity > 0)))
    
    # total_assets
    assets = row.get("total_assets")
    results.append(("total_assets", f"값: {assets}", assets is None or (isinstance(assets, (int, float)) and assets > 0)))
    
    # cash_flow_op
    cf_op = row.get("cash_flow_op")
    results.append(("cash_flow_op", f"값: {cf_op}", cf_op is None or isinstance(cf_op, (int, float))))
    
    # cash_flow_inv
    cf_inv = row.get("cash_flow_inv")
    results.append(("cash_flow_inv", f"값: {cf_inv}", cf_inv is None or isinstance(cf_inv, (int, float))))
    
    # cash_flow_fin
    cf_fin = row.get("cash_flow_fin")
    results.append(("cash_flow_fin", f"값: {cf_fin}", cf_fin is None or isinstance(cf_fin, (int, float))))
    
    # div_amount
    div = row.get("div_amount")
    results.append(("div_amount", f"값: {div}", div is None or (isinstance(div, (int, float)) and div >= 0)))
    
    # eps
    eps = row.get("eps")
    results.append(("eps", f"값: {eps}", eps is None or isinstance(eps, (int, float))))
    
    # roe
    roe = row.get("roe")
    results.append(("roe", f"값: {roe}", roe is None or isinstance(roe, (int, float))))
    
    # bps
    bps = row.get("bps")
    results.append(("bps", f"값: {bps}", bps is None or (isinstance(bps, (int, float)) and bps > 0)))
    
    # debt_ratio
    debt_ratio = row.get("debt_ratio")
    results.append(("debt_ratio", f"값: {debt_ratio}", debt_ratio is None or (isinstance(debt_ratio, (int, float)) and debt_ratio >= 0)))
    
    # 재무 일관성 검증
    if equity and assets and assets > 0:
        expected_debt_ratio = ((assets - equity) / equity * 100) if equity > 0 else None
        if expected_debt_ratio and debt_ratio:
            results.append(("재무일관성", f"debt_ratio 계산값: {expected_debt_ratio:.2f}%, 실제: {debt_ratio:.2f}%", 
                           abs(debt_ratio - expected_debt_ratio) < 5))
    
    return results

def verify_macro(row: Dict, code: str, date: str) -> List[Tuple[str, str, bool]]:
    """Macro 컬럼 검증"""
    results = []
    
    # usdkrw (원/달러)
    usdkrw = row.get("usdkrw")
    results.append(("usdkrw", f"값: {usdkrw}", usdkrw is None or (isinstance(usdkrw, (int, float)) and 1000 < usdkrw < 2000)))
    
    # us10y_yield (미국 10년 국채)
    us10y = row.get("us10y_yield")
    results.append(("us10y_yield", f"값: {us10y}", us10y is None or (isinstance(us10y, (int, float)) and 0 < us10y < 20)))
    
    # kr10y_yield (한국 10년 국채)
    kr10y = row.get("kr10y_yield")
    results.append(("kr10y_yield", f"값: {kr10y}", kr10y is None or (isinstance(kr10y, (int, float)) and 0 < kr10y < 20)))
    
    # wti (원유)
    wti = row.get("wti")
    results.append(("wti", f"값: {wti}", wti is None or (isinstance(wti, (int, float)) and 0 < wti < 200)))
    
    # dxy (달러 인덱스)
    dxy = row.get("dxy")
    results.append(("dxy", f"값: {dxy}", dxy is None or (isinstance(dxy, (int, float)) and 50 < dxy < 150)))
    
    # cnykrw (위안/원)
    cnykrw = row.get("cnykrw")
    results.append(("cnykrw", f"값: {cnykrw}", cnykrw is None or (isinstance(cnykrw, (int, float)) and 100 < cnykrw < 300)))
    
    # gold (금)
    gold = row.get("gold")
    results.append(("gold", f"값: {gold}", gold is None or (isinstance(gold, (int, float)) and 1000 < gold < 5000)))
    
    # earnings_date
    earnings = row.get("earnings_date")
    results.append(("earnings_date", f"값: {earnings}", earnings is None or (isinstance(earnings, str) and len(earnings) == 8)))
    
    return results

def verify_one_date(code: str, date: str):
    """단일 날짜 검증"""
    print("\n" + "=" * 80)
    print(f"47개 컬럼 데이터 정확성 검증 - 날짜: {date}")
    print("=" * 80)
    
    # 데이터 수집
    collector = V52Collector(CollectConfig(code=code, date=date))
    row = collector.run()
    
    all_results = []
    
    # 카테고리별 검증
    print("\n[1] Basic 컬럼 검증 (7개)")
    print("-" * 80)
    basic_results = verify_basic(row, code, date)
    all_results.extend(basic_results)
    for col, info, ok in basic_results:
        status = "✅" if ok else "❌"
        print(f"{status} {col:20s} {info}")
    
    print("\n[2] Price & Liquidity 컬럼 검증 (10개)")
    print("-" * 80)
    price_results = verify_price_liquidity(row, code, date)
    all_results.extend(price_results)
    for col, info, ok in price_results:
        status = "✅" if ok else "❌"
        print(f"{status} {col:20s} {info}")
    
    print("\n[3] Flow 컬럼 검증 (9개)")
    print("-" * 80)
    flow_results = verify_flow(row, code, date)
    all_results.extend(flow_results)
    for col, info, ok in flow_results:
        status = "✅" if ok else "❌"
        print(f"{status} {col:20s} {info}")
    
    print("\n[4] Finance 컬럼 검증 (14개)")
    print("-" * 80)
    finance_results = verify_finance(row, code, date)
    all_results.extend(finance_results)
    for col, info, ok in finance_results:
        status = "✅" if ok else "❌"
        print(f"{status} {col:20s} {info}")
    
    print("\n[5] Macro & Event 컬럼 검증 (8개)")
    print("-" * 80)
    macro_results = verify_macro(row, code, date)
    all_results.extend(macro_results)
    for col, info, ok in macro_results:
        status = "✅" if ok else "❌"
        print(f"{status} {col:20s} {info}")
    
    # 최종 요약
    print("\n" + "=" * 80)
    print(f"검증 결과 요약 - 날짜: {date}")
    print("=" * 80)
    
    total = len(all_results)
    passed = sum(1 for _, _, ok in all_results if ok)
    failed = total - passed
    
    print(f"전체 검증 항목: {total}개")
    print(f"✅ 통과: {passed}개 ({passed/total*100:.1f}%)")
    print(f"❌ 실패: {failed}개 ({failed/total*100:.1f}%)")
    
    if failed > 0:
        print("\n❌ 실패한 항목:")
        for col, info, ok in all_results:
            if not ok:
                print(f"  - {col}: {info}")
    
    return passed, total, failed

def main():
    code = "005930"  # 삼성전자
    dates = ["20251205", "20150105"]  # 최근 날짜 + 과거 날짜
    
    print("=" * 80)
    print("47개 컬럼 데이터 정확성 종합 검증")
    print(f"종목: {code} (삼성전자)")
    print("=" * 80)
    
    all_passed = 0
    all_total = 0
    all_failed = 0
    
    for date in dates:
        passed, total, failed = verify_one_date(code, date)
        all_passed += passed
        all_total += total
        all_failed += failed
    
    # 전체 요약
    print("\n" + "=" * 80)
    print("전체 검증 결과 종합")
    print("=" * 80)
    print(f"검증 날짜: {len(dates)}개")
    print(f"전체 검증 항목: {all_total}개")
    print(f"✅ 통과: {all_passed}개 ({all_passed/all_total*100:.1f}%)")
    print(f"❌ 실패: {all_failed}개 ({all_failed/all_total*100:.1f}%)")
    print("=" * 80)
    
    if all_failed == 0:
        print("✅ 모든 컬럼 데이터가 정확합니다!")
    else:
        print(f"⚠️ {all_failed}개 항목에 문제가 있습니다.")
    print("=" * 80)

if __name__ == "__main__":
    main()

