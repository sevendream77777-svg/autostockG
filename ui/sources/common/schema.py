# -*- coding: utf-8 -*-
"""
공통 컬럼 정의/그룹핑
- V58 (p0 기준 58개)
- V52 (47db 하이브리드 추가 8개)
- 8db_final 등에서 사용된 필드까지 포함한 전체 66개 세트
"""

V58_COLUMNS = [
    # 1. Price (12)
    "date", "code", "name", "market", "open", "high", "low", "close",
    "volume", "amount", "adj_factor", "vwap",
    # 2. Flow (12)
    "inst_net_qty", "inst_net_amt", "frgn_net_qty", "frgn_net_amt",
    "nps_net_qty", "nps_net_amt", "dealer_net_qty", "dealer_net_amt",
    "short_sell_qty", "short_sell_amt", "loan_balance_qty", "loan_balance_amt",
    # 3. Finance (11)
    "revenue", "op_income", "net_income", "eps", "bps", "roe", "roa",
    "debt_ratio", "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    # 4. Sector/Theme (5)
    "sector_code", "sector_name", "theme_code", "theme_name", "sector_index_close",
    # 5. Macro (8)
    "usdkrw", "cnykrw", "dxy", "us10y_yield", "kr10y_yield", "wti", "gold", "vix",
    # 6. Event (10)
    "earnings_announce_date", "earnings_surprise", "earnings_effective_date",
    "ex_div_date", "div_amount",
    "split_announce_date", "split_effective_date",
    "rights_issue_announce_date", "rights_issue_effective_date",
    "mna_announce_date",
]

# V52 Hybrid 전용 추가 필드
V52_EXTRA_COLUMNS = [
    "listing_status", "market_cap", "shares_out", "tust_net_amt",
    "announce_date", "total_equity", "total_assets", "earnings_date",
]

# 8db_final이 강제 확보했던 필드는 모두 V58/V52에 포함되어 있음(개수 보존 목적)
# 전체 컬럼(66개) - 순서는 V58 → V52 추가
ALL_COLUMNS = V58_COLUMNS + [c for c in V52_EXTRA_COLUMNS if c not in V58_COLUMNS]

# 소스별 기본 우선순위(앞쪽이 낮음, 뒤가 덮어씀)
SOURCE_PRIORITY = [
    "Kiwoom",   # 강제수집 8개 + 시총/주식수
    "PyKRX",    # 가격/수급
    "V58Light", # 경량 V58 (p0_light_collector) - 재무/매크로/섹터 백업
    "Naver",    # 매크로/백업
    "FDR",      # 매크로 백업
    "FnGuide",  # 섹터/테마
    "DART",     # 재무
    "Yahoo",    # 이벤트 백업
]

PYKRX_KEYS = {
    "date", "code", "name", "market", "listing_status",
    "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap",
    "inst_net_qty", "inst_net_amt", "frgn_net_qty", "frgn_net_amt",
    "nps_net_qty", "nps_net_amt", "tust_net_amt",
    "dealer_net_qty", "dealer_net_amt",
    "short_sell_qty", "short_sell_amt", "loan_balance_qty", "loan_balance_amt",
    "market_cap", "shares_out",
}

FINANCE_KEYS = {
    "revenue", "op_income", "net_income", "eps", "bps", "roe", "roa",
    "debt_ratio", "cash_flow_op", "cash_flow_inv", "cash_flow_fin",
    "total_equity", "total_assets",
}

MACRO_KEYS = {
    "usdkrw", "cnykrw", "dxy", "us10y_yield", "kr10y_yield", "wti", "gold", "vix",
}

SECTOR_KEYS = {"sector_code", "sector_name", "theme_code", "theme_name", "sector_index_close"}

EVENT_KEYS = {
    "earnings_announce_date", "earnings_surprise", "earnings_effective_date",
    "earnings_date",
    "ex_div_date", "div_amount",
    "split_announce_date", "split_effective_date",
    "rights_issue_announce_date", "rights_issue_effective_date",
    "mna_announce_date",
    "announce_date",
}
