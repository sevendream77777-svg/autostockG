# -*- coding: utf-8 -*-
"""
Yahoo Finance 이벤트/백업 수집
"""
import datetime as dt
from typing import Any, Dict


def collect_yahoo(code: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    try:
        import yfinance as yf  # type: ignore
        import pandas as pd  # type: ignore
        ticker = code + ".KS"
        yf_obj = yf.Ticker(ticker)
        info = yf_obj.info
        if info:
            res["sector_name"] = info.get("sector", "") or info.get("industry", "")
            ex_div = info.get("exDividendDate", "")
            if isinstance(ex_div, int):
                ex_div = dt.datetime.fromtimestamp(ex_div).strftime("%Y%m%d")
            res["ex_div_date"] = ex_div
            res["debt_ratio"] = info.get("debtToEquity")
            res["roe"] = info.get("returnOnEquity")
            res["earnings_announce_date"] = info.get("earningsDate")
        try:
            ed = yf_obj.get_earnings_dates(limit=1)
            if isinstance(ed, pd.DataFrame) and not ed.empty:
                ts = ed.index[0]
                res["earnings_announce_date"] = ts.strftime("%Y%m%d")
        except Exception:
            pass
        try:
            splits = yf_obj.splits
            if hasattr(splits, "index") and len(splits) > 0:
                ts = splits.index[-1]
                res["split_effective_date"] = ts.strftime("%Y%m%d")
                res["split_announce_date"] = ts.strftime("%Y%m%d")
        except Exception:
            pass
    except Exception:
        pass
    return res
