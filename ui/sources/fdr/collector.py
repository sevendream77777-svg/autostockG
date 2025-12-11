# -*- coding: utf-8 -*-
"""
FinanceDataReader 매크로 수집 (VIX 포함)
"""
from typing import Any, Dict
import pandas as pd


def fetch_fdr_macro(base_dt: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    try:
        import FinanceDataReader as fdr  # type: ignore
        macro_map = {
            "usdkrw": "USD/KRW",
            "cnykrw": "CNY/KRW",
            "dxy": "DX-Y.NYB",
            "us10y_yield": "US10YT",
            "kr10y_yield": "KR10YT",
            "wti": "CL=F",
            "gold": "GC=F",
            "vix": "VIX",
        }
        start_dt = (pd.to_datetime(base_dt) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        end_dt = pd.to_datetime(base_dt).strftime("%Y-%m-%d")
        for key, sym in macro_map.items():
            try:
                df = fdr.DataReader(sym, start_dt, end_dt)
                if not df.empty:
                    df = df.fillna(method="ffill")
                    val = df.iloc[-1]["Close"]
                    res[key] = float(val)
            except Exception:
                pass
    except Exception:
        pass
    return res

