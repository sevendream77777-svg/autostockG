# -*- coding: utf-8 -*-
"""
8db_final.py 기반 Kiwoom 강제수집 8개 필드
"""
import importlib.util
import pathlib
from typing import Any, Dict, Optional

from ui.sources.common.schema import ALL_COLUMNS

_EIGHT_DB_PATH = pathlib.Path(__file__).resolve().parents[2] / "pages" / "p0_index" / "8db_final.py"


def _load_eight_db():
    spec = importlib.util.spec_from_file_location("eight_db_module", _EIGHT_DB_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load 8db_final from {_EIGHT_DB_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


def collect_kiwoom(code: str) -> Dict[str, Any]:
    """
    - kiwoom REST 토큰 발급 실패 시, 네이버/백업을 통해 최대 확보
    - 8db_final에 정의된 fetch_xx 함수 재사용
    """
    mod = _load_eight_db()
    out: Dict[str, Any] = {}

    # 필수 매핑: name/sector_code/sector_name/market_cap/shares_out/div_amount/kr10y_yield/dxy
    try:
        cfg = {}
        # Kiwoom REST
        appkey = getattr(mod, "os", None) and getattr(mod, "os").environ.get("KIWOOM_APPKEY")
        secret = getattr(mod, "os", None) and getattr(mod, "os").environ.get("KIWOOM_SECRET")
        if appkey and secret:
            try:
                rest = mod.KiwoomREST(appkey, secret)
                if rest.issue_token():
                    resp = rest.ka10001(code)
                    if isinstance(resp, dict):
                        out["market_cap"] = resp.get("tot_mv") or resp.get("tot_mktcap")
                        out["shares_out"] = resp.get("stk_vol") or resp.get("listed_shrs")
                        out["sector_code"] = resp.get("ind_cd") or resp.get("sector_cd")
                        out["sector_name"] = resp.get("ind_nm") or resp.get("sector_nm")
                        out["name"] = resp.get("stk_nm") or resp.get("itm_nm")
            except Exception:
                pass
    except Exception:
        pass

    # 네이버/기타 백업
    try:
        if "div_amount" not in out or out.get("div_amount") is None:
            out["div_amount"] = mod.fetch_div_amount(code)
    except Exception:
        pass

    try:
        out.setdefault("kr10y_yield", mod.fetch_kr10y_yield())
    except Exception:
        pass

    try:
        if "dxy" not in out or out.get("dxy") is None:
            out["dxy"] = mod.fetch_dxy()
    except Exception:
        pass

    # Naver HTML fallback for shares_out/market_cap/sector_code/name
    try:
        import requests, re

        html = requests.get(
            f"https://finance.naver.com/item/main.naver?code={code}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        ).text
        m_sh = re.search(r"상장주식수[^0-9]*([0-9,]+)", html)
        if m_sh:
            out.setdefault("shares_out", mod._to_float(m_sh.group(1)))
        m_mc = re.search(r"시가총액[^0-9]*([0-9,]+)", html)
        if m_mc:
            out.setdefault("market_cap", mod._to_float(m_mc.group(1)))
        m_sector = re.search(r"sectorCode=([0-9]+)", html)
        if m_sector:
            out.setdefault("sector_code", m_sector.group(1))
        if "name" not in out or not out.get("name"):
            m_nm = re.search(r"stockName\\s*=\\s*'([^']+)'", html)
            if m_nm:
                out["name"] = m_nm.group(1)
    except Exception:
        pass

    # 필드가 ALL_COLUMNS에 없으면 제거
    return {k: v for k, v in out.items() if k in ALL_COLUMNS}
