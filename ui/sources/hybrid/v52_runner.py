# -*- coding: utf-8 -*-
"""
기존 47db 하이브리드 수집기 실행 래퍼
- 파일명이 숫자로 시작해 직접 import가 어려워 importlib로 로딩
"""

import importlib.util
import pathlib
from typing import Any, Dict

_V52_PATH = pathlib.Path(__file__).resolve().parents[2] / "pages" / "p0_index" / "47db.py"


def _load_v52_module():
    spec = importlib.util.spec_from_file_location("v52_collector_module", _V52_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load V52 collector from {_V52_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


def collect_v52(code: str, date: str) -> Dict[str, Any]:
    """
    V52Collector 실행해 full payload 반환
    """
    mod = _load_v52_module()
    cfg_cls = getattr(mod, "Cfg")
    collector_cls = getattr(mod, "V52Collector")
    cfg = cfg_cls(code=code, date=date, log_path=None, out_json=None)
    collector = collector_cls(cfg)
    return collector.run()
