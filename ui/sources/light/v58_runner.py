# -*- coding: utf-8 -*-
"""
p0_light_collector.py 래퍼
- DataCollector(code, date).run() 결과를 그대로 반환
"""
import importlib.util
import pathlib
from typing import Any, Dict

_V58_PATH = pathlib.Path(__file__).resolve().parents[2] / "pages" / "p0_index" / "p0_light_collector.py"


def _load_v58_module():
    spec = importlib.util.spec_from_file_location("p0_light_module", _V58_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load p0_light_collector from {_V58_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[arg-type]
    return mod


def collect_v58_light(code: str, date: str) -> Dict[str, Any]:
    mod = _load_v58_module()
    collector_cls = getattr(mod, "DataCollector")
    collector = collector_cls(code, date)
    return collector.run()
