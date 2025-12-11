# -*- coding: utf-8 -*-
import math
from typing import Any, Dict, List, Tuple

def is_valid(val: Any) -> bool:
    """빈값/NaN을 제외하고 0은 허용"""
    if val is None:
        return False
    if isinstance(val, str):
        return bool(val.strip())
    if isinstance(val, (list, dict)) and not val:
        return False
    if isinstance(val, (int, float)):
        try:
            if math.isnan(val) or math.isinf(val):
                return False
        except Exception:
            pass
        return True
    try:
        # pandas/np 호환
        import pandas as pd  # type: ignore
        if hasattr(val, "size") and val.size == 0:
            return False
        if pd.isna(val):  # type: ignore[attr-defined]
            return False
    except Exception:
        pass
    return True


def merge_sources(prioritized_sources: List[str], payloads: Dict[str, Dict[str, Any]]) -> Dict[str, List[Tuple[str, Any]]]:
    """
    소스 우선순위에 따라 필드를 병합하면서, 서로 다른 소스는 모두 보존한다.
    반환: {field: [(src, value), ...]}
    """
    merged: Dict[str, List[Tuple[str, Any]]] = {}
    for src in prioritized_sources:
        data = payloads.get(src) or {}
        for k, v in data.items():
            if not is_valid(v):
                continue
            merged.setdefault(k, [])
            # 같은 소스 중복 제거
            if any(s == src for s, _ in merged[k]):
                continue
            merged[k].append((src, v))
    return merged

