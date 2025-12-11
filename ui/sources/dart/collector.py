# -*- coding: utf-8 -*-
"""
DART 전담 추출기
- V52Collector 결과에서 재무 필드만 추출
"""

from typing import Any, Dict
from ui.sources.common.schema import FINANCE_KEYS


def slice_dart(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {}
    return {k: payload.get(k) for k in FINANCE_KEYS if k in payload}

