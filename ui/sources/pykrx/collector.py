# -*- coding: utf-8 -*-
"""
PyKRX 전담 추출기
- V52Collector 결과에서 시세/수급/시총 관련 필드만 슬라이스
"""

from typing import Any, Dict
from ui.sources.common.schema import PYKRX_KEYS


def slice_pykrx(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {}
    return {k: payload.get(k) for k in PYKRX_KEYS if k in payload}

