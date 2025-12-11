# -*- coding: utf-8 -*-
"""
Naver/기타 웹 크롤링 결과 전담 추출기
- V52Collector 내 매크로/배당/섹터 관련 필드 슬라이스
"""

from typing import Any, Dict
from ui.sources.common.schema import MACRO_KEYS, SECTOR_KEYS, EVENT_KEYS


def slice_naver(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not payload:
        return {}
    keys = MACRO_KEYS | SECTOR_KEYS | EVENT_KEYS
    return {k: payload.get(k) for k in keys if k in payload}

