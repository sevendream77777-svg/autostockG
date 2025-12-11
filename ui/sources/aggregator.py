# -*- coding: utf-8 -*-
"""
소스별 수집 → 필드별 병합 → p0 UI 전달
- V52Collector 결과를 PyKRX/DART/Naver로 분리
- 8db_final로 Kiwoom 강제 필드 추가
- Yahoo Finance 백업
"""
from typing import Any, Dict, List, Tuple

from ui.sources.common.schema import (
    ALL_COLUMNS,
    SOURCE_PRIORITY,
    FINANCE_KEYS,
    MACRO_KEYS,
    SECTOR_KEYS,
    EVENT_KEYS,
)
from ui.sources.common.utils import is_valid, merge_sources
from ui.sources.hybrid.v52_runner import collect_v52
from ui.sources.light.v58_runner import collect_v58_light
from ui.sources.pykrx.collector import slice_pykrx
from ui.sources.dart.collector import slice_dart
from ui.sources.naver.collector import slice_naver
from ui.sources.kiwoom.collector import collect_kiwoom
from ui.sources.yahoo.collector import collect_yahoo
from ui.sources.fdr.collector import fetch_fdr_macro
from ui.sources.fnguide.collector import fetch_fnguide


def collect_all(code: str, date: str) -> Dict[str, Any]:
    """
    소스별 raw + 필드 병합 결과를 반환
    return {
        "by_source": {src: payload},
        "by_field": {field: [(src, val), ...]},
    }
    """
    by_source: Dict[str, Dict[str, Any]] = {}

    # 1) V52 하이브리드 한 번 실행
    v52_payload = collect_v52(code, date)
    if v52_payload:
        by_source["PyKRX"] = slice_pykrx(v52_payload)
        by_source["DART"] = slice_dart(v52_payload)
        # V52의 매크로/섹터/이벤트 파트는 네이버/FDR/기타 웹 크롤링이 섞여 있음
        by_source["Naver"] = slice_naver(v52_payload)

    # 1-1) V58 경량 (p0_light) - 재무/섹터/매크로 백업
    v58_payload = collect_v58_light(code, date)
    if v58_payload:
        by_source["V58Light"] = v58_payload

    # 2) Kiwoom (8db_final)
    by_source["Kiwoom"] = collect_kiwoom(code)

    # 2-1) FnGuide (섹터 스냅샷)
    by_source["FnGuide"] = fetch_fnguide(code)

    # 2-2) FDR (매크로 백업)
    by_source["FDR"] = fetch_fdr_macro(date)

    # 3) Yahoo 백업
    by_source["Yahoo"] = collect_yahoo(code)

    # 4) 병합 (소스별 값 모두 보존)
    merged = merge_sources(SOURCE_PRIORITY, by_source)

    # 5) 누락 필드도 Key만 생성해 UI에서 빈칸으로 표시되게 함
    for col in ALL_COLUMNS:
        merged.setdefault(col, [])

    return {"by_source": by_source, "by_field": merged}


def collapse_field(values: List[Tuple[str, Any]]) -> str:
    """여러 소스 값을 `[SRC] value` 형태로 연결"""
    if not values:
        return ""
    parts = []
    for src, val in values:
        if not is_valid(val):
            continue
        parts.append(f"[{src}] {val}")
    return " | ".join(parts)
