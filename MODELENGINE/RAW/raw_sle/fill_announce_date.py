# -*- coding: utf-8 -*-
"""
수집 후 announce_date를 list.json 캐시로 채우는 후처리 스크립트
- 입력 CSV: 기본 MODELENGINE/RAW/raw_sle/out/csv/*.csv (announce_date 비어있어도 무방)
- 캐시: MODELENGINE/RAW/raw_sle/out/list/by_corp/{stock}_{corp}.json
- 기본 동작: out_dir에 같은 이름에 '_ann' 접미사 붙여 저장. --in-place 옵션 시 원본 덮어쓰기.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from MODELENGINE.RAW.raw_sle.run_raw_sle import (
    _select_announce_date_from_rows,
    load_list_cache,
)


def fill_file(csv_path: Path, cache_dir: Path, out_dir: Path, in_place: bool, suffix: str) -> Tuple[Path, int, int]:
    df = pd.read_csv(csv_path, dtype={"code": str, "corp_code": str, "reprt_code": str, "bsns_year": str, "announce_date": str})
    if df.empty:
        return csv_path, 0, 0

    # 캐시 메모: (code, corp_code) -> rows
    cache_map: Dict[Tuple[str, str], list] = {}

    filled = 0
    total = len(df)
    for idx, row in df.iterrows():
        code = str(row.get("code") or "").zfill(6)
        corp = str(row.get("corp_code") or "")
        reprt_code = str(row.get("reprt_code") or "")
        try:
            year = int(str(row.get("bsns_year") or "0"))
        except Exception:
            year = None
        if not (code and corp and reprt_code and year):
            continue

        key = (code, corp)
        rows = cache_map.get(key)
        if rows is None:
            rows = load_list_cache(code, corp, cache_dir)
            cache_map[key] = rows
        if not rows:
            continue

        ann = _select_announce_date_from_rows(rows, year, reprt_code)
        if ann:
            df.at[idx, "announce_date"] = ann
            filled += 1

    out_dir.mkdir(parents=True, exist_ok=True)
    if in_place:
        out_path = csv_path
    else:
        out_path = out_dir / f"{csv_path.stem}{suffix}{csv_path.suffix}"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path, filled, total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, default=str(Path(__file__).resolve().parent / "out" / "csv"))
    parser.add_argument("--list-cache", type=str, default=str(Path(__file__).resolve().parent / "out" / "list" / "by_corp"))
    parser.add_argument("--out-dir", type=str, default=None, help="지정 없으면 csv-dir 사용")
    parser.add_argument("--in-place", action="store_true", help="원본 덮어쓰기")
    parser.add_argument("--suffix", type=str, default="_ann", help="in-place 아닐 때 파일명 접미사")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    cache_dir = Path(args.list_cache)
    out_dir = Path(args.out_dir) if args.out_dir else csv_dir

    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        print("[WARN] CSV 파일이 없습니다.")
        return

    print(f"[INFO] 대상 CSV {len(csv_files)}개, cache_dir={cache_dir}")

    total_filled = 0
    total_rows = 0
    for f in csv_files:
        out_path, filled, rows = fill_file(f, cache_dir, out_dir, args.in_place, args.suffix)
        total_filled += filled
        total_rows += rows
        print(f"[DONE] {f.name} -> {out_path.name} filled={filled}/{rows}")

    print(f"[SUMMARY] filled={total_filled}/{total_rows}")


if __name__ == "__main__":
    main()







