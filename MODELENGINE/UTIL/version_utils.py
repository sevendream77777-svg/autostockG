
import os
import re
from pathlib import Path
from typing import Optional, List, Tuple
import pandas as pd

# ============================================================
# version_utils.py  —  Stable utilities (V31 policy)
#   - find_latest_file: pick latest file by internal or filename date
#   - save_dataframe_with_date: tag by internal max(Date), no overwrite, _1/_2 suffix
#   - versioned_filename: helper (kept for compatibility)
# ============================================================

_TAG_RE = re.compile(r'_(\d{6})(?:_\d+)?\.parquet$', re.IGNORECASE)

def _extract_date_tag_from_name(name: str) -> Optional[pd.Timestamp]:
    m = _TAG_RE.search(name)
    if not m:
        return None
    try:
        return pd.to_datetime(m.group(1), format="%y%m%d")
    except Exception:
        return None

def _max_date_from_parquet(path: Path, date_col: str = "Date") -> Optional[pd.Timestamp]:
    try:
        df = pd.read_parquet(path, columns=[date_col])
        s = pd.to_datetime(df[date_col], errors="coerce").dropna()
        return s.max() if not s.empty else None
    except Exception:
        return None

# ------------------------------------------------------------
# Public: find_latest_file
#   - scans dir for files starting with prefix and ending .parquet
#   - picks latest by internal max(Date) if available,
#     otherwise falls back to filename date tag,
#     finally falls back to mtime.
# ------------------------------------------------------------
def find_latest_file(dir_path, prefix: str, date_col: str = "Date") -> Optional[Path]:
    dir_p = Path(dir_path)
    if not dir_p.exists():
        return None

    candidates: List[Path] = [
        p for p in dir_p.iterdir()
        if p.is_file() and p.name.startswith(prefix) and p.suffix.lower() == ".parquet"
    ]
    if not candidates:
        return None

    scored: List[Tuple[pd.Timestamp, Path]] = []
    fallback_name: List[Tuple[pd.Timestamp, Path]] = []
    fallback_mtime: List[Tuple[float, Path]] = []

    for p in candidates:
        # Try internal date
        md = _max_date_from_parquet(p, date_col=date_col)
        if md is not None:
            scored.append((md, p))
            continue
        # Try filename tag
        dt = _extract_date_tag_from_name(p.name)
        if dt is not None:
            fallback_name.append((dt, p))
            continue
        # Fallback: mtime
        try:
            fallback_mtime.append((p.stat().st_mtime, p))
        except Exception:
            pass

    if scored:
        scored.sort(key=lambda x: (x[0], x[1].name))
        return scored[-1][1]
    if fallback_name:
        fallback_name.sort(key=lambda x: (x[0], x[1].name))
        return fallback_name[-1][1]
    if fallback_mtime:
        fallback_mtime.sort(key=lambda x: (x[0], x[1].name))
        return fallback_mtime[-1][1]
    return None

# ------------------------------------------------------------
# Public: save_dataframe_with_date
#   - Tag = internal max(Date) of df
#   - Skip if any existing file (same prefix) has internal max(Date) >= new
#   - Otherwise save with no overwrite; use _1, _2 ... suffix if needed
#   - Return saved path (str); return None if skipped
# ------------------------------------------------------------
def save_dataframe_with_date(
    df: pd.DataFrame,
    dir_path,
    prefix: str,
    date_col: str = "Date",
    ext: str = ".parquet"
) -> Optional[str]:
    dir_p = Path(dir_path)
    dir_p.mkdir(parents=True, exist_ok=True)

    # Determine new date from dataframe
    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if dates.empty:
        print(f"[WARN] {prefix}: no valid '{date_col}' to determine date tag; skip")
        return None
    new_date = dates.max().date()
    date_tag = pd.to_datetime(new_date).strftime("%y%m%d")

    # Scan existing files and read their internal dates
    existing: List[Path] = [
        p for p in dir_p.iterdir()
        if p.is_file() and p.name.startswith(prefix) and p.suffix.lower() == ext.lower()
    ]

    max_exist: Optional[pd.Timestamp] = None
    for p in existing:
        md = _max_date_from_parquet(p, date_col=date_col)
        if md is not None:
            if (max_exist is None) or (md > max_exist):
                max_exist = md

    # Skip if existing >= new
    if (max_exist is not None) and (max_exist.date() >= new_date):
        print(f"[SKIP] {prefix}: existing {max_exist.date()} >= new {new_date}")
        return None

    # Build output path with suffix increment (no overwrite)
    base = dir_p / f"{prefix}_{date_tag}{ext}"
    out = base
    idx = 1
    while out.exists():
        out = dir_p / f"{prefix}_{date_tag}_{idx}{ext}"
        idx += 1

    # Save
    df.to_parquet(out, index=False)
    return str(out)

# ------------------------------------------------------------
# Public: versioned_filename (compat helper)
# ------------------------------------------------------------
def versioned_filename(prefix: str, date_tag: str, idx: int = 0, ext: str = ".parquet") -> str:
    if idx and idx > 0:
        return f"{prefix}_{date_tag}_{idx}{ext}"
    return f"{prefix}_{date_tag}{ext}"

# ============================================================
# [추가] build_features.py 지원을 위한 데이터 로드 함수
# (raw_utils.py가 없어서 발생하는 에러를 여기서 처리)
# ============================================================
def load_raw_data(file_path):
    """
    주식 원본 데이터(RAW)를 로드합니다.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {path}")

    print(f"  [Loader] 읽는 중: {path.name}")
    try:
        if path.suffix == '.parquet':
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
            
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        return df
    except Exception as e:
        print(f"  ❌ 데이터 로드 실패: {e}")
        raise

def load_kospi_index(file_path):
    """
    KOSPI 지수 데이터를 로드합니다.
    """
    return load_raw_data(file_path)