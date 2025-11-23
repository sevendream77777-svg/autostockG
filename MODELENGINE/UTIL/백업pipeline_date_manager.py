# pipeline_date_manager.py  (FM date-based controller)
# - Single source of truth for last processed dates
# - Reads Date.max() from parquet datasets
# - Reads engine last_date from optional sidecar JSON (pkl.json), else None
# - Computes required update steps for FEATURE, DBs, ENGINES

import os, json, sys
from datetime import date
import pandas as pd

# Make sure we can import config_paths from sibling/parent util directory at runtime
CUR = os.path.dirname(__file__)
PARENT = os.path.dirname(CUR)
if PARENT not in sys.path:
    sys.path.append(PARENT)
try:
    from config_paths import get_path
except Exception:
    # Fallback: assume MODELENGINE root structure if import path differs
    def get_path(*parts):
        base = r"F:\autostockG\MODELENGINE"
        clean = [str(p).strip() for p in parts if str(p).strip()]
        return os.path.join(base, *clean)

def _parquet_last_date(path):
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path, columns=["Date"])
    except Exception:
        try:
            df = pd.read_parquet(path)
        except Exception:
            return None
    if "Date" not in df.columns:
        return None
    d = pd.to_datetime(df["Date"], errors="coerce").max()
    if pd.isna(d):
        return None
    return d.date()

def _engine_last_date(pkl_path):
    """Optional: read last_date from sidecar JSON next to PKL: <pkl>.json"""
    meta_path = pkl_path + ".json"
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            s = meta.get("last_date")
            if s:
                return pd.to_datetime(s).date()
        except Exception:
            pass
    return None

def snapshot():
    """Return a dict snapshot of all relevant last dates."""
    paths = {
        "RAW": get_path("RAW", "stocks", "all_stocks_cumulative.parquet"),
        "KOSPI": get_path("RAW", "kospi_data", "kospi_data.parquet"),
        "FEATURE": get_path("FEATURE", "features_V31.parquet"),
        "DB_REAL": get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet"),
        "DB_RESEARCH": get_path("HOJ_DB", "RESEARCH", "HOJ_DB_RESEARCH_V31.parquet"),
        "ENGINE_REAL": get_path("HOJ_ENGINE", "REAL", "HOJ_ENGINE_REAL_V31.pkl"),
        "ENGINE_RESEARCH": get_path("HOJ_ENGINE", "RESEARCH", "HOJ_ENGINE_RESEARCH_V31.pkl"),
    }
    raw_last = _parquet_last_date(paths["RAW"])
    kospi_last = _parquet_last_date(paths["KOSPI"])
    feat_last = _parquet_last_date(paths["FEATURE"])
    db_real_last = _parquet_last_date(paths["DB_REAL"])
    db_research_last = _parquet_last_date(paths["DB_RESEARCH"])
    eng_real_last = _engine_last_date(paths["ENGINE_REAL"])
    eng_research_last = _engine_last_date(paths["ENGINE_RESEARCH"])

    common_latest = None
    if raw_last and kospi_last:
        common_latest = min(raw_last, kospi_last)

    snap = {
        "paths": paths,
        "RAW_latest": raw_last,
        "KOSPI_latest": kospi_last,
        "FEATURE_latest": feat_last,
        "DB_REAL_latest": db_real_last,
        "DB_RESEARCH_latest": db_research_last,
        "ENGINE_REAL_latest": eng_real_last,
        "ENGINE_RESEARCH_latest": eng_research_last,
        "COMMON_latest": common_latest,
    }
    return snap

def plan():
    """Compute which steps are required based on date comparison rules."""
    s = snapshot()

    raw_last = s["RAW_latest"]
    kospi_last = s["KOSPI_latest"]
    feat_last = s["FEATURE_latest"]
    db_real_last = s["DB_REAL_latest"]
    db_research_last = s["DB_RESEARCH_latest"]
    eng_real_last = s["ENGINE_REAL_latest"]
    eng_research_last = s["ENGINE_RESEARCH_latest"]
    common_latest = s["COMMON_latest"]

    need = {
        "FEATURE": False,
        "DB_REAL": False,
        "DB_RESEARCH": False,
        "ENGINE_REAL": False,
        "ENGINE_RESEARCH": False,
    }

    # FEATURE needs update if RAW/KOSPI have newer common date than feature
    if common_latest and (feat_last is None or common_latest > feat_last):
        need["FEATURE"] = True

    # DBs need update if FEATURE newer than DB
    if feat_last and (db_real_last is None or feat_last > db_real_last):
        need["DB_REAL"] = True
    if feat_last and (db_research_last is None or feat_last > db_research_last):
        need["DB_RESEARCH"] = True

    # Engines need update if corresponding DB newer than engine
    # If engine date missing, require update when DB exists
    if db_real_last and (eng_real_last is None or db_real_last > eng_real_last):
        need["ENGINE_REAL"] = True
    if db_research_last and (eng_research_last is None or db_research_last > eng_research_last):
        need["ENGINE_RESEARCH"] = True

    return {"snapshot": s, "need": need}