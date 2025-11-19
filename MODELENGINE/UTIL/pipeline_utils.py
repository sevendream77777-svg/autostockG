# pipeline_utils.py
import os, pandas as pd

def ensure_exists(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"필수 파일 없음: {path}")

def load_clean_df(path):
    df = pd.read_parquet(path)
    return df.select_dtypes(exclude=['object'])
