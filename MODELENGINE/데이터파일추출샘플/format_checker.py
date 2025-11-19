# format_checker.py
import pandas as pd

paths = {
    "RAW": r"F:\autostockG\MODELENGINE\RAW\all_stocks_cumulative.parquet",
    "FEATURE": r"F:\autostockG\MODELENGINE\FEATURE\features_V31.parquet",
    "HOJ_REAL": r"F:\autostockG\MODELENGINE\HOJ_DB\REAL\HOJ_DB_REAL_V31.parquet",
    "HOJ_RESEARCH": r"F:\autostockG\MODELENGINE\HOJ_DB\RESEARCH\HOJ_DB_RESEARCH_V31.parquet"
}

def inspect(path):
    df = pd.read_parquet(path)
    print("\n=== 검사:", path, "===")
    print("컬럼:", df.columns.tolist()[:20])
    print("Date 타입:", df['Date'].dtype)
    print("Date 샘플:", df['Date'].head(3).tolist())
    print("Code 타입:", df['Code'].dtype)
    print("Code 샘플:", df['Code'].head(3).tolist())

for name, p in paths.items():
    inspect(p)
