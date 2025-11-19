# build_HOJ_DB_RESEARCH.py - 원본 + 버전 저장 최소 추가

import os
import pandas as pd
from config_paths import get_feature_path, get_path, versioned_filename   # ★ 추가

FEATURE_FILE = get_feature_path("features_V1.parquet")
OUTPUT_FILE = get_path("HOJ_DB_RESEARCH","WRITE","HOJ_DB_RESEARCH_V31.parquet") if False else "F:\\autostockG\\MODELENGINE\\HOJ_DB\\RESEARCH\\HOJ_DB_RESEARCH_V31.parquet"

def add_labels(df):
    df = df.sort_values(["Code","Date"]).copy()
    df["Close_shift_5"] = df.groupby("Code")["Close"].shift(-5)
    df["Return_5d"] = (df["Close_shift_5"] - df["Close"]) / df["Close"]
    df["Label_5d"] = (df["Return_5d"] > 0).astype(int)
    df = df.dropna(subset=["Return_5d","Label_5d"]).reset_index(drop=True)
    return df

def main():
    print("[HOJ_DB_RESEARCH] 연구 DB 생성")

    if not os.path.exists(FEATURE_FILE):
        raise FileNotFoundError(FEATURE_FILE)

    df = pd.read_parquet(FEATURE_FILE)
    df = add_labels(df)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_parquet(OUTPUT_FILE,index=False)

    # ★ 추가: 버전 저장
    version_path = versioned_filename(OUTPUT_FILE)
    df.to_parquet(version_path,index=False)
    print("[HOJ_DB_RESEARCH] 버전 저장:", version_path)

    print("[HOJ_DB_RESEARCH] 완료:", OUTPUT_FILE)

if __name__ == "__main__":
    main()
