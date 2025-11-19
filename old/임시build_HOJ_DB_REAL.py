# -*- coding: utf-8 -*-
import os
import pandas as pd
from config_paths import get_feature_path, get_path, versioned_filename

def main():
    # 1) FEATURE 파일 로드 (RESEARCH와 동일)
    feature_file = get_feature_path("features_V1.parquet")

    if not os.path.exists(feature_file):
        raise FileNotFoundError(f"❌ FEATURE 파일 없음: {feature_file}")

    print("[HOJ_DB_REAL] FEATURE 로드 중…")
    df = pd.read_parquet(feature_file)

    # 2) 정렬
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)

    # 3) 5일 수익률 계산
    print("[HOJ_DB_REAL] Return_5d / Label_5d 생성…")

    def calc_return(g):
        g = g.copy()
        g["Return_5d"] = g["Close"].shift(-5) / g["Close"] - 1
        return g

    df = df.groupby("Code", group_keys=False).apply(calc_return)
    df["Label_5d"] = (df["Return_5d"] > 0).astype(int)

    # 결측 제거
    df = df.dropna(subset=["Return_5d", "Label_5d"]).reset_index(drop=True)

    # 4) REAL DB 저장 경로
    out_file = get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")

    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    # 5) 저장
    print(f"[HOJ_DB_REAL] 저장: {out_file}")
    df.to_parquet(out_file, index=False)

    # 6) 버전 백업
    version_path = versioned_filename(out_file)
    print(f"[HOJ_DB_REAL] 백업 생성: {version_path}")
    df.to_parquet(version_path, index=False)

    print("[HOJ_DB_REAL] 완료!")

if __name__ == "__main__":
    main()
