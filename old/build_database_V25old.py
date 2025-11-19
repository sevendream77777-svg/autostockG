# ============================================================
# [build_database_V25.py] V25 DB 병합 + 라벨(5d 수익률) 생성
# ============================================================
import pandas as pd
import numpy as np
import traceback

print("=================================================")
print("[build_database_V25.py] ▶️ 실행 시작...")
print("=================================================")

try:
    feat = pd.read_parquet("all_features_cumulative_V21_Hoj.parquet")
    print(f"✅ V21 피처 로드: {len(feat):,}행")

    need = {"Date","Code","Close"}
    if need - set(feat.columns):
        raise KeyError(f"❌ 필수 컬럼 누락: {need - set(feat.columns)}")

    # 라벨: 5일 뒤 수익률
    feat = feat.sort_values(["Code","Date"]).reset_index(drop=True)
    feat["Close_fwd_5"] = feat.groupby("Code")["Close"].shift(-5)
    feat["Return_5d"] = feat["Close_fwd_5"] / feat["Close"] - 1.0
    feat = feat.drop(columns=["Close_fwd_5"])

    # 학습용 라벨 (양수=1, 음수=0) - 필요시 경계값 조정
    feat["Label_5d"] = (feat["Return_5d"] > 0).astype(int)

    # 정리
    feat = feat.dropna(subset=["Return_5d"])  # 끝쪽 NaN 제거
    feat = feat.loc[:, ~feat.columns.duplicated()]

    # 저장 (표준 파일명 2종)
    feat.to_parquet("V25_Hoj_DB.parquet")
    feat.to_parquet("new_Hoj_DB_V25.parquet")
    print(f"✅ [저장 완료] V25_Hoj_DB.parquet / new_Hoj_DB_V25.parquet ({len(feat):,}행)")
    print("=================================================")
    print("[build_database_V25.py] ✅ 성공")
    print("=================================================")

except Exception as e:
    print("❌ [치명적 오류 발생] 자동 종료합니다.")
    print("오류 내용:", str(e))
    traceback.print_exc()
    print("=================================================")
