import pandas as pd

df = pd.read_parquet("new_Hoj_DB_V25.parquet")

# 1) Expected_Return_5d가 존재하고 Return_5d가 없을 때 복원
if "Return_5d" not in df.columns and "Expected_Return_5d" in df.columns:
    df["Return_5d"] = df["Expected_Return_5d"]
    print("🔁 Return_5d 컬럼 복원 완료 (Expected_Return_5d 기반).")
else:
    print("✔ Return_5d 이미 존재하거나 Expected_Return_5d 없음.")

df.to_parquet("new_Hoj_DB_V25.parquet")
print("💾 저장 완료.")
