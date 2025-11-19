import pandas as pd

df = pd.read_parquet("new_Hoj_DB_V25.parquet")

if "Expected_Return_5d" not in df.columns:
    df = df.rename(columns={"Return_5d": "Expected_Return_5d"})
    print("🔁 Return_5d → Expected_Return_5d 컬럼명 변경 완료.")
else:
    print("✔ Expected_Return_5d 이미 존재합니다.")

df.to_parquet("new_Hoj_DB_V25.parquet")
print("💾 저장 완료.")
