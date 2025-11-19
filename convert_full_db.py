import pandas as pd

input_path  = r"F:\autostockG\new_Hoj_DB_V25_FULL.parquet"
output_path = r"F:\autostockG\new_Hoj_DB_V25_FULL.csv"

print("변환 중... 잠시만 기다려주세요...")
df = pd.read_parquet(input_path)
df.to_csv(output_path, index=False, encoding='utf-8-sig')
print("CSV 저장 완료:", output_path)
