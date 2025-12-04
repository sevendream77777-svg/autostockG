import pandas as pd

src = r"F:\autostockG\MODELENGINE\RAW\EXTERNAL\2015\2015-01-15.parquet"
dst = r"F:\autostockG\MODELENGINE\RAW\EXTERNAL\2015\2015-01-15.csv"

df = pd.read_parquet(src)
df.to_csv(dst, index=False)

print("CSV 변환 완료:", dst)
