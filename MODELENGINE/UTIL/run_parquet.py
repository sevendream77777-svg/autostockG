import pandas as pd; df = pd.read_parquet("F:/autostockG/MODELENGINE/FEATURE/features_V1.parquet"); print(df.head()); print("\n컬럼 리스트:"); print(df.columns.tolist())
