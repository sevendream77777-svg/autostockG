# KOSPI 지수 10년 저장 (KS11)
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta

END = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START = "2015-01-01"

kospi = fdr.DataReader("KS11", START, END).reset_index()
kospi = kospi.rename(columns={"Date":"Date","Close":"KOSPI_Close"})
# 정리: 필요한 컬럼만
kospi = kospi[["Date","KOSPI_Close"]]
kospi.to_parquet("kospi_index_10y.parquet")
print("✅ 저장 완료: kospi_index_10y.parquet", kospi.shape)
