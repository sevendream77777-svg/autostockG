# safe_raw_validator.py
import pandas as pd
import numpy as np
import os
import datetime

BASE = r"F:\autostockG\MODELENGINE\RAW"
DAILY = os.path.join(BASE, "DAILY")
LOGS = os.path.join(BASE, "logs")

os.makedirs(LOGS, exist_ok=True)

def validate():
    log = []
    files = sorted(os.listdir(DAILY))

    for f in files:
        path = os.path.join(DAILY, f)
        try:
            df = pd.read_parquet(path)
        except:
            log.append(f"[FAIL] 파일 자체 오류: {f}")
            continue

        date = f.replace(".parquet", "")
        if len(df) == 0:
            log.append(f"[EMPTY] {date} = 0개")
            continue

        cnt = df["Code"].nunique()

        if cnt < 3500:
            log.append(f"[PARTIAL] {date} 종목 {cnt}개")

        # 가격 이상값
        bad = df[
            (df["Open"] <= 0) |
            (df["High"] <= 0) |
            (df["Low"] <= 0) |
            (df["Close"] <= 0) |
            (df["High"] < df["Low"])
        ]

        if len(bad) > 0:
            log.append(f"[BAD PRICE] {date} 이상값 {len(bad)}개")

    # 로그 저장
    today = datetime.datetime.now().strftime("%y%m%d")
    log_path = os.path.join(LOGS, f"validate_{today}.txt")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(log))

    print("검증 종료. 로그:", log_path)

if __name__ == "__main__":
    validate()
