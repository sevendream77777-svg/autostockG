# --- 코드 버전: V17R2 (id/url 컬럼 구조 대응 + 병렬처리 + 중단복구 완벽) ---
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os, time, math
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------
# 설정
# -------------------------------------
INPUT_FILE = "targets.csv"       # CSV 파일: id,url
OUTPUT_DIR = "output_chunks"     # 결과 저장 폴더
MAX_WORKERS = 20                 # 병렬 스레드 수 (i9이면 20~30 추천)
CHUNK_SIZE = 1000                # 1회 저장 단위
RETRY = 3                        # 실패시 재시도 횟수
SLEEP_SEC = 0.2                  # 요청 간격 (IP 차단 방지)
TIMEOUT = 5                      # 요청 타임아웃

# -------------------------------------
# 준비
# -------------------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)
done_files = {f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")}
print(f"[준비 완료] 기존 저장 파일 {len(done_files)}개 감지됨")

df_all = pd.read_csv(INPUT_FILE)
print(f"[시작] 총 {len(df_all):,}건 대상 PER/PBR 수집 시작...")

# ✅ id 컬럼에서 date, code 분리
df_all["date"] = df_all["id"].apply(lambda x: str(x).split("_")[0])
df_all["code"] = df_all["id"].apply(lambda x: str(x).split("_")[1])

# ✅ 이미 처리된 청크 수만큼 스킵
processed_idx = len(done_files) * CHUNK_SIZE
df_target = df_all.iloc[processed_idx:]
print(f"[진행] 이번 회차 대상: {len(df_target):,}건")

# -------------------------------------
# 함수: Naver Finance에서 PER/PBR 가져오기
# -------------------------------------
def fetch_per_pbr(row):
    code, date, url = row["code"], row["date"], row["url"]

    for _ in range(RETRY):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code != 200:
                time.sleep(SLEEP_SEC)
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            # PER/PBR 추출
            per_tag = soup.select_one("em#_per")
            pbr_tag = soup.select_one("em#_pbr")

            per = float(per_tag.text.replace(",", "")) if per_tag and per_tag.text.strip() else None
            pbr = float(pbr_tag.text.replace(",", "")) if pbr_tag and pbr_tag.text.strip() else None

            return {"date": date, "code": code, "PER": per, "PBR": pbr}
        except Exception:
            time.sleep(SLEEP_SEC)
    # 실패 시 None 기록
    return {"date": date, "code": code, "PER": None, "PBR": None}

# -------------------------------------
# 메인 루프
# -------------------------------------
total_chunks = math.ceil(len(df_target) / CHUNK_SIZE)
chunk_id = len(done_files) + 1

for i in range(total_chunks):
    start = i * CHUNK_SIZE
    end = min((i + 1) * CHUNK_SIZE, len(df_target))
    df_chunk = df_target.iloc[start:end]

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_per_pbr, row): row for _, row in df_chunk.iterrows()}
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"[{chunk_id:04d}] 수집 중"):
            results.append(future.result())

    df_out = pd.DataFrame(results)
    out_path = os.path.join(OUTPUT_DIR, f"out_{chunk_id:04d}.csv")
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[저장 완료] {len(df_out):,}건 → {out_path}")

    chunk_id += 1
    time.sleep(1)

print("\n✅ 모든 청크 저장 완료!")
