# --- sle_sample_runner_multithread.py ---
import os, time, math, json, gc
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from collections import defaultdict

# =========================
# 경로/파일
# =========================
BASE_DIR = "F:/autostock"
ENGINE_DIR = os.path.join(BASE_DIR, "sle")
os.makedirs(ENGINE_DIR, exist_ok=True)

SAMPLE_FILE = os.path.join(BASE_DIR, "v11_sample_list.parquet")   # 입력(50만 샘플)
CHECKPOINT_FILE = os.path.join(ENGINE_DIR, "pbr_per_checkpoint.parquet")  # 중간저장
OUTPUT_BASE = os.path.join(ENGINE_DIR, "pbr_per_sample.parquet")  # 최종결과 (자동 카운터 저장)

# =========================
# 실행 파라미터(튜닝 포인트)
# =========================
MAX_WORKERS = 24                 # 병렬 스레드 수 (네트워크/서버 상태 보고 12~48 사이)
BATCH_FLUSH = 2000               # n건 수집될 때마다 디스크로 플러시
RETRY_TOTAL = 5                  # 요청 재시도 횟수
BACKOFF_FACTOR = 0.6             # 백오프 계수
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 12

# =========================
# 세션/재시도
# =========================
def build_session():
    s = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET","POST"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    })
    return s

# =========================
# ★ 여기에 기존 단일 호출 함수 연결 ★
#   반드시 dict 반환: {"종목코드":code,"날짜":date,"PBR":pbr,"PER":per}
#   실패/결측 시 None 반환
# =========================
def fetch_pbr_per_one(code: str, date_YYYYMMDD: str, session: requests.Session) -> dict | None:
    """
    TODO: 여기 내부를 현재 쓰는 V16 함수 로직으로 교체하세요.
    이 함수만 교체하면 전체 병렬 러너가 그대로 동작합니다.
    """
    # 예시(의사코드): r = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)); 파싱...
    # return {"종목코드":code, "날짜":date_YYYYMMDD, "PBR":pbr, "PER":per}
    return None  # 임시

# =========================
# 파일 유틸
# =========================
def unique_output_name(base_path: str) -> str:
    if not os.path.exists(base_path):
        return base_path
    base, ext = os.path.splitext(base_path)
    k = 1
    while True:
        cand = f"{base}_{k}{ext}"
        if not os.path.exists(cand):
            return cand
        k += 1

def safe_to_parquet_append(path: str, df: pd.DataFrame):
    # 파케이로 한 번에 모아서 저장(append 아님) → 우리는 체크포인트 파일을 overwrite 방식으로 누적
    df.to_parquet(path, index=False)

# =========================
# 실행
# =========================
def main():
    t0 = time.time()
    print("[INFO] SLE 멀티스레드 러너 시작...")

    df_idx = pd.read_parquet(SAMPLE_FILE)   # ['종목코드','날짜'] 존재 가정
    df_idx["날짜"] = pd.to_datetime(df_idx["날짜"]).dt.strftime("%Y%m%d")

    # 이미 수집된 체크포인트 제거(중복 제거)
    fetched = set()
    if os.path.exists(CHECKPOINT_FILE):
        df_ckp = pd.read_parquet(CHECKPOINT_FILE)
        df_ckp["키"] = df_ckp["종목코드"].astype(str) + "_" + pd.to_datetime(df_ckp["날짜"]).dt.strftime("%Y%m%d")
        fetched = set(df_ckp["키"].tolist())
        print(f"[INFO] 체크포인트 로드: {len(fetched):,}건 스킵 예정")

    # 타겟 작업 큐
    df_idx["키"] = df_idx["종목코드"].astype(str) + "_" + df_idx["날짜"].astype(str)
    df_todo = df_idx[~df_idx["키"].isin(fetched)].copy()
    print(f"[INFO] 남은 작업: {len(df_todo):,}건")

    session = build_session()
    results = []
    flush_count = 0

    def worker(row):
        code, date_str = row["종목코드"], row["날짜"]
        try:
            rec = fetch_pbr_per_one(code, date_str, session)
            return rec
        except Exception as e:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, row) for _, row in df_todo.iterrows()]
        for i, fut in enumerate(as_completed(futures), 1):
            rec = fut.result()
            if rec is not None:
                results.append(rec)

            if i % 500 == 0:
                ok = sum(r is not None for r in results[-500:])
                print(f"[PROG] {i:,}/{len(futures):,}  (최근500건 성공 {ok}건)")

            if len(results) >= BATCH_FLUSH:
                df_part = pd.DataFrame(results)
                if not df_part.empty:
                    # 체크포인트 누적 저장(덮어쓰기)
                    if os.path.exists(CHECKPOINT_FILE):
                        df_old = pd.read_parquet(CHECKPOINT_FILE)
                        df_new = pd.concat([df_old, df_part], ignore_index=True).drop_duplicates(subset=["종목코드","날짜"], keep="last")
                        safe_to_parquet_append(CHECKPOINT_FILE, df_new)
                    else:
                        safe_to_parquet_append(CHECKPOINT_FILE, df_part)
                    flush_count += len(results)
                    results.clear()
                    gc.collect()
                    print(f"[FLUSH] 누적 저장: {flush_count:,}건")

    # 잔여 저장
    if results:
        df_part = pd.DataFrame(results)
        if not df_part.empty:
            if os.path.exists(CHECKPOINT_FILE):
                df_old = pd.read_parquet(CHECKPOINT_FILE)
                df_new = pd.concat([df_old, df_part], ignore_index=True).drop_duplicates(subset=["종목코드","날짜"], keep="last")
                safe_to_parquet_append(CHECKPOINT_FILE, df_new)
            else:
                safe_to_parquet_append(CHECKPOINT_FILE, df_part)

    # 최종 병합: 인덱스와 조인해서 “정확히 50만 샘플 구조”로 정렬
    df_all = pd.read_parquet(CHECKPOINT_FILE)
    df_all["날짜"] = pd.to_datetime(df_all["날짜"])
    out = df_idx.merge(df_all, on=["종목코드","날짜"], how="left")
    out = out[["종목코드","날짜","PER","PBR"]]

    out_path = unique_output_name(OUTPUT_BASE)
    out.to_parquet(out_path, index=False)
    print(f"[DONE] 최종 저장: {out_path}  / 총 {len(out):,}건  / 경과 {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
