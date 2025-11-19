# --- 코드 버전: V16R (PER/PBR 안정화 버전) ---
import sys
import asyncio
import aiohttp
import aiofiles
import async_timeout
import csv
import os
import time
import random
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
import pandas as pd

# ============================================
# === 1. 설정 ===============================
# ============================================
INPUT_CSV = "targets.csv"        # id,url 형식
OUTPUT_DIR = "output_chunks"
DONE_FILE = "done_ids.txt"
FAILED_FILE = "failed_ids.txt"
BATCH_SIZE = 5000                # CSV 1개당 저장 건수
CONCURRENCY = 5                  # 동시 접속 개수
REQUEST_TIMEOUT = 10
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
RANDOM_DELAY = (0.3, 0.8)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
]

DEFAULT_HEADERS = {
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://finance.naver.com/"
}

PROXIES: Optional[List[str]] = None

# ============================================
# === 2. 유틸 함수 ==========================
# ============================================

def ensure_dirs():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def load_done_ids() -> set:
    if not os.path.exists(DONE_FILE):
        return set()
    with open(DONE_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

async def append_done_id(id_str: str):
    async with aiofiles.open(DONE_FILE, "a", encoding="utf-8") as f:
        await f.write(f"{id_str}\n")

async def append_failed_id(id_str: str):
    async with aiofiles.open(FAILED_FILE, "a", encoding="utf-8") as f:
        await f.write(f"{id_str}\n")

def load_targets(input_csv: str) -> List[Tuple[str, str]]:
    """targets.csv 포맷: id,url"""
    targets = []
    with open(input_csv, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        if "id" not in reader.fieldnames or "url" not in reader.fieldnames:
            raise ValueError("INPUT CSV must contain 'id' and 'url' columns")
        for row in reader:
            targets.append((row["id"], row["url"]))
    return targets

# ============================================
# === 3. PBR/PER 파싱 =======================
# ============================================

def parse_naver_page(html: str, id_str: str) -> Dict:
    """네이버 금융 페이지에서 PER/PBR 추출"""
    soup = BeautifulSoup(html, "lxml")

    # HTML 길이 체크 (차단 감지)
    if len(html) < 1500 or "캡챠" in html or "보안" in html:
        return {
            "id": id_str,
            "date": id_str.split("_")[0],
            "ticker": id_str.split("_")[1],
            "PBR": None,
            "PER": None,
            "status": "BLOCKED"
        }

    per, pbr = None, None
    per_tag = soup.select_one("em#_per")
    pbr_tag = soup.select_one("em#_pbr")

    if per_tag:
        try:
            text = per_tag.text.strip().replace(",", "")
            if text and text != "N/A":
                per = float(text)
        except:
            pass

    if pbr_tag:
        try:
            text = pbr_tag.text.strip().replace(",", "")
            if text and text != "N/A":
                pbr = float(text)
        except:
            pass

    return {
        "id": id_str,
        "date": id_str.split("_")[0],
        "ticker": id_str.split("_")[1],
        "PBR": pbr,
        "PER": per,
        "status": "OK"
    }

# ============================================
# === 4. 크롤러 본체 =========================
# ============================================

class Crawler:
    def __init__(self, targets: List[Tuple[str, str]]):
        self.targets = targets
        self.total = len(targets)
        self.semaphore = asyncio.Semaphore(CONCURRENCY)
        self.done_ids = load_done_ids()
        self.results_buffer = []
        self.buffer_count = 0
        self.file_index = 0
        self.lock = asyncio.Lock()

    async def _create_session(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, ssl=False),
            timeout=aiohttp.ClientTimeout(total=None)
        )

    async def _close_session(self):
        await self.session.close()

    async def fetch(self, url: str) -> Optional[str]:
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                headers = DEFAULT_HEADERS.copy()
                headers["User-Agent"] = random.choice(USER_AGENTS)
                async with async_timeout.timeout(REQUEST_TIMEOUT):
                    async with self.session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            text = await resp.text(errors="ignore")
                            return text
                        elif resp.status in (403, 429):
                            await asyncio.sleep(backoff)
                            backoff *= 2
                        else:
                            await asyncio.sleep(backoff)
            except Exception:
                await asyncio.sleep(backoff)
                backoff *= 2
        return None

    async def worker(self, id_url: Tuple[str, str]):
        id_str, url = id_url
        if id_str in self.done_ids:
            return
        async with self.semaphore:
            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
            html = await self.fetch(url)
            if not html:
                await append_failed_id(id_str)
                return

            data = parse_naver_page(html, id_str)
            if data["status"] == "BLOCKED":
                await append_failed_id(id_str)
                return

            async with self.lock:
                self.results_buffer.append(data)
                self.buffer_count += 1
                await append_done_id(id_str)
                if self.buffer_count >= BATCH_SIZE:
                    await self.flush_buffer()

    async def flush_buffer(self):
        if not self.results_buffer:
            return
        async with self.lock:
            self.file_index += 1
            out_path = Path(OUTPUT_DIR) / f"out_{self.file_index:04d}.csv"
            df = pd.DataFrame(self.results_buffer)
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"[저장] {len(self.results_buffer)}건 → {out_path}")
            self.results_buffer.clear()
            self.buffer_count = 0

    async def run(self):
        ensure_dirs()
        await self._create_session()
        try:
            tasks = [self.worker(t) for t in self.targets]
            for chunk in range(0, len(tasks), 5000):
                sub = tasks[chunk:chunk+5000]
                await asyncio.gather(*sub)
                await self.flush_buffer()
        finally:
            await self._close_session()
        print("✅ 크롤링 완료")

# ============================================
# === 5. 실행부 =============================
# ============================================

def main():
    ensure_dirs()
    targets = load_targets(INPUT_CSV)
    print(f"[시작] 총 {len(targets):,}건 대상 PER/PBR 수집 시작...")

    crawler = Crawler(targets)
    if sys.platform == "win32" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(crawler.run())

if __name__ == "__main__":
    main()

