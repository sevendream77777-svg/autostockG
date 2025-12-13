# -*- coding: utf-8 -*-
"""
run_raw_realtime.py [초강력 안전 모드]
- IP 차단 방지를 위한 극단적 속도 제한 버전
- WORKERS: 2 (병렬 처리 최소화)
- DELAY: 3~6초 (사람의 클릭 속도 모방)
- 에러 발생 시 60초 쿨타임 적용
"""
import os
import time
import json
import sys
import random
import multiprocessing as mp
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from threading import Thread
from queue import Empty
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- CONFIG ---------------- #
# [중요] 욕심 부리지 말고 2개로 유지하세요. 늘리면 또 차단됩니다.
WORKERS = 2 
TIMEOUT = 30 # 타임아웃도 넉넉하게
LOG_FILE = Path(__file__).parent / "run_dump_log.txt"

START_YEAR = 2016
END_YEAR = 2025

REPRT_CODES = {"11013": "1Q", "11012": "2Q", "11014": "3Q", "11011": "4Q"}

KEY_FILE_PATHS = [
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\8sevendrenaver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\6109_kitchennaver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\7109kitchen109naver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\2slkdaum_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\3naver_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\4se77777gmail_dart.txt",
    r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\5se1117gmail_dart.txt"
]

# ---------------- UTILS ---------------- #
def load_api_keys():
    keys = []
    print(">>> API 키 파일 로딩 중...")
    for p in KEY_FILE_PATHS:
        path = Path(p)
        if path.exists():
            try:
                content = path.read_text(encoding='utf-8', errors='ignore').strip()
                for k in content.splitlines():
                    k = k.strip()
                    if k and len(k) > 10: keys.append(k)
            except: pass
    
    if not keys:
        print("\n[경고] API 키를 찾지 못했습니다. 경로를 확인해주세요.")
    
    return list(set(keys))

def get_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive'
    })
    # 재시도 횟수를 줄이고 간격을 늘림
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def _log(msg):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    except: pass

# ---------------- WORKER ---------------- #
def worker_task(args):
    stock_code, corp_code, out_dir, api_keys, q = args
    sess = get_session()
    corp_dir = out_dir / f"{stock_code}"

    # 연속 에러 감지용
    consecutive_error_count = 0

    for year in range(START_YEAR, END_YEAR + 1):
        for rc_code, rc_name in REPRT_CODES.items():
            if not api_keys: 
                q.put("FILE_FAIL")
                break

            idx = (year + int(rc_code)) % len(api_keys)
            api_key = api_keys[idx]
            
            for fs_div in ["CFS", "OFS"]: 
                filename = f"{year}_{rc_name}_{fs_div}.json"
                file_path = corp_dir / filename
                
                # 이어받기 (이미 있으면 패스)
                if file_path.exists() and file_path.stat().st_size > 50:
                    continue
                
                try:
                    # [핵심] 3초 ~ 6초 랜덤 대기 (사람 속도 흉내)
                    # 답답해도 이렇게 해야 안 막힙니다.
                    sleep_time = random.uniform(3.0, 6.0)
                    time.sleep(sleep_time)
                    
                    params = {
                        "crtfc_key": api_key, "corp_code": corp_code, 
                        "bsns_year": str(year), "reprt_code": rc_code, "fs_div": fs_div
                    }
                    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
                    res = sess.get(url, params=params, timeout=TIMEOUT)
                    
                    try:
                        data = res.json()
                    except:
                        q.put("FILE_FAIL")
                        consecutive_error_count += 1
                        continue

                    status = data.get("status")

                    if status in ["000", "013"]:
                        if not corp_dir.exists():
                            corp_dir.mkdir(parents=True, exist_ok=True)

                        tmp_path = file_path.with_suffix(".tmp")
                        with open(tmp_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        
                        if file_path.exists(): file_path.unlink()
                        tmp_path.rename(file_path)
                        
                        q.put("FILE_OK")
                        consecutive_error_count = 0 # 성공하면 에러 카운트 리셋
                    
                    elif status in ["020", "800"]:
                        _log(f"LIMIT {stock_code}: {status}")
                        q.put("FILE_FAIL")
                        # 한도 초과나 차단 메시지면 60초 휴식
                        time.sleep(60) 
                    else:
                        q.put("FILE_FAIL")
                        consecutive_error_count += 1
                        
                except Exception as e:
                    _log(f"ERR {stock_code}: {e}")
                    q.put("FILE_FAIL")
                    consecutive_error_count += 1
                    
                    # [긴급 회피] 연결 끊김(10054) 발생 시 60초간 강제 휴식
                    # 서버가 화났을 때 잠깐 죽은 척 하는 것
                    if "10054" in str(e) or "Connection" in str(e):
                        time.sleep(60)

    q.put("CORP_DONE")

# ---------------- MONITOR ---------------- #
def ui_monitor_thread(q, total_corps):
    done_corps = 0
    files_suc = 0
    files_fail = 0
    start_time = time.time()
    last_print_time = start_time
    
    print("-" * 70)
    print(f"수집 시작 (총 {total_corps}개 종목) - [초강력 안전 모드]")
    print(f"속도가 매우 느리지만 차단을 방지합니다.")
    print("-" * 70)

    while done_corps < total_corps:
        try:
            msg = q.get(timeout=0.2)
            
            if msg == "FILE_OK": files_suc += 1
            elif msg == "FILE_FAIL": files_fail += 1
            elif msg == "CORP_DONE": done_corps += 1
            
            # 1초에 한 번만 갱신 (화면 멈춤 방지)
            current_time = time.time()
            if current_time - last_print_time > 1.0 or msg == "CORP_DONE":
                elapsed = current_time - start_time
                mins, secs = divmod(int(elapsed), 60)
                
                status = (
                    f"\r[진행중] 종목: {done_corps}/{total_corps} "
                    f"| 파일수집: {files_suc:,}개 (실패 {files_fail}) "
                    f"| 시간: {mins}분 {secs}초   "
                )
                sys.stdout.write(status)
                sys.stdout.flush()
                last_print_time = current_time
            
        except Empty:
            continue

    print("\n[완료] 수집 끝.")

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    mp.freeze_support()
    
    BASE_DIR = Path(r"F:\autostockG\MODELENGINE\RAW\raw_sle\date\raw_stable")
    XML_PATH = Path(r"F:\autostockG\MODELENGINE\RAW\raw_sle\dart_corp_list.xml")
    
    if not BASE_DIR.exists(): BASE_DIR.mkdir(parents=True, exist_ok=True)
    api_keys = load_api_keys()
    
    codes = []
    try:
        tree = ET.parse(XML_PATH)
        root = tree.getroot()
        for x in root.findall("list"):
            sc = x.findtext("stock_code").strip()
            cc = x.findtext("corp_code").strip()
            if sc and cc: codes.append((sc, cc))
    except: pass
    
    total_corps = len(codes)
    m = mp.Manager()
    q = m.Queue()
    tasks = [(sc, cc, BASE_DIR, api_keys, q) for sc, cc in codes]
    
    monitor = Thread(target=ui_monitor_thread, args=(q, total_corps), daemon=True)
    monitor.start()
    
    try:
        with mp.Pool(WORKERS) as pool:
            pool.map(worker_task, tasks)
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    
    monitor.join(timeout=1)