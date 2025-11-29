
import os, glob, json, requests

TELEGRAM_TOKEN=""
TELEGRAM_CHAT_ID=""
INFO_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

def find_latest_json():
    files = glob.glob(os.path.join(INFO_DIR, "HOJ_ENGINE_REAL_*.json"))
    files_sorted = sorted(files, key=lambda x: os.path.getmtime(x))
    return files_sorted[-1]

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram 설정 필요.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    r = requests.post(url,data={"chat_id":TELEGRAM_CHAT_ID,"text":msg})
    return r.status_code==200

def make_msg(data):
    meta=data.get("engine_meta",{})
    top=data.get("top10",[])
    ai=data.get("ai_report","")
    pdate=meta.get("prediction_date","")
    pend=meta.get("prediction_end_date","")
    header=(
        "┌─────────────────────────────────────────────┐\n"
        f"│   HOJ {meta.get('version','')} 실전엔진 / {meta.get('horizon','')}일 예측            │\n"
        f"│   예측기간: {pdate} ~ {pend}     │\n"
        "└─────────────────────────────────────────────┘\n\n"
    )
    body="📊 오늘의 추천 종목 (TOP 10)\n"
    return header+body

def main():
    latest=find_latest_json()
    with open(latest,"r",encoding="utf-8") as f:
        data=json.load(f)
    msg=make_msg(data)
    send_telegram(msg)

if __name__=="__main__":
    main()
