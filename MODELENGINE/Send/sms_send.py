
import os, glob, json

INFO_DIR = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"

def find_latest_json():
    files = glob.glob(os.path.join(INFO_DIR,"HOJ_ENGINE_REAL_*.json"))
    files_sorted=sorted(files, key=lambda x: os.path.getmtime(x))
    return files_sorted[-1]

def send_sms(msg):
    print("[SMS] API 설정 필요. 메시지 출력:\n",msg)

def make_msg(data):
    meta=data.get("engine_meta",{})
    return f"[HOJ {meta.get('version','')} / {meta.get('horizon','')}일 예측] SMS 요약 메시지"

def main():
    latest=find_latest_json()
    with open(latest,"r",encoding="utf-8") as f:
        data=json.load(f)
    msg=make_msg(data)
    send_sms(msg)

if __name__=="__main__":
    main()
