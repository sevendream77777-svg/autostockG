# ============================================================
# daily_recommender.py (KOR FINAL)
# ------------------------------------------------------------
# - 모든 출력/JSON 컬럼을 한국어 기반으로 통일
# - 엑셀 기능 완전 제거
# - AI 분석은 옵션(--ai 1)일 때만 실행
# - 자동 엔진 탐색 / 자동 DB 선택 / JSON 병합 100% 유지
# ============================================================

import os, sys, argparse, pickle, warnings, json, re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
import numpy as np
import pandas as pd
from textwrap import dedent

# ------------------------------------------------------------
# 프로젝트 경로 설정
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir  = os.path.dirname(current_dir)   
root_dir    = os.path.dirname(parent_dir)    
sys.path.append(root_dir)

try:
    from MODELENGINE.UTIL.config_paths import get_path
    from MODELENGINE.UTIL.version_utils import find_latest_file
except Exception:
    sys.path.append(parent_dir)
    from UTIL.config_paths import get_path
    from UTIL.version_utils import find_latest_file

# ------------------------------------------------------------
# Gemini (옵션)
# ------------------------------------------------------------
def _load_api_key() -> Optional[str]:
    key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\googlegemini_api.txt"
    try:
        if os.path.exists(key_path):
            with open(key_path, "r", encoding="utf-8") as f:
                return f.read().strip()
    except Exception:
        pass
    return None

GEMINI_API_KEY = _load_api_key()

def _safe_import_gemini():
    try:
        import google.generativeai as genai
        return True
    except Exception:
        return False

# ------------------------------------------------------------
# 유틸 함수
# ------------------------------------------------------------
def pick_close_col(df: pd.DataFrame) -> str:
    cand = ["Close","close","ClosePrice","종가","가격","Adj Close","AdjClose"]
    for c in cand:
        if c in df.columns:
            return c
    nums = [c for c in df.columns if df[c].dtype.kind in ("i","f")]
    if len(nums) == 1:
        return nums[0]
    raise KeyError("종가 컬럼을 찾지 못했습니다.")

def _json_safe(x):
    if isinstance(x, np.generic):
        return x.item()
    if isinstance(x, Path):
        return str(x)
    return x

def _dict_merge_safe(base: Dict[str, Any], add: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base) if isinstance(base, dict) else {}
    if not isinstance(add, dict):
        return out
    for k, v in add.items():
        if k not in out:
            out[k] = v
            continue
        if isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _dict_merge_safe(out[k], v)
        else:
            out[k] = v
    return out

# ------------------------------------------------------------
# 엔진 자동 선택
# ------------------------------------------------------------
def find_engine_real() -> str:
    TARGET_H = 5
    TARGET_W = 60
    TARGET_N = 1000

    base_root = get_path("HOJ_ENGINE")
    if os.path.isfile(base_root):
        base_root = os.path.dirname(base_root)
    real_dir = os.path.join(base_root, "REAL")

    if not os.path.isdir(real_dir):
        raise FileNotFoundError("REAL 폴더 없음: " + real_dir)

    cands = [fn for fn in os.listdir(real_dir)
             if fn.startswith("HOJ_ENGINE_REAL") and fn.endswith(".pkl")]

    if not cands:
        raise FileNotFoundError("REAL 폴더에 엔진 파일이 없습니다.")

    def _parse(fn: str) -> Tuple[int, float, int, int, int]:
        parts = fn.split("_")
        date_token = parts[-1].replace(".pkl","")
        d = -1
        try:
            if len(date_token) == 6 and date_token.startswith("25"):
                d = int(date_token)
            elif len(date_token) == 4:
                d = int("25" + date_token)
            elif len(date_token) == 8 and date_token.startswith("20"):
                d = int(date_token[2:])
            else:
                m = re.search(r'(\d{6})\.pkl$', fn)
                if m and m.group(1).startswith("25"):
                    d = int(m.group(1))
        except:
            d = -1

        h=w=n=None
        for p in parts:
            if p.startswith("h"):
                try: h=int(p[1:])
                except: pass
            if p.startswith("w"):
                if "full" in p.lower():
                    w = 0
                else:
                    try: w=int(p[1:])
                    except: pass
            if p.startswith("n"):
                try: n=int(p[1:])
                except: pass

        full_path = os.path.join(real_dir, fn)
        try: mt = os.path.getmtime(full_path)
        except: mt = 0
        
        return d, mt, (h or -1), (w or -1), (n or -1)

    valid = []
    for fn in cands:
        d, mt, h, w, n = _parse(fn)
        if h == TARGET_H and w == TARGET_W and n == TARGET_N:
            valid.append((d, mt, fn))

    if not valid:
        raise FileNotFoundError(
            f"조건(h={TARGET_H}, w={TARGET_W}, n={TARGET_N}) 일치 엔진 없음"
        )

    valid.sort(key=lambda x: (x[0], x[1]), reverse=True)
    best = valid[0][2]
    return os.path.join(real_dir, best)

# ------------------------------------------------------------
# DB 로드
# ------------------------------------------------------------
def get_unified_db_path(version: str) -> str:
    base = get_path("HOJ_DB")
    if os.path.isfile(base):
        base = os.path.dirname(base)

    prefix = f"HOJ_DB_{version}"
    latest = find_latest_file(base, prefix)
    if latest:
        return latest
    return os.path.join(base, f"{prefix}.parquet")

def load_latest_db(version: str) -> Tuple[pd.DataFrame, str]:
    p = get_unified_db_path(version)
    if not os.path.exists(p):
        raise FileNotFoundError("DB 파일 없음: " + p)
    df = pd.read_parquet(p)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df, p

# ------------------------------------------------------------
# 엔진 로더
# ------------------------------------------------------------
def load_engine(engine_path: str) -> Dict[str, Any]:
    if not os.path.exists(engine_path):
        raise FileNotFoundError("엔진 파일 없음: " + engine_path)
    with open(engine_path, "rb") as f:
        data = pickle.load(f)

    if "features" not in data:
        data["features"] = data.get("feature_names", [])
    if "meta" not in data:
        data["meta"] = {}

    data.setdefault("model_reg", None)
    data.setdefault("model_cls", None)
    return data

# ------------------------------------------------------------
# 공용 예측 코어
# ------------------------------------------------------------
def run_prediction_core(
    engine_path: str,
    target_date: Optional[str],
    top_n: int,
    rank_by: str,
    version_override: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], str, datetime]:

    payload = load_engine(engine_path)
    model_reg = payload.get("model_reg")
    model_cls = payload.get("model_cls")
    features  = payload.get("features", [])
    meta      = payload.get("meta", {})
    version   = version_override or meta.get("version") or "V31"

    df, db_path = load_latest_db(version)

    if target_date:
        td = pd.to_datetime(target_date)
    else:
        td = df["Date"].max()

    daily = df[df["Date"] == td].copy()
    if daily.empty:
        raise ValueError(f"해당 날짜({td.date()}) 데이터 없음")

    missing = [f for f in features if f not in daily.columns]
    if missing:
        raise KeyError(f"필수 피처 누락: {missing[:5]} ...")

    X = daily[features].copy()
    mask = X.notnull().all(axis=1)
    daily = daily[mask]
    X = X[mask]

    name_col = "Name" if "Name" in daily.columns else ("name" if "name" in daily.columns else None)
    code_col = "Code" if "Code" in daily.columns else ("code" if "code" in daily.columns else None)
    close_col = pick_close_col(daily)

    prob = model_cls.predict_proba(X)[:,1] if model_cls else np.zeros(len(X))
    ret  = model_reg.predict(X) if model_reg else np.zeros(len(X))
    ret_clip = np.clip(ret, -0.10, None)
    combo = prob * ret_clip

    df_out = pd.DataFrame({
        "종목명": daily[name_col] if name_col else "",
        "종목코드": daily[code_col] if code_col else "",
        "현재가": daily[close_col],
        "상승확률(%)": (prob*100).round(2),
        "예측수익률(%)": (ret*100).round(2),
        "동시적용 기대수익(%)": (combo*100).round(2)
    })

    keymap = {
        "combo": "동시적용 기대수익(%)",
        "prob": "상승확률(%)",
        "ret": "예측수익률(%)",
        "score": "예측수익률(%)"
    }
    sort_key = keymap.get(rank_by.lower(), "동시적용 기대수익(%)")

    df_out = df_out.sort_values(sort_key, ascending=False).head(top_n)
    return df_out.reset_index(drop=True), payload, db_path, td

# ------------------------------------------------------------
# Gemini 분석
# ------------------------------------------------------------
def get_gemini_analysis(df_out: pd.DataFrame, do_ai: bool) -> str:
    if not do_ai:
        return "[AI] 비활성화됨"

    if not _safe_import_gemini():
        return "[AI] 라이브러리 미설치"

    if not GEMINI_API_KEY:
        return "[AI] API Key 없음"

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)

        model = genai.GenerativeModel("models/gemini-1.5-flash")

        prompt = f"""
아래는 오늘의 Top 리스트입니다. 상승 가능성이 높은 3개 종목과 간단 사유를 제시하세요.

{df_out.to_string(index=False)}
"""

        resp = model.generate_content(prompt)
        return resp.text or "[AI] 결과 없음"
    except Exception as e:
        return f"[AI] 오류: {e}"

# ------------------------------------------------------------
# JSON 저장
# ------------------------------------------------------------
def save_json_payload(
    engine_path: str,
    db_path: str,
    payload: Dict[str, Any],
    df_out: pd.DataFrame,
    ai_text: str,
    rank_by: str,
    top_n: int,
    prediction_date: datetime,
):
    info_dir = r"F:\autostockG\MODELENGINE\INFO\hoj_engine_info"
    os.makedirs(info_dir, exist_ok=True)

    json_name = os.path.basename(engine_path).replace(".pkl", ".json")
    json_path = os.path.join(info_dir, json_name)

    engine_meta = dict(payload.get("meta", {}))
    features = payload.get("features", [])

    feat_imps = []
    mr = payload.get("model_reg")
    if mr is not None and hasattr(mr, "feature_importances_"):
        fi = sorted(zip(features, mr.feature_importances_), key=lambda x: x[1], reverse=True)
        feat_imps = [{"name": str(n), "importance": float(v)} for n, v in fi]

    top_records = []
    for i, row in df_out.reset_index(drop=True).iterrows():
        rec = {"순위": i+1}
        for c in df_out.columns:
            rec[c] = _json_safe(row[c])
        top_records.append(rec)

    new_payload = {
        "engine_meta": {
            "engine_path": engine_path,
            "db_path": db_path,
            **engine_meta,
            "features": features,
            "feature_importances": feat_imps,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "prediction_date": prediction_date.strftime("%Y-%m-%d"),
            "rank_by": rank_by,
            "topk": top_n,
        },
        "top10": top_records,
        "ai_report": ai_text,
    }

    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                old = json.load(f)
        except:
            old = {}
        data_to_save = _dict_merge_safe(old, new_payload)
    else:
        data_to_save = new_payload

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, ensure_ascii=False, indent=2)

    print(f"[SAVE] JSON: {json_path}")
    return json_path

# ------------------------------------------------------------
# 메인
# ------------------------------------------------------------
def main(
    rank_by="combo",
    topk=10,
    version="V31",
    engine_path=None,
    date_str=None,
    ai_flag=0,
):

    if engine_path:
        eng = engine_path
        print("[Mode] 수동 엔진")
    else:
        print("[Mode] 자동탐색")
        eng = find_engine_real()

    df_out, payload, db_path, pred_dt = run_prediction_core(
        engine_path=eng,
        target_date=date_str,
        top_n=topk,
        rank_by=rank_by,
        version_override=version,
    )

    ai_text = get_gemini_analysis(df_out, bool(ai_flag))

    save_json_payload(
        engine_path=eng,
        db_path=db_path,
        payload=payload,
        df_out=df_out,
        ai_text=ai_text,
        rank_by=rank_by,
        top_n=topk,
        prediction_date=pred_dt,
    )

    print("="*60)
    print(df_out.to_string(index=False))
    print("="*60)
    print(ai_text)
    print("="*60)
    print(f"[ENGINE] {os.path.basename(eng)}")
    print(f"[DB]     {os.path.basename(db_path)}")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank_by", default="combo")
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--version", default="V31")
    ap.add_argument("--engine", type=str, default=None)
    ap.add_argument("--date", type=str, default=None)
    ap.add_argument("--ai", type=int, default=0)

    args = ap.parse_args()
    main(
        rank_by=args.rank_by,
        topk=args.topk,
        version=args.version,
        engine_path=args.engine,
        date_str=args.date,
        ai_flag=args.ai,
    )
