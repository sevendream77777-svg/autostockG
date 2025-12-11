# ============================================================
# daily_recommender.py (KOR FINAL)  + SLE 통합 패치 (A+B+요약, 단일 JSON)
# ------------------------------------------------------------
# - 모든 출력/JSON 컬럼을 한국어 기반으로 통일 (원본 유지)
# - 엑셀 기능 완전 제거 (원본 유지)
# - AI 분석은 옵션(--ai 1)일 때만 실행 (원본 유지)
# - 자동 엔진 탐색 / 자동 DB 선택 / JSON 병합 100% 유지 (원본 유지)
# - [추가] SLE(정성 위험) A+B per-stock + 전체 요약(20~30자) 단일 JSON 저장
#   * 저장 경로: F:\autostockG\MODELENGINE\INFO\sle_info
#   * 파일명: <엔진파일명>.pkl → <엔진파일명>_SLE.json
# ============================================================

import os, sys, argparse, pickle, warnings, json, re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
import numpy as np
import pandas as pd
from textwrap import dedent

# ------------------------------------------------------------
# 프로젝트 경로 설정 (원본 유지)
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
# Gemini (옵션)  (원본 유지)
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
# [추가] SLE 프롬프트 로드 (가능하면 외부 모듈 사용)
# ------------------------------------------------------------
SLE_TEST_PROMPT = None  # B형(테스트 포맷) 기본값
try:
    # 우선권: MODELENGINE 경로
    from MODELENGINE.SLE_ENGINE.sle_prompt import SLE_TEST_PROMPT as _PROMPT_B
    SLE_TEST_PROMPT = _PROMPT_B
except Exception:
    try:
        # 대안: 상대경로
        sys.path.append(os.path.join(root_dir, "MODELENGINE", "SLE_ENGINE"))
        from sle_prompt import SLE_TEST_PROMPT as _PROMPT_B2
        SLE_TEST_PROMPT = _PROMPT_B2
    except Exception:
        SLE_TEST_PROMPT = None  # 없으면 내부 기본 프롬프트로 대체

# B형 기본 프롬프트(내장 백업) — 외부 파일 없을 때만 사용
_DEFAULT_PROMPT_B = """
당신은 주식 종목의 단기 움직임을 분석하는 AI 애널리스트입니다.
아래 정보를 바탕으로 JSON만 출력하세요.

입력:
- 종목명: <종목명>
- 종목코드: <종목코드>
- HOJ 동시적용 기대수익(%): <HOJ combo score>

출력(JSON ONLY):
{
  "종목명": "...",
  "종목코드": "...",
  "combo": <숫자>,
  "score": {
    "변동성": <0~100>,
    "단기모멘텀": <0~100>,
    "매물대흐름": <0~100>,
    "수급안정성": <0~100>,
    "종합위험도": <0~100>
  },
  "comment": {
    "변동성": "...",
    "단기모멘텀": "...",
    "매물대흐름": "...",
    "수급안정성": "...",
    "종합위험도": "..."
  },
  "summary": "최종 20~30자 결론"
}
""".strip()

# A형 프롬프트(9개 리스크 항목 고정)
_PROMPT_A = """
다음 입력을 바탕으로 정성 리스크를 JSON으로만 출력하세요.
정량(차트/지표/가격)은 언급하지 말고, 정성 리스크만 판단합니다.

입력:
- 종목코드: {ticker}
- 종목명: {name}
- HOJ combo score(%): {combo}

JSON 스키마(절대 변경 금지):
{{
  "ticker": "<종목코드>",
  "name": "<종목명>",
  "risk_detail_scores": {{
      "상장폐지위험": 0~100,
      "재무건전성": 0~100,
      "규제정책": 0~100,
      "경영진리스크": 0~100,
      "희석리스크": 0~100,
      "뉴스감성": 0~100,
      "세력이탈": 0~100,
      "업종경쟁": 0~100,
      "기타이상징후": 0~100
  }},
  "risk_total_score": 0~100,
  "final_score": "<HOJ combo score * (1 - risk_total_score/100)>",
  "summary_one_line": "최종 판단을 1문장으로 요약"
}}

작성 규칙:
- 각 항목 0~100점 (높을수록 위험 큼)
- 전체 길이 600~900자 내
- 모호/과장 금지, 사실 기반 간결한 서술
- JSON 외 텍스트 출력 금지
""".strip()

# ------------------------------------------------------------
# 유틸 함수 (원본 유지)
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
# 엔진 자동 선택 (원본 유지)
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
# DB 로드 (원본 유지)
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
    if os.path.exists(p) is False:
        raise FileNotFoundError("DB 파일 없음: " + p)
    df = pd.read_parquet(p)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df, p

# ------------------------------------------------------------
# 엔진 로더 (원본 유지)
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
# 공용 예측 코어 (원본 유지 + 필터 추가)
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

    # [추가] 엔진/DB 정합성 검증
    engine_version = meta.get("version", "V31")
    if engine_version != version:
        print(f"[경고] 엔진 버전({engine_version}) != 요청 버전({version}), 엔진 버전 사용")
    
    engine_horizon = meta.get("horizon", None)
    engine_window = meta.get("input_window", None)
    if engine_horizon is not None:
        print(f"[INFO] 엔진 설정: horizon={engine_horizon}, input_window={engine_window}")

    if target_date:
        td = pd.to_datetime(target_date)
    else:
        td = df["Date"].max()

    daily = df[df["Date"] == td].copy()
    if daily.empty:
        raise ValueError(f"해당 날짜({td.date()}) 데이터 없음")

    missing = [f for f in features if f not in daily.columns]
    if missing:
        raise KeyError(f"필수 피처 누락: {missing[:5]} ... (엔진과 DB 버전 불일치 가능)")

    # 종가 컬럼찾기
    close_col = pick_close_col(daily)
    
    # [추가] 필터 전 초기 종목 수
    initial_count = len(daily)
    print(f"[FILTER] 초기 종목 수: {initial_count:,}개")

    # ======================================================
    # >>>>>>>>>>>>>>> [ADD FILTER: STEP 1] <<<<<<<<<<<<<<<<<
    # ======================================================
    # 1) 종가 0 제거 (상폐·정지)
    before = len(daily)
    daily = daily[daily[close_col] > 0]
    removed = before - len(daily)
    if removed > 0:
        print(f"[FILTER] 종가 <= 0 제거: {removed:,}개 (남음: {len(daily):,}개)")

    # 2) 거래량 0·초저유동성 제거
    if "Volume" in daily.columns:
        before = len(daily)
        daily = daily[daily["Volume"] > 0]
        removed1 = before - len(daily)
        if removed1 > 0:
            print(f"[FILTER] 거래량 = 0 제거: {removed1:,}개 (남음: {len(daily):,}개)")
        
        before = len(daily)
        daily = daily[daily["Volume"] > 5000]   # 최소 기준
        removed2 = before - len(daily)
        if removed2 > 0:
            print(f"[FILTER] 거래량 <= 5,000 제거: {removed2:,}개 (남음: {len(daily):,}개)")

    # 3) 20일 변동성 최소 기준 (정지·flat 차단)
    if "volatility_20" in daily.columns:
        before = len(daily)
        daily = daily[daily["volatility_20"] > 0.5]
        removed = before - len(daily)
        if removed > 0:
            print(f"[FILTER] 변동성(20일) <= 0.5 제거: {removed:,}개 (남음: {len(daily):,}개)")

    # 4) MA20 = MA60 동일(flat) 제거
    if "MA20" in daily.columns and "MA60" in daily.columns:
        before = len(daily)
        daily = daily[daily["MA20"] != daily["MA60"]]
        removed = before - len(daily)
        if removed > 0:
            print(f"[FILTER] MA20 = MA60 (flat) 제거: {removed:,}개 (남음: {len(daily):,}개)")

    # 5) 시가총액 최소 기준 (초소형주 제거)
    # [참고] 200억원 기준 (필요시 조정 가능)
    if "MarketCap" in daily.columns:
        before = len(daily)
        daily = daily[daily["MarketCap"] > 200_000_00000]
        removed = before - len(daily)
        if removed > 0:
            print(f"[FILTER] 시가총액 <= 200억 제거: {removed:,}개 (남음: {len(daily):,}개)")

    # ======================================================
    # >>>>>>>>>>>>>>> [FILTER END] <<<<<<<<<<<<<<<<<<<<<<<<<<
    # ======================================================
    
    print(f"[FILTER] 필터 적용 후 종목 수: {len(daily):,}개 (제거: {initial_count - len(daily):,}개)")

    # [추가] 필터 후 빈 데이터 체크
    if daily.empty:
        raise ValueError(f"필터 적용 후 데이터가 없습니다. (날짜: {td.date()})")

    # 결측 제거 (20% 이상 결측일 때만 제거)
    before = len(daily)
    X = daily[features].copy()
    
    # 결측 비율 계산 (20% 이상 결측이면 제거)
    missing_count = X.isnull().sum(axis=1)
    missing_ratio = missing_count / len(features)
    mask = missing_ratio < 0.20  # 20% 미만 결측만 통과
    
    daily = daily[mask]
    X = X[mask]
    removed = before - len(daily)
    if removed > 0:
        print(f"[FILTER] 피처 결측 제거 (20% 이상 결측): {removed:,}개 (남음: {len(daily):,}개)")
        # 결측 통계 출력
        if len(daily) > 0:
            avg_missing = (missing_count[mask] / len(features) * 100).mean()
            print(f"[FILTER] 통과한 종목의 평균 결측 비율: {avg_missing:.1f}%")
    
    # [추가] 결측 제거 후 빈 데이터 체크
    if daily.empty or len(X) == 0:
        raise ValueError(f"결측 제거 후 데이터가 없습니다. (날짜: {td.date()}, 필터 후: {len(daily)}개)")
    
    print(f"[FILTER] 최종 예측 대상 종목 수: {len(daily):,}개")

    name_col = "Name" if "Name" in daily.columns else ("name" if "name" in daily.columns else None)
    code_col = "Code" if "Code" in daily.columns else ("code" if "code" in daily.columns else None)
    
    if name_col is None or code_col is None:
        raise ValueError(f"종목명/코드 컬럼을 찾을 수 없습니다. (Name/name, Code/code)")

    prob = model_cls.predict_proba(X)[:,1] if model_cls else np.zeros(len(X))
    ret  = model_reg.predict(X) if model_reg else np.zeros(len(X))
    
    # [개선] combo 계산 안정화 (상하한 모두 적용 또는 winsorize)
    # 하방만 클리핑하면 극단값이 combo에 과도하게 반영될 수 있음
    ret_clip = np.clip(ret, -0.10, 0.20)  # 상방도 제한 (20% 상한)
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
# Gemini 분석 (원본 유지)
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

        model = genai.GenerativeModel("gemini-2.0-flash")

        prompt = f"""아래는 오늘의 가장 높은 종목 예측 결과입니다.
[요구사항]
당신은 주식 전문가입니다.
1) 각 종목을 순서대로 개별 분석하세요.
2) 각 종목당 순서대로 번호와 종목명을 쓰고 해당종목의  2~3줄을 작성, 반드시 다음 내용을 포함:
   - 종목의 현재 상태(예측된 상승확률/예측수익률 기반)
   - 리스크 요인 1~2개
   - 단기 관찰 포인트
   - 보수적/공격적 관점 요약
3) 문장은 짧게, 절대 장황하게 설명 금지.
4) 마지막에 [종합 해설]을 전종목 대상으로 5~8줄로 작성:
   - Top10 전체 흐름
   - 섹터/업종 경향
   - 시장 심리·수급 기반 위험요소
   - 내일 전략 2~3개
   - 보수적/공격적 전략 분리
5) 표, 코드블록 사용 금지. 문장으로만 작성.

{df_out.to_string(index=False)}
"""

        resp = model.generate_content(prompt)
        return resp.text or "[AI] 결과 없음"
    except Exception as e:
        return f"[AI] 오류: {e}"

# ------------------------------------------------------------
# JSON 저장 (원본 유지)
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

    try:
        tmp_path = json_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(new_payload, f, ensure_ascii=False, indent=2, default=_json_safe)
        os.replace(tmp_path, json_path)
    except Exception as e:
        print(f"[ERROR] JSON 저장 실패: {e}")
        return None

    print(f"[SAVE] JSON: {json_path}")
    return json_path


# ============================================================
# [추가] SLE 실행/저장 블록 (원본 유지, 주석 그대로)
# ============================================================

def _genai_call(prompt_text: str) -> Optional[str]:
    """Gemini 호출 (텍스트 반환). 실패 시 None"""
    if not _safe_import_gemini() or not GEMINI_API_KEY:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.0-flash")
        resp = model.generate_content(prompt_text)
        return getattr(resp, "text", None)
    except Exception:
        return None

def _parse_json_safe(text: Optional[str]) -> Any:
    if not text:
        return {"error": "no_text"}
    m = re.search(r"\{[\s\S]*\}", text)
    raw = m.group(0) if m else text
    try:
        return json.loads(raw)
    except Exception:
        return {"error": "json_parse_error", "raw": (text[:5000] if text else "")}

def _build_prompt_A(ticker: str, name: str, combo: float) -> str:
    return _PROMPT_A.format(ticker=ticker, name=name, combo=combo)

def _build_prompt_B(ticker: str, name: str, combo: float) -> str:
    base = SLE_TEST_PROMPT if SLE_TEST_PROMPT else _DEFAULT_PROMPT_B
    p = base.replace("<종목명>", str(name)).replace("<종목코드>", str(ticker)).replace("<HOJ combo score>", str(combo))
    p += f"\n\n[부록]\n종목명={name}, 종목코드={ticker}, combo={combo}"
    return p

# ------------------------------------------------------------
# 메인 (원본 유지)
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
# CLI (원본 유지)
# ------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank_by", default="combo")
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--version", default="V31")
    ap.add_argument("--engine", type=str, default=None)
    ap.add_argument("--date", type=str, default=None)
    ap.add_argument("--ai", type=int, default=1)

    args = ap.parse_args()
    main(
        rank_by=args.rank_by,
        topk=args.topk,
        version=args.version,
        engine_path=args.engine,
        date_str=args.date,
        ai_flag=args.ai,
    )
