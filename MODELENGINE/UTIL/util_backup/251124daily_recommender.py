# daily_recommender.py
# [V27-Fixed 4 + Hybrid AI] : '15개 피처 호엔진 + Gemini 분석'

import pandas as pd
import joblib
import os
import sys
from datetime import datetime
import time
import google.generativeai as genai  # Gemini API

# ==========================================
# [필수 설정]    AI Studio API Key 입력 (파일에서 읽어오기)
# ==========================================
def load_api_key():
    """외부 텍스트 파일에서 API 키를 읽어옵니다."""
    key_path = r"C:\공유주방\!개인폴더\!이호정이사\각종key_appkey_decret\googlegemini_api.txt"
    try:
        if os.path.exists(key_path):
            with open(key_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        else:
            print(f"⚠ [Warning] 키 파일을 찾을 수 없습니다: {key_path}")
    except Exception as e:
        print(f"⚠ [Error] 키 파일 읽기 실패: {e}")
    return None

GEMINI_API_KEY = load_api_key()
# ==========================================

# --- [MODELENGINE 경로 설정] ---
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
MODELENGINE_DIR = os.path.join(PROJECT_ROOT, "MODELENGINE")
UTIL_DIR = os.path.join(MODELENGINE_DIR, "UTIL")
if UTIL_DIR not in sys.path:
    sys.path.append(UTIL_DIR)

from config_paths import get_path
from version_utils import find_latest_file


# 최신 모델 및 DB 파일 자동 검색
MODEL_FILE = find_latest_file(
    get_path("HOJ_ENGINE", "REAL"),
    "HOJ_ENGINE_REAL_V31",
    extension=".pkl"
)
DB_FILE = find_latest_file(
    get_path("HOJ_DB"),
    "HOJ_DB_V31",
    extension=".parquet"
)

# --- 기본 사용 피처(그대로 유지) ---
DEFAULT_FEATURES = [
    "Change",
    "SMA_5", "SMA_20", "SMA_60",
    "VOL_SMA_20",
    "MOM_10", "ROC_20",
    "MACD_12_26", "MACD_SIGNAL_9",
    "BBP_20",
    "ATR_14",
    "STOCH_K", "STOCH_D",
    "CCI_20",
    "ALPHA_SMA_20",
]

FEATURES = DEFAULT_FEATURES.copy()


def get_latest_data(df):
    """DB에서 최신 날짜만 분리"""
    try:
        df['Date'] = pd.to_datetime(df['Date'])
        latest_date = df['Date'].max()
        print(f"  > 'Date' 컬럼 기준 최신 날짜: {latest_date.strftime('%Y-%m-%d')} 데이터를 사용합니다.")
        latest_df = df[df['Date'] == latest_date].copy()
        return latest_df, latest_date
    except Exception as e:
        print(f"    오류: DB 날짜 파싱 실패. {e}")
        return None, None


def predict_top10(model, latest_df, features):
    """HOJ 엔진 예측 Top10"""
    try:
        valid_features = [f for f in features if f in latest_df.columns]

        if len(valid_features) != len(features):
            print(f"[경고] '15개 피처 체계' 일부가 DB에 없습니다. 반드시 점검 필요.")
            missing = [f for f in features if f not in latest_df.columns]
            print(f"   > 누락된 피처: {missing}")
            sys.exit(1)

        print(f"[INFO] 총 {len(valid_features)}개 피처로 예측 실행...")
        X_latest = latest_df[valid_features]

        probabilities = model.predict_proba(X_latest)
        latest_df['Pred_Prob'] = probabilities[:, 1]

        final_df = latest_df.sort_values(by='Pred_Prob', ascending=False).head(10)

        if 'Close' in final_df.columns:
            final_df['ClosePrice'] = final_df['Close']

        final_df['Pred_Prob(%)'] = (final_df['Pred_Prob'] * 100).round(2)

        output_column_name = 'Name' if 'Name' in final_df.columns else '종목명'

        result = final_df[[output_column_name, 'Code', 'ClosePrice', 'Pred_Prob(%)']].copy()
        result = result.rename(columns={output_column_name: '종목명'})
        return result

    except Exception as e:
        print(f"   오류: Top 10 생성 실패. {e}")
        return None


def analyze_with_gemini(df):
    """Gemini를 이용한 AI 포트폴리오 분석"""
    print("\n" + "="*60)
    print("[Gemini AI] Top10 종목에 대한 AI 분석 시작")
    print("="*60)

    if not GEMINI_API_KEY:
        print("[SKIP] API Key 없음 (파일 로드 실패).")
        return

    try:
        genai.configure(api_key=GEMINI_API_KEY)

        # 가장 빠른 모델 자동 선택
        all_models = list(genai.list_models())
        valid_model_name = None

        for m in all_models:
            if 'generateContent' in m.supported_generation_methods:
                if 'flash' in m.name:
                    valid_model_name = m.name
                    print(f"[INFO] Flash 모델 자동선택: {valid_model_name}")
                    break

        if valid_model_name is None:
            for m in all_models:
                if 'generateContent' in m.supported_generation_methods:
                    if 'pro' in m.name:
                        valid_model_name = m.name
                        print(f"[INFO] Flash가 없어 Pro 모델 사용: {valid_model_name}")
                        break

        if valid_model_name is None:
            valid_model_name = "models/gemini-1.5-flash"
            print("[INFO] 기본 Flash 모델 사용")

        model = genai.GenerativeModel(valid_model_name)

        target_list_str = df.to_string(index=False)
        prompt = f"""
아래는 오늘의 HOJ Top10 종목 리스트입니다. 
이 종목들을 기반으로 상승 가능성이 높은 종목을 3개 추천해 주세요.

[Top10 종목 데이터]
{target_list_str}

[요구사항]
- 추천 사유 1~2줄 포함
- 상승 가능성이 높은 순서대로 3개만 제시

[형식]
=== Gemini's Pick ===
1. 종목명: 사유
2. 종목명: 사유
3. 종목명: 사유
"""

        print(f"[Gemini] 모델 '{valid_model_name}' 분석 실행 중...")

        response = model.generate_content(prompt)

        print("\n" + "="*60)
        print("   [Gemini AI 결과]")
        print("="*60 + "\n")
        print(response.text)
        print("\n" + "-"*60)

    except Exception as e:
        print(f"⚠ Gemini 오류 발생: {e}")
        print("사용 가능한 모델 목록:")
        try:
            for m in genai.list_models():
                print(" -", m.name)
        except:
            pass


if __name__ == "__main__":
    # 1) 모델 불러오기
    try:
        print(f"[0] HOJ 엔진 로드 중... ({MODEL_FILE})")
        engine_data = joblib.load(MODEL_FILE)

        if isinstance(engine_data, dict) and "model_cls" in engine_data:
            model = engine_data["model_cls"]
            features = engine_data.get("features", DEFAULT_FEATURES)
        else:
            model = engine_data
            features = DEFAULT_FEATURES

        print("[OK] 모델 로드 완료.")
    except Exception as e:
        print(f"[ERROR] 모델 로드 실패: {e}")
        sys.exit(1)

    # 2) DB 불러오기
    try:
        print(f"[1] HOJ REAL DB 로드 중... ({DB_FILE})")
        start = time.time()
        df = pd.read_parquet(DB_FILE)
        print(f"[OK] DB 로드 완료. (총 {len(df)}행, {time.time() - start:.1f}초)")
    except Exception as e:
        print(f"[ERROR] DB 로드 실패: {e}")
        sys.exit(1)

    # 3) 최신 날짜 필터링
    latest_df, latest_date = get_latest_data(df)
    if latest_df is None:
        sys.exit(1)

    # 4) Top10 예측
    top10_df = predict_top10(model, latest_df, features)
    if top10_df is None:
        sys.exit(1)

    date_str = latest_date.strftime('%Y-%m-%d')
    print("\n" + "=" * 80)
    print(f"📈  '{date_str}' HOJ 예측 Top 10")
    print("=" * 80)

    print(top10_df.to_string(index=False))
    print("=" * 80)

    # 5) CSV 저장
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    outname = f"recommendation_HOJ_V31_{date_str}_{timestamp}.csv"

    try:
        top10_df.to_csv(outname, index=False, encoding='utf-8-sig')
        print(f"[SAVE] 결과 저장 완료: {outname}")
    except Exception as e:
        print(f"[ERROR] CSV 저장 실패: {e}")

    # 6) Gemini 분석
    try:
        analyze_with_gemini(top10_df)
    except Exception as e:
        print(f"[WARN] Gemini 분석 스킵: {e}")