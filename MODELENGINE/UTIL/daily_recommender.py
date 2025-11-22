# daily_recommender.py
# [V27-Fixed 4차] : '진짜 15개 피처' 커리큘럼 적용

import pandas as pd
import joblib
import os
import sys
from datetime import datetime
import time 

# --- [MODELENGINE 경로 설정] ---
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
MODELENGINE_DIR = os.path.join(PROJECT_ROOT, "MODELENGINE")
UTIL_DIR = os.path.join(MODELENGINE_DIR, "UTIL")
if UTIL_DIR not in sys.path:
    sys.path.append(UTIL_DIR)

from config_paths import get_path  # pylint: disable=wrong-import-position

MODEL_FILE = get_path("HOJ_ENGINE", "REAL", "HOJ_ENGINE_REAL_V31.pkl")
DB_FILE = get_path("HOJ_DB", "REAL", "HOJ_DB_REAL_V31.parquet")
# --------------------

# --- 기본 피처 목록 (엔진 저장값이 우선) ---
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
    try:
        df['Date'] = pd.to_datetime(df['Date'])
        latest_date = df['Date'].max()
        print(f"  > 'Date' 기준 '{latest_date.strftime('%Y-%m-%d')}' 데이터로 '예상 수익률' 예측 중...")
        latest_df = df[df['Date'] == latest_date].copy()
        return latest_df, latest_date
    except Exception as e:
        print(f"오류: V25 DB에서 최신 날짜 데이터를 필터링하는 중 실패. {e}")
        return None, None

def predict_top10(model, latest_df, features):
    try:
        valid_features = [f for f in features if f in latest_df.columns]
        if len(valid_features) != len(features):
             print(f"⚠ 오류: '진짜 15개 피처' 중 일부가 DB에 없습니다. 2/3단계를 확인하세요.")
             missing = [f for f in features if f not in latest_df.columns]
             print(f"   > 누락된 피처: {missing}")
             sys.exit(1)

        print(f"✅ 사용할 {len(valid_features)}개 피처 준비...")
        X_latest = latest_df[valid_features]

        probabilities = model.predict_proba(X_latest)
        latest_df['예측확률'] = probabilities[:, 1]

        final_df = latest_df
        top_10 = final_df.sort_values(by='예측확률', ascending=False).head(10)

        if '종가' not in top_10.columns and 'Close' in top_10.columns:
            top_10['종가'] = top_10['Close']

        top_10['예측확률(%)'] = (top_10['예측확률'] * 100).round(2)

        output_column_name = '종목명' if '종목명' in top_10.columns else 'Name'

        # 출력 정리: 예측확률(%)만 노출, 컬럼명 통일
        result = top_10[[output_column_name, 'Code', '종가', '예측확률(%)']].copy()
        result = result.rename(columns={output_column_name: '종목명'})
        return result

    except Exception as e:
        print(f"경고: Top 10 생성 실패. {e}")
        return None


if __name__ == "__main__":
    try:
        print(f"[0] HOJ 실전 엔진('{MODEL_FILE}') 로드 중...")
        engine_data = joblib.load(MODEL_FILE)

        if isinstance(engine_data, dict) and "model_cls" in engine_data:
            model = engine_data["model_cls"]
            features = engine_data.get("features", DEFAULT_FEATURES)
        else:
            model = engine_data
            features = DEFAULT_FEATURES

        print("✅ 모델 로드 완료.")
    except Exception as e:
        print(f"❌ 치명적 오류: {MODEL_FILE} 로드 실패. {e}")
        sys.exit(1)

    try:
        print(f"[1] '{DB_FILE}' (HOJ REAL DB) 로드 중...")
        start_time = time.time()
        df = pd.read_parquet(DB_FILE)
        print(f"✅ 로드 완료. (총 {len(df)} 행, {time.time() - start_time:.0f}초)")
    except Exception as e:
        print(f"❌ 치명적 오류: {DB_FILE} 로드 실패. {e}")
        sys.exit(1)

    latest_df, latest_date = get_latest_data(df)
    if latest_df is None:
        sys.exit(1)

    top_10_df = predict_top10(model, latest_df, features)
    if top_10_df is None:
        sys.exit(1)

    date_str = latest_date.strftime('%Y-%m-%d')
    print("\n" + "=" * 80)
    print(f"★★★ '{date_str}' HOJ 실전 추천 Top 10 ★★★")
    print("=" * 80)
    # 고정 폭 포맷으로 정렬 출력
    formatters = {
        '종목명': lambda x: f"{str(x):<12}",
        'Code': lambda x: f"{str(x):<6}",
        '종가': lambda x: f"{int(x):>8}",
        '예측확률(%)': lambda x: f"{x:>8.2f}",
    }
    print(top_10_df.to_string(index=False, formatters=formatters))
    print("=" * 80)

    now_str = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    output_filename = f"recommendation_HOJ_V31_{date_str}_{now_str}.csv"
    try:
        top_10_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"✅ 추천 결과가 '{output_filename}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"경고: CSV 저장 실패. {e}")
