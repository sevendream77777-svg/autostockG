# ============================================================
# Predict Top 10 (Stage 3) - Inference Engine
#   - 저장된 엔진(.pkl)을 로드하여 특정 날짜의 Top 10 종목 추천
#   - 엔진 내부에 저장된 피처 리스트를 자동으로 사용하여 안전함
# ============================================================

import os
import sys
import pickle
import argparse
import pandas as pd
import numpy as np

# ------------------------------------------------------------
# 1. 프로젝트 환경 설정
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # MODELENGINE
root_dir = os.path.dirname(parent_dir)     # Root
sys.path.append(root_dir)

try:
    from MODELENGINE.UTIL.config_paths import get_path
except ImportError:
    sys.path.append(parent_dir)
    from UTIL.config_paths import get_path

# ------------------------------------------------------------
# 2. 핵심 예측 함수
# ------------------------------------------------------------
def load_engine(engine_path):
    """엔진 파일(.pkl)을 로드하고 모델과 메타데이터 반환"""
    if not os.path.exists(engine_path):
        raise FileNotFoundError(f"❌ 엔진 파일을 찾을 수 없습니다: {engine_path}")
    
    with open(engine_path, "rb") as f:
        data = pickle.load(f)
    
    # 구버전/신버전 호환성 체크
    if "meta" not in data or "features" not in data:
        # 구버전(V30 이하)일 경우 예외처리 필요할 수 있음
        print("⚠️ 주의: 구버전 엔진 형식이거나 메타데이터가 부족합니다.")
    
    return data

def get_unified_db_path(version="V31"):
    """통합 DB 경로 반환"""
    # 기본적으로 MODELENGINE/HOJ_DB/HOJ_DB_V31.parquet 위치 가정
    base = get_path("HOJ_DB")
    # 혹시 REAL/RESEARCH 하위폴더가 경로에 잡혀있다면 상위로 이동
    if "REAL" in base or "RESEARCH" in base:
        base = os.path.dirname(base)
    return os.path.join(base, f"HOJ_DB_{version}.parquet")

def run_prediction(engine_path, target_date=None, top_n=10):
    """
    특정 엔진으로 특정 날짜의 Top N 종목 추천
    """
    print(f"\n=== 🔮 [Prediction] Top {top_n} 종목 추천 시작 ===")
    print(f"  ⚙️ 엔진: {os.path.basename(engine_path)}")

    # [A] 엔진 로드
    engine_data = load_engine(engine_path)
    model_reg = engine_data.get("model_reg")
    model_cls = engine_data.get("model_cls")
    required_features = engine_data.get("features", [])
    meta = engine_data.get("meta", {})
    
    print(f"  🧬 필요 피처: {len(required_features)}개 (from Engine Meta)")
    
    # [B] 데이터 로드 (통합 DB)
    # 엔진 버전에 맞는 DB 찾기 (메타에 없으면 파일명이나 기본값 V31 사용)
    version = meta.get("version", "V31")
    db_path = get_unified_db_path(version)
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ DB 파일을 찾을 수 없습니다: {db_path}")
        
    print(f"  📂 DB 로딩 중: {os.path.basename(db_path)} ...")
    df = pd.read_parquet(db_path)
    
    # 날짜 변환 및 필터링
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
    
    # target_date가 없으면 DB의 가장 최근 날짜 사용
    if target_date is None:
        target_date = df["Date"].max()
    else:
        target_date = pd.to_datetime(target_date)
        
    target_date_str = target_date.strftime('%Y-%m-%d')
    print(f"  📅 예측 기준일: {target_date_str}")

    # 해당 날짜 데이터 추출
    daily_df = df[df["Date"] == target_date].copy()
    
    if daily_df.empty:
        print(f"❌ 해당 날짜({target_date_str})의 데이터가 DB에 없습니다.")
        return None

    # [C] 피처 유효성 검사 및 준비
    # 엔진이 학습할 때 썼던 피처가 현재 DB에 다 있는지 확인
    missing_feats = [f for f in required_features if f not in daily_df.columns]
    if missing_feats:
        raise KeyError(f"❌ DB에 다음 필수 피처가 누락되었습니다: {missing_feats[:3]}...")
        
    X_test = daily_df[required_features]
    
    # NaN 체크 (예측 불가 데이터 제외)
    valid_mask = X_test.notnull().all(axis=1)
    if not valid_mask.all():
        drop_count = len(daily_df) - valid_mask.sum()
        print(f"  ⚠️ 결측치로 인해 {drop_count}개 종목 제외됨")
        daily_df = daily_df[valid_mask]
        X_test = X_test[valid_mask]

    if len(daily_df) == 0:
        print("❌ 예측 가능한 종목이 없습니다 (전체 결측).")
        return None

    # [D] 예측 수행
    # 1. 회귀 점수 (수익률 예측)
    pred_score = model_reg.predict(X_test)
    daily_df["Pred_Score"] = pred_score
    
    # 2. 분류 확률 (상승 확률) - 모델이 있을 경우만
    if model_cls:
        pred_prob = model_cls.predict_proba(X_test)[:, 1]
        daily_df["Pred_Prob"] = pred_prob
    else:
        daily_df["Pred_Prob"] = 0.0

    # [E] 순위 선정 (Score 내림차순)
    # 필터링 로직 추가 가능 (예: 거래대금 하위 제외, 관리종목 제외 등)
    results = daily_df.sort_values("Pred_Score", ascending=False).head(top_n)
    
    # 결과 정리 (출력용 컬럼 선택)
    display_cols = ["Code", "Name", "Close", "Pred_Score", "Pred_Prob"]
    # DB에 Name이 없으면 Code만 출력
    final_cols = [c for c in display_cols if c in results.columns]
    
    print(f"\n🔥 [{target_date_str}] Top {top_n} 추천 종목 🔥")
    print(results[final_cols].to_string(index=False))
    
    return results[final_cols]

# ------------------------------------------------------------
# 3. CLI 실행부
# ------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", type=str, required=True, help="엔진 파일 경로 (.pkl)")
    parser.add_argument("--date", type=str, default=None, help="예측 날짜 (YYYY-MM-DD), 미입력시 최신일")
    parser.add_argument("--top", type=int, default=10, help="출력할 종목 수")
    
    args = parser.parse_args()
    
    try:
        run_prediction(args.engine, args.date, args.top)
    except Exception as e:
        print(f"\n❌ [Error] {e}")