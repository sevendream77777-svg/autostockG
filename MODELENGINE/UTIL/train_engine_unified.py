# ============================================================
# Unified HOJ Trainer (V32) - The Engine Factory
#   - 통합 DB(HOJ_DB_V31.parquet) 하나로 Real/Research 모두 처리
#   - 동적 타겟 생성 (Horizon 자유 조절)
#   - 엄격한 파일명 규칙 적용 (d:데이터날짜, t:학습날짜)
# ============================================================

import os
import sys
import pickle
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import lightgbm as lgb

# ------------------------------------------------------------
# 1. 프로젝트 환경 설정 (기존 유틸 연결)
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # MODELENGINE
root_dir = os.path.dirname(parent_dir)     # Root
sys.path.append(root_dir)

try:
    from MODELENGINE.UTIL.config_paths import get_path
    from MODELENGINE.UTIL.version_utils import backup_existing_file
except ImportError:
    # UTIL 폴더 내부에서 실행될 경우를 대비
    sys.path.append(parent_dir)
    from UTIL.config_paths import get_path
    from UTIL.version_utils import backup_existing_file

# ------------------------------------------------------------
# 2. 핵심 함수 정의
# ------------------------------------------------------------

def get_db_path(version="V31"):
    """
    [규칙 변경 반영]
    DB는 이제 'HOJ_DB' 폴더 바로 아래에 통합 파일 하나만 존재함.
    예: MODELENGINE/HOJ_DB/HOJ_DB_V31.parquet
    """
    base_dir = get_path("HOJ_DB") # 보통 .../MODELENGINE/HOJ_DB
    # config_paths가 하위폴더(REAL/RESEARCH)를 가리킬 수 있으므로, 상위로 한 번 보정
    if "REAL" in base_dir or "RESEARCH" in base_dir:
        base_dir = os.path.dirname(base_dir)
        
    db_name = f"HOJ_DB_{version}.parquet"
    db_path = os.path.join(base_dir, db_name)
    return db_path

def ensure_datetime(df, col="Date"):
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def build_dynamic_target(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """
    요청한 Horizon(예: 3일)에 맞는 정답지(Return_3d)가 없으면
    Close 데이터를 이용해 즉석에서 만들어냄.
    """
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    if ret_col in df.columns and lab_col in df.columns:
        return df

    print(f"  ⚡ [Auto-Gen] '{ret_col}' 타겟 생성 중 (Horizon={horizon})...")
    
    if "Close" not in df.columns:
        raise KeyError("DB에 'Close' 컬럼이 없어 타겟을 생성할 수 없습니다.")

    df = df.sort_values(["Code", "Date"]).copy()
    
    # 수익률 = (미래 h일 종가 / 오늘 종가) - 1
    # groupby().shift() 사용
    df[ret_col] = df.groupby("Code")["Close"].shift(-horizon) / df["Close"] - 1.0
    
    # 라벨 = 수익률 > 0 (1 or 0)
    df[lab_col] = (df[ret_col] > 0).astype(int)
    
    return df

def get_save_filename(mode, version, data_date, horizon, n_estimators, train_date):
    """
    [파일명 규칙 확정안]
    HOJ_ENGINE_{MODE}_{VER}_d{DATA}_h{HOR}_n{TREES}_t{TRAIN}.pkl
    """
    name = (
        f"HOJ_ENGINE_{mode.upper()}_{version}_"
        f"d{data_date}_"     # 데이터 마지막 날짜 (Sync Check용)
        f"h{horizon}_"       # 예측 기간
        f"n{n_estimators}_"  # 학습 강도
        f"t{train_date}"     # 실제 학습 수행일
        ".pkl"
    )
    return name

# ------------------------------------------------------------
# 3. 메인 트레이닝 로직
# ------------------------------------------------------------
def run_unified_training(
    mode="research",
    horizon=5,
    valid_days=365,
    n_estimators=1000,
    version="V31"
):
    mode = mode.lower()
    print(f"\n=== 🏭 [HOJ Engine Factory] 가동 시작 ({mode.upper()}) ===")
    print(f"  ⚙️ 설정: Horizon={horizon}d | Valid={valid_days}d | Trees={n_estimators}")

    # [A] 통합 DB 로드
    db_path = get_db_path(version)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ DB 파일을 찾을 수 없습니다: {db_path}\n   -> 1단계(데이터 업데이트)를 먼저 실행하세요.")
    
    print(f"  📂 DB 로딩: {os.path.basename(db_path)}")
    df = pd.read_parquet(db_path)
    df = ensure_datetime(df)
    df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

    # [B] 데이터 정보 확인 (날짜 기준점)
    min_date = df["Date"].min().date()
    max_date_obj = df["Date"].max()
    max_date = max_date_obj.date()
    
    # 파일명에 쓸 'd' 태그 (데이터 날짜)
    data_date_tag = max_date.strftime("%y%m%d")
    
    print(f"  📅 데이터 기간: {min_date} ~ {max_date} (Total {len(df):,} rows)")
    print(f"  🏷️ 데이터 버전 태그: d{data_date_tag}")

    # [C] 타겟(정답) 준비
    df = build_dynamic_target(df, horizon)
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    # [D] 피처 선정 (수치형만 자동 선택, 메타데이터 제외)
    exclude_cols = [
        "Code", "Date", "Name", "Market", 
        "Open", "High", "Low", "Close", "Volume", "Amount", "Marcap",
        "KOSPI_종가", "KOSPI_수익률",
        ret_col, lab_col, f"Expected_{ret_col}"
    ]
    # 다른 horizon 라벨들도 학습에서 배제
    exclude_cols += [c for c in df.columns if (c.startswith("Return_") or c.startswith("Label_"))]

    feature_cols = df.columns.difference(exclude_cols).tolist()
    # float, int, bool 타입만 남기기
    feature_cols = df[feature_cols].select_dtypes(include=["number", "bool"]).columns.tolist()
    
    if not feature_cols:
        raise ValueError("❌ 학습할 피처가 하나도 없습니다! DB 컬럼을 확인하세요.")
    
    print(f"  🧬 학습 피처 ({len(feature_cols)}개): {feature_cols[:5]} ...")

    # [E] 결측 제거 (학습용 데이터셋 생성)
    # 피처나 타겟이 없는 행은 학습 불가 -> 제거
    mask = df[feature_cols].notnull().all(axis=1) & df[ret_col].notnull()
    df_train = df[mask].copy()
    
    print(f"  🧹 NaN 제거 후 학습 샘플: {len(df_train):,} rows")

    # [F] 모드별 분할 및 학습 설정
    X_valid = None
    metrics = {}
    
    params_common = {
        "n_estimators": n_estimators,
        "learning_rate": 0.03,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 3,
        "n_jobs": -1,
        "verbose": -1,
        "random_state": 42
    }

    if mode == "research":
        # 검증셋 분리 (뒤에서 valid_days 만큼)
        split_date = max_date_obj - timedelta(days=valid_days)
        print(f"  🧪 Research Mode: 검증 구간 분리 ({valid_days}일)")
        print(f"     Split Date: {split_date.date()}")
        
        mask_tr = df_train["Date"] < split_date
        mask_va = df_train["Date"] >= split_date
        
        X_tr = df_train.loc[mask_tr, feature_cols]
        y_tr_reg = df_train.loc[mask_tr, ret_col]
        y_tr_cls = df_train.loc[mask_tr, lab_col]
        
        X_va = df_train.loc[mask_va, feature_cols]
        y_va_reg = df_train.loc[mask_va, ret_col]
        y_va_cls = df_train.loc[mask_va, lab_col]
        
        print(f"     Train: {len(X_tr):,} / Valid: {len(X_va):,}")
        
        # 학습 (Early Stopping 적용)
        print("  🤖 회귀 모델(Regressor) 학습 중...")
        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg, eval_set=[(X_va, y_va_reg)], 
                      callbacks=[lgb.early_stopping(100, verbose=False)])
        
        print("  🤖 분류 모델(Classifier) 학습 중...")
        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls, eval_set=[(X_va, y_va_cls)], 
                      callbacks=[lgb.early_stopping(100, verbose=False)])
        
        # 성능 측정
        rmse = np.sqrt(np.mean((model_reg.predict(X_va) - y_va_reg) ** 2))
        acc = np.mean(model_cls.predict(X_va) == y_va_cls)
        print(f"  📊 [검증 결과] RMSE: {rmse:.5f} | ACC: {acc:.2%}")
        metrics = {"rmse": rmse, "acc": acc}
        
        X_valid = X_va # 저장용 참조

    else: # REAL Mode
        print("  🚀 Real Mode: 전체 데이터 학습 (No Valid Split)")
        X_tr = df_train[feature_cols]
        y_tr_reg = df_train[ret_col]
        y_tr_cls = df_train[lab_col]
        
        print("  🤖 전체 데이터 학습 중...")
        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg)
        
        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls)
        
        metrics = {"note": "Real mode trained on full data"}

    # [G] 저장 (파일명 규칙 적용)
    train_date_tag = datetime.now().strftime("%y%m%d") # 오늘 날짜 t
    save_name = get_save_filename(mode, version, data_date_tag, horizon, n_estimators, train_date_tag)
    
    # 저장 경로 (Real / Research 폴더 유지)
    save_dir = get_path("HOJ_ENGINE", mode.upper())
    save_path = os.path.join(save_dir, save_name)
    
    # 메타데이터 패키징
    payload = {
        "model_reg": model_reg,
        "model_cls": model_cls,
        "features": feature_cols,
        "meta": {
            "mode": mode,
            "version": version,
            "horizon": horizon,
            "valid_days": valid_days if mode == 'research' else 0,
            "n_estimators": n_estimators,
            "data_date": str(max_date),
            "train_date": str(datetime.now().date()),
            "metrics": metrics
        }
    }
    
    with open(save_path, "wb") as f:
        pickle.dump(payload, f)
        
    print(f"\n💾 엔진 저장 완료:")
    print(f"   📁 경로: {save_path}")
    print(f"   🏷️ 파일명: {save_name}")
    print("=== 🏁 Factory Operation Complete ===")

# ------------------------------------------------------------
# 4. CLI 실행부
# ------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="research", choices=["real", "research"])
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--valid_days", type=int, default=365)
    parser.add_argument("--n_estimators", type=int, default=1000)
    parser.add_argument("--version", type=str, default="V31")
    
    args = parser.parse_args()
    
    try:
        run_unified_training(
            mode=args.mode,
            horizon=args.horizon,
            valid_days=args.valid_days,
            n_estimators=args.n_estimators,
            version=args.version
        )
    except Exception as e:
        print(f"\n❌ [Error] {e}")