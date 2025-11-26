# ============================================================
# Unified HOJ Trainer (V33 - Input Window Patch)
#   - 통합 DB(HOJ_DB_V31.parquet) 하나로 Real/Research 모두 처리
#   - 동적 타겟 생성 (Horizon 자유 조절)
#   - Input Window 필터링 (설정된 기간보다 긴 지표 자동 제외)
#   * 본 파일은 기능 변경 없이 주석/로그/정렬만 정리한 클린 버전입니다.
# ============================================================

import os
import sys
import pickle
import argparse
import re
import glob  # [추가] 파일 존재 여부 확인용
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import lightgbm as lgb

# ------------------------------------------------------------
# 1. 프로젝트 환경 설정 (기존 유틸 연결)
# ------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)   # MODELENGINE
root_dir = os.path.dirname(parent_dir)      # Root
sys.path.append(root_dir)

try:
    from MODELENGINE.UTIL.config_paths import get_path
    from MODELENGINE.UTIL.version_utils import backup_existing_file
except ImportError:
    # UTIL 폴더 내부에서 실행될 경우 대비
    sys.path.append(parent_dir)
    from UTIL.config_paths import get_path
    from UTIL.version_utils import backup_existing_file

# 최신 날짜 태그가 붙은 DB 파일 자동 탐색 (기존 동작 유지)
from MODELENGINE.UTIL.version_utils import find_latest_file


# ------------------------------------------------------------
# 2. 핵심 함수 정의 (로직 변경 없음)
# ------------------------------------------------------------
def get_db_path(version: str = "V31") -> str:
    """
    HOJ_DB 디렉토리에서 version 태그가 붙은 최신 parquet 파일을 우선 탐색.
    없으면 기본 파일명(HOJ_DB_{version}.parquet) 경로를 반환.
    """
    base_dir = get_path("HOJ_DB")
    # REAL/RESEARCH 하위로 내려가 있는 경우 상위로 보정
    if "REAL" in base_dir or "RESEARCH" in base_dir:
        base_dir = os.path.dirname(base_dir)

    db_name = f"HOJ_DB_{version}.parquet"
    db_path = os.path.join(base_dir, db_name)

    latest_db = find_latest_file(base_dir, f"HOJ_DB_{version}")
    return latest_db if latest_db else db_path


def ensure_datetime(df: pd.DataFrame, col: str = "Date") -> pd.DataFrame:
    """Date 컬럼을 datetime으로 보정."""
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def build_dynamic_target(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """
    요청한 Horizon(예: 5일)에 맞는 정답지(Return_{h}d, Label_{h}d)가 없으면 즉시 생성.
    """
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    if ret_col in df.columns and lab_col in df.columns:
        return df

    print(f"[Target] '{ret_col}' 생성 중 (Horizon={horizon})...")
    if "Close" not in df.columns:
        raise KeyError("DB에 'Close' 컬럼이 없어 타겟을 생성할 수 없습니다.")

    df = df.sort_values(["Code", "Date"]).copy()
    df[ret_col] = df.groupby("Code")["Close"].shift(-horizon) / df["Close"] - 1.0
    df[lab_col] = (df[ret_col] > 0).astype(int)
    return df


def get_save_filename(
    mode: str,
    version: str,
    data_date: str,
    horizon: int,
    input_window: int,
    n_estimators: int,
    train_date: str,
) -> str:
    """
    파일명 규칙: HOJ_ENGINE_{MODE}_{VER}_d{yyyymmdd}_h{H}_w{IW or Full}_n{N}_t{yyMMdd}.pkl
    (원본 규칙 유지, 주석만 명확화)
    """
    iw_tag = f"w{input_window}" if input_window > 0 else "wFull"
    name = (
        f"HOJ_ENGINE_{mode.upper()}_{version}_"
        f"d{data_date}_"
        f"h{horizon}_"
        f"{iw_tag}_"
        f"n{n_estimators}_"
        f"t{train_date}"
        ".pkl"
    )
    return name


# ------------------------------------------------------------
# 3. 메인 트레이닝 로직 (기능/로직 변경 없음)
# ------------------------------------------------------------
def run_unified_training(
    mode: str = "research",
    horizon: int = 5,
    input_window: int = 60,
    valid_days: int = 365,
    n_estimators: int = 1000,
    version: str = "V31",
) -> None:
    mode = mode.lower()
    print("\n=== [HOJ Engine Factory V33] 시작 =========================")
    print(f"[Config] Mode={mode.upper()} | Horizon={horizon}d | InputWindow={input_window}d | Valid={valid_days}d")

    # [A] 통합 DB 로드
    db_path = get_db_path(version)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {db_path}")

    print(f"[Load] DB: {os.path.basename(db_path)}")
    df = pd.read_parquet(db_path)
    df = ensure_datetime(df)
    df = df.sort_values(["Date", "Code"]).reset_index(drop=True)

    # [B] 데이터 정보
    min_date = df["Date"].min().date()
    max_date_obj = df["Date"].max()
    max_date = max_date_obj.date()
    data_date_tag = max_date.strftime("%y%m%d")
    print(f"[Info] 데이터 기간: {min_date} ~ {max_date} | Rows={len(df):,}")

    # --------------------------------------------------------------------------
    # [추가] 중복 파일 확인 (Skip Logic)
    # 데이터 날짜(d), 모드, 옵션이 모두 같으면 이미 생성된 것으로 간주하고 SKIP
    # --------------------------------------------------------------------------
    # 파일명 패턴 생성 (t는 *로 와일드카드 처리)
    check_filename = get_save_filename(
        mode=mode,
        version=version,
        data_date=data_date_tag,
        horizon=horizon,
        input_window=input_window,
        n_estimators=n_estimators,
        train_date="*"
    )
    save_dir_check = get_path("HOJ_ENGINE", mode.upper())
    search_pattern = os.path.join(save_dir_check, check_filename)
    
    # 해당 패턴의 파일이 하나라도 있으면 SKIP
    existing_files = glob.glob(search_pattern)
    if existing_files:
        print("=" * 60)
        print(f"🛑 [SKIP] 동일 조건의 엔진이 이미 존재합니다.")
        print(f"   조건: Mode={mode.upper()}, Date={data_date_tag}, H={horizon}, W={input_window}, N={n_estimators}")
        print(f"   발견된 파일: {os.path.basename(existing_files[0])}")
        print("=" * 60)
        return  # 함수 종료
    # --------------------------------------------------------------------------

    # [C] 타겟(정답) 준비
    df = build_dynamic_target(df, horizon)
    ret_col = f"Return_{horizon}d"
    lab_col = f"Label_{horizon}d"

    # [D] 피처 선정 (기본 제외 목록 유지)
    exclude_cols = [
        "Code", "Date", "Name", "Market",
        "Open", "High", "Low", "Close", "Volume", "Amount", "Marcap",
        "KOSPI_종가", "KOSPI_수익률",
        ret_col, lab_col, f"Expected_{ret_col}",
    ]
    # 다른 horizon 라벨들도 학습에서 배제 (원본 로직 유지)
    exclude_cols += [c for c in df.columns if (c.startswith("Return_") or c.startswith("Label_"))]

    feature_cols = df.columns.difference(exclude_cols).tolist()
    feature_cols = df[feature_cols].select_dtypes(include=["number", "bool"]).columns.tolist()

    # [D-1] Input Window에 따른 피처 필터링 (원본 로직/동작 동일)
    if input_window > 0:
        print(f"[Filter] InputWindow={input_window} 적용 중 (기간이 더 긴 지표 제외)...")
        final_features = []
        dropped_features = []

        for col in feature_cols:
            nums = re.findall(r"\d+", col)  # 끝의 숫자를 기간으로 간주
            if nums:
                period = int(nums[-1])
                if period > input_window:
                    dropped_features.append(col)
                    continue
            final_features.append(col)

        if dropped_features:
            print(f"         제외({len(dropped_features)}): {dropped_features}")
        feature_cols = final_features

    if not feature_cols:
        raise ValueError("학습할 피처가 없습니다. Input Window 설정 또는 DB 컬럼을 확인하세요.")

    print(f"[Feat] 최종 학습 피처 수: {len(feature_cols)}")

    # [E] 결측 제거 (선택된 피처에 대해서만 dropna → 원본 동작)
    mask = df[feature_cols].notnull().all(axis=1) & df[ret_col].notnull()
    df_train = df[mask].copy()
    print(f"[Data] NaN 제거 후 학습 데이터: {len(df_train):,} rows (From {df_train['Date'].min().date()})")

    # [F] 모델 파라미터 및 학습 (원본 동일)
    params_common = {
        "n_estimators": n_estimators,
        "learning_rate": 0.03,
        "num_leaves": 63,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 3,
        "n_jobs": -1,
        "verbose": -1,
        "random_state": 42,
    }

    metrics = {}
    if mode == "research":
        split_date = max_date_obj - timedelta(days=valid_days)
        print(f"[Split] Research 모드 검증 분리: split={split_date.date()}")

        mask_tr = df_train["Date"] < split_date
        mask_va = df_train["Date"] >= split_date

        X_tr = df_train.loc[mask_tr, feature_cols]
        y_tr_reg = df_train.loc[mask_tr, ret_col]
        y_tr_cls = df_train.loc[mask_tr, lab_col]

        X_va = df_train.loc[mask_va, feature_cols]
        y_va_reg = df_train.loc[mask_va, ret_col]
        y_va_cls = df_train.loc[mask_va, lab_col]

        print(f"[Size] Train={len(X_tr):,} | Valid={len(X_va):,}")

        print("[Train] 회귀(Reg) & 분류(Cls) 학습...")
        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg, eval_set=[(X_va, y_va_reg)], callbacks=[lgb.early_stopping(100, verbose=False)])

        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls, eval_set=[(X_va, y_va_cls)], callbacks=[lgb.early_stopping(100, verbose=False)])

        rmse = np.sqrt(np.mean((model_reg.predict(X_va) - y_va_reg) ** 2))
        acc = np.mean(model_cls.predict(X_va) == y_va_cls)
        print(f"[Eval] RMSE={rmse:.5f} | ACC={acc:.2%}")
        metrics = {"rmse": rmse, "acc": acc}

    else:
        print("[Train] Real 모드: 전체 데이터로 학습...")
        X_tr = df_train[feature_cols]
        y_tr_reg = df_train[ret_col]
        y_tr_cls = df_train[lab_col]

        model_reg = lgb.LGBMRegressor(objective="regression", metric="rmse", **params_common)
        model_reg.fit(X_tr, y_tr_reg)

        model_cls = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", **params_common)
        model_cls.fit(X_tr, y_tr_cls)

        metrics = {"note": "Real mode full train"}

    # [G] 저장 (규칙/동작 동일)
    train_date_tag = datetime.now().strftime("%y%m%d")
    save_name = get_save_filename(
        mode=mode,
        version=version,
        data_date=data_date_tag,
        horizon=horizon,
        input_window=input_window,
        n_estimators=n_estimators,
        train_date=train_date_tag,
    )

    save_dir = get_path("HOJ_ENGINE", mode.upper())
    save_path = os.path.join(save_dir, save_name)

    payload = {
        "model_reg": model_reg,
        "model_cls": model_cls,
        "features": feature_cols,
        "meta": {
            "mode": mode,
            "version": version,
            "horizon": horizon,
            "input_window": input_window,
            "n_estimators": n_estimators,
            "data_date": str(max_date),
            "train_date": str(datetime.now().date()),
            "metrics": metrics,
        },
    }

    with open(save_path, "wb") as f:
        pickle.dump(payload, f)

    print(f"[Save] 엔진 저장 완료: {save_name}")
    print("=== [HOJ Engine Factory V33] 완료 =========================\n")


# ------------------------------------------------------------
# 4. CLI 실행부 (수정: Research -> Real 순차 자동 실행)
# ------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="all", help="기본값 all: Research 실행 후 Real 자동 실행")
    parser.add_argument("--horizon", type=int, default=5, help="예측할 미래 일수 (예: 5)")
    parser.add_argument("--input_window", type=int, default=60, help="입력 관찰 기간 (예: 60, 0이면 전체)")
    parser.add_argument("--valid_days", type=int, default=365)
    parser.add_argument("--n_estimators", type=int, default=1000)
    parser.add_argument("--version", type=str, default="V31")

    args = parser.parse_args()

    try:
        # [변경] 사용자가 --mode real 이라고 명시하지 않는 한, 기본적으로 Research -> Real 순서로 둘 다 실행
        modes_to_run = []
        
        if args.mode.lower() == "all" or args.mode.lower() == "research":
            modes_to_run.append("research")
        
        if args.mode.lower() == "all" or args.mode.lower() == "real":
            modes_to_run.append("real")
            
        # 순차 실행 (Research 먼저, 그 다음 Real)
        for m in modes_to_run:
            print(f"\n🚀 [Pipeline] {m.upper()} 엔진 학습 프로세스 진입...")
            run_unified_training(
                mode=m,
                horizon=args.horizon,
                input_window=args.input_window,
                valid_days=args.valid_days,
                n_estimators=args.n_estimators,
                version=args.version,
            )
            
    except Exception as e:
        print(f"\n[Error] {e}")