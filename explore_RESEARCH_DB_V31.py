# explore_RESEARCH_DB_V31.py
# --- V31 연구 DB 구조/상태 점검용 스크립트 ---

import pandas as pd
import numpy as np
import os

# 🔧 여기만 실제 파일 경로에 맞게 수정해서 쓰세요
RESEARCH_DB_FILE = r"F:\autostockG\MODELENGINE\HOJ_DB\RESEARCH\HOJ_DB_RESEARCH_V31.parquet"  # 예시

def main():
    if not os.path.exists(RESEARCH_DB_FILE):
        print(f"❌ 오류: 연구 DB 파일을 찾을 수 없습니다.\n -> 경로를 확인해주세요: {RESEARCH_DB_FILE}")
        return

    print(f"[1] 연구 DB(V31) 로드 중...\n    파일: {RESEARCH_DB_FILE}")
    df = pd.read_parquet(RESEARCH_DB_FILE)
    print(f"✅ 로드 완료: {df.shape[0]:,} 행, {df.shape[1]:,} 컬럼\n")

    # --- 1. 기본 메타 정보 ---
    print("[2] 기본 정보")
    print("- 상위 5행 미리보기:")
    print(df.head(5))
    print("\n- 컬럼 리스트:")
    print(list(df.columns))
    print()

    # --- 2. 핵심 키 컬럼 자동 감지 ---
    print("[3] 핵심 컬럼 체크 (Code / 종목 / 날짜 / 수익률 / 라벨 등)")

    candidate_code_cols = ["Code", "code", "종목코드", "ticker"]
    candidate_date_cols = ["Date", "date", "날짜", "dt"]
    candidate_close_cols = ["Close", "close", "종가"]
    candidate_label_cols = ["Label_5d", "label_5d", "Label", "label"]
    candidate_target_cols = [
        "Expected_Return_5d", "Return_5d",
        "expected_return_5d", "return_5d"
    ]

    def find_existing(candidates):
        return [c for c in candidates if c in df.columns]

    code_cols = find_existing(candidate_code_cols)
    date_cols = find_existing(candidate_date_cols)
    close_cols = find_existing(candidate_close_cols)
    label_cols = find_existing(candidate_label_cols)
    target_cols = find_existing(candidate_target_cols)

    print(f" - 종목 코드 후보: {code_cols}")
    print(f" - 날짜 컬럼 후보: {date_cols}")
    print(f" - 종가 컬럼 후보: {close_cols}")
    print(f" - 라벨 컬럼 후보: {label_cols}")
    print(f" - 타깃(수익률) 컬럼 후보: {target_cols}")
    print()

    # --- 3. 날짜/종목 범위 요약 ---
    print("[4] 날짜 / 종목 범위 요약")
    code_col = code_cols[0] if code_cols else None
    date_col = date_cols[0] if date_cols else None

    if date_col is not None:
        try:
            df[date_col] = pd.to_datetime(df[date_col])
        except Exception:
            pass

        print(f" - 날짜 최소값: {df[date_col].min()}")
        print(f" - 날짜 최대값: {df[date_col].max()}")
    else:
        print(" - 날짜 컬럼을 찾지 못했습니다.")

    if code_col is not None:
        print(f" - 종목 개수: {df[code_col].nunique():,} 개")
        print(f" - 예시 종목 5개: {df[code_col].dropna().unique()[:5]}")
    else:
        print(" - 종목 코드 컬럼을 찾지 못했습니다.")

    print()

    # --- 4. 결측치(NaN) 요약 ---
    print("[5] 결측치 요약 (상위 30컬럼)")
    null_sum = df.isna().sum().sort_values(ascending=False)
    null_ratio = (null_sum / len(df)).sort_values(ascending=False)

    null_summary = pd.DataFrame({
        "null_count": null_sum,
        "null_ratio": null_ratio
    })
    print(null_summary.head(30))
    print()

    # --- 5. 라벨/타깃 분포 체크 ---
    print("[6] 라벨/타깃 분포")
    if label_cols:
        lbl = label_cols[0]
        print(f" - 라벨 컬럼: {lbl}")
        print(df[lbl].value_counts(dropna=False))
        print()
    else:
        print(" - 라벨 컬럼을 찾지 못했습니다.\n")

    if target_cols:
        tgt = target_cols[0]
        print(f" - 타깃 컬럼: {tgt}")
        print(df[tgt].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]))
        print()
    else:
        print(" - 타깃(수익률) 컬럼을 찾지 못했습니다.\n")

    # --- 6. 피처 개수/목록 ---
    print("[7] 피처 컬럼 개수 및 예시")

    base_cols = set(code_cols + date_cols + close_cols + label_cols + target_cols)
    feature_cols = [c for c in df.columns if c not in base_cols]

    print(f" - 전체 컬럼 수: {len(df.columns)}")
    print(f" - 피처(특징) 컬럼 수: {len(feature_cols)}")
    print(f" - 피처 컬럼 예시(최대 40개):")
    print(feature_cols[:40])
    print()

    # --- 7. 샘플 종목 한 개 타임라인 보기 ---
    print("[8] 샘플 종목 타임라인 (상위 1개 종목)")
    if code_col is not None and date_col is not None:
        sample_code = df[code_col].dropna().unique()[0]
        print(f" - 샘플 종목: {sample_code}")
        sub = df[df[code_col] == sample_code].sort_values(date_col).head(20)
        print(sub[[col for col in [date_col, code_col] + target_cols + label_cols if col in sub.columns]])
    else:
        print(" - 종목/날짜 컬럼이 없어 샘플 타임라인을 출력할 수 없습니다.")

    # --- 8. 결과 요약 CSV로 저장 (옵션) ---
    print("\n[9] 요약 리포트 CSV 저장")
    out_dir = os.path.dirname(RESEARCH_DB_FILE)
    null_summary_path = os.path.join(out_dir, "V31_null_summary.csv")
    cols_info_path = os.path.join(out_dir, "V31_columns_list.csv")

    null_summary.to_csv(null_summary_path, encoding="utf-8-sig")
    pd.DataFrame({"columns": df.columns}).to_csv(cols_info_path, index=False, encoding="utf-8-sig")

    print(f" - 결측치 요약: {null_summary_path}")
    print(f" - 컬럼 리스트: {cols_info_path}")
    print("\n✅ V31 연구 DB 탐색 1차 리포트 생성 완료.")

if __name__ == "__main__":
    main()
