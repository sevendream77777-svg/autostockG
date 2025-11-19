# ============================================================
# (EOF PATCH) 파일 설명 매핑 기능 추가
# ============================================================

# 파일명 → 설명 매핑
FILE_DESCRIPTIONS = {
    "merge_pbr_per_V17.py": "PER/PBR 병합기 (보조). SLE DB 생성 파이프라인 중간 단계.",
    "train_sle_engine_V31.py": "SLE 엔진 학습 코드 (보조).",
    "sle_sample_runner_multithread.py": "SLE 멀티스레드 러너 (보조).",
    "train_REAL_Hoj_ENGINE_V25.py": "HOJ 실전 엔진 학습 코드 (핵심).",
    "build_FULL_Hoj_DB_V25.py": "HOJ FULL DB 생성기 (핵심).",
    "naver_pbr_per_crawler_V16.py": "네이버 PBR/PER 크롤러 (보조).",
    "merge_hoj_sle_V30.py": "HOJ+SLE 하이브리드 병합기 (보조).",
    "daily_recommender.py": "HOJ 종목 추천 엔진 (실전).",
    "run_hybrid_test_V34.py": "하이브리드 백테스트 실행기 (연구)."
}

# 설명이 포함된 라인 생성 함수
def make_described(path: Path, root_dir: Path) -> str:
    relp = rel(path, root_dir)
    desc = FILE_DESCRIPTIONS.get(path.name, "")
    if desc:
        return f"{relp}  ← {desc}"
    return relp

# sort_rel_list 오버라이드 (카테고리별 정렬+설명추가)
def sort_rel_list(paths: List[Path]) -> List[str]:  # 기존 함수 덮어쓰기
    return [make_described(p, root_dir) for p in sorted(paths)]
