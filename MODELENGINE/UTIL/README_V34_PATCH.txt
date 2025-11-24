HOJ V34 Full Patch (Trainer + Recommender)
------------------------------------------
- Trainer: train_engine_unified_V34_complete.py (A-mask, horizon tail, input_window=0 기본)
- Recommender: daily_recommender_V34_plus.py
  * Close/ClosePrice/종가/가격 자동 매핑
  * feature hash 검증 (엔진 meta vs DB)
  * 엔진 학습일 vs DB 최신일 mismatch 경고
  * 동시적용 스코어: prob * clip(ret, -10%, +inf)
  * rank_by: combo | prob | ret

실행 예:
1) 엔진 학습(리서치+리얼 함께)
   python train_engine_unified_V34_complete.py --mode all --horizon 5 --input_window 0 --valid_days 365

2) Top10 생성(동시적용 기준)
   python daily_recommender_V34_plus.py --rank_by combo --topk 10

파일 생성: 2025-11-23T15:39:06