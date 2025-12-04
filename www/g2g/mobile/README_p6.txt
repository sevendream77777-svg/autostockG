
배포 방법 (Cafe24, p6 규칙 반영판)

1) /g2g/mobile/ 에 아래 3개 업로드
   - index.html
   - app.js
   - styles.css

2) /g2g/data/ 에 files.json 업로드
   - 예시:
     [
       {"filename":"HOJ_ENGINE_REAL_V31_h5_w60_n1000_251128.json","title":"REAL_V31 h5 (251128)"},
       {"filename":"HOJ_ENGINE_REAL_V31_h10_w60_n1000_251128.json","title":"REAL_V31 h10 (251128)"},
       {"filename":"HOJ_ENGINE_REAL_V31_h1_w40_n1000_251128.json","title":"REAL_V31 h1_w40 (251128)"}
     ]

3) 작동 방식
   - 달력 셀을 클릭하면(일~토) 해당 날짜가 '예측 윈도우(마지막데이터+1영업일 ~ +h영업일)'에 포함되는 파일만 드롭다운에 나타납니다.
   - 파일 선택 후 '불러오기' → Top10 표 출력 → '현재가 갱신'은 야후(가입 불필요)로 수행.
   - 달력 셀 하단 '(+N)'은 해당 날짜에 예측 가능한 파일 개수.
