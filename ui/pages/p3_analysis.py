import glob
import os
import pickle
import json
from datetime import datetime
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QListWidget, QTextEdit,
    QPushButton, QLabel, QSplitter, QTableWidget, QTableWidgetItem,
    QHeaderView, QTabWidget
)
from PySide6.QtCore import Qt


class P3_Analysis(QWidget):
    """
    엔진 분석 페이지 — FULL VERSION
    좌측: REAL / RESEARCH 엔진 탭
    우측: ①~⑦ 탭(총 7개)
    """

    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("엔진 목록"))
        btn_refresh = QPushButton("새로고침")
        btn_refresh.clicked.connect(self.load_engines)
        header.addWidget(btn_refresh)
        layout.addLayout(header)

        splitter = QSplitter(Qt.Horizontal)

        # ===========================
        # 좌측: REAL / RESEARCH 탭
        # ===========================
        self.left_tabs = QTabWidget()
        self.list_real = QListWidget()
        self.list_research = QListWidget()

        self.list_real.itemClicked.connect(self.analyze_engine)
        self.list_research.itemClicked.connect(self.analyze_engine)

        self.left_tabs.addTab(self.list_real, "REAL")
        self.left_tabs.addTab(self.list_research, "RESEARCH")

        splitter.addWidget(self.left_tabs)

        # ===========================
        # 우측: 7개 탭
        # ===========================
        self.tabs = QTabWidget()

        # ① 기본 정보
        self.tab_info = QTextEdit()
        self.tab_info.setReadOnly(True)
        self.tabs.addTab(self.tab_info, "① 기본 정보")

        # ② 학습 정보
        self.tab_training = QTextEdit()
        self.tab_training.setReadOnly(True)
        self.tabs.addTab(self.tab_training, "② 학습 정보")

        # ③ 피처 중요도
        self.tab_feat_imp = QTableWidget()
        self.tab_feat_imp.setColumnCount(2)
        self.tab_feat_imp.setHorizontalHeaderLabels(["Feature", "Importance"])
        self.tab_feat_imp.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabs.addTab(self.tab_feat_imp, "③ 피처 중요도")

        # ④ 피처 목록
        self.tab_feat_list = QTextEdit()
        self.tab_feat_list.setReadOnly(True)
        self.tabs.addTab(self.tab_feat_list, "④ 피처 목록")

        # ⑤ 요약/메모
        self.tab_notes = QTextEdit()
        self.tab_notes.setReadOnly(True)
        self.tabs.addTab(self.tab_notes, "⑤ 요약/메모")

        # ⑥ Top10 예측 결과
        self.tab_top10 = QTextEdit()
        self.tab_top10.setReadOnly(True)
        self.tabs.addTab(self.tab_top10, "⑥ Top10 결과")

        # ⑦ AI 해설
        self.tab_ai = QTextEdit()
        self.tab_ai.setReadOnly(True)
        self.tabs.addTab(self.tab_ai, "⑦ AI 해설")

        splitter.addWidget(self.tabs)
        splitter.setSizes([300, 700])

        layout.addWidget(splitter)
        self.load_engines()

    # ==========================================================
    # 엔진 디렉토리
    # ==========================================================
    def _engine_dirs(self):
        base = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_ENGINE")
        )
        return {
            "REAL": os.path.join(base, "REAL"),
            "RESEARCH": os.path.join(base, "RESEARCH"),
        }

    # ==========================================================
    # 엔진 목록 로드 (날짜 최신순으로 정렬 + 파일명만 표시)
    # ==========================================================
    def load_engines(self):
        self.list_real.clear()
        self.list_research.clear()

        dirs = self._engine_dirs()

        # REAL
        d_real = dirs["REAL"]
        if os.path.exists(d_real):
            files = sorted(
                glob.glob(os.path.join(d_real, "*.pkl")),
                key=lambda x: os.path.basename(x).split("_")[-1].split(".")[0],
                reverse=True
            )

            for f in files:
                filename = os.path.basename(f)
                self.list_real.addItem(f"{filename}")

        # RESEARCH
        d_res = dirs["RESEARCH"]
        if os.path.exists(d_res):
            files = sorted(
                glob.glob(os.path.join(d_res, "*.pkl")),
                key=lambda x: os.path.basename(x).split("_")[-1].split(".")[0],
                reverse=True
            )
            for f in files:
                filename = os.path.basename(f)
                self.list_research.addItem(f"{filename}")

    # ==========================================================
    # 엔진 분석
    # ==========================================================
    def analyze_engine(self, item):
        text = item.text()
        name = text  # 리스트에는 파일명만 표시
        dirs = self._engine_dirs()
        sub = "REAL" if self.left_tabs.currentIndex() == 0 else "RESEARCH"

        path = os.path.join(dirs[sub], name)
        if not os.path.exists(path):  # 예외: 탭-파일 불일치 대비
            for d in dirs.values():
                cand = os.path.join(d, name)
                if os.path.exists(cand):
                    path = cand
                    break
        self.current_name = name
        self.current_path = path

        self.tab_info.setText(f"로드 중...\n{name}")
        self.tab_training.clear()
        self.tab_feat_imp.setRowCount(0)
        self.tab_feat_list.clear()
        self.tab_notes.clear()
        self.tab_top10.clear()
        self.tab_ai.clear()

        # ----------------------------
        # pkl 로드
        # ----------------------------
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
        except Exception as e:
            self.tab_info.setText(f"엔진 로드 실패: {e}")
            return

        # 모델 메타
        meta = data.get("meta", {})
        features = data.get("features", [])
        feature_count = len(features)

        # feature_importances
        feat_imp = []
        model_reg = data.get("model_reg")
        if model_reg is not None and hasattr(model_reg, "feature_importances_"):
            imp = list(model_reg.feature_importances_)
            feat_imp = list(zip(features, imp))

        # ----------------------------
        # JSON 경로
        # ----------------------------
        json_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "..",
                "MODELENGINE", "INFO", "hoj_engine_info"
            )
        )
        json_path = os.path.join(json_dir, name.replace(".pkl", ".json"))

        json_data = {}
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    json_data = json.load(jf)
            except:
                json_data = {}

        # ==========================================================
        # ① 기본 정보
        # ==========================================================
        info_txt = []
        info_txt.append(f"파일명: {name}")
        info_txt.append(f"전체경로: {path}")
        info_txt.append("")

        if meta:
            kv = [
                ("버전", meta.get("version")),
                ("데이터 날짜", meta.get("data_date")),
                ("horizon", meta.get("horizon")),
                ("input_window", meta.get("input_window")),
                ("valid_days", meta.get("valid_days")),
                ("n_estimators", meta.get("n_estimators")),
                ("trained_at", meta.get("trained_at")),
                ("DB경로", meta.get("db_path")),
                ("feature_count", feature_count),
            ]
            for k, v in kv:
                info_txt.append(f"{k}: {v}")
        else:
            info_txt.append("meta 정보 없음")

        self.tab_info.setText("\n".join(info_txt))

        # ==========================================================
        # ② 학습 정보 (engine_training_info)
        # ==========================================================
        et = json_data.get("engine_training_info", {})
        tr_txt = []
        if et:
            for k, v in et.items():
                tr_txt.append(f"{k}: {v}")
        else:
            tr_txt.append("학습정보 없음")
        self.tab_training.setText("\n".join(tr_txt))

        # ==========================================================
        # ③ 피처 중요도 (상위 20)
        # ==========================================================
        if feat_imp:
            feat_imp = sorted(feat_imp, key=lambda x: x[1], reverse=True)
            top20 = feat_imp[:20]
            self.tab_feat_imp.setRowCount(0)
            for n, v in top20:
                r = self.tab_feat_imp.rowCount()
                self.tab_feat_imp.insertRow(r)
                self.tab_feat_imp.setItem(r, 0, QTableWidgetItem(str(n)))
                self.tab_feat_imp.setItem(r, 1, QTableWidgetItem(f"{float(v):.4f}"))
        else:
            self.tab_feat_imp.setRowCount(1)
            self.tab_feat_imp.setItem(0, 0, QTableWidgetItem("없음"))
            self.tab_feat_imp.setItem(0, 1, QTableWidgetItem("-"))

        # ==========================================================
        # ④ 피처 목록
        # ==========================================================
        feat_txt = []
        for i, f in enumerate(features, 1):
            feat_txt.append(f"{i}. {f}")
        self.tab_feat_list.setText("\n".join(feat_txt))

        # ==========================================================
        # ⑤ 요약/메모
        # ==========================================================
        base = os.path.splitext(name)[0]
        info_txt_path = os.path.join(
            os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..",
                             "MODELENGINE", "HOJ_ENGINE", "HOJ_ENGINE_INFO")
            ),
            f"{base}.txt"
        )

        if os.path.exists(info_txt_path):
            try:
                with open(info_txt_path, "r", encoding="utf-8", errors="replace") as f:
                    self.tab_notes.setText(f.read())
            except:
                self.tab_notes.setText("요약/메모 로딩 실패")
        else:
            self.tab_notes.setText("요약 파일 없음")

        # ==========================================================
        # ⑥ Top10 결과
        # ==========================================================
        top10 = json_data.get("top10", [])
        if top10:
            t_txt = []
            for t in top10:
                t_txt.append(
                    f"{t.get('순위')}. {t.get('종목명')} "
                    f"({t.get('종목코드')})  "
                    f"현재가:{t.get('현재가')}  "
                    f"확률:{t.get('상승확률(%)')}%  "
                    f"수익:{t.get('예측수익률(%)')}%  "
                    f"동시:{t.get('동시적용 기대수익(%)')}%"
                )
            self.tab_top10.setText("\n".join(t_txt))
        else:
            self.tab_top10.setText("Top10 결과 없음")

        # ==========================================================
        # ⑦ AI 해설
        # ==========================================================
        ai = json_data.get("ai_report", "")
        if ai:
            self.tab_ai.setText(ai)
        else:
            self.tab_ai.setText("AI 해설 없음")
