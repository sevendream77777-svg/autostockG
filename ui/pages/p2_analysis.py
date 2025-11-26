# ui/pages/p2_analysis.py
import glob
import os
import pickle
from datetime import datetime
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QListWidget,
    QTextEdit,
    QPushButton,
    QLabel,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QTabWidget,
)
from PySide6.QtCore import Qt


class AnalysisPage(QWidget):
    """
    엔진 분석 페이지:
    - 좌측: REAL/RESEARCH 엔진 리스트
    - 우측: 기본 정보, 요약/메모(HOJ_ENGINE_INFO), 피처 중요도
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

        # 좌측: 엔진 리스트
        self.list_engines = QListWidget()
        self.list_engines.itemClicked.connect(self.analyze_engine)
        splitter.addWidget(self.list_engines)

        # 우측: 탭
        self.tabs = QTabWidget()

        # 탭 1: 기본 정보
        self.txt_info = QTextEdit()
        self.txt_info.setReadOnly(True)
        self.tabs.addTab(self.txt_info, "기본 정보")

        # 탭 2: 요약/메모 (info txt)
        self.txt_notes = QTextEdit()
        self.txt_notes.setReadOnly(True)
        self.tabs.addTab(self.txt_notes, "요약/메모")

        # 탭 3: 피처 중요도 (reg 기준 상위 20)
        self.table_feat = QTableWidget()
        self.table_feat.setColumnCount(2)
        self.table_feat.setHorizontalHeaderLabels(["Feature", "Importance"])
        self.table_feat.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabs.addTab(self.table_feat, "피처 중요도")

        splitter.addWidget(self.tabs)
        splitter.setSizes([280, 720])

        layout.addWidget(splitter)
        self.load_engines()

    def _engine_dirs(self):
        base = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "MODELENGINE", "HOJ_ENGINE")
        )
        return [
            os.path.join(base, "REAL"),
            os.path.join(base, "RESEARCH"),
        ]

    def load_engines(self):
        """REAL/RESEARCH 엔진 목록을 불러온다."""
        self.list_engines.clear()
        files = []
        for d in self._engine_dirs():
            if os.path.exists(d):
                files.extend(glob.glob(os.path.join(d, "*.pkl")))
        for f in sorted(files, reverse=True):
            self.list_engines.addItem(f)

    def analyze_engine(self, item):
        fname = item.text()
        self.txt_info.setText(f"엔진: {fname}\n분석 중...")
        self.txt_notes.clear()
        self.table_feat.setRowCount(0)

        try:
            with open(fname, "rb") as f:
                data = pickle.load(f)
        except Exception as e:
            self.txt_info.setText(f"엔진 로드 실패: {e}")
            return

        info_lines = [f"파일: {fname}"]
        meta = {}
        features = []
        feat_importances = []

        if isinstance(data, dict):
            meta = data.get("meta", {})
            features = data.get("features", [])
            model_reg = data.get("model_reg")
            if model_reg is not None and hasattr(model_reg, "feature_importances_"):
                feat_importances = list(zip(features, model_reg.feature_importances_))

        if meta:
            info_lines.append(f"버전: {meta.get('version')}")
            info_lines.append(f"데이터 날짜: {meta.get('data_date')}")
            info_lines.append(f"horizon: {meta.get('horizon')}")
            info_lines.append(f"input_window: {meta.get('input_window')}")
            info_lines.append(f"valid_days: {meta.get('valid_days')}")
            info_lines.append(f"n_estimators: {meta.get('n_estimators')}")
            info_lines.append(f"trained_at: {meta.get('trained_at')}")
            info_lines.append(f"features: {len(features)}개")
        else:
            info_lines.append("meta 정보 없음")

        self.txt_info.setText("\n".join(info_lines))

        # 요약/메모: HOJ_ENGINE_INFO에서 동일 파일명 txt 읽기
        info_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "MODELENGINE",
                "HOJ_ENGINE",
                "HOJ_ENGINE_INFO",
            )
        )
        base = os.path.splitext(os.path.basename(fname))[0]
        info_path = os.path.join(info_dir, f"{base}.txt")
        if os.path.exists(info_path):
            try:
                with open(info_path, "r", encoding="utf-8", errors="replace") as f:
                    self.txt_notes.setText(f.read())
            except Exception as e:
                self.txt_notes.setText(f"요약 파일 읽기 실패: {e}")
        else:
            self.txt_notes.setText("요약 파일 없음")

        # 피처 중요도 (상위 20)
        if feat_importances:
            feat_importances = sorted(feat_importances, key=lambda x: x[1], reverse=True)
            top_k = feat_importances[:20]
            self.table_feat.setRowCount(0)
            for name, val in top_k:
                r = self.table_feat.rowCount()
                self.table_feat.insertRow(r)
                self.table_feat.setItem(r, 0, QTableWidgetItem(str(name)))
                self.table_feat.setItem(r, 1, QTableWidgetItem(f"{val:.4f}"))
        else:
            self.table_feat.setRowCount(0)
            self.table_feat.insertRow(0)
            self.table_feat.setItem(0, 0, QTableWidgetItem("중요도 정보 없음"))
            self.table_feat.setItem(0, 1, QTableWidgetItem("-"))
