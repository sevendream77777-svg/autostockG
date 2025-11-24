# ui/pages/p2_analysis.py
import glob
import os
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QListWidget, 
                               QTextEdit, QPushButton, QLabel, QSplitter)
from PySide6.QtCore import Qt

class AnalysisPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 상단 컨트롤
        h = QHBoxLayout()
        h.addWidget(QLabel("📂 엔진 목록 (.pkl)"))
        btn_refresh = QPushButton("새로고침")
        btn_refresh.clicked.connect(self.load_engines)
        h.addWidget(btn_refresh)
        layout.addLayout(h)
        
        # 좌우 분할
        splitter = QSplitter(Qt.Horizontal)
        
        self.list_engines = QListWidget()
        self.list_engines.itemClicked.connect(self.show_info)
        splitter.addWidget(self.list_engines)
        
        self.txt_info = QTextEdit()
        self.txt_info.setReadOnly(True)
        self.txt_info.setPlaceholderText("엔진을 선택하면 상세 정보가 표시됩니다.")
        splitter.addWidget(self.txt_info)
        splitter.setSizes([300, 700])
        
        layout.addWidget(splitter)
        self.load_engines()

    def load_engines(self):
        self.list_engines.clear()
        # 예시 경로: 실제 경로에 맞춰 수정 필요
        base_dir = r"../MODELENGINE/HOJ_ENGINE/REAL"
        if not os.path.exists(base_dir):
            self.list_engines.addItem("경로 없음")
            return
            
        files = glob.glob(os.path.join(base_dir, "*.pkl"))
        for f in sorted(files, reverse=True):
            self.list_engines.addItem(os.path.basename(f))

    def show_info(self, item):
        fname = item.text()
        # 실제로는 pickle load 해서 메타데이터 보여주기
        self.txt_info.setText(f"선택된 파일: {fname}\n\n(여기에 메타데이터 로딩 로직 추가 예정)")