# ui/pages/p2_analysis.py
import glob
import os
import pickle
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QListWidget, 
                               QTextEdit, QPushButton, QLabel, QSplitter, QTableWidget, 
                               QTableWidgetItem, QHeaderView, QTabWidget)
from PySide6.QtCore import Qt

class AnalysisPage(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        
        h = QHBoxLayout()
        h.addWidget(QLabel("📂 학습된 엔진 목록"))
        btn_refresh = QPushButton("새로고침")
        btn_refresh.clicked.connect(self.load_engines)
        h.addWidget(btn_refresh)
        layout.addLayout(h)
        
        splitter = QSplitter(Qt.Horizontal)
        
        # 왼쪽: 파일 목록
        self.list_engines = QListWidget()
        self.list_engines.itemClicked.connect(self.analyze_engine)
        splitter.addWidget(self.list_engines)
        
        # 오른쪽: 상세 분석 탭
        self.tabs = QTabWidget()
        
        # 탭 1: 기본 정보
        self.txt_info = QTextEdit()
        self.txt_info.setReadOnly(True)
        self.tabs.addTab(self.txt_info, "📝 기본 정보")
        
        # 탭 2: 성능 지표 (Metrics)
        self.table_metrics = QTableWidget()
        self.table_metrics.setColumnCount(2)
        self.table_metrics.setHorizontalHeaderLabels(["지표 (Metric)", "값 (Value)"])
        self.table_metrics.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabs.addTab(self.table_metrics, "📊 성능 지표")
        
        splitter.addWidget(self.tabs)
        splitter.setSizes([250, 750])
        
        layout.addWidget(splitter)
        self.load_engines()

    def load_engines(self):
        self.list_engines.clear()
        # 경로 설정 (실제 경로에 맞게 수정)
        base_dirs = [
            r"../MODELENGINE/HOJ_ENGINE/REAL",
            r"../MODELENGINE/HOJ_ENGINE/RESEARCH"
        ]
        
        files = []
        for d in base_dirs:
            if os.path.exists(d):
                files.extend(glob.glob(os.path.join(d, "*.pkl")))
                
        for f in sorted(files, reverse=True):
            self.list_engines.addItem(f) # 전체 경로 저장 (텍스트는 이름만 보이게 할 수도 있음)

    def analyze_engine(self, item):
        fname = item.text()
        self.txt_info.setText(f"파일 경로: {fname}\n분석 중...")
        
        try:
            # Pickle 로드 시도 (메타데이터 확인용)
            # 주의: 신뢰할 수 없는 pickle 파일 로드는 위험할 수 있음
            with open(fname, 'rb') as f:
                data = pickle.load(f)
            
            # 데이터 구조에 따라 처리 (딕셔너리 형태라고 가정)
            info_text = ""
            metrics = {}
            
            if isinstance(data, dict):
                if 'model' in data: info_text += f"Model Type: {type(data['model'])}\n"
                if 'features' in data: info_text += f"Features Count: {len(data['features'])}\n"
                if 'params' in data: info_text += f"Parameters: {data['params']}\n"
                
                # 가상의 성능 지표 추출 (실제 저장 구조에 따라 수정 필요)
                metrics = data.get('metrics', {
                    'IC (Information Coefficient)': 'N/A',
                    'Hit Ratio': 'N/A',
                    'Top10 Avg Return': 'N/A'
                })
            else:
                info_text += "Unknown format object."

            self.txt_info.setText(info_text)
            
            # 메트릭 테이블 채우기
            self.table_metrics.setRowCount(0)
            for k, v in metrics.items():
                r = self.table_metrics.rowCount()
                self.table_metrics.insertRow(r)
                self.table_metrics.setItem(r, 0, QTableWidgetItem(str(k)))
                self.table_metrics.setItem(r, 1, QTableWidgetItem(str(v)))
                
        except Exception as e:
            self.txt_info.setText(f"파일 분석 실패: {e}\n(단순 모델 객체일 경우 메타데이터가 없을 수 있습니다)")
            self.table_metrics.setRowCount(0)