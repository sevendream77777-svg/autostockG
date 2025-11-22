
###############################################
# HOJ Engine Manager – NextGen FULL BUILD
#  (Dashboard + Data Update + Training + Manage + Predict + Trade shell)
#  Integrated with existing backends (run_unified_training, run_prediction, etc.)
###############################################

import os
import sys
import glob
import pickle
import random
import time
import pandas as pd
from datetime import datetime

from PySide6.QtWidgets import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtCharts import *

###############################################
# Backend import (adjusted to your project layout)
###############################################
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # .../ui/ui_pages -> project root
sys.path.append(root_dir)

model_engine_dir = os.path.join(root_dir, "MODELENGINE")
util_dir = os.path.join(model_engine_dir, "UTIL")
raw_dir = os.path.join(model_engine_dir, "RAW")

sys.path.append(util_dir)
sys.path.append(raw_dir)

# Required backend modules
from MODELENGINE.UTIL.train_engine_unified import run_unified_training
from MODELENGINE.UTIL.predict_daily_top10 import run_prediction
from MODELENGINE.UTIL.config_paths import get_path
import update_raw_data
import build_features
import build_unified_db

###############################################
# Re-usable UI parts
###############################################
class InfoCard(QWidget):
    def __init__(self, title, value):
        super().__init__()
        layout = QVBoxLayout(self)
        self.setStyleSheet("background:#2A2B32;border-radius:10px;padding:15px;")
        t = QLabel(title); t.setStyleSheet("font-size:14px;color:#85C1FF;")
        v = QLabel(value); v.setStyleSheet("font-size:22px;font-weight:bold;")
        layout.addWidget(t); layout.addWidget(v)

class SimpleChart(QWidget):
    def __init__(self):
        super().__init__()
        lay = QVBoxLayout(self)
        series = QLineSeries()
        for i in range(80):
            series.append(i, random.randint(80, 120))
        chart = QChart()
        chart.addSeries(series)
        chart.createDefaultAxes()
        chart.setBackgroundBrush(QBrush(QColor("#1A1B25")))
        chart.legend().hide()
        view = QChartView(chart)
        view.setRenderHint(QPainter.Antialiasing)
        lay.addWidget(view)

###############################################
# Worker Threads
###############################################
class DataWorker(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(int)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks

    def run(self):
        try:
            total = len(self.tasks)
            for idx, task in enumerate(self.tasks):
                step = idx + 1
                self.progress_signal.emit(int((idx / total) * 100))

                if task == 'stock':
                    self.log_signal.emit(f"[{step}/{total}] RAW 업데이트 중...")
                    update_raw_data.main()
                elif task == 'kospi':
                    self.log_signal.emit(f"[{step}/{total}] KOSPI 업데이트 중...")
                    sys.path.append(raw_dir)
                    import make_kospi_index_10y
                    make_kospi_index_10y.main()
                elif task == 'feature':
                    self.log_signal.emit(f"[{step}/{total}] 특징량 생성 중...")
                    build_features.main()
                elif task == 'db':
                    self.log_signal.emit(f"[{step}/{total}] 통합 DB 구축 중...")
                    build_unified_db.build_unified_db()

                self.log_signal.emit(f"   완료: {task}")
                time.sleep(0.2)

            self.progress_signal.emit(100)
            self.finished_signal.emit("데이터 작업 완료!")

        except Exception as e:
            self.error_signal.emit(str(e))

class TrainWorker(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, params):
        super().__init__()
        self.params = params

    def run(self):
        try:
            self.log_signal.emit("엔진 학습 시작...")
            run_unified_training(
                mode=self.params['mode'],
                horizon=self.params['horizon'],
                valid_days=self.params['valid'],
                n_estimators=self.params['trees'],
                version=self.params['version']
            )
            self.finished_signal.emit("학습 완료!")
        except Exception as e:
            self.error_signal.emit(str(e))

class PredictWorker(QThread):
    finished_signal = Signal(object)
    error_signal = Signal(str)

    def __init__(self, engine, date, topn):
        super().__init__()
        self.engine = engine
        self.date = date
        self.topn = topn

    def run(self):
        try:
            df = run_prediction(self.engine, self.date, self.topn)
            self.finished_signal.emit(df)
        except Exception as e:
            self.error_signal.emit(str(e))

###############################################
# Main UI
###############################################
class EngineManager(QWidget):
    def __init__(self):
        super().__init__()
        self.all_engines = []
        self.init()

    def init(self):
        main = QHBoxLayout(self)

        # Sidebar
        frame = QFrame(); frame.setStyleSheet("background:#1B1C22;")
        nav = QVBoxLayout(frame)

        title = QLabel("HOJ ENGINE")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size:22px;font-weight:bold;color:#85C1FF;padding:20px;")
        nav.addWidget(title)

        self.btns = []
        tabs = [("🏠 대시보드", 0), ("💾 데이터", 1), ("🏭 학습", 2),
                ("📊 관리", 3), ("🔮 예측", 4), ("📈 매매", 5)]
        for name, idx in tabs:
            b = QPushButton(name); b.setCheckable(True)
            b.clicked.connect(lambda _, i=idx: self.go(i))
            b.setStyleSheet(
                "QPushButton{padding:12px;color:#E0E0E0;background:#2A2B32;text-align:left;font-size:15px;}"
                "QPushButton:hover{background:#3A3B45;}"
                "QPushButton:checked{background:#1976D2;color:white;font-weight:bold;}"
            )
            nav.addWidget(b); self.btns.append(b)
        nav.addStretch(); main.addWidget(frame)

        # Pages
        self.pages = QStackedWidget(); main.addWidget(self.pages, 1)
        self.pages.addWidget(self.page_dash())
        self.pages.addWidget(self.page_data())
        self.pages.addWidget(self.page_train())
        self.pages.addWidget(self.page_manage())
        self.pages.addWidget(self.page_predict())
        self.pages.addWidget(self.page_trade())
        self.go(0)

        # Theme
        self.setStyleSheet("""
        QWidget{background:#202027;color:#E0E0E0;font-family:'Segoe UI';font-size:14px;}
        QTextEdit,QLineEdit{background:#11121A;border:1px solid #3A3B45;border-radius:6px;padding:6px;}
        QComboBox{background:#11121A;border:1px solid #3A3B45;border-radius:6px;padding:4px;}
        QGroupBox{border:1px solid #3A3B45;border-radius:8px;margin-top:12px;padding:10px;}
        QTableWidget{background:#1A1B25;gridline-color:#333;}
        QHeaderView::section{background:#2A2B32;padding:6px;border:none;}
        """)

    def go(self, idx):
        for b in self.btns: b.setChecked(False)
        self.btns[idx].setChecked(True)
        self.pages.setCurrentIndex(idx)

    # Page 0: Dashboard
    def page_dash(self):
        w = QWidget(); l = QVBoxLayout(w)
        ttl = QLabel("📊 대시보드"); ttl.setStyleSheet("font-size:24px;font-weight:bold;color:#85C1FF;")
        l.addWidget(ttl)

        cards = QHBoxLayout()
        cards.addWidget(InfoCard("RAW 업데이트", "-"))
        cards.addWidget(InfoCard("사용 가능한 엔진", "0"))
        cards.addWidget(InfoCard("최근 예측정확도", "-"))
        l.addLayout(cards)

        chart = SimpleChart(); l.addWidget(chart)
        return w

    # Page 1: Data
    def page_data(self):
        w = QWidget(); l = QVBoxLayout(w)
        ttl = QLabel("💾 데이터 업데이트"); ttl.setStyleSheet("font-size:20px;color:#85C1FF;")
        l.addWidget(ttl)

        row = QHBoxLayout()
        b1 = QPushButton("RAW"); b2 = QPushButton("KOSPI"); b3 = QPushButton("FEATURE"); b4 = QPushButton("DB")
        b_all = QPushButton("전체 실행")

        for b in [b1, b2, b3, b4, b_all]:
            b.setFixedHeight(40)
            b.setStyleSheet("background:#2A2B32;color:white;font-weight:bold;")

        b1.clicked.connect(lambda: self.run_data(['stock']))
        b2.clicked.connect(lambda: self.run_data(['kospi']))
        b3.clicked.connect(lambda: self.run_data(['feature']))
        b4.clicked.connect(lambda: self.run_data(['db']))
        b_all.clicked.connect(lambda: self.run_data(['stock','kospi','feature','db']))

        for b in [b1,b2,b3,b4,b_all]: row.addWidget(b)
        l.addLayout(row)

        self.data_prog = QProgressBar(); l.addWidget(self.data_prog)
        self.data_log = QTextEdit(); l.addWidget(self.data_log)
        return w

    def run_data(self, tasks):
        self.data_worker = DataWorker(tasks)
        self.data_worker.log_signal.connect(self.data_log.append)
        self.data_worker.progress_signal.connect(self.data_prog.setValue)
        self.data_worker.finished_signal.connect(lambda msg: QMessageBox.information(self,"완료",msg))
        self.data_worker.error_signal.connect(lambda err: QMessageBox.critical(self,"오류",err))
        self.data_worker.start()

    # Page 2: Training
    def page_train(self):
        w = QWidget(); l = QVBoxLayout(w)
        l.addWidget(QLabel("🏭 엔진 학습"))

        row = QHBoxLayout()

        self.train_mode = QComboBox(); self.train_mode.addItems(["research","real"])
        row.addWidget(self.train_mode)

        # 큰 입력 위젯(숫자 직접 입력 + 폭 넓음)
        self.train_h = QLineEdit(); self.train_h.setPlaceholderText("예측일수 (예: 5)"); self.train_h.setFixedWidth(140)
        row.addWidget(self.train_h)

        self.train_valid = QLineEdit(); self.train_valid.setPlaceholderText("검증일수 (예: 365)"); self.train_valid.setFixedWidth(160)
        row.addWidget(self.train_valid)

        self.train_trees = QLineEdit(); self.train_trees.setPlaceholderText("나무 수 (예: 1000)"); self.train_trees.setFixedWidth(160)
        row.addWidget(self.train_trees)

        self.train_ver = QLineEdit(); self.train_ver.setPlaceholderText("버전 태그 (예: V32)"); self.train_ver.setFixedWidth(140)
        row.addWidget(self.train_ver)

        btn = QPushButton("학습 시작"); btn.clicked.connect(self.start_train)
        row.addWidget(btn)
        l.addLayout(row)

        self.train_log = QTextEdit(); l.addWidget(self.train_log)
        return w

    def start_train(self):
        try:
            params = {
                'mode': self.train_mode.currentText(),
                'horizon': int(self.train_h.text()),
                'valid': int(self.train_valid.text()),
                'trees': int(self.train_trees.text()),
                'version': self.train_ver.text()
            }
        except Exception:
            QMessageBox.warning(self, "입력 확인", "학습 파라미터(숫자)를 정확히 입력하세요.")
            return

        self.worker = TrainWorker(params)
        self.worker.log_signal.connect(self.train_log.append)
        self.worker.finished_signal.connect(lambda msg: (self.train_log.append(msg), self.refresh_engines(), QMessageBox.information(self,"완료",msg)))
        self.worker.error_signal.connect(lambda err: QMessageBox.critical(self,"오류",err))
        self.worker.start()

    # Page 3: Manage
    def page_manage(self):
        w = QWidget(); l = QVBoxLayout(w)
        l.addWidget(QLabel("📊 엔진 관리"))

        self.tbl = QTableWidget(0, 1)
        self.tbl.setHorizontalHeaderLabels(["엔진 파일명"])
        header = self.tbl.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)  # draggable
        header.setDefaultSectionSize(380)                     # default wide
        self.tbl.setMinimumWidth(420)
        self.tbl.itemClicked.connect(self.load_meta)
        l.addWidget(self.tbl)

        self.meta = QTextEdit(); l.addWidget(self.meta)

        self.refresh_engines()
        return w

    def refresh_engines(self):
        base = get_path("HOJ_ENGINE")
        files = glob.glob(os.path.join(base, "**", "*.pkl"), recursive=True)
        files.sort(key=os.path.getmtime, reverse=True)
        self.all_engines = []
        self.tbl.setRowCount(0)
        for f in files:
            name = os.path.basename(f)
            row = self.tbl.rowCount(); self.tbl.insertRow(row)
            item = QTableWidgetItem(name); item.setData(Qt.UserRole, f)
            self.tbl.setItem(row, 0, item)
            h = -1
            try:
                m = pickle.load(open(f, "rb"))
                h = m.get('meta', {}).get('horizon', -1)
            except Exception:
                pass
            self.all_engines.append({'name': name, 'path': f, 'h': h})
        # also refresh predict engine selector if built
        if hasattr(self, "pred_engine"):
            self.refresh_predict_engines()

    def load_meta(self, item):
        path = item.data(Qt.UserRole)
        try:
            data = pickle.load(open(path, "rb"))
            meta = data.get('meta', {})
            s = f"파일: {os.path.basename(path)}\n"
            s += f"학습일자: {meta.get('train_date','?')}\n"
            s += f"데이터기준: {meta.get('data_date','?')}\n"
            s += f"Horizon: {meta.get('horizon','?')}\n"
            self.meta.setText(s)
        except Exception as e:
            self.meta.setText(str(e))

    # Page 4: Predict
    def page_predict(self):
        w = QWidget(); l = QVBoxLayout(w)
        l.addWidget(QLabel("🔮 예측"))

        row = QHBoxLayout()
        self.pred_date = QDateEdit()
        self.pred_date.setDate(QDate.currentDate().addDays(-1))
        self.pred_date.setDisplayFormat("yyyy-MM-dd")
        row.addWidget(self.pred_date)

        self.pred_engine = QComboBox(); row.addWidget(self.pred_engine)
        self.pred_top = QLineEdit(); self.pred_top.setPlaceholderText("Top N (예: 10)")
        row.addWidget(self.pred_top)

        btn = QPushButton("예측 실행"); btn.clicked.connect(self.start_predict)
        row.addWidget(btn)
        l.addLayout(row)

        self.tbl_pred = QTableWidget(0, 5)
        self.tbl_pred.setHorizontalHeaderLabels(["코드","이름","현재가","점수","확률"])
        self.tbl_pred.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        l.addWidget(self.tbl_pred)

        self.refresh_predict_engines()
        return w

    def refresh_predict_engines(self):
        self.pred_engine.clear()
        for eng in self.all_engines:
            self.pred_engine.addItem(eng['name'], eng['path'])

    def start_predict(self):
        try:
            path = self.pred_engine.currentData()
            date = self.pred_date.date().toString("yyyy-MM-dd")
            top = int(self.pred_top.text())
        except Exception:
            QMessageBox.warning(self, "입력 확인", "Top N을 정확히 입력하세요.")
        else:
            self.pw = PredictWorker(path, date, top)
            self.pw.finished_signal.connect(self.fill_pred)
            self.pw.error_signal.connect(lambda err: QMessageBox.critical(self, "오류", err))
            self.pw.start()

    def fill_pred(self, df):
        self.tbl_pred.setRowCount(0)
        if df is None or df.empty:
            QMessageBox.information(self, "알림", "데이터가 없거나 휴장일입니다.")
            return
        for _, r in df.iterrows():
            row = self.tbl_pred.rowCount(); self.tbl_pred.insertRow(row)
            self.tbl_pred.setItem(row, 0, QTableWidgetItem(str(r['Code'])))
            self.tbl_pred.setItem(row, 1, QTableWidgetItem(str(r.get('Name','?'))))
            self.tbl_pred.setItem(row, 2, QTableWidgetItem(f"{r['Close']}"))
            self.tbl_pred.setItem(row, 3, QTableWidgetItem(f"{r['Pred_Score']:.4f}"))
            self.tbl_pred.setItem(row, 4, QTableWidgetItem(f"{r['Pred_Prob']*100:.1f}%"))

    # Page 5: Trade (shell)
    def page_trade(self):
        w = QWidget(); l = QVBoxLayout(w)
        l.addWidget(QLabel("📈 매매 모듈 (추가 예정)"))
        self.trade_log = QTextEdit(); l.addWidget(self.trade_log)
        return w

###############################################
# MAIN
###############################################
if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = EngineManager()
    win.resize(1400, 900)
    win.setWindowTitle("HOJ Engine Manager – FULL BUILD")
    win.show()
    sys.exit(app.exec())
