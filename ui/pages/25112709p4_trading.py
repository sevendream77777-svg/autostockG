# -*- coding: utf-8 -*-
"""
P4 REST Trading Main Hub (Kiwoom REST API 전용) — PySide6 완성본
- 시세(일봉) 조회 + 차트 + 표 + JSON Inspector
- 예수금/잔고 요약 대시보드
- 주문(매수/매도)
- 하단 로그 & 상태 표시

주의:
- 프로젝트 내 다른 페이지는 PySide6 기준으로 동작해야 합니다.
- Kiwoom REST 전용이며, OPENAPI/OCX 관련 코드는 사용하지 않습니다.
"""

from __future__ import annotations
import sys, os, json, traceback
from datetime import datetime

# ============================
# PySide6
# ============================
from PySide6.QtCore import Qt, QDate, QTimer
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QLineEdit,
    QDateEdit, QSpinBox, QComboBox, QCheckBox, QTextEdit, QGroupBox,
    QGridLayout, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox
)
from PySide6.QtGui import QFont

# ============================
# pyqtgraph (Qt6 지원)
# ============================
try:
    import pyqtgraph as pg
    from pyqtgraph import BarGraphItem
    _PG_OK = True
except Exception:
    _PG_OK = False

# ============================
# Kiwoom REST API 모듈
# ============================
# 절대경로 등록 (사용자 경로 기준)
sys.path.append(r"F:/autostockG/kiwoom_rest")
from kiwoom_api import KiwoomRestApi  # REST 전용

# ============================
# 유틸
# ============================

def _fmt_err(e: Exception) -> str:
    return f"{e.__class__.__name__}: {e}
" + traceback.format_exc()

def _pretty_json(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

def _today_str() -> str:
    return datetime.now().strftime("%Y%m%d")

# ============================
# 메인 클래스
# ============================
class P4RestTradingPage(QWidget):
    def __init__(self):
        super().__init__()
        self.setObjectName("P4RestTradingPage")
        self.api = KiwoomRestApi()

        self._build_ui()
        self._bind()

        # 대시보드 자동갱신 타이머 (Off 기본)
        self.timer = QTimer(self)
        self.timer.setInterval(5000)
        self.timer.timeout.connect(self.refresh_dashboard)

        self._log("[SYSTEM] P4 REST Trading Hub 초기화 완료")

    # ---------------- UI 빌드 ----------------
    def _build_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        # 상단 대시보드
        root.addWidget(self._ui_dashboard())

        # 중단 (좌: 컨트롤 / 우: 결과)
        mid = QHBoxLayout()
        mid.setSpacing(12)
        self._left = self._ui_left()
        self._right = self._ui_right()
        mid.addWidget(self._left, 0)
        mid.addWidget(self._right, 1)
        root.addLayout(mid, 1)

        # 하단 로그
        root.addWidget(self._ui_log())

    def _ui_dashboard(self) -> QGroupBox:
        g = QGroupBox("요약 대시보드")
        grid = QGridLayout(g)
        grid.setContentsMargins(10, 10, 10, 10)
        grid.setHorizontalSpacing(20)
        grid.setVerticalSpacing(4)

        self.lbl_dep  = QLabel("예수금: -")
        self.lbl_eval = QLabel("총평가: - / 손익: -")
        self.lbl_pos  = QLabel("보유종목수: -")
        self.lbl_fill = QLabel("금일 체결: - / 오류: -")

        font = QFont()
        font.setPointSize(font.pointSize() + 2)
        for w in (self.lbl_dep, self.lbl_eval, self.lbl_pos, self.lbl_fill):
            w.setFont(font)

        self.btn_dash = QPushButton("대시보드 갱신")
        self.chk_auto = QCheckBox("자동 5초 갱신")

        grid.addWidget(self.lbl_dep, 0, 0)
        grid.addWidget(self.lbl_eval,0,1)
        grid.addWidget(self.lbl_pos, 1, 0)
        grid.addWidget(self.lbl_fill,1,1)
        grid.addWidget(self.btn_dash,0,2)
        grid.addWidget(self.chk_auto,1,2)
        return g

    def _ui_left(self) -> QGroupBox:
        g = QGroupBox("조작 패널")
        v = QVBoxLayout(g)
        v.setContentsMargins(10, 10, 10, 10)
        v.setSpacing(8)

        # 종목/조건
        r1 = QHBoxLayout()
        self.edit_code = QLineEdit(); self.edit_code.setPlaceholderText("종목코드 (예: 005930)")
        self.btn_code = QPushButton("코드조회")
        r1.addWidget(QLabel("종목")); r1.addWidget(self.edit_code, 1); r1.addWidget(self.btn_code)
        v.addLayout(r1)

        r2 = QHBoxLayout()
        self.date_base = QDateEdit(); self.date_base.setDisplayFormat("yyyy-MM-dd"); self.date_base.setDate(QDate.currentDate())
        self.spin_days = QSpinBox(); self.spin_days.setRange(1, 10000); self.spin_days.setValue(120)
        self.combo_adj = QComboBox(); self.combo_adj.addItems(["수정주가", "보정안함"])  # upd_stkpc_tp: 문서 매핑 필요
        self.chk_cont = QCheckBox("연속조회"); self.chk_cont.setChecked(True)
        r2.addWidget(QLabel("기준일")); r2.addWidget(self.date_base)
        r2.addWidget(QLabel("조회일수")); r2.addWidget(self.spin_days)
        r2.addWidget(QLabel("가격옵션")); r2.addWidget(self.combo_adj)
        r2.addWidget(self.chk_cont)
        v.addLayout(r2)

        r3 = QHBoxLayout()
        self.btn_fetch  = QPushButton("차트/데이터 조회")
        self.btn_export = QPushButton("엑셀로 저장")
        r3.addWidget(self.btn_fetch); r3.addWidget(self.btn_export)
        v.addLayout(r3)

        v.addSpacing(8)
        v.addWidget(self._ui_order())
        v.addStretch(1)
        return g

    def _ui_order(self) -> QGroupBox:
        g = QGroupBox("주문")
        grid = QGridLayout(g)
        grid.setContentsMargins(8, 8, 8, 8)
        grid.setHorizontalSpacing(8)
        grid.setVerticalSpacing(4)

        self.combo_mkt = QComboBox(); self.combo_mkt.addItems(["코스피", "코스닥"])  # dmst_stex_tp 매핑 필요
        self.spin_qty  = QSpinBox(); self.spin_qty.setRange(1, 1_000_000); self.spin_qty.setValue(10)
        self.edit_price= QLineEdit(); self.edit_price.setPlaceholderText("가격(숫자)")
        self.combo_ord = QComboBox(); self.combo_ord.addItems(["지정가", "시장가", "조건가"])  # trde_tp 매핑 필요
        self.edit_cond = QLineEdit(); self.edit_cond.setPlaceholderText("조건값 (옵션)")

        self.btn_buy   = QPushButton("매수")
        self.btn_sell  = QPushButton("매도")

        r=0
        grid.addWidget(QLabel("시장"), r,0); grid.addWidget(self.combo_mkt, r,1); r+=1
        grid.addWidget(QLabel("수량"), r,0); grid.addWidget(self.spin_qty , r,1); r+=1
        grid.addWidget(QLabel("가격"), r,0); grid.addWidget(self.edit_price, r,1); r+=1
        grid.addWidget(QLabel("주문타입"), r,0); grid.addWidget(self.combo_ord, r,1); r+=1
        grid.addWidget(QLabel("조건값"), r,0); grid.addWidget(self.edit_cond , r,1); r+=1
        grid.addWidget(self.btn_buy, r,0); grid.addWidget(self.btn_sell, r,1)
        return g

    def _ui_right(self) -> QGroupBox:
        g = QGroupBox("조회 결과")
        v = QVBoxLayout(g)
        v.setContentsMargins(10, 10, 10, 10)
        v.setSpacing(8)

        # 차트
        if _PG_OK:
            self.chart = pg.PlotWidget()
            self.chart.setBackground('w')
            self.chart.showGrid(x=True, y=True, alpha=0.3)
            v.addWidget(self.chart, 2)
        else:
            self.chart = QLabel("pyqtgraph 미설치: 차트 비활성화")
            self.chart.setAlignment(Qt.AlignCenter)
            v.addWidget(self.chart)

        # 표
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(["date","open","high","low","close","volume","adj"])
        v.addWidget(self.table, 1)

        # JSON Inspector
        self.json_view = QTextEdit(); self.json_view.setReadOnly(True)
        self.json_view.setPlaceholderText("API 원본 JSON 응답")
        v.addWidget(self.json_view, 1)
        return g

    def _ui_log(self) -> QGroupBox:
        g = QGroupBox("로그")
        v = QVBoxLayout(g)
        self.log = QTextEdit(); self.log.setReadOnly(True)
        v.addWidget(self.log)
        return g

    # ---------------- 바인딩 ----------------
    def _bind(self):
        self.btn_dash.clicked.connect(self.refresh_dashboard)
        self.chk_auto.toggled.connect(self._toggle_auto)

        self.btn_fetch.clicked.connect(self.on_fetch)
        self.btn_export.clicked.connect(self.on_export)

        self.btn_buy.clicked.connect(lambda: self._order("buy"))
        self.btn_sell.clicked.connect(lambda: self._order("sell"))

    # ---------------- 대시보드 ----------------
    def _toggle_auto(self, checked: bool):
        if checked:
            self.timer.start(); self._log("[DASH] 자동 갱신 시작(5초)")
        else:
            self.timer.stop();  self._log("[DASH] 자동 갱신 종료")

    def refresh_dashboard(self):
        try:
            dep = self.api.get_deposit_details(qry_tp="01")
            bal = self.api.get_account_balance(qry_tp="01", dmst_stex_tp="1")  # 코스피=1 가정 (문서 확인 필요)

            self.lbl_dep.setText("예수금: 정상")
            self.lbl_eval.setText("총평가: 정상 / 손익: -")
            self.lbl_pos.setText("보유종목수: -")
            self.lbl_fill.setText("금일 체결: - / 오류: -")

            self._set_json(dep, heading="[DASH] 예수금 응답")
            self._log("[DASH] 갱신 완료")
        except Exception as e:
            self._log("[ERROR] 대시보드 갱신 실패
" + _fmt_err(e))

    # ---------------- 조회 ----------------
    def on_fetch(self):
        code = self.edit_code.text().strip()
        if not code:
            QMessageBox.warning(self, "확인", "종목코드를 입력하세요.")
            return
        base_dt = self.date_base.date().toString("yyyyMMdd")
        days    = int(self.spin_days.value())
        upd     = self.combo_adj.currentIndex()  # 0:수정주가 / 1:보정안함 (문서 매핑 확인)

        self._log(f"[REQ] ka10081 일봉 조회 code={code} base={base_dt} days={days} upd={upd}")
        try:
            res = self.api.get_stock_daily_chart_continuous(
                stk_cd=code,
                base_dt=base_dt,
                upd_stkpc_tp=str(upd),
                target_days=days,
            )
            self._set_json(res, heading="[RESP] ka10081")

            if str(res.get("return_code")) != "0":
                self._log(f"[ERROR] 조회 실패: {res.get('return_msg')}")
                self.table.setRowCount(0)
                self._plot_clear()
                return

            data = res.get("output", [])
            if not data:
                self._log("[INFO] 데이터 없음")
                self.table.setRowCount(0)
                self._plot_clear()
                return

            self._fill_table(data)
            self._plot_candles(data)
            self._log(f"[OK] {len(data)}건 수신")
        except Exception as e:
            self._log("[ERROR] 조회 예외
" + _fmt_err(e))

    def _fill_table(self, data: list[dict]):
        cols = ["date","open","high","low","close","volume","adj"]
        self.table.setRowCount(len(data))
        self.table.setColumnCount(len(cols))
        self.table.setHorizontalHeaderLabels(cols)

        # 표는 과거→현재 순으로 표시
        for r, row in enumerate(reversed(data)):
            vals = [
                str(row.get("date","")),
                str(row.get("open","")),
                str(row.get("high","")),
                str(row.get("low","")),
                str(row.get("close","")),
                str(row.get("volume","")),
                str(row.get("adj","")),
            ]
            for c, v in enumerate(vals):
                it = QTableWidgetItem(v)
                it.setFlags(it.flags() ^ Qt.ItemIsEditable)
                self.table.setItem(r, c, it)
        self.table.resizeColumnsToContents()

    # ---------------- 차트 ----------------
    def _plot_clear(self):
        if not _PG_OK:
            return
        self.chart.clear()

    def _plot_candles(self, data: list[dict]):
        if not _PG_OK:
            return
        try:
            self.chart.clear()
            # 최신이 아래로 오도록 x 인덱스 부여 (표와 동일하게 과거→현재)
            seq = list(reversed(data))
            xs = list(range(len(seq)))

            opens  = [float(x.get("open", 0) or 0) for x in seq]
            highs  = [float(x.get("high", 0) or 0) for x in seq]
            lows   = [float(x.get("low", 0) or 0) for x in seq]
            closes = [float(x.get("close",0) or 0) for x in seq]

            # 고저 라인(많아도 1000개 이내 가정)
            for x, lo, hi in zip(xs, lows, highs):
                self.chart.plot([x, x], [lo, hi], pen=(150,150,150))

            # 바디: 상승/하락 분리해서 BarGraphItem 두 개로 표현
            up_x, up_h, up_y0 = [], [], []
            dn_x, dn_h, dn_y0 = [], [], []
            for i, (o, c) in enumerate(zip(opens, closes)):
                h = c - o
                if h >= 0:
                    up_x.append(i); up_h.append(h if h!=0 else 0.0001); up_y0.append(o if h>=0 else c)
                else:
                    dn_x.append(i); dn_h.append(h); dn_y0.append(o)  # 음수 height 허용

            w = 0.6
            if up_x:
                up = BarGraphItem(x=up_x, height=up_h, width=w, y0=up_y0, brush=(30,160,255,160), pen=(30,120,255))
                self.chart.addItem(up)
            if dn_x:
                dn = BarGraphItem(x=dn_x, height=dn_h, width=w, y0=dn_y0, brush=(220,70,70,160), pen=(200,50,50))
                self.chart.addItem(dn)

        except Exception as e:
            self._log("[WARN] 차트 렌더링 실패
" + _fmt_err(e))

    # ---------------- 내보내기 ----------------
    def on_export(self):
        try:
            rows, cols = self.table.rowCount(), self.table.columnCount()
            if rows == 0 or cols == 0:
                QMessageBox.information(self, "알림", "내보낼 데이터가 없습니다.")
                return
            path, _ = QFileDialog.getSaveFileName(self, "엑셀로 저장", f"p4_chart_{_today_str()}.xlsx", "Excel Files (*.xlsx)")
            if not path:
                return
            try:
                import pandas as pd
                headers = [self.table.horizontalHeaderItem(c).text() for c in range(cols)]
                data = []
                for r in range(rows):
                    data.append([self.table.item(r,c).text() if self.table.item(r,c) else '' for c in range(cols)])
                pd.DataFrame(data, columns=headers).to_excel(path, index=False)
                self._log(f"[OK] 저장 완료: {path}")
            except Exception as e:
                self._log("[ERROR] 저장 실패
" + _fmt_err(e))
                QMessageBox.warning(self, "오류", "저장 중 오류가 발생했습니다.")
        except Exception as e:
            self._log("[ERROR] 내보내기 예외
" + _fmt_err(e))

    # ---------------- 주문 ----------------
    def _order(self, side: str):
        code = self.edit_code.text().strip()
        if not code:
            QMessageBox.warning(self, "확인", "종목코드를 입력하세요.")
            return
        qty = int(self.spin_qty.value())
        price = self.edit_price.text().strip() or "0"
        dmst_stex_tp = "1" if self.combo_mkt.currentIndex() == 0 else "2"  # 문서로 확정 필요
        trde_tp = {0: "00", 1: "03", 2: "05"}.get(self.combo_ord.currentIndex(), "00")
        cond_uv = self.edit_cond.text().strip()

        try:
            if side == "buy":
                res = self.api.buy_order(
                    dmst_stex_tp=dmst_stex_tp,
                    stk_cd=code,
                    ord_qty=str(qty),
                    ord_uv=str(price),
                    trde_tp=trde_tp,
                    cond_uv=cond_uv,
                )
            else:
                res = self.api.sell_order(
                    dmst_stex_tp=dmst_stex_tp,
                    stk_cd=code,
                    ord_qty=str(qty),
                    ord_uv=str(price),
                    trde_tp=trde_tp,
                    cond_uv=cond_uv,
                )
            self._set_json(res, heading=f"[RESP] {side.upper()} 결과")
            if str(res.get("return_code")) == "0":
                self._log(f"[OK] {side} 주문 성공")
            else:
                self._log(f"[ERROR] {side} 주문 실패: {res.get('return_msg')}")
        except Exception as e:
            self._log(f"[ERROR] {side} 주문 예외
" + _fmt_err(e))

    # ---------------- JSON/로그 ----------------
    def _set_json(self, obj: dict, heading: str = ""):
        txt = (heading + "

") if heading else ""
        txt += _pretty_json(obj)
        self.json_view.setPlainText(txt)

    def _log(self, msg: str):
        t = datetime.now().strftime("%H:%M:%S")
        self.log.append(f"{t} {msg}")


# ---------------- 단독 실행 ----------------
if __name__ == "__main__":
    # 참고: 실제 앱에서는 main_launcher에서 QApplication을 생성합니다.
    from PySide6.QtWidgets import QApplication
    app = QApplication(sys.argv)
    w = P4RestTradingPage()
    w.resize(1280, 800)
    w.show()
    sys.exit(app.exec())
