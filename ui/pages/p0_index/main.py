# -*- coding: utf-8 -*-
"""
p0_index.py (restructured)
- UI만 유지, 수집 로직은 ui/sources 모듈에서 담당
- 소스: Kiwoom / PyKRX / DART / Naver / Yahoo (기존 47db, p0_light, 8db_final, p0_index 로직을 호출)
"""
import os
import sys
import time
import math
from typing import Any, Dict

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
)

# 상대/절대 병행 임포트로 ui.py 실행 시 순환 회피
try:
    from ..sources import ALL_COLUMNS, collect_all, collapse_field  # 패키지 실행
except Exception:  # pragma: no cover
    from ui.sources import ALL_COLUMNS, collect_all, collapse_field  # ui.py 직접 실행


class FieldCard(QFrame):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.has_value = False
        self.setObjectName("FieldCard")
        self.setStyleSheet(
            """
            QFrame#FieldCard {
                background-color: #1f2937;
                border: 1px solid #334155;
                border-radius: 8px;
            }
        """
        )
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 8, 10, 8)
        lay.setSpacing(8)

        self.icon = QLabel("⚪")
        self.icon.setFixedWidth(20)
        self.icon.setStyleSheet("font-size: 14px;")

        self.name_label = QLabel(name)
        self.name_label.setStyleSheet("color: #e5e7eb; font-weight: 700;")

        self.value_label = QLabel("—")
        self.value_label.setStyleSheet("color: #cbd5e1;")
        self.value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.value_label.setWordWrap(True)

        lay.addWidget(self.icon)
        lay.addWidget(self.name_label, 1)
        lay.addWidget(self.value_label, 2)

    def set_value(self, text: str):
        if not text:
            return
        self.icon.setText("✅")
        disp = text
        if len(disp) > 120:
            disp = disp[:117] + "..."
        self.value_label.setText(disp)
        self.has_value = True

    def clear(self):
        self.icon.setText("⚪")
        self.value_label.setText("—")
        self.has_value = False


class P0_Index(QWidget):
    def __init__(self):
        super().__init__()
        self.columns = ALL_COLUMNS
        # v52 확정 리스트(52개) - 테이블용
        self.v52_display = [
            # Meta 7
            "date", "code", "name", "market", "listing_status", "sector_code", "sector_name",
            # Price 10
            "open", "high", "low", "close", "volume", "amount", "adj_factor", "vwap", "market_cap", "shares_out",
            # Flow 12
            "frgn_net_amt", "inst_net_amt", "nps_net_amt", "tust_net_amt", "dealer_net_amt",
            "frgn_net_qty", "inst_net_qty", "nps_net_qty",
            "short_sell_amt", "short_sell_qty", "loan_balance_amt", "loan_balance_qty",
            # Finance 12
            "announce_date", "revenue", "op_income", "net_income", "total_equity", "total_assets",
            "cash_flow_op", "cash_flow_inv", "cash_flow_fin", "div_amount", "eps", "roe",
            # Macro & Event 11
            "usdkrw", "us10y_yield", "kr10y_yield", "wti", "dxy", "cnykrw", "gold",
            "ex_div_date", "earnings_date", "bps", "debt_ratio",
        ]
        self.cards: Dict[str, FieldCard] = {}
        self.last_payloads: Dict[str, Dict[str, Any]] = {}
        self.last_by_field: Dict[str, Any] = {}
        self._init_ui()

    def _init_ui(self):
        root = QVBoxLayout(self)

        header = QHBoxLayout()
        title = QLabel("V58+ 통합 수집기 (모듈화)")
        title.setStyleSheet("font-size: 20px; font-weight: bold; color: #facc15;")
        header.addWidget(title)
        root.addLayout(header)

        root.addWidget(self._build_controls())

        self.log_area = QTextEdit()
        self.log_area.setPlaceholderText("로그 출력...")
        self.log_area.setFixedHeight(160)
        self.log_area.setStyleSheet(
            "background-color: #0f172a; color: #cbd5e1; font-family: Consolas; font-size: 12px;"
        )
        root.addWidget(self.log_area)

        # splitter로 테이블/카드 영역을 리사이즈 가능하게 분리
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_source_table())
        splitter.addWidget(self._build_field_grid())
        splitter.setSizes([300, 600])
        root.addWidget(splitter, 1)

    def _build_source_table(self):
        box = QGroupBox("컬럼별 소스 현황 (v52)")
        vlay = QVBoxLayout(box)
        self.summary_label = QLabel("확정 0 | 의심 0 | 실패 0")
        vlay.addWidget(self.summary_label)

        self.source_table = QTableWidget()
        self.source_table.setColumnCount(3)
        self.source_table.setHorizontalHeaderLabels(["컬럼", "상태", "소스-값"])
        self.source_table.setRowCount(len(self.v52_display))
        self.source_table.verticalHeader().setVisible(False)
        self.source_table.horizontalHeader().setStretchLastSection(True)
        self.source_table.setEditTriggers(QTableWidget.NoEditTriggers)
        vlay.addWidget(self.source_table)
        return box

    def _build_controls(self):
        box = QGroupBox("설정")
        lay = QHBoxLayout(box)
        self.code_edit = QLineEdit("005930")
        self.code_edit.setPlaceholderText("종목코드")
        self.date_edit = QLineEdit(QDate.currentDate().toString("yyyyMMdd"))

        btn_run = QPushButton("🚀 실행 (모듈)")
        btn_run.setStyleSheet("background-color: #dc2626; color: white; font-weight: bold; padding: 6px;")
        btn_run.clicked.connect(self.run_full_analysis)

        btn_save = QPushButton("💾 로그 저장")
        btn_save.clicked.connect(self.save_payloads_to_file)

        lay.addWidget(QLabel("Code:"))
        lay.addWidget(self.code_edit)
        lay.addWidget(QLabel("Date:"))
        lay.addWidget(self.date_edit)
        lay.addWidget(btn_run)
        lay.addWidget(btn_save)
        return box

    def _build_field_grid(self):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: none;")
        con = QWidget()
        self.grid = QGridLayout(con)
        self.grid.setSpacing(6)

        cols = 4
        for i, c in enumerate(self.columns):
            card = FieldCard(c)
            self.cards[c] = card
            self.grid.addWidget(card, i // cols, i % cols)

        scroll.setWidget(con)
        return scroll

    def log(self, msg: str):
        self.log_area.append(msg)
        print(msg)
        QApplication.processEvents()

    def _normalize_date(self, text: str):
        return text.replace("-", "").replace(".", "").strip()

    def run_full_analysis(self):
        self.log_area.clear()
        for c in self.cards.values():
            c.clear()

        code = self.code_edit.text().strip()
        base_dt = self._normalize_date(self.date_edit.text().strip())
        self.log(f"=== [분석 시작] Target: {code}, Date: {base_dt} ===")

        payload = collect_all(code, base_dt)
        self.last_payloads = payload.get("by_source", {})
        merged = payload.get("by_field", {})
        self.last_by_field = merged

        filled = 0
        for col in self.columns:
            values = merged.get(col, [])
            disp = collapse_field(values)
            if disp:
                self.cards[col].set_value(disp)
                filled += 1
        self.log(f"[완료] {filled}/{len(self.columns)} 필드 업데이트")

        # v52 테이블 업데이트
        self._update_source_table()

    def _collect_card_results(self) -> Dict[str, Any]:
        results = {}
        for key, card in self.cards.items():
            if card.has_value:
                results[key] = card.value_label.text()
        return results

    def _update_source_table(self):
        """v52 리스트 기준으로 상태/소스별 값을 테이블에 표시"""
        confirmed = suspect = failed = 0

        for row, col in enumerate(self.v52_display):
            values = self.last_by_field.get(col, [])
            status = "❌ 실패"
            if not values:
                failed += 1
            else:
                # 서로 다른 값이 있는지 판정
                unique_vals = []
                for _, val in values:
                    if val not in unique_vals:
                        unique_vals.append(val)
                if len(unique_vals) > 1:
                    status = "⚠ 의심"
                    suspect += 1
                else:
                    status = "✅ 확정"
                    confirmed += 1

            self.source_table.setItem(row, 0, QTableWidgetItem(col))
            self.source_table.setItem(row, 1, QTableWidgetItem(status))
            self.source_table.setItem(row, 2, QTableWidgetItem(collapse_field(values)))

        self.summary_label.setText(f"확정 {confirmed} | 의심 {suspect} | 실패 {failed}")

    def save_payloads_to_file(self):
        if not self.last_payloads:
            QMessageBox.information(self, "저장 불가", "먼저 실행하세요.")
            return
        code = self.code_edit.text().strip() or "unknown"
        base_dt = self._normalize_date(self.date_edit.text().strip()) or QDate.currentDate().toString("yyyyMMdd")
        root_dir = os.path.dirname(os.path.abspath(__file__))
        logs_dir = os.path.join(root_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        fname = f"p0_dump_{code}_{base_dt}_{int(time.time())}.json"
        path = os.path.join(logs_dir, fname)
        data = {
            "code": code,
            "date": base_dt,
            "result": self._collect_card_results(),
            "raw": self.last_payloads,
        }
        try:
            import json

            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log(f"[파일 저장] {path}")
            QMessageBox.information(self, "저장 완료", f"로그를 저장했습니다:\n{path}")
        except Exception as e:
            QMessageBox.warning(self, "저장 실패", str(e))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = P0_Index()
    win.show()
    sys.exit(app.exec())
