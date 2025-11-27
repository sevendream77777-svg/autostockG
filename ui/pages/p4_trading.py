# ui/pages/p4_trading.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, csv, traceback
from typing import List, Dict, Any, Optional

from PySide6.QtCore import Qt, QDate
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QSpinBox, QCheckBox, QTextEdit, QDateEdit, QTableWidget, QTableWidgetItem,
    QFileDialog, QGroupBox
)

import requests

# ---------------------------------------------------------
# 환경 변수
# ---------------------------------------------------------
KIWOOM_HOST = os.getenv("KIWOOM_HOST", "https://api.kiwoom.com")
AUTH_TOKEN  = os.getenv("KIWOOM_TOKEN", "")  # "Bearer ..." 또는 순수 토큰

def _bearer(token: str) -> str:
    t = (token or "").strip()
    return t if t.lower().startswith("bearer ") else f"Bearer {t}" if t else ""


# ---------------------------------------------------------
# 공통 유틸
# ---------------------------------------------------------
def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

def normalize_ohlcv(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in items:
        date = r.get("dt") or r.get("date")
        out.append({
            "date": date,
            "open": r.get("open_pric"),
            "high": r.get("high_pric"),
            "low":  r.get("low_pric"),
            "close": r.get("close_pric"),
            "volume": r.get("trde_qty"),
        })
    return out


# ---------------------------------------------------------
# 디버그 가능한 POST 함수
# ---------------------------------------------------------
def debug_post(url: str, headers: Dict[str, str], body: Dict[str, Any], timeout: int = 15):
    """실제 전송된 헤더/바디/URL + 응답/에러까지 모두 반환"""
    session = requests.Session()
    req = requests.Request("POST", url, headers=headers, json=body)
    prepped = session.prepare_request(req)

    result = {
        "outgoing_url": prepped.url,
        "outgoing_headers": dict(prepped.headers),
        "outgoing_body": body,
        "status": None,
        "resp_headers": {},
        "json": None,
        "text": None,
        "error": None
    }

    try:
        resp = session.send(prepped, timeout=timeout)
        result["status"] = resp.status_code
        result["resp_headers"] = dict(resp.headers)

        try:
            result["json"] = resp.json()
        except Exception:
            result["text"] = resp.text

        resp.raise_for_status()

    except Exception:
        result["error"] = traceback.format_exc()

    return result


# ---------------------------------------------------------
# TradingPage UI
# ---------------------------------------------------------
class TradingPage(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()
        self._connect()
        self._log("[SYSTEM] P4 (Kiwoom REST, Debug Mode Enabled) Loaded.")
        self._log(f"[ENV] KIWOOM_HOST={KIWOOM_HOST}")
        self._log(f"[ENV] KIWOOM_TOKEN={'SET' if AUTH_TOKEN else 'EMPTY'}")

    # ---------------- UI 구성 ----------------
    def _setup_ui(self):
        root = QVBoxLayout(self)

        # 대시보드
        box_dash = QGroupBox("요약 대시보드")
        g = QGridLayout(box_dash)
        self.lbl_dep = QLabel("예수금: -")
        self.lbl_hold = QLabel("보유종목수: -")
        self.lbl_pnl = QLabel("총평가/손익: -")
        self.btn_dash = QPushButton("대시보드 갱신")

        g.addWidget(self.lbl_dep, 0, 0)
        g.addWidget(self.lbl_hold, 0, 1)
        g.addWidget(self.lbl_pnl, 1, 0)
        g.addWidget(self.btn_dash, 1, 1)
        root.addWidget(box_dash)

        # 디버그 옵션
        dbg_box = QGroupBox("디버그 옵션")
        dg = QGridLayout(dbg_box)
        self.chk_debug = QCheckBox("디버그 모드(전체 요청/응답 출력)")
        self.chk_dual_auth = QCheckBox("authorization 헤더 이중전송")
        self.chk_save_req = QCheckBox("요청 결과 저장(CSV/JSON)")

        dg.addWidget(self.chk_debug, 0, 0)
        dg.addWidget(self.chk_dual_auth, 0, 1)
        dg.addWidget(self.chk_save_req, 0, 2)
        root.addWidget(dbg_box)

        # 본문
        body = QHBoxLayout()
        root.addLayout(body)

        # 좌측
        left = QVBoxLayout()
        body.addLayout(left, 4)

        # 조회 박스
        gb_q = QGroupBox("조회 패널 (ka10081)")
        qg = QGridLayout(gb_q)

        self.ed_code = QLineEdit(); self.ed_code.setPlaceholderText("종목코드 예: 005930")
        self.btn_code = QPushButton("코드조회")

        self.de_base = QDateEdit(QDate.currentDate())
        self.de_base.setDisplayFormat("yyyy-MM-dd")
        self.de_base.setCalendarPopup(True)

        self.sp_cnt = QSpinBox(); self.sp_cnt.setRange(1, 3000); self.sp_cnt.setValue(120)
        self.cmb_adj = QComboBox(); self.cmb_adj.addItems(["수정주가(1)", "원주가(0)"])
        self.chk_cont = QCheckBox("연속조회")

        self.btn_query = QPushButton("차트/데이터 조회")
        self.btn_export = QPushButton("CSV 저장")

        r = 0
        qg.addWidget(QLabel("종목"), r,0); qg.addWidget(self.ed_code, r,1); qg.addWidget(self.btn_code, r,2); r+=1
        qg.addWidget(QLabel("기준일"), r,0); qg.addWidget(self.de_base, r,1); qg.addWidget(QLabel("조회일수"), r,2); qg.addWidget(self.sp_cnt, r,3); r+=1
        qg.addWidget(QLabel("옵션"), r,0); qg.addWidget(self.cmb_adj, r,1); qg.addWidget(self.chk_cont, r,2); r+=1
        qg.addWidget(self.btn_query, r,0,1,2); qg.addWidget(self.btn_export, r,2,1,2)

        left.addWidget(gb_q)

        # 주문 박스
        gb_o = QGroupBox("주문 (kt10000/kt10001)")
        og = QGridLayout(gb_o)

        self.cmb_mkt = QComboBox(); self.cmb_mkt.addItems(["KRX", "NXT", "SOR"])
        self.sp_qty = QSpinBox(); self.sp_qty.setRange(1, 1000000); self.sp_qty.setValue(10)
        self.ed_price = QLineEdit(); self.ed_price.setPlaceholderText("가격(시장가=0)")
        self.cmb_type = QComboBox(); self.cmb_type.addItems(["지정가(0)", "시장가(3)", "조건부지정가(5)"])
        self.ed_cond = QLineEdit(); self.ed_cond.setPlaceholderText("조건단가(optional)")
        self.btn_buy = QPushButton("매수"); self.btn_sell = QPushButton("매도")

        r=0
        og.addWidget(QLabel("거래소"), r,0); og.addWidget(self.cmb_mkt, r,1); r+=1
        og.addWidget(QLabel("수량"), r,0); og.addWidget(self.sp_qty, r,1); r+=1
        og.addWidget(QLabel("가격"), r,0); og.addWidget(self.ed_price, r,1); r+=1
        og.addWidget(QLabel("주문타입"), r,0); og.addWidget(self.cmb_type, r,1); r+=1
        og.addWidget(QLabel("조건값"), r,0); og.addWidget(self.ed_cond, r,1); r+=1
        og.addWidget(self.btn_buy, r,0); og.addWidget(self.btn_sell, r,1)

        left.addWidget(gb_o)
        left.addStretch(1)

        # 우측
        right = QVBoxLayout()
        body.addLayout(right, 6)

        self.tbl = QTableWidget(0, 6)
        self.tbl.setHorizontalHeaderLabels(["date","open","high","low","close","volume"])
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setEditTriggers(QTableWidget.NoEditTriggers)

        self.txt_json = QTextEdit(); self.txt_json.setReadOnly(True)
        self.txt_json.setPlaceholderText("API JSON / 요청·응답 디버그 전체 표시")

        right.addWidget(self.tbl, 6)
        right.addWidget(self.txt_json, 4)

        self.txt_log = QTextEdit(); self.txt_log.setReadOnly(True)
        root.addWidget(self.txt_log)

    # ---------------------------------------------------------
    def _connect(self):
        self.btn_dash.clicked.connect(self._refresh_dashboard)
        self.btn_query.clicked.connect(self._on_query)
        self.btn_export.clicked.connect(self._on_export)
        self.btn_buy.clicked.connect(lambda: self._on_order("BUY"))
        self.btn_sell.clicked.connect(lambda: self._on_order("SELL"))

    # ---------------------------------------------------------
    # 대시보드
    # ---------------------------------------------------------
    def _refresh_dashboard(self):
        self.lbl_dep.setText(f"예수금: - (token={'SET' if AUTH_TOKEN else 'EMPTY'})")
        self.lbl_hold.setText("보유종목수: -")
        self.lbl_pnl.setText("총평가/손익: -")
        self._log("대시보드 갱신")

    # ---------------------------------------------------------
    # 조회
    # ---------------------------------------------------------
    def _on_query(self):
        code = self.ed_code.text().strip()
        if not code:
            self._log("종목코드를 입력하세요.")
            return

        base_dt = self.de_base.date().toString("yyyyMMdd")
        upd = "1" if self.cmb_adj.currentText().startswith("수정") else "0"
        want = self.sp_cnt.value()
        use_cont = self.chk_cont.isChecked()

        # 헤더 구성
        token = _bearer(AUTH_TOKEN)
        headers = {
            "api-id": "ka10081",
            "Content-Type": "application/json;charset=UTF-8"
        }
        if token:
            headers["authorization"] = token
            if self.chk_dual_auth.isChecked():
                headers["Authorization"] = token

        url = f"{KIWOOM_HOST}/api/dostk/chart"
        body = {
    "stk_cd": code,
    "base_dt": base_dt,
    "term_cnt": str(want),         # 조회 개수 필수
    "adj_prc_tp": upd              # 수정주가
}


        # 첫 요청
        out_all = []
        calls = []

        res = debug_post(url, headers, body)
        calls.append(self._trim(res))

        items = []
        if isinstance(res.get("json"), dict):
            items = res["json"].get("data", [])
        elif isinstance(res.get("json"), list):
            items = res["json"]

        out_all.extend(items or [])

        cont_yn = res.get("resp_headers", {}).get("cont-yn", "N")
        next_key = res.get("resp_headers", {}).get("next-key", None)

        # 연속조회
        while use_cont and cont_yn == "Y" and next_key and len(out_all) < want:
            h2 = headers.copy()
            h2["cont-yn"] = "Y"
            h2["next-key"] = next_key

            res2 = debug_post(url, h2, body)
            calls.append(self._trim(res2))

            items2 = []
            if isinstance(res2.get("json"), dict):
                items2 = res2["json"].get("data", [])
            elif isinstance(res2.get("json"), list):
                items2 = res2["json"]

            out_all.extend(items2 or [])

            cont_yn = res2.get("resp_headers", {}).get("cont-yn", "N")
            next_key = res2.get("resp_headers", {}).get("next-key", None)

        # 표 표시
        norm = normalize_ohlcv(out_all[:want])
        self._fill_table(norm)

        preview = {"preview": norm[:5], "total": len(norm), "calls": calls}
        self.txt_json.setText(pretty(preview))

        if self.chk_save_req.isChecked():
            self._save_debug_dump("ka10081", code, preview)

        self._log(f"[조회완료] {code} count={len(norm)} status={res.get('status')}")

    # ---------------------------------------------------------
    # 주문
    # ---------------------------------------------------------
    def _on_order(self, side: str):
        code = self.ed_code.text().strip()
        if not code:
            self._log("종목코드를 입력하세요.")
            return

        mkt = self.cmb_mkt.currentText().strip()
        qty = str(self.sp_qty.value())
        price = self.ed_price.text().strip() or "0"
        if not price.isdigit():
            self._log("가격은 숫자만 입력(시장가=0)")
            return

        typ_txt = self.cmb_type.currentText()
        trde_tp = "0" if "지정가" in typ_txt else ("3" if "시장가" in typ_txt else "5")
        cond = self.ed_cond.text().strip()

        token = _bearer(AUTH_TOKEN)
        headers = {
            "api-id": "kt10000" if side == "BUY" else "kt10001",
            "Content-Type": "application/json;charset=UTF-8"
        }
        if token:
            headers["authorization"] = token
            if self.chk_dual_auth.isChecked():
                headers["Authorization"] = token

        url = f"{KIWOOM_HOST}/api/dostk/ordr"
        body = {
            "dmst_stex_tp": mkt,
            "stk_cd": code,
            "ord_qty": qty,
            "ord_uv": price,
            "trde_tp": trde_tp,
            "cond_uv": cond or ""
        }

        res = debug_post(url, headers, body)
        payload = self._trim(res)

        self.txt_json.setText(pretty(payload))

        if self.chk_save_req.isChecked():
            self._save_debug_dump("order_"+side.lower(), code, payload)

        self._log(f"[주문전송] {side} {code} x{qty} @{price} status={res.get('status')}")

    # ---------------------------------------------------------
    # 헬퍼: 응답에서 핵심만 남겨 정리
    # ---------------------------------------------------------
    def _trim(self, res: Dict[str, Any]):
        return {
            "status": res.get("status"),
            "outgoing_url": res.get("outgoing_url"),
            "outgoing_headers": res.get("outgoing_headers"),
            "resp_headers": res.get("resp_headers"),
            "json": res.get("json"),
            "text": res.get("text"),
            "error": res.get("error")
        }

    # ---------------------------------------------------------
    # CSV 테이블 출력
    # ---------------------------------------------------------
    def _fill_table(self, rows: List[Dict[str, Any]]):
        self.tbl.setRowCount(0)
        for r in rows:
            i = self.tbl.rowCount()
            self.tbl.insertRow(i)
            self.tbl.setItem(i, 0, QTableWidgetItem(str(r.get("date",""))))
            self.tbl.setItem(i, 1, QTableWidgetItem(str(r.get("open",""))))
            self.tbl.setItem(i, 2, QTableWidgetItem(str(r.get("high",""))))
            self.tbl.setItem(i, 3, QTableWidgetItem(str(r.get("low",""))))
            self.tbl.setItem(i, 4, QTableWidgetItem(str(r.get("close",""))))
            self.tbl.setItem(i, 5, QTableWidgetItem(str(r.get("volume",""))))

    # ---------------------------------------------------------
    # 조회 결과 CSV 저장
    # ---------------------------------------------------------
    def _on_export(self):
        """조회 결과 테이블을 CSV 파일로 저장"""
        if self.tbl.rowCount() == 0:
            self._log("저장할 데이터 없음.")
            return

        fn, _ = QFileDialog.getSaveFileName(
            self,
            "조회 결과 CSV 저장",
            "chart_result.csv",
            "CSV (*.csv)"
        )
        if not fn:
            return

        try:
            headers = ["date","open","high","low","close","volume"]
            with open(fn, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(headers)
                for i in range(self.tbl.rowCount()):
                    row = []
                    for c in range(self.tbl.columnCount()):
                        it = self.tbl.item(i,c)
                        row.append(it.text() if it else "")
                    w.writerow(row)
            self._log(f"[CSV 저장완료] {fn}")

        except Exception as e:
            self._log(f"[CSV 저장오류] {e}")

    # ---------------------------------------------------------
    # 요청/응답 디버그 저장(JSON + CSV)
    # ---------------------------------------------------------
    def _save_debug_dump(self, tag: str, code: str, data: Dict[str, Any]):
        """디버그 요청/응답 저장"""

        # JSON 저장
        try:
            fn_json, _ = QFileDialog.getSaveFileName(
                self,
                f"{tag} 저장(JSON)",
                f"{tag}_{code}.json",
                "JSON (*.json)"
            )
            if fn_json:
                with open(fn_json, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                self._log(f"[JSON 저장완료] {fn_json}")
        except Exception as e:
            self._log(f"[JSON 저장오류] {e}")

        # 테이블 CSV 저장
        try:
            if self.tbl.rowCount() == 0:
                return

            fn_csv, _ = QFileDialog.getSaveFileName(
                self,
                f"{tag} CSV 저장",
                f"{tag}_{code}.csv",
                "CSV (*.csv)"
            )
            if fn_csv:
                headers = ["date","open","high","low","close","volume"]
                with open(fn_csv, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(headers)
                    for i in range(self.tbl.rowCount()):
                        row = []
                        for c in range(self.tbl.columnCount()):
                            t = self.tbl.item(i,c)
                            row.append(t.text() if t else "")
                        w.writerow(row)

                self._log(f"[CSV 저장완료] {fn_csv}")

        except Exception as e:
            self._log(f"[CSV 저장오류] {e}")

    # ---------------------------------------------------------
    # 로그 출력
    # ---------------------------------------------------------
    def _log(self, msg: str):
        self.txt_log.append(msg)
