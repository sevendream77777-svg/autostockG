#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_v48 폴더용 간단 실행 UI
- 폴더 내 파이썬 실행 파일 선택
- 날짜 구간/종목 코드 입력
- 로그 파일 경로 지정 및 실행 로그 표시
- 결과 파일(선택) 앞/뒤 일부 미리보기
"""

from __future__ import annotations

import queue
import shlex
import subprocess
import sys
import threading
import os
import signal
import shutil
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext


BASE_DIR = Path(__file__).resolve().parent
HOJ_DIR = BASE_DIR / "raw_hoj"
SLE_DIR = BASE_DIR / "raw_sle"
SNAPSHOT_FILE = BASE_DIR / "ui_snapshot.txt"


def find_python_scripts(base: Path) -> List[Path]:
    """지정 폴더 내 실행 가능한 파이썬 스크립트 목록 반환."""
    scripts: List[Path] = []
    if not base.exists():
        return scripts
    for path in base.rglob("*.py"):
        if path.name.startswith("__"):
            continue
        if path.name == Path(__file__).name:
            continue
        # 캐시/가상환경 제외
        if any(p in path.parts for p in ("__pycache__", ".venv", "env", "venv")):
            continue
        scripts.append(path)
    scripts.sort()
    return scripts


def read_preview_lines(path: Path, n: int = 20, max_bytes: int = 200_000) -> Tuple[List[str], List[str], bool]:
    """
    파일 미리보기용 라인 반환.
    - 파일이 작으면(<=max_bytes) 전체를 반환.
    - 크면 앞/뒤 n줄만 반환.
    반환: head, tail, is_full (전체 반환 여부)
    """
    head: List[str] = []
    tail: deque[str] = deque(maxlen=n)
    is_full = True
    try:
        if path.stat().st_size > max_bytes:
            is_full = False
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for idx, line in enumerate(f):
                line = line.rstrip("\n")
                if is_full:
                    head.append(line)
                else:
                    if idx < n:
                        head.append(line)
                    tail.append(line)
    except Exception as e:
        return [f"[미리보기 실패] {e}"], [], True
    return head, list(tail), is_full


class RunnerUI:
    def __init__(self, master: tk.Tk):
        self.master = master
        master.title("raw 데이터 실행 UI (hoj/sle 테스트)")
        master.geometry("900x720")

        self.scripts: List[Path] = []
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.running_thread: threading.Thread | None = None
        self.is_running = False
        self.current_proc: subprocess.Popen | None = None

        # 입력 변수
        self.start_date = tk.StringVar(value="20100102")
        self.end_date = tk.StringVar(value="20251205")
        self.code1 = tk.StringVar(value="005930")  # 삼성전자
        self.code2 = tk.StringVar(value="000660")  # SK하이닉스
        self.code3 = tk.StringVar()
        self.codes_file = tk.StringVar(value="")
        self.output_dir = tk.StringVar()
        self.result_file = tk.StringVar()
        self.log_path = tk.StringVar()
        self.dart_mode = tk.StringVar(value="annual")
        self.extra_args = tk.StringVar()
        self.workers = tk.StringVar(value="1")
        self.announce_mode = tk.StringVar(value="none")  # run_raw_sle 전용
        self.list_cache_dir = tk.StringVar()
        self.dataset = tk.StringVar(value="hoj")  # hoj | sle

        self.current_base = HOJ_DIR
        self._build_layout()
        self._apply_dataset_defaults()
        self.refresh_scripts()
        self.master.after(200, self._poll_log_queue)

    # ------------------------------------------------------------------ UI 구성
    def _build_layout(self):
        # 스크립트 영역
        frame_scripts = tk.LabelFrame(self.master, text="실행 파일 목록")
        frame_scripts.pack(fill="x", padx=10, pady=6)

        self.listbox = tk.Listbox(frame_scripts, height=6)
        self.listbox.pack(side="left", fill="x", expand=True, padx=(10, 4), pady=6)

        btn_frame = tk.Frame(frame_scripts)
        btn_frame.pack(side="right", padx=10, pady=6)
        tk.Button(btn_frame, text="새로고침", command=self.refresh_scripts, width=12).pack(pady=2)
        tk.Button(btn_frame, text="경로 복사", command=self.copy_selected_path, width=12).pack(pady=2)

        # 데이터셋 선택
        frame_dataset = tk.Frame(frame_scripts)
        frame_dataset.pack(side="bottom", fill="x", padx=10, pady=4)
        tk.Label(frame_dataset, text="데이터셋").pack(side="left")
        tk.Radiobutton(frame_dataset, text="HOJ(36)", variable=self.dataset, value="hoj", command=self.on_dataset_change).pack(side="left", padx=4)
        tk.Radiobutton(frame_dataset, text="SLE(11)", variable=self.dataset, value="sle", command=self.on_dataset_change).pack(side="left", padx=4)

        # 기본 입력
        frame_inputs = tk.LabelFrame(self.master, text="실행 옵션")
        frame_inputs.pack(fill="x", padx=10, pady=6)

        # 날짜
        row_dates = tk.Frame(frame_inputs)
        row_dates.pack(fill="x", padx=6, pady=2)
        tk.Label(row_dates, text="시작일(YYYYMMDD)").pack(side="left")
        tk.Entry(row_dates, textvariable=self.start_date, width=12).pack(side="left", padx=4)
        tk.Label(row_dates, text="종료일").pack(side="left")
        tk.Entry(row_dates, textvariable=self.end_date, width=12).pack(side="left", padx=4)
        tk.Label(row_dates, text="DART 모드").pack(side="left", padx=(10, 2))
        tk.OptionMenu(row_dates, self.dart_mode, "off", "annual", "full").pack(side="left")
        tk.Label(row_dates, text="워커").pack(side="left", padx=(10, 2))
        tk.Entry(row_dates, textvariable=self.workers, width=6).pack(side="left")

        # announce 옵션 (SLE)
        row_ann = tk.Frame(frame_inputs)
        row_ann.pack(fill="x", padx=6, pady=2)
        tk.Label(row_ann, text="announce_mode").pack(side="left")
        tk.OptionMenu(row_ann, self.announce_mode, "none", "cache", "hybrid", "live").pack(side="left", padx=4)
        tk.Label(row_ann, text="list cache").pack(side="left", padx=(10, 2))
        tk.Entry(row_ann, textvariable=self.list_cache_dir, width=45).pack(side="left", padx=4, fill="x", expand=True)
        tk.Button(row_ann, text="선택", command=self.pick_list_cache_dir, width=8).pack(side="left", padx=4)

        # 종목 코드
        row_codes = tk.Frame(frame_inputs)
        row_codes.pack(fill="x", padx=6, pady=2)
        tk.Label(row_codes, text="종목코드(최대 3개)").pack(side="left")
        tk.Entry(row_codes, textvariable=self.code1, width=10).pack(side="left", padx=3)
        tk.Entry(row_codes, textvariable=self.code2, width=10).pack(side="left", padx=3)
        tk.Entry(row_codes, textvariable=self.code3, width=10).pack(side="left", padx=3)

        row_codes_file = tk.Frame(frame_inputs)
        row_codes_file.pack(fill="x", padx=6, pady=2)
        tk.Label(row_codes_file, text="종목코드 파일(선택)").pack(side="left")
        tk.Entry(row_codes_file, textvariable=self.codes_file, width=40).pack(side="left", padx=4, fill="x", expand=True)
        tk.Button(row_codes_file, text="선택", command=self.pick_codes_file, width=8).pack(side="left", padx=4)

        # 출력/로그/결과 파일 경로
        row_paths = tk.Frame(frame_inputs)
        row_paths.pack(fill="x", padx=6, pady=2)
        tk.Label(row_paths, text="출력/데이터 폴더").pack(side="left")
        tk.Entry(row_paths, textvariable=self.output_dir, width=45).pack(side="left", padx=4, fill="x", expand=True)
        tk.Button(row_paths, text="선택", command=self.pick_output_dir, width=8).pack(side="left", padx=4)

        row_result = tk.Frame(frame_inputs)
        row_result.pack(fill="x", padx=6, pady=2)
        tk.Label(row_result, text="결과 파일(미리보기)").pack(side="left")
        tk.Entry(row_result, textvariable=self.result_file, width=45).pack(side="left", padx=4, fill="x", expand=True)
        tk.Button(row_result, text="선택", command=self.pick_result_file, width=8).pack(side="left", padx=4)
        tk.Button(row_result, text="최근 결과", command=self.pick_latest_result_file, width=10).pack(side="left", padx=4)

        row_log = tk.Frame(frame_inputs)
        row_log.pack(fill="x", padx=6, pady=2)
        tk.Label(row_log, text="로그 파일").pack(side="left")
        tk.Entry(row_log, textvariable=self.log_path, width=45).pack(side="left", padx=4, fill="x", expand=True)
        tk.Button(row_log, text="선택", command=self.pick_log_file, width=8).pack(side="left", padx=4)
        tk.Button(row_log, text="최근 로그", command=self.pick_latest_log_file, width=10).pack(side="left", padx=4)

        # 추가 인자
        row_extra = tk.Frame(frame_inputs)
        row_extra.pack(fill="x", padx=6, pady=2)
        tk.Label(row_extra, text="추가 인자(공백 구분)").pack(side="left")
        tk.Entry(row_extra, textvariable=self.extra_args).pack(side="left", padx=4, fill="x", expand=True)

        # 실행 버튼
        row_actions = tk.Frame(frame_inputs)
        row_actions.pack(fill="x", padx=6, pady=4)
        tk.Button(row_actions, text="실행", command=self.on_run, bg="#2d8cff", fg="white", width=12).pack(side="left", padx=4)
        tk.Button(row_actions, text="중단", command=self.on_stop, width=10).pack(side="left", padx=4)

        self.status_var = tk.StringVar(value="대기 중")
        self.proc_var = tk.StringVar(value="PID: -")
        tk.Label(row_actions, textvariable=self.status_var, fg="#008000").pack(side="left", padx=6)
        tk.Label(row_actions, textvariable=self.proc_var, fg="#555555").pack(side="left", padx=6)

        # 로그/출력
        frame_logs = tk.LabelFrame(self.master, text="실행 로그")
        frame_logs.pack(fill="both", expand=True, padx=10, pady=6)
        self.log_text = scrolledtext.ScrolledText(frame_logs, height=12, state="disabled")
        self.log_text.pack(fill="both", expand=True, padx=6, pady=6)

        frame_cleanup = tk.LabelFrame(self.master, text="정리/삭제")
        frame_cleanup.pack(fill="x", padx=10, pady=4)
        btn_cleanup = tk.Frame(frame_cleanup)
        btn_cleanup.pack(fill="x", padx=6, pady=4)
        tk.Button(btn_cleanup, text="로그 파일 삭제", command=self.delete_log_file, width=14).pack(side="left", padx=4)
        tk.Button(btn_cleanup, text="출력 폴더 삭제", command=self.delete_output_data, width=14).pack(side="left", padx=4)
        tk.Button(btn_cleanup, text="결과 파일 삭제", command=self.delete_result_file, width=14).pack(side="left", padx=4)
        tk.Button(btn_cleanup, text="강제 종료(위험)", command=self.on_force_kill, width=14, fg="white", bg="#d9534f").pack(side="left", padx=4)

        frame_preview = tk.LabelFrame(self.master, text="결과 미리보기 (파일 앞/뒤)")
        frame_preview.pack(fill="both", expand=True, padx=10, pady=6)
        self.preview_text = scrolledtext.ScrolledText(frame_preview, height=12, state="disabled")
        self.preview_text.pack(fill="both", expand=True, padx=6, pady=6)

    # ------------------------------------------------------------------ 유틸
    def refresh_scripts(self):
        self.scripts = find_python_scripts(self.current_base)
        self.listbox.delete(0, tk.END)
        for p in self.scripts:
            try:
                rel = p.relative_to(self.current_base)
            except ValueError:
                rel = p.name
            self.listbox.insert(tk.END, str(rel))
        if self.scripts:
            self.listbox.selection_set(0)
        self._log(f"[INFO] 스크립트 {len(self.scripts)}개 로드 완료. (base={self.current_base})")

    def copy_selected_path(self):
        path = self._get_selected_path()
        if not path:
            return
        self.master.clipboard_clear()
        self.master.clipboard_append(str(path))
        messagebox.showinfo("복사됨", f"{path} 복사 완료.")

    def pick_output_dir(self):
        chosen = filedialog.askdirectory(initialdir=self.output_dir.get() or str(BASE_DIR))
        if chosen:
            self.output_dir.set(chosen)

    def pick_codes_file(self):
        chosen = filedialog.askopenfilename(initialdir=self.output_dir.get() or str(BASE_DIR))
        if chosen:
            self.codes_file.set(chosen)

    def pick_list_cache_dir(self):
        chosen = filedialog.askdirectory(initialdir=self.list_cache_dir.get() or str(self.output_dir.get() or BASE_DIR))
        if chosen:
            self.list_cache_dir.set(chosen)

    def pick_result_file(self):
        chosen = filedialog.askopenfilename(initialdir=self.output_dir.get() or str(BASE_DIR))
        if chosen:
            self.result_file.set(chosen)

    def pick_latest_result_file(self):
        path = self._find_latest_file(self.output_dir.get(), exts=("csv", "parquet"))
        if not path:
            messagebox.showinfo("안내", "최근 결과 파일을 찾지 못했습니다.")
            return
        self.result_file.set(str(path))
        self._log(f"[INFO] 최근 결과 파일 선택: {path}")
        self._show_preview_safe()

    def pick_log_file(self):
        chosen = filedialog.asksaveasfilename(initialdir=self.output_dir.get() or str(BASE_DIR), defaultextension=".log")
        if chosen:
            self.log_path.set(chosen)

    def pick_latest_log_file(self):
        path = self._find_latest_file(self.output_dir.get(), exts=("log", "txt", "jsonl"))
        if not path:
            messagebox.showinfo("안내", "최근 로그 파일을 찾지 못했습니다.")
            return
        self.log_path.set(str(path))
        self._log(f"[INFO] 최근 로그 파일 선택: {path}")
        head, tail, is_full = read_preview_lines(path, n=10)
        self._write_preview("[로그 미리보기]" + ("" if is_full else " (앞/뒤 10줄)"), path, head, tail)

    def _get_selected_path(self) -> Path | None:
        sel = self.listbox.curselection()
        if not sel:
            messagebox.showwarning("선택 필요", "실행할 스크립트를 선택하세요.")
            return None
        return self.scripts[sel[0]]

    def _collect_codes(self) -> str:
        codes = [self.code1.get().strip(), self.code2.get().strip(), self.code3.get().strip()]
        codes = [c for c in codes if c]
        return ",".join(codes)

    def _build_command(self, script: Path) -> List[str]:
        codes_arg = self._collect_codes()
        codes_file = self.codes_file.get().strip()
        start = self.start_date.get().strip()
        end = self.end_date.get().strip()
        out_dir = self.output_dir.get().strip() or str(BASE_DIR / "out")
        workers = self.workers.get().strip()
        extra = self.extra_args.get().strip()
        codes_list = [c for c in codes_arg.split(",") if c.strip()]
        if codes_file and not codes_list:
            first = self._load_first_code_from_file(Path(codes_file))
            if first:
                codes_list = [first]

        cmd: List[str] = [sys.executable, "-u", str(script)]
        script_name = script.name

        # 스크립트별 기본 인자 구성
        if script_name == "run_raw_v48.py":
            if codes_file:
                cmd += ["--codes", codes_file]
            elif codes_arg:
                cmd += ["--codes", codes_arg]
            if start:
                cmd += ["--start-date", start]
            if end:
                cmd += ["--end-date", end]
            if out_dir:
                cmd += ["--out-dir", out_dir]
            mode = self.dart_mode.get().strip()
            if mode in {"off", "annual", "full"}:
                cmd += ["--dart-mode", mode]
            if workers.isdigit():
                cmd += ["--workers", workers]
            # 결과 파일 기본값 추정
            self._set_default_result_path(script_name, codes_list, out_dir)
        elif script_name in {"dart.py", "dart_v49.py", "run_raw_sle.py"}:
            if codes_file:
                cmd += ["--codes", codes_file]
            elif codes_arg:
                cmd += ["--codes", codes_arg]
            if start:
                cmd += ["--start", start]
            if end:
                cmd += ["--end", end]
            if out_dir:
                cmd += ["--out", out_dir]
            if workers.isdigit():
                cmd += ["--workers", workers]
            mode = self.dart_mode.get().strip()
            if script_name == "dart_v49.py":
                if mode in {"off", "annual", "full"}:
                    cmd += ["--mode", mode]
            else:
                if mode in {"annual", "full"}:
                    cmd += ["--mode", mode]
            if script_name == "run_raw_sle.py":
                ann_mode = self.announce_mode.get().strip()
                if ann_mode:
                    cmd += ["--announce-mode", ann_mode]
                cache_dir = self.list_cache_dir.get().strip()
                if cache_dir:
                    cmd += ["--list-cache", cache_dir]
            self._set_default_result_path(script_name, codes_list, out_dir)
        else:
            # 기타 스크립트는 추가 인자만 붙여서 실행
            pass

        if extra:
            cmd += shlex.split(extra)

        return cmd

    # ------------------------------------------------------------------ 실행/중단
    def on_run(self):
        if self.is_running:
            messagebox.showinfo("안내", "이미 실행 중입니다.")
            return
        script = self._get_selected_path()
        if not script:
            return

        codes_arg = self._collect_codes()
        codes_file = self.codes_file.get().strip()
        if not codes_file and not codes_arg:
            messagebox.showwarning("종목코드 필요", "종목 코드를 최소 1개 입력하거나 코드 파일을 선택하세요.")
            return

        log_file = Path(self.log_path.get().strip() or (BASE_DIR / "ui_run.log"))
        log_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = self._build_command(script)
        self._log(f"[CMD] {' '.join(cmd)}")

        def runner():
            self.is_running = True
            self.status_var.set("실행 중...")
            self.proc_var.set("PID: -")
            try:
                popen_kwargs = {
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.STDOUT,
                    "text": True,
                    "bufsize": 1,
                    "cwd": str(script.parent),
                }
                # 프로세스 그룹 분리 → Ctrl+Break(Win) / SIGTERM(Unix)로 안전 종료 가능
                if os.name == "nt":
                    popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
                else:
                    popen_kwargs["start_new_session"] = True
                env = os.environ.copy()
                env.setdefault("PYTHONUNBUFFERED", "1")
                popen_kwargs["env"] = env

                with open(log_file, "a", encoding="utf-8") as lf:
                    lf.write(f"\n[{datetime.now().isoformat()}] START CMD: {' '.join(cmd)}\n")
                    self._write_latest_info(cmd, log_file, Path(self.result_file.get()))
                    proc = subprocess.Popen(cmd, **popen_kwargs)
                    self.current_proc = proc
                    self.proc_var.set(f"PID: {proc.pid}")
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        lf.write(line)
                        lf.flush()
                        self.log_queue.put(line.rstrip("\n"))
                    ret = proc.wait()
                    lf.write(f"\n[END] return_code={ret}\n")
                    # 실행 스냅샷 기록
                    self._write_snapshot(ret, cmd, log_file, Path(self.result_file.get()))
                    if ret == 0:
                        self.log_queue.put("[INFO] 실행 완료")
                        self.master.after(0, self._show_preview_safe)
                    else:
                        self.log_queue.put(f"[ERROR] 비정상 종료 (code={ret})")
            except Exception as e:
                self.log_queue.put(f"[ERROR] 실행 실패: {e}")
            finally:
                self.current_proc = None
                self.is_running = False
                self.status_var.set("대기 중")
                self.proc_var.set("PID: -")

        self.running_thread = threading.Thread(target=runner, daemon=True)
        self.running_thread.start()

    def on_dataset_change(self):
        """데이터셋(HOJ/SLE) 전환 시 기본 경로/날짜를 재설정."""
        self._apply_dataset_defaults()
        self.refresh_scripts()

    def _apply_dataset_defaults(self):
        ds = self.dataset.get()
        if ds == "sle":
            self.current_base = SLE_DIR
            self.start_date.set("20160101")
        else:
            self.current_base = HOJ_DIR
            self.start_date.set("20100102")
        # 종료일 고정
        self.end_date.set("20251205")
        # 출력/로그 기본 경로
        out_base = self.current_base / "out"
        self.output_dir.set(str(out_base))
        self.log_path.set(str(self.current_base / "ui_run.log"))
        # announce 기본값: SLE는 캐시 사용 준비
        if ds == "sle":
            self.announce_mode.set("none")
            self.list_cache_dir.set(str(SLE_DIR / "out" / "list" / "by_corp"))
        else:
            self.announce_mode.set("live")
            self.list_cache_dir.set("")
        # 결과 파일 기본
        default_name = "raw_sle_all.csv" if ds == "sle" else "raw_hoj_all.csv"
        self.result_file.set(str(out_base / default_name))

    def on_stop(self):
        if not self.current_proc:
            messagebox.showinfo("안내", "현재 실행 중인 프로세스가 없습니다.")
            return
        self._request_stop()

    def _request_stop(self):
        """데이터 손실을 최소화하기 위해 부드럽게 종료 요청."""
        proc = self.current_proc
        if not proc:
            return

        try:
            if os.name == "nt":
                # Windows: 같은 프로세스 그룹에 CTRL_BREAK_EVENT 전송
                os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
            else:
                # Unix: 세션 리더에 SIGTERM
                os.killpg(proc.pid, signal.SIGTERM)
            self.log_queue.put("[INFO] 종료 신호 전송 (부드러운 종료)")
            proc.wait(timeout=10)
            self.log_queue.put("[INFO] 프로세스가 정상 종료되었습니다.")
        except subprocess.TimeoutExpired:
            self.log_queue.put("[WARN] 10초 내 종료되지 않음. 데이터 손실 방지를 위해 강제 종료는 수행하지 않습니다. 필요 시 수동으로 확인 후 종료하세요.")
        except Exception as e:
            self.log_queue.put(f"[ERROR] 종료 신호 전달 실패: {e}")

    def on_force_kill(self):
        """강제 종료 (데이터 손실 위험)."""
        proc = self.current_proc
        if not proc:
            messagebox.showinfo("안내", "현재 실행 중인 프로세스가 없습니다.")
            return
        if not messagebox.askyesno("강제 종료", "실행 중인 프로세스를 즉시 종료할까요? (데이터 손실 위험)"):
            return
        try:
            if os.name == "nt":
                proc.kill()
            else:
                os.killpg(proc.pid, signal.SIGKILL)
            self.log_queue.put("[WARN] 강제 종료 수행")
        except Exception as e:
            messagebox.showerror("오류", f"강제 종료 실패: {e}")

    # ------------------------------------------------------------------ 로그/미리보기
    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{ts}] {msg}")

    def _poll_log_queue(self):
        try:
            while True:
                line = self.log_queue.get_nowait()
                self.log_text.configure(state="normal")
                self.log_text.insert(tk.END, line + "\n")
                self.log_text.see(tk.END)
                self.log_text.configure(state="disabled")
        except queue.Empty:
            pass
        self.master.after(200, self._poll_log_queue)

    def _show_preview_safe(self):
        path = Path(self.result_file.get().strip())
        if not path.exists():
            # 자동 대체: 출력 폴더에서 최신 결과 탐색
            alt = self._find_latest_file(self.output_dir.get(), exts=("csv", "parquet"))
            if alt:
                path = alt
                self.result_file.set(str(path))
                self.log_queue.put(f"[INFO] 결과 파일을 자동으로 최신 파일로 대체: {path}")
            else:
                self.log_queue.put(f"[WARN] 결과 파일을 찾을 수 없음: {path}")
                return
        head, tail, is_full = read_preview_lines(path, n=200, max_bytes=500_000)
        title = "[결과 미리보기]" if is_full else "[결과 미리보기] (앞/뒤 50줄)"
        self._write_preview(title, path, head, tail)
        # 결과 확인 시 스냅샷도 최신화
        self._write_snapshot(None, None, Path(self.log_path.get()), path)

    def _write_preview(self, title: str, path: Path, head: List[str], tail: List[str]):
        self.preview_text.configure(state="normal")
        self.preview_text.delete("1.0", tk.END)
        self.preview_text.insert(tk.END, f"{title}\n[파일] {path}\n\n")
        self.preview_text.insert(tk.END, "[앞부분]\n")
        for ln in head:
            self.preview_text.insert(tk.END, ln + "\n")
        self.preview_text.insert(tk.END, "\n[뒷부분]\n")
        for ln in tail:
            self.preview_text.insert(tk.END, ln + "\n")
        self.preview_text.configure(state="disabled")

    # ------------------------------------------------------------------ 정리/삭제
    def delete_log_file(self):
        path = Path(self.log_path.get().strip() or (BASE_DIR / "ui_run.log"))
        if not path.exists():
            messagebox.showinfo("안내", f"로그 파일이 없습니다: {path}")
            return
        if not messagebox.askyesno("확인", f"로그 파일을 삭제할까요?\n{path}"):
            return
        try:
            path.unlink(missing_ok=True)
            self.log_queue.put(f"[INFO] 로그 파일 삭제: {path}")
        except Exception as e:
            messagebox.showerror("오류", f"로그 삭제 실패: {e}")

    def delete_result_file(self):
        path = Path(self.result_file.get().strip())
        if not path.exists():
            messagebox.showinfo("안내", f"결과 파일이 없습니다: {path}")
            return
        if not messagebox.askyesno("확인", f"결과 파일을 삭제할까요?\n{path}"):
            return
        try:
            path.unlink(missing_ok=True)
            self.log_queue.put(f"[INFO] 결과 파일 삭제: {path}")
            self.preview_text.configure(state="normal")
            self.preview_text.delete("1.0", tk.END)
            self.preview_text.insert(tk.END, "[INFO] 결과 파일이 삭제되었습니다.\n")
            self.preview_text.configure(state="disabled")
        except Exception as e:
            messagebox.showerror("오류", f"결과 파일 삭제 실패: {e}")

    def delete_output_data(self):
        path = Path(self.output_dir.get().strip() or (BASE_DIR / "out"))
        if not path.exists():
            messagebox.showinfo("안내", f"출력 폴더가 없습니다: {path}")
            return
        # 안전 가드: 루트/드라이브 루트는 거부
        if path == Path(path.anchor):
            messagebox.showerror("오류", "드라이브 루트는 삭제할 수 없습니다.")
            return
        if not messagebox.askyesno("확인", f"출력 폴더 전체를 삭제할까요?\n{path}"):
            return
        try:
            shutil.rmtree(path)
            self.log_queue.put(f"[INFO] 출력 폴더 삭제: {path}")
        except Exception as e:
            messagebox.showerror("오류", f"출력 폴더 삭제 실패: {e}")

    # ------------------------------------------------------------------ 파일 탐색/상태 기록
    def _find_latest_file(self, base: str, exts: tuple[str, ...]) -> Optional[Path]:
        base_path = Path(base) if base else BASE_DIR / "out"
        if not base_path.exists():
            return None
        latest: tuple[float, Path] | None = None
        for ext in exts:
            for p in base_path.rglob(f"*.{ext}"):
                try:
                    m = p.stat().st_mtime
                except Exception:
                    continue
                if (latest is None) or (m > latest[0]):
                    latest = (m, p)
        return latest[1] if latest else None

    def _load_first_code_from_file(self, path: Path) -> Optional[str]:
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for ln in f:
                    ln = ln.strip()
                    if ln:
                        return ln
        except Exception:
            return None
        return None

    def _set_default_result_path(self, script_name: str, codes: List[str], out_dir: str):
        out_base = Path(out_dir) if out_dir else BASE_DIR / "out"
        if script_name in {"dart.py", "dart_v49.py", "run_raw_sle.py"} and codes:
            path = out_base / "csv" / f"{codes[0]}.csv"
            self.result_file.set(str(path))
        elif script_name == "run_raw_v48.py":
            path = out_base / "raw_v48_all.csv"
            self.result_file.set(str(path))

    def _write_latest_info(self, cmd: List[str], log_file: Path, result_path: Path):
        info_path = BASE_DIR / "ui_latest.txt"
        payload = [
            f"time={datetime.now().isoformat()}",
            f"cmd={' '.join(cmd)}",
            f"log={log_file}",
            f"result={result_path}",
            f"out_dir={self.output_dir.get()}",
            f"dart_mode={self.dart_mode.get()}",
            f"announce_mode={self.announce_mode.get()}",
            f"list_cache={self.list_cache_dir.get()}",
        ]
        try:
            info_path.write_text("\n".join(payload), encoding="utf-8")
        except Exception:
            pass

    def _write_snapshot(self, ret_code: Optional[int], cmd: Optional[List[str]], log_path: Path, result_path: Path):
        """
        실행 상태/로그/결과 미리보기 스냅샷을 저장.
        - 경로를 찾지 못하면 최신 파일로 대체 시도.
        - head/tail을 기록해 이후 '결과확인' 요청 시 바로 확인 가능.
        """
        lines: List[str] = []
        lines.append(f"time={datetime.now().isoformat()}")
        if cmd:
            lines.append(f"cmd={' '.join(cmd)}")
        lines.append(f"ret_code={ret_code}")

        # 로그 요약
        log_resolved = log_path if log_path.exists() else self._find_latest_file(self.output_dir.get(), exts=("log", "txt", "jsonl"))
        lines.append(f"log={log_resolved}")
        if log_resolved and log_resolved.exists():
            head, tail, is_full = read_preview_lines(log_resolved, n=20)
            try:
                size = log_resolved.stat().st_size
            except Exception:
                size = "unknown"
            lines.append(f"log_exists=True size={size}")
            lines.append("[log_head]" + (" (full)" if is_full else ""))
            lines.extend(head)
            if not is_full:
                lines.append("[log_tail]")
                lines.extend(tail)
        else:
            lines.append("log_exists=False")

        # 결과 요약
        result_resolved = result_path if result_path.exists() else self._find_latest_file(self.output_dir.get(), exts=("csv", "parquet"))
        lines.append(f"result={result_resolved}")
        if result_resolved and result_resolved.exists():
            head, tail, is_full = read_preview_lines(result_resolved, n=50)
            try:
                size = result_resolved.stat().st_size
            except Exception:
                size = "unknown"
            lines.append(f"result_exists=True size={size}")
            lines.append("[result_head]" + (" (full)" if is_full else ""))
            lines.extend(head)
            if not is_full:
                lines.append("[result_tail]")
                lines.extend(tail)
        else:
            lines.append("result_exists=False")

        try:
            SNAPSHOT_FILE.write_text("\n".join(lines), encoding="utf-8")
        except Exception:
            pass


def main():
    root = tk.Tk()
    RunnerUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

