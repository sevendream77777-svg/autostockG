// =========================
// 기본 셋업
// =========================

const DATA_BASE = "/g2g/data";
const FILE_MANIFEST = DATA_BASE + "/files.json";
const PROXY_URL = "https://crimson-thunder-4bf3.sevendream77777.workers.dev";

// 종목코드 정규화
function normalize(code) {
    code = code.replace(/[^0-9]/g, "");
    if (code.length === 5) return "0" + code;
    return code;
}

// DOM 헬퍼
const $ = (s) => document.querySelector(s);
const $$ = (s) => Array.from(document.querySelectorAll(s));


// =========================
// 현재가(프록시)
// =========================
async function fetchCurrentPrice(code) {
    const code6 = normalize(code);
    try {
        const r = await fetch(`${PROXY_URL}/?code=${code6}`, { cache: "no-store" });
        const j = await r.json();
        if (!j || j.price == null) return null;
        return j;
    } catch (e) { return null; }
}


// =========================
// 파일목록 로딩
// =========================
async function loadManifest() {
    try {
        const r = await fetch(FILE_MANIFEST + "?t=" + Date.now());
        return await r.json();
    } catch (e) { return null; }
}


// =========================
// 엔진 파일명 파싱
// =========================
function parseInfo(fname) {
    const md = fname.match(/(\d{6})/);
    const mh = fname.match(/_h(\d+)_/);

    let base = null;
    if (md) {
        const y = "20" + md[1].slice(0,2);
        const M = md[1].slice(2,4);
        const d = md[1].slice(4,6);
        base = new Date(`${y}-${M}-${d}`);
    }
    const h = mh ? parseInt(mh[1]) : 0;
    return { base, horizon: h };
}


// =========================
// 영업일 계산
// =========================
function addBiz(date, n) {
    const d = new Date(date);
    let c = 0;
    while (c < n) {
        d.setDate(d.getDate() + 1);
        const w = d.getDay();
        if (w !== 0 && w !== 6) c++;
    }
    return d;
}


// =========================
// 달력 렌더링
// =========================
let curYear = 2025;
let curMonth = 12;
let selectedDate = null;

function renderCalendar(selected) {
    const first = new Date(curYear, curMonth - 1, 1);
    const fm = first.getMonth();
    const dowRow = $("#dow");
    const body = $("#calBody");
    const title = $("#calTitle");

    title.textContent = `${curYear}년 ${curMonth}월`;

    dowRow.innerHTML = "";
    ["일","월","화","수","목","금","토"].forEach(d => {
        const th = document.createElement("th");
        th.textContent = d;
        dowRow.appendChild(th);
    });

    body.innerHTML = "";

    let d = new Date(first);
    d.setDate(d.getDate() - d.getDay());

    for (let i=0;i<6;i++) {
        const tr = document.createElement("tr");
        for (let j=0;j<7;j++) {
            const td = document.createElement("td");
            td.textContent = d.getDate();

            if (d.getMonth() !== fm) td.classList.add("dim");
            if (selected && sameDate(d, selected)) td.classList.add("sel");

            td.dataset.date = fmtDate(d);
            td.onclick = () => {
                selectedDate = new Date(d);
                renderCalendar(selectedDate);
                syncFileOptions(true);
            };

            tr.appendChild(td);
            d.setDate(d.getDate() + 1);
        }
        body.appendChild(tr);
    }
}

function sameDate(a,b){
    return a.getFullYear()===b.getFullYear() &&
           a.getMonth()===b.getMonth() &&
           a.getDate()===b.getDate();
}
function fmtDate(d){
    return d.toISOString().slice(0,10);
}


// =========================
// 파일 목록 빌드
// =========================
let manifest = [];
let engineInfo = [];

function buildEngines() {
    engineInfo = manifest.map(x => {
        const pi = parseInfo(x.filename);
        const base = pi.base;
        const h = pi.horizon;
        const start = addBiz(base,1);
        const end   = addBiz(base,h);
        return {
            fname: x.filename,
            title: x.title,
            base,
            h,
            start,
            end
        };
    });
    syncFileOptions(false);
}


// =========================
// 날짜 선택 → 파일 자동 필터
// =========================
function syncFileOptions(selectAfter) {
    const sel = $("#fileSel");
    sel.innerHTML = "";

    engineInfo.forEach(e => {
        if (!selectedDate || (selectedDate>=e.start && selectedDate<=e.end)) {
            const op = document.createElement("option");
            op.value = e.fname;
            op.textContent = e.title;
            sel.appendChild(op);
        }
    });

    if (selectAfter && sel.options.length===1) sel.selectedIndex = 0;
}



// =========================
// JSON 파일 로딩 (Top10)
// =========================
async function loadEngineFile(fname) {
    try {
        const r = await fetch(`${DATA_BASE}/${fname}?t=` + Date.now());
        return await r.json();
    } catch (e) { return null; }
}


// =========================
// Top10 테이블 렌더
// =========================
function renderTable(rows) {
    const tbody = $("#tbl tbody");
    tbody.innerHTML = "";

    rows.forEach(row => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td class="rank">${row["순위"]}</td>
            <td>${row["종목명"]}</td>
            <td class="code">${row["종목코드"]}</td>
            <td class="num">${num(row["현재가"])}</td>
            <td class="num">${pct(row["예측수익률(%)"])}</td>
            <td class="num cur" data-code="${row["종목코드"]}">-</td>
            <td class="num day" data-code="${row["종목코드"]}">-</td>
            <td class="num vol" data-code="${row["종목코드"]}">-</td>
            <td class="num">${pct(row["동시적용 기대수익(%)"])}</td>
            <td class="num">${pct(row["상승확률(%)"])}</td>
        `;
        tr.onclick = () => showDetail(row);
        tbody.appendChild(tr);
    });
}

function num(v){ return v==null?"-":Number(v).toLocaleString(); }
function pct(v){ return v==null?"-":(Number(v).toFixed(2)+"%"); }


// =========================
// 상세 정보 표시
// =========================
function showDetail(r){
    $("#detail").innerHTML = `
        <div>순위: ${r["순위"]}</div>
        <div>종목명: ${r["종목명"]} (${r["종목코드"]})</div>
        <div>추천가: ${num(r["현재가"])}</div>
        <div>예측수익률: ${pct(r["예측수익률(%)"])}</div>
        <div>동시적용 기대수익: ${pct(r["동시적용 기대수익(%)"])}</div>
        <div>상승확률: ${pct(r["상승확률(%)"])}</div>
    `;
}


// =========================
// 현재가 갱신 (프록시)
// =========================
async function refreshPrices(rows) {
    for (const r of rows) {
        const code = r["종목코드"];
        const info = await fetchCurrentPrice(code);
        const c = normalize(code);

        const cellPrice = $(`td.cur[data-code='${code}']`);
        const cellDay   = $(`td.day[data-code='${code}']`);
        const cellVol   = $(`td.vol[data-code='${code}']`);

        if (!info) {
            if (cellPrice) cellPrice.textContent="-";
            if (cellDay) cellDay.textContent="-";
            if (cellVol) cellVol.textContent="-";
            continue;
        }

        if (cellPrice) cellPrice.textContent = info.price.toLocaleString();
        if (cellDay) cellDay.textContent = (info.change>=0?"+":"") + info.change;
        if (cellVol) cellVol.textContent = info.volume.toLocaleString();
    }
}


// =========================
// 메타 패널 표시
// =========================
function renderMeta(meta){
    if (!meta) { $("#meta").innerHTML=""; return; }
    $("#meta").innerHTML = `
      <div>버전: ${meta.version || "-"}</div>
      <div>훈련일자: ${meta.data_date || "-"}</div>
      <div>예측일: ${meta.prediction_date || "-"}</div>
      <div>horizon: ${meta.horizon || "-"}</div>
      <div>window: ${meta.input_window || "-"}</div>
      <div>n_estimators: ${meta.n_estimators || "-"}</div>
      <div style="margin-top:4px;font-size:12px;color:#9aa6c9">
        features: ${meta.features?.length || 0}
      </div>
    `;
}


// =========================
// 메인
// =========================
async function start() {
    const m = await loadManifest();
    if (!m) { alert("파일목록 로드 실패"); return; }

    manifest = m;
    buildEngines();

    const now = new Date();
    curYear = now.getFullYear();
    curMonth = now.getMonth()+1;
    selectedDate = now;

    renderCalendar(selectedDate);

    $("#prevM").onclick = ()=>{ curMonth--; if(curMonth<1){curMonth=12;curYear--; } renderCalendar(selectedDate); };
    $("#nextM").onclick = ()=>{ curMonth++; if(curMonth>12){curMonth=1;curYear++; } renderCalendar(selectedDate); };

    $("#btnLoad").onclick = async ()=>{
        const fname = $("#fileSel").value;
        if (!fname) return;
        const j = await loadEngineFile(fname);
        if (!j) { alert("JSON 로드 실패"); return; }

        renderMeta(j.engine_meta);
        renderTable(j.top10);
        await refreshPrices(j.top10);
    };

    $("#btnRefresh").onclick = async ()=>{
        const fname = $("#fileSel").value;
        if (!fname) return;
        const j = await loadEngineFile(fname);
        if (!j) return;
        await refreshPrices(j.top10);
    };
}

window.onload = start;
