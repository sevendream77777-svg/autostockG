# -*- coding: utf-8 -*-
import os
import re
import csv
import zipfile
from io import BytesIO
from collections import Counter

import pandas as pd

# =========================
# 설정
# =========================
FINANCE_DIR = r"F:\autostockG\MODELENGINE\RAW\raw_sle\raw_sle_date\finance_data"
YEARS = list(range(2015, 2026))  # 2015..2025 고정 출력

OUT_SUMMARY = "columns_count_by_year.csv"
OUT_UNMAPPED = "unmapped_accounts_by_year.csv"
OUT_DEBUG = "debug_inventory.csv"

# =========================
# 123개 마스터 컬럼(고정 출력)
# =========================
MASTER_TITLES = [
    "매출액(revenue)","매출원가(cost_of_sales)","매출총이익(gross_profit)","판매비와관리비(sga)","영업이익(op_income)",
    "기타수익(other_income)","기타비용(other_expense)","금융수익(financial_income)","금융비용(financial_expense)","지분법손익(equity_method_gain_loss)",
    "법인세비용차감전순이익(pretax_income)","법인세비용(income_tax_expense)","계속영업이익(income_from_cont_ops)","중단영업손익(discontinued_ops_gain_loss)","당기순이익(net_income_total)",
    "당기순이익(지배주주지분)(net_income_parent)","당기순이익(비지배지분)(net_income_nci)","기본주당이익(basic_eps)","희석주당이익(diluted_eps)",

    "당기순이익(재기재)","기타포괄손익(OCI_total)","확정급여제도 재측정요소(OCI_Remeasurement)","해외사업환산차액(OCI_FX_translation)","FVOCI 금융자산평가손익(OCI_FVOCI)",
    "현금흐름헤지평가손익(OCI_CashFlowHedge)","법인세효과(OCI_tax_effect)","총포괄손익(total_comprehensive_income)","총포괄손익(지배주주지분)","총포괄손익(비지배지분)",

    "자산총계(total_assets)","유동자산(current_assets)","현금및현금성자산(cash_and_cash_equivalents)","단기금융상품(short_term_financial_assets)","매출채권(trade_receivables)",
    "재고자산(inventories)","기타유동자산(other_current_assets)","비유동자산(noncurrent_assets)","장기금융자산(long_term_financial_assets)","투자부동산(investment_property)",
    "유형자산(property_plant_equipment)","무형자산(intangible_assets)","사용권자산(right_of_use_assets)","리스자산(lease_assets)","관계기업·공동기업투자(investments_in_associates)",
    "이연법인세자산(deferred_tax_assets)","기타비유동자산(other_noncurrent_assets)","부채총계(total_liabilities)","유동부채(current_liabilities)","매입채무(trade_payables)",
    "단기차입금(short_term_borrowings)","유동성장기부채(current_portion_lt_debt)","리스부채(유동)(lease_liabilities_current)","충당부채(유동)(provisions_current)","기타유동부채(other_current_liabilities)",
    "비유동부채(noncurrent_liabilities)","장기차입금(long_term_borrowings)","사채(bonds_payable)","리스부채(비유동)(lease_liabilities_noncurrent)","순확정급여부채(net_defined_benefit_liability)",
    "이연법인세부채(deferred_tax_liabilities)","기타비유동부채(other_noncurrent_liabilities)","자본총계(total_equity)","자본금(share_capital)","주식발행초과금(자본잉여금)(share_premium)",
    "기타자본(other_equity)","이익잉여금(retained_earnings)","기타포괄손익누계액(accumulated_OCI)","지배기업 소유주 지분(equity_attributable_to_owners)","비지배지분(non_controlling_interests)",

    "영업활동현금흐름(cash_flow_op)","당기순이익(현금흐름조정용)","비현금수익·비용조정(non_cash_adjustments)","운전자본 변동(working_capital_changes)","이자수취(현금기준)",
    "이자지급(현금기준)","배당금수취(현금기준)","법인세납부(현금기준)","투자활동현금흐름(cash_flow_inv)","유형자산 취득(PPE_acquisitions)",
    "유형자산 처분(PPE_disposals)","무형자산 취득(intangible_acquisitions)","종속·관계기업 취득(acq_subsidiaries_associates)","금융자산 취득/처분(purchase_sale_financial_assets)","재무활동현금흐름(cash_flow_fin)",
    "차입금증가(proceeds_from_borrowings)","차입금상환(repayments_of_borrowings)","사채발행/상환(bonds_issued_redeemed)","배당금지급(dividends_paid)","자기주식취득/처분(treasury_shares_change)",
    "현금및현금성자산의증가(net_increase_in_cash)","기초현금및현금성자산(beginning_cash)","기말현금및현금성자산(ending_cash)",

    "기초자본총계(opening_total_equity)","기말자본총계(closing_total_equity)","기초자본금(opening_share_capital)","기말자본금(closing_share_capital)","자본잉여금 변동(share_premium_changes)",
    "기타자본 변동(other_equity_changes)","이익잉여금 변동(retained_earnings_changes)","기타포괄손익누계액 변동(OCI_changes)","주식기준보상비용(stock_compensation)","배당(dividends_declared)",
    "기타자본변동(others_in_equity)","지배주주지분 변동(parent_equity_changes)","비지배지분 변동(NCI_changes)",

    "EPS(재무제표 표기값/또는 계산)","BPS(표기값/또는 계산)","ROE(계산)","ROA(계산)","부채비율(계산)",

    "회계기간기준일(fiscal_date)","공시일(announce_date)","보고서구분(reprt_code)","연결여부(fs_div)","기업식별자(corp_code)",

    "(연결) 매출액(revenue_cfs)","(별도) 매출액(revenue_sep)","(연결) 영업이익(op_income_cfs)","(별도) 영업이익(op_income_sep)",
    "(연결) 당기순이익(net_income_parent_cfs)","(별도) 당기순이익(net_income_sep)","(연결) 자산/부채/자본 총계(CFS 각 항목)","(별도) 자산/부채/자본 총계(SEP 각 항목)",
]
assert len(MASTER_TITLES) == 123, f"MASTER_TITLES must be 123, got {len(MASTER_TITLES)}"

# (괄호 전 한글 라벨) -> (마스터 제목)
MASTER_PREFIX = {t.split("(")[0].strip(): t for t in MASTER_TITLES}

# 최소 alias (실제 데이터 표기가 다르면 unmapped 보고 여기 추가)
ALIASES = {
    "매출": "매출액(revenue)",
    "매출액": "매출액(revenue)",
    "영업이익": "영업이익(op_income)",
    "당기순이익": "당기순이익(net_income_total)",
    "자산총계": "자산총계(total_assets)",
    "부채총계": "부채총계(total_liabilities)",
    "자본총계": "자본총계(total_equity)",
    "영업활동현금흐름": "영업활동현금흐름(cash_flow_op)",
    "투자활동현금흐름": "투자활동현금흐름(cash_flow_inv)",
    "재무활동현금흐름": "재무활동현금흐름(cash_flow_fin)",
    "공시일": "공시일(announce_date)",
    "announce_date": "공시일(announce_date)",
    "reprt_code": "보고서구분(reprt_code)",
    "corp_code": "기업식별자(corp_code)",
    "fs_div": "연결여부(fs_div)",
    "fiscal_date": "회계기간기준일(fiscal_date)",
}

# account / value 컬럼 후보
ACCOUNT_COL_CANDIDATES = [
    "account_nm", "accnt_nm", "account", "accnt",
    "계정명", "계정과목", "항목명", "과목명"
]
VALUE_COL_CANDIDATES = [
    "thstrm_amount","frmtrm_amount","bfefrmtrm_amount","thstrm_add_amount",
    "당기금액","전기금액","전전기금액","금액","value","amount"
]


def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_key(name: str) -> str:
    """계정명 정규화: 단위/괄호/머릿글 제거"""
    a = norm(name)
    if not a:
        return ""
    # 괄호/대괄호 내용 제거
    a = re.sub(r"\(.*?\)", "", a).strip()
    a = re.sub(r"\[.*?\]", "", a).strip()
    # 로마숫자/번호 접두 제거
    a = re.sub(r"^[IVXLC]+\.", "", a).strip()
    a = re.sub(r"^\d+[\.\)]\s*", "", a).strip()
    a = a.replace("\u00A0", " ").strip()
    return a


def map_to_master(name: str) -> str | None:
    if not name:
        return None
    raw = norm(name)
    if raw in ALIASES:
        return ALIASES[raw]

    k = norm_key(raw)
    if k in ALIASES:
        return ALIASES[k]
    if k in MASTER_PREFIX:
        return MASTER_PREFIX[k]
    return None


def decode_bytes(b: bytes) -> str | None:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    return None


def sniff_delimiter(sample: str) -> str | None:
    """표 형태 TXT/CSV 구분자 추정"""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", "|", ";"])
        return dialect.delimiter
    except Exception:
        return None


def parse_table(text: str):
    """
    텍스트를 '표'로 파싱해서 (header:list, rows:list[list], delimiter:str|None, mode:str)를 반환.
    mode:
      - "delim"  : , \t | ; 기반
      - "space"  : 2칸 이상 공백 기반(고정폭 비슷한 표)
      - "fail"
    """
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if len(lines) < 2:
        return None, None, None, "fail"

    # 샘플로 구분자 추정
    sample = "\n".join(lines[:20])
    delim = sniff_delimiter(sample)
    if delim:
        reader = csv.reader(lines, delimiter=delim)
        try:
            header = next(reader)
        except Exception:
            return None, None, delim, "fail"
        header = [norm(h) for h in header]
        rows = []
        for r in reader:
            if not r:
                continue
            rows.append([x for x in r])
        return header, rows, delim, "delim"

    # fallback: 2칸 이상 공백 / 탭 섞인 표
    # header를 공백 다중분리
    header = re.split(r"\s{2,}|\t+", lines[0].strip())
    header = [norm(h) for h in header]
    if len(header) < 2:
        return None, None, None, "fail"

    rows = []
    for ln in lines[1:]:
        parts = re.split(r"\s{2,}|\t+", ln.strip())
        rows.append(parts)
    return header, rows, None, "space"


def find_column_index(header: list[str], candidates: list[str]) -> int | None:
    low = [h.lower() for h in header]
    for c in candidates:
        if c in header:
            return header.index(c)
        if c.lower() in low:
            return low.index(c.lower())
    return None


def find_value_indices(header: list[str]) -> list[int]:
    idxs = []
    low = [h.lower() for h in header]
    for c in VALUE_COL_CANDIDATES:
        if c in header:
            idxs.append(header.index(c))
        elif c.lower() in low:
            idxs.append(low.index(c.lower()))
    return sorted(set(idxs))


def cell_nonempty(x) -> bool:
    if x is None:
        return False
    s = str(x).strip()
    return s != ""


def iter_zip_members_with_nested(z: zipfile.ZipFile):
    """중첩 zip 1단계까지 펼쳐서 (member_name, bytes) yield"""
    for name in z.namelist():
        data = z.read(name)
        if name.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(BytesIO(data), "r") as z2:
                    for name2 in z2.namelist():
                        yield f"{name}::{name2}", z2.read(name2)
            except Exception:
                yield name, data
        else:
            yield name, data


def main():
    # 결과 카운트(마스터 123개 고정)
    counts = {t: {y: 0 for y in YEARS} for t in MASTER_TITLES}
    unmapped_by_year = {y: Counter() for y in YEARS}
    debug_rows = []

    zip_files = sorted([f for f in os.listdir(FINANCE_DIR) if f.lower().endswith(".zip")])

    for zname in zip_files:
        # 연도: 파일명 앞 4자리
        try:
            year = int(zname[:4])
        except Exception:
            continue
        if year not in YEARS:
            continue

        zpath = os.path.join(FINANCE_DIR, zname)

        try:
            with zipfile.ZipFile(zpath, "r") as z:
                for member, data in iter_zip_members_with_nested(z):
                    mlow = member.lower()

                    # ZIP 내부: txt/csv/tsv/dat 우선 처리 (사용자 말대로 txt가 핵심)
                    if not (mlow.endswith(".txt") or mlow.endswith(".csv") or mlow.endswith(".tsv") or mlow.endswith(".dat")):
                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": "skip_ext", "delim": "", "cols": 0, "rows": 0,
                            "account_col": "", "value_cols": "", "note": ""
                        })
                        continue

                    text = decode_bytes(data)
                    if text is None:
                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": "decode_fail", "delim": "", "cols": 0, "rows": 0,
                            "account_col": "", "value_cols": "", "note": ""
                        })
                        continue

                    header, rows, delim, mode = parse_table(text)
                    if mode == "fail" or not header or not rows:
                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": "table_fail", "delim": str(delim or ""),
                            "cols": 0, "rows": 0, "account_col": "", "value_cols": "",
                            "note": "not_a_table_or_too_small"
                        })
                        continue

                    # 컬럼명 정리
                    header = [norm(h) for h in header]
                    acct_idx = find_column_index(header, ACCOUNT_COL_CANDIDATES)
                    val_idxs = find_value_indices(header)

                    # (A) 롱포맷: account column 존재 → 계정명별 카운트
                    if acct_idx is not None:
                        for r in rows:
                            if acct_idx >= len(r):
                                continue
                            acct = r[acct_idx]
                            # 값 컬럼이 있으면 값 있는 행만
                            ok = True
                            if val_idxs:
                                ok = False
                                for vi in val_idxs:
                                    if vi < len(r) and cell_nonempty(r[vi]):
                                        ok = True
                                        break
                            if not ok:
                                continue

                            mapped = map_to_master(acct)
                            if mapped:
                                counts[mapped][year] += 1
                            else:
                                nm = norm(acct)
                                if nm:
                                    unmapped_by_year[year][nm] += 1

                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": f"long:{mode}", "delim": str(delim or ""),
                            "cols": len(header), "rows": len(rows),
                            "account_col": header[acct_idx] if acct_idx < len(header) else "",
                            "value_cols": ",".join([header[i] for i in val_idxs if i < len(header)]),
                            "note": ""
                        })
                        continue

                    # (B) 와이드포맷: header 자체가 계정과목 → 헤더 매핑 후 셀 존재 카운트
                    mapped_indices = {}
                    for i, h in enumerate(header):
                        mt = map_to_master(h)
                        if mt:
                            mapped_indices[i] = mt
                        else:
                            # prefix 직접 매칭 시도
                            k = norm_key(h)
                            if k in MASTER_PREFIX:
                                mapped_indices[i] = MASTER_PREFIX[k]

                    if mapped_indices:
                        for r in rows:
                            for i, mt in mapped_indices.items():
                                if i < len(r) and cell_nonempty(r[i]):
                                    counts[mt][year] += 1

                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": f"wide:{mode}", "delim": str(delim or ""),
                            "cols": len(header), "rows": len(rows),
                            "account_col": "", "value_cols": "",
                            "note": f"mapped_cols={len(mapped_indices)}"
                        })
                    else:
                        debug_rows.append({
                            "zip": zname, "year": year, "member": member, "size": len(data),
                            "parse_mode": f"wide_nomap:{mode}", "delim": str(delim or ""),
                            "cols": len(header), "rows": len(rows),
                            "account_col": "", "value_cols": "",
                            "note": "no_header_mapped"
                        })

        except zipfile.BadZipFile as e:
            debug_rows.append({
                "zip": zname, "year": year, "member": "", "size": 0,
                "parse_mode": "badzip", "delim": "", "cols": 0, "rows": 0,
                "account_col": "", "value_cols": "", "note": str(e)[:200]
            })
            continue

    # =========================
    # summary 저장(123개 전부)
    # =========================
    out_rows = []
    for i, title in enumerate(MASTER_TITLES, start=1):
        row = {"번호": i, "제목": title}
        for y in YEARS:
            row[str(y)] = counts[title][y]
        out_rows.append(row)

    pd.DataFrame(out_rows).to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")

    # =========================
    # unmapped 저장
    # =========================
    unm_rows = []
    for y in YEARS:
        for name, c in unmapped_by_year[y].most_common():
            unm_rows.append({"year": y, "account_name": name, "count": c})
    pd.DataFrame(unm_rows).to_csv(OUT_UNMAPPED, index=False, encoding="utf-8-sig")

    # =========================
    # debug 저장
    # =========================
    pd.DataFrame(debug_rows).to_csv(OUT_DEBUG, index=False, encoding="utf-8-sig")

    print(f"[OK] {OUT_SUMMARY}")
    print(f"[OK] {OUT_UNMAPPED}")
    print(f"[OK] {OUT_DEBUG}")


if __name__ == "__main__":
    main()
