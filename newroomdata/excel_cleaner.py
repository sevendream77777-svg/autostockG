import pandas as pd
import re
import os

input_files = [
    r"F:\autostockG\newroomdata\키움 REST API 문서.xlsx",
    r"F:\autostockG\newroomdata\영웅문4(HTS) - 키움 REST API, 키움 OPEN API + TR 매칭_251106.xlsx"
]

output_dir = r"F:\autostockG\newroomdata\converted_clean"
os.makedirs(output_dir, exist_ok=True)

FIELD_PATTERNS = [
    r"[가-힣A-Za-z0-9_]+(?=\s*[:：]\s*)",
    r"[가-힣A-Za-z0-9_]+\s*\((.*?)\)",
    r"[A-Za-z0-9_]{3,30}",
]

def extract_fields_from_text(text):
    fields = set()
    for p in FIELD_PATTERNS:
        found = re.findall(p, str(text))
        for f in found:
            val = f.strip()
            if len(val) >= 2:
                fields.add(val)
    return fields

for file_path in input_files:
    xl = pd.ExcelFile(file_path)
    all_fields = set()

    print(f"[SCAN] {file_path} ...")

    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, dtype=str)
        except:
            continue

        for col in df.columns:
            for cell in df[col].astype(str).tolist():
                fields = extract_fields_from_text(cell)
                all_fields.update(fields)

    clean_df = pd.DataFrame(sorted(list(all_fields)), columns=["field_name"])

    base = os.path.basename(file_path).replace(".xlsx", "")
    out_csv = os.path.join(output_dir, f"{base}_CLEAN_FIELDS.csv")

    clean_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[DONE] {out_csv}")
