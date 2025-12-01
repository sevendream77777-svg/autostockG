
# sle_test_runner.py
import pandas as pd
from sle_test_engine import run_sle_test
from daily_recommender import run_prediction_core
import json

def run_range(start_date, end_date, engine_path, api_key):
    dt_range = pd.date_range(start_date, end_date)
    all_results = []

    for d in dt_range:
        try:
            df_out, payload, db_path, pred_dt = run_prediction_core(
                engine_path=engine_path,
                target_date=d.strftime("%Y-%m-%d"),
                top_n=10,
                rank_by="combo",
                version_override=None
            )
            sle_res = run_sle_test(df_out, api_key)
            all_results.append({
                "date": d.strftime("%Y-%m-%d"),
                "results": sle_res
            })
        except Exception as e:
            all_results.append({
                "date": d.strftime("%Y-%m-%d"),
                "error": str(e)
            })

    with open("sle_test_output.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
