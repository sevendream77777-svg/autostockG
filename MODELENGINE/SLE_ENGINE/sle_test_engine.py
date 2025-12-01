
# sle_test_engine.py
import google.generativeai as genai
import json

SLE_TEST_PROMPT = """<SLE_PROMPT_PLACEHOLDER>"""  # replace manually if needed

def run_sle_test(top10_df, api_key):
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("models/gemini-1.5-flash")
    outputs = []
    for _, row in top10_df.iterrows():
        prompt = SLE_TEST_PROMPT.replace("<종목명>", str(row["종목명"])).replace("<종목코드>", str(row["종목코드"])).replace("<HOJ combo score>", str(row["동시적용 기대수익(%)"]))
        resp = model.generate_content(prompt)
        outputs.append(json.loads(resp.text))
    return outputs
