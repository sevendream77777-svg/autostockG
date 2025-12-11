import os
import sys

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CUR_DIR, os.pardir, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from api.kiwoom_rest.token_manager import KiwoomTokenManager

if __name__ == "__main__":
    config_path = os.path.join(PROJECT_ROOT, "api", "config.ini")
    token_path = os.path.join(CUR_DIR, "token.json")
    mgr = KiwoomTokenManager(config_file=config_path, token_file=token_path)
    token = mgr.get_token()
    print("ACCESS TOKEN:", token)
