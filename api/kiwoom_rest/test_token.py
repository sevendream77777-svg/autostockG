import os
import sys

# F:\autostockG 를 패키지 루트로 인식시키기
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from kiwoom_rest.token_manager import KiwoomTokenManager

if __name__ == "__main__":
    mgr = KiwoomTokenManager(
        config_file=r"F:\autostockG\kiwoom_rest\config.ini",
        token_file=r"F:\autostockG\kiwoom_rest\token.json",
    )
    token = mgr.get_token()
    print("ACCESS TOKEN:", token)

