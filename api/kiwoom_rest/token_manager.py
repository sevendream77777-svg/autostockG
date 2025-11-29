# kiwoom_rest/token_manager.py
import os
import json
import configparser
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

class KiwoomTokenManager:
    def __init__(self, config_file: str = "config.ini", token_file: str = "token.json") -> None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        api_root = os.path.abspath(os.path.join(base_dir, os.pardir))
        shared_config = os.path.join(api_root, "config.ini")

        # config는 api/config.ini 우선, 없으면 전달받은 경로/로컬 사용
        if os.path.isabs(config_file):
            self.config_path = config_file
        else:
            candidate = shared_config if os.path.exists(shared_config) else os.path.join(base_dir, config_file)
            self.config_path = candidate

        self.token_path = token_file if os.path.isabs(token_file) else os.path.join(base_dir, token_file)
        self.config = self._load_config()
        self.token_data: Optional[Dict[str, Any]] = self._load_token()

    def _load_config(self) -> Dict[str, str]:
        parser = configparser.ConfigParser()
        if not parser.read(self.config_path, encoding="utf-8"):
            raise FileNotFoundError(f"config.ini를 찾을 수 없습니다: {self.config_path}")

        if "SETTINGS" not in parser:
            raise ValueError("[SETTINGS] 섹션 누락")
        
        settings = parser["SETTINGS"]
        mode = settings.get("MODE", "real").strip().lower()
        
        # 기본 URL 설정
        base_url = settings.get("BASE_URL", "https://api.kiwoom.com").strip()
        if mode == "paper":
            base_url = settings.get("BASE_URL_PAPER", "https://mockapi.kiwoom.com").strip()

        if "API" not in parser:
            raise ValueError("[API] 섹션 누락")

        api_conf = parser["API"]
        app_key = api_conf.get("APP_KEY", "").strip()
        app_secret = api_conf.get("APP_SECRET", "").strip()

        if not app_key or not app_secret:
            raise ValueError("APP_KEY 또는 APP_SECRET가 비어있습니다.")

        return {"app_key": app_key, "app_secret": app_secret, "base_url": base_url}

    def _load_token(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.token_path):
            return None
        try:
            with open(self.token_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 호환성 처리
            token = data.get("access_token") or data.get("token")
            exp_str = data.get("expires_at") or data.get("expires_dt")
            
            expires_at = None
            if exp_str:
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y%m%d%H%M%S"):
                    try:
                        expires_at = datetime.strptime(exp_str, fmt)
                        break
                    except ValueError:
                        continue
            return {"access_token": token, "expires_at": expires_at}
        except Exception:
            return None

    def _save_token(self, token: str, expires_at: datetime) -> None:
        with open(self.token_path, "w", encoding="utf-8") as f:
            json.dump(
                {"access_token": token, "expires_at": expires_at.isoformat()},
                f,
                indent=4,
                ensure_ascii=False
            )

    def _is_valid(self) -> bool:
        if not self.token_data:
            return False
        token = self.token_data.get("access_token")
        exp = self.token_data.get("expires_at")
        if not token or not exp:
            return False
        if isinstance(exp, str):
            try:
                exp = datetime.fromisoformat(exp)
            except ValueError:
                return False
        return exp - timedelta(seconds=30) > datetime.now()

    def _issue_new_token(self) -> str:
        """키움 REST API 토큰 발급"""
        url = f"{self.config['base_url'].rstrip('/')}/oauth2/token"
        
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json"
        }

        # [중요] 키움 공식 문서대로 'secretkey' 사용
        body = {
            "grant_type": "client_credentials",
            "appkey": self.config["app_key"],
            "secretkey": self.config["app_secret"]
        }

        try:
            res = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
            res.raise_for_status()
        except Exception as e:
            # 상세 에러 로깅
            print(f"[KiwoomTokenManager] 발급 요청 실패: {e}")
            if 'res' in locals():
                print(f"[응답 본문] {res.text}")
            raise

        data = res.json()
        
        # 응답 키 호환성 처리 (access_token vs token)
        token = data.get("access_token") or data.get("token")
        
        if not token:
            code = data.get("return_code") or data.get("error")
            msg = data.get("return_msg") or data.get("error_description")
            raise ValueError(f"토큰 발급 오류: {code} - {msg}")

        expires_at = None
        # expires_in(초) 또는 expires_dt(일시) 처리
        if "expires_in" in data:
            expires_at = datetime.now() + timedelta(seconds=int(data["expires_in"]))
        elif "expires_dt" in data:
            for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
                try:
                    expires_at = datetime.strptime(data["expires_dt"], fmt)
                    break
                except ValueError:
                    continue
        
        if not expires_at:
            expires_at = datetime.now() + timedelta(hours=6)

        self._save_token(token, expires_at)
        self.token_data = {"access_token": token, "expires_at": expires_at}
        return token

    def get_token(self) -> str:
        if self._is_valid():
            return self.token_data["access_token"]
        print("[KiwoomTokenManager] 토큰 갱신 시도...")
        return self._issue_new_token()
