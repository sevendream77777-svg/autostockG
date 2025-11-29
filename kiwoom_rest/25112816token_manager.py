import os
import json
import configparser
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

class KiwoomTokenManager:
    """Issue/refresh access token for Kiwoom REST API."""

    def __init__(self, config_file: str = "config.ini", token_file: str = "token.json") -> None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = config_file if os.path.isabs(config_file) else os.path.join(base_dir, config_file)
        self.token_path = token_file if os.path.isabs(token_file) else os.path.join(base_dir, token_file)
        self.config = self._load_config()
        self.token_data: Optional[Dict[str, Any]] = self._load_token()

    # --------------------- internal helpers ---------------------
    def _load_config(self) -> Dict[str, str]:
        parser = configparser.ConfigParser()
        if not parser.read(self.config_path, encoding="utf-8"):
            raise FileNotFoundError(f"config.ini를 찾을 수 없습니다: {self.config_path}")

        settings = parser["SETTINGS"]
        mode = settings.get("MODE", "real").strip().lower()

        # 기본 URL
        base_url = settings.get("BASE_URL", "https://api.kiwoom.com").strip()

        # 모의투자 모드면 mock 서버 사용
        if mode == "paper":
            base_url = settings.get("BASE_URL_PAPER", "https://mockapi.kiwoom.com").strip()

        api_conf = parser["API"]
        app_key = api_conf.get("APP_KEY", "").strip()
        app_secret = api_conf.get("APP_SECRET", "").strip()

        if not app_key or not app_secret:
            raise ValueError("APP_KEY/APP_SECRET가 설정되지 않았습니다.")

        return {"app_key": app_key, "app_secret": app_secret, "base_url": base_url}

    def _load_token(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.token_path):
            return None
        try:
            with open(self.token_path, "r", encoding="utf-8") as f:
                data = json.load(f)
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

    # --------------------- request token ---------------------
    def _issue_new_token(self) -> str:
        """키움 REST API 토큰 발급 (au10001 스펙 준수)"""

        url = f"{self.config['base_url'].rstrip('/')}/oauth2/token"

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json"
        }

        # 🔥 필수 파라미터: appkey + secretkey
        body = {
            "grant_type": "client_credentials",
            "appkey": self.config["app_key"],
            "secretkey": self.config["app_secret"]
        }

        res = requests.post(url, headers=headers, data=json.dumps(body))

        try:
            res.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"토큰 발급 요청 실패: {e}\n서버 응답: {res.text}")

        data = res.json()

        token = data.get("access_token") or data.get("token")
        if not token:
            raise ValueError(f"access_token을 응답에서 찾을 수 없습니다: {data}")

        # expires_in or expires_dt 처리
        expires_at: Optional[datetime] = None

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
            expires_at = datetime.now() + timedelta(hours=1)

        self._save_token(token, expires_at)
        self.token_data = {"access_token": token, "expires_at": expires_at}
        return token

    # --------------------- public ---------------------
    def get_token(self) -> str:
        """유효한 토큰 반환 / 없으면 새로 발급"""
        if self._is_valid():
            return self.token_data["access_token"]  # type: ignore
        return self._issue_new_token()

    def get_access_token(self) -> str:
        """
        기존 코드 호환용 래퍼.
        REST API 기준에서는 get_token()과 동일하게 동작.
        """
        return self.get_token()
