import configparser
import os
import requests
import json
import time
from datetime import datetime, timedelta

class KiwoomTokenManager:
    """키움증권 API의 접근 토큰을 발급, 저장, 관리하는 클래스"""

    def __init__(self, config_file='config.ini', token_file='token.json'):
        self.config = self._read_config(config_file)
        self.token_file_path = token_file
        self.access_token = None
        self._load_token_from_file() # 파일에서 토큰 로드 시도

    def _read_config(self, config_file):
        """config.ini 파일에서 API 키와 모드 정보를 읽어옴"""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"설정 파일({config_file})을 찾을 수 없습니다.")
        
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file, encoding='utf-8')
        
        try:
            config = {
                'APP_KEY': config_parser['API']['APP_KEY'].strip(),
                'APP_SECRET': config_parser['API']['APP_SECRET'].strip(),
                'MODE': config_parser['SETTINGS']['MODE'].strip()
            }
            
            if config['MODE'] == 'paper':
                config['BASE_URL'] = "https://mockapi.kiwoom.com"
                print("모드: 모의투자")
            elif config['MODE'] == 'real':
                config['BASE_URL'] = "https://api.kiwoom.com"
                print("모드: 실전투자")
            else:
                raise ValueError(f"config.ini의 MODE 설정이 잘못되었습니다: {config['MODE']}")
            
            return config
            
        except KeyError as e:
            raise KeyError(f"config.ini 파일에 {e} 키가 존재하지 않습니다.")

    def _save_token_to_file(self, token_data):
        """토큰 정보(토큰, 만료시간)를 JSON 파일에 저장"""
        try:
            with open(self.token_file_path, 'w', encoding='utf-8') as f:
                json.dump(token_data, f, indent=4)
            print(f"✅ 새 토큰을 {self.token_file_path} 파일에 저장했습니다.")
        except IOError as e:
            print(f"오류: 토큰 파일 저장 실패: {e}")

    def _load_token_from_file(self):
        """파일에서 토큰을 로드하고 유효성을 검사"""
        if not os.path.exists(self.token_file_path):
            print("저장된 토큰 파일이 없습니다. 새로 발급합니다.")
            return False # 파일 없음

        try:
            with open(self.token_file_path, 'r', encoding='utf-8') as f:
                token_data = json.load(f)
            
            expiry_str = token_data.get('expires_dt')
            if not expiry_str:
                print("토큰 파일 형식이 잘못되었습니다. (expires_dt 없음)")
                return False

            # 만료 시각 (예: "20251112000045")
            # 만료 1분 전까지만 유효하다고 보수적으로 판단
            expiry_time = datetime.strptime(expiry_str, '%Y%m%d%H%M%S') - timedelta(minutes=1)
            
            if datetime.now() < expiry_time:
                # 만료되지 않았음
                self.access_token = token_data.get('token')
                print(f"✅ 유효한 토큰을 파일에서 로드했습니다. (만료: {expiry_time})")
                return True
            else:
                # 만료되었음
                print("토큰이 만료되었습니다. 새로 발급합니다.")
                return False

        except (IOError, json.JSONDecodeError, ValueError) as e:
            print(f"오류: 토큰 파일 읽기/파싱 실패: {e}")
            return False # 파일이 깨졌거나 형식이 다름

    def _issue_new_token(self):
        """API 서버에 새 토큰을 요청"""
        TOKEN_URL = f"{self.config['BASE_URL']}/oauth2/token"
        headers = {"content-type": "application/json;charset=UTF-8"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.config['APP_KEY'],
            "secretkey": self.config['APP_SECRET']
        }

        try:
            res = requests.post(TOKEN_URL, headers=headers, data=json.dumps(body))
            res.raise_for_status() # 200 OK가 아니면 예외 발생
            
            result = res.json()
            
            if "token" in result and "expires_dt" in result:
                # 성공 시, 파일에 저장
                self._save_token_to_file(result)
                self.access_token = result["token"]
                print(f"✅ 새 접근 토큰 발급 성공!")
                return True
            else:
                print(f"오류: API 응답에 'token' 또는 'expires_dt'가 없습니다. (응답: {result})")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"오류: API 요청 실패: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f" > 응답 내용: {e.response.text}")
            return False

    def get_token(self):
        """외부에서 호출할 최종 토큰 반환 함수"""
        
        # --- [수정된 부분] ---
        # if self.access -> if self.access_token:
        if self.access_token:
        # --- [여기까지 수정됨] ---
        
            # (파일에서 로드 성공 or 이미 발급받음)
            print("기존 토큰을 사용합니다.")
            return self.access_token
        
        # 토큰이 없으면 새로 발급
        if self._issue_new_token():
            return self.access_token
        else:
            print("오류: 토큰 발급에 최종 실패했습니다.")
            return None

# --- 메인 실행 (테스트용) ---
if __name__ == "__main__":
    try:
        # KiwoomTokenManager 클래스의 인스턴스(실체)를 생성
        # config.ini 파일과 token.json 파일의 경로를 알려줌
        manager = KiwoomTokenManager(config_file='config.ini', token_file='token.json')
        
        # manager 객체의 get_token() 함수를 호출
        token = manager.get_token()
        
        if token:
            print(f"\n[최종 확보 토큰 (일부)]: {token[:10]}...")
            
            # (선택) 한 번 더 호출해보기 (파일에서 읽어오는지 테스트)
            print("\n--- (테스트) 5초 후 토큰 다시 요청 ---")
            time.sleep(5)
            
            # 두 번째 호출: 이때는 _issue_new_token()이 실행되지 않아야 함
            token2 = manager.get_token()
            print(f"[두 번째 토큰 (일부)]: {token2[:10]}...")
            
            if token == token2:
                print("결과: 정상 (파일에서 동일한 토큰을 재사용했습니다)")
            else:
                print("결과: 비정상 (토큰이 일치하지 않습니다)")

    except Exception as e:
        print(f"\n[메인 실행 중 오류]: {e}")