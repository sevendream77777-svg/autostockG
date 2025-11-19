# kakao_notifier.py - 카카오톡 알림 전송 및 토큰 관리 모듈 (디버그 코드 통합)

import requests
import json
import configparser
import os
import time
from typing import Optional, Dict, Any
from datetime import datetime

# --- 파일 경로 설정 ---
CONFIG_FILE = 'config.ini'
KAKAO_TOKEN_FILE = 'kakao_token.json'
# 💡 [핵심] REDIRECT_URI는 config.ini에서 읽어와야 유연합니다.
# 하지만 현재는 코드 내에 고정합니다. 이 값이 디벨로퍼스에 등록된 값과 100% 일치해야 합니다.
REDIRECT_URI = "https://localhost:5000/oauth" 

class KakaoNotifier:
    """
    카카오톡 '나에게 보내기' 기능을 위한 알림 전송 및 토큰 관리 클래스
    """
    
    def __init__(self):
        self.config = self._read_config()
        self.rest_api_key = self.config.get('KAKAO_REST_API_KEY')
        self.auth_code = self.config.get('KAKAO_AUTH_CODE')
        self.access_token = None
        self.refresh_token = None
        
        # 1. 저장된 토큰 로드 시도
        if not self._load_tokens():
            # 2. 토큰 로드 실패 시, 최초 인증 코드로 새 토큰 발급 시도
            self._issue_initial_tokens()
        else:
            # 3. 액세스 토큰 유효성 검증 및 리프레시 시도 (간소화)
            self._refresh_access_token()


    def _read_config(self) -> Dict[str, str]:
        """config.ini에서 카카오 설정값을 읽어옵니다."""
        config_parser = configparser.ConfigParser()
        if not config_parser.read(CONFIG_FILE, encoding='utf-8'):
            raise FileNotFoundError(f"설정 파일({CONFIG_FILE})을 찾을 수 없습니다.")
        
        try:
            return {
                'KAKAO_REST_API_KEY': config_parser['KAKAO']['KAKAO_REST_API_KEY'].strip(),
                'KAKAO_AUTH_CODE': config_parser['KAKAO']['KAKAO_AUTH_CODE'].strip()
            }
        except KeyError as e:
            raise KeyError(f"config.ini의 [KAKAO] 섹션에 {e} 키가 누락되었습니다.")

    def _load_tokens(self) -> bool:
        """저장된 토큰 파일에서 Access Token과 Refresh Token을 로드합니다."""
        if not os.path.exists(KAKAO_TOKEN_FILE):
            return False
            
        try:
            with open(KAKAO_TOKEN_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')
                print("✅ KakaoNotifier: 토큰 파일 로드 성공.")
                return True
        except Exception as e:
            print(f"❌ KakaoNotifier: 토큰 로드 실패 - {e}")
            return False

    def _save_tokens(self, access_token: str, refresh_token: str):
        """Access Token과 Refresh Token을 파일에 저장합니다."""
        data = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'saved_at': datetime.now().strftime("%Y%m%d %H:%M:%S")
        }
        with open(KAKAO_TOKEN_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"✅ KakaoNotifier: 새 토큰을 {KAKAO_TOKEN_FILE}에 저장했습니다.")

    def _issue_initial_tokens(self):
        """최초 인증 코드를 사용하여 Access Token과 Refresh Token을 발급합니다."""
        print("💡 KakaoNotifier: 최초 인증 코드로 토큰 발급 시도 중...")
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.rest_api_key,
            "redirect_uri": REDIRECT_URI,
            "code": self.auth_code
        }
        
        try:
            res = requests.post(url, data=data)
            
            # --- [핵심 디버그 코드] ---
            if res.status_code != 200:
                print(f"❌ KakaoNotifier: 토큰 발급 HTTP 오류 {res.status_code}")
                try:
                    # 카카오 서버의 구체적인 에러 메시지 출력
                    print(f" > 서버 응답 에러: {res.json()}") 
                except:
                    print(f" > 서버 응답 에러: {res.text}")
                return
            # ---------------------------
            
            token_data = res.json()
            
            self.access_token = token_data['access_token']
            self.refresh_token = token_data['refresh_token']
            self._save_tokens(self.access_token, self.refresh_token)
            
            print("✅ KakaoNotifier: 최초 토큰 발급 및 저장 성공.")
            
        except requests.exceptions.RequestException as e:
            print(f"❌ KakaoNotifier: 최초 토큰 발급 실패. {e}")
            print(" > config.ini의 KAKAO_AUTH_CODE가 만료되었거나 틀렸을 수 있습니다.")

    def _refresh_access_token(self):
        """Refresh Token을 사용하여 Access Token을 갱신합니다."""
        print("💡 KakaoNotifier: Access Token 갱신 시도 중...")
        url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.rest_api_key,
            "refresh_token": self.refresh_token
        }
        
        try:
            res = requests.post(url, data=data)
            
            # 토큰 만료 등 400번대 오류 시 실패 메시지 출력
            if res.status_code != 200:
                 print(f"❌ KakaoNotifier: 토큰 갱신 HTTP 오류 {res.status_code}")
                 print(f" > 서버 응답 에러: {res.json()}")
                 return
                 
            token_data = res.json()
            
            new_access_token = token_data['access_token']
            self.access_token = new_access_token
            
            new_refresh_token = token_data.get('refresh_token', self.refresh_token)
            self._save_tokens(new_access_token, new_refresh_token)
            
            print("✅ KakaoNotifier: Access Token 갱신 성공.")
            
        except requests.exceptions.RequestException as e:
            print(f"❌ KakaoNotifier: Access Token 갱신 실패. {e}")

    def send_message(self, text_content: str) -> bool:
        """카카오톡 '나에게 보내기' 메시지 전송 함수"""
        if not self.access_token:
            print("❌ KakaoNotifier: Access Token이 유효하지 않아 메시지 전송 불가.")
            return False

        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        # 텍스트 메시지 템플릿
        template_object = {
            "object_type": "text",
            "text": text_content,
            "link": {
                "web_url": "https://kiwoom.com",
                "mobile_web_url": "https://kiwoom.com"
            }
        }
        
        data = {"template_object": json.dumps(template_object)}

        try:
            res = requests.post(url, headers=headers, data=data)
            
            if res.status_code == 401:
                print("❌ 카카오톡 전송 실패: Access Token 만료. 갱신 시도 필요.")
                # 실제 로직에서는 갱신 시도 후 재전송해야 합니다.
                return False
                
            if res.json().get('result_code') == 0:
                print("🎉 카카오톡 알림 전송 성공!")
                return True
            else:
                print(f"❌ 카카오톡 전송 실패: {res.json()}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ 카카오톡 전송 중 예외 발생: {e}")
            return False

# --- 메인 실행 (테스트용) ---
if __name__ == '__main__':
    try:
        notifier = KakaoNotifier()
        if notifier.access_token:
            notifier.send_message(f"V34 호엔진 시스템 테스트 알림입니다. (시간: {datetime.now().strftime('%H:%M')})")
        else:
            print("❌ 카카오톡 시스템 초기화 실패. config.ini의 키를 확인하세요.")

    except Exception as e:
        print(f"\n[KakaoNotifier Main Error]: {e}")