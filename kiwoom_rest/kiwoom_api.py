from typing import Optional, Dict, Any, List
import sys
import os
import time
import pandas as pd
import logging
import configparser  # [수정] 누락된 라이브러리 추가
import json          # [수정] 누락된 라이브러리 추가
import requests      # [수정] 누락된 라이브러리 추가

# Open API(OCX)용 모듈 (설치되어 있지 않다면 주석 처리 필요할 수 있음)
try:
    from PyQt5.QAxContainer import QAxWidget
    from PyQt5.QtCore import QEventLoop
    from PyQt5.QtWidgets import QApplication
except ImportError:
    pass # REST API만 사용할 경우 무시 가능

# ---------------------------------------------------------
# 같은 패키지 내의 토큰 매니저 호출
# ---------------------------------------------------------
try:
    from .token_manager import KiwoomTokenManager
except ImportError:
    try:
        from token_manager import KiwoomTokenManager
    except ImportError:
        print("[ERROR] token_manager.py를 찾을 수 없습니다.")
        class KiwoomTokenManager:
            def __init__(self, config_file, token_file): pass
            def get_token(self): return "DUMMY_TOKEN"

# ==========================================================
# Kiwoom REST API 클래스 (여기가 핵심입니다)
# ==========================================================
class KiwoomRestApi:
    
    def __init__(self):
        # config.ini 읽기
        self.mock_mode = self._read_config()
        
        self.base_url = "https://api.kiwoom.com"
        if self.mock_mode:
            self.base_url = "https://mockapi.kiwoom.com" 

        # TokenManager 초기화 (같은 폴더 내 config.ini, token.json 사용)
        current_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_path, 'config.ini')
        token_path = os.path.join(current_path, 'token.json')
        
        self.token_manager = KiwoomTokenManager(config_file=config_path, token_file=token_path)

    def _read_config(self):
        """config.ini 파일에서 모드 설정 읽기"""
        parser = configparser.ConfigParser()
        current_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_path, 'config.ini')
        
        if not os.path.exists(config_path):
            print(f"[KiwoomRestApi] 경고: 설정 파일({config_path})이 없습니다. 기본값(실전)으로 진행합니다.")
            return False

        parser.read(config_path, encoding='utf-8')
        
        if 'SETTINGS' in parser and 'MODE' in parser['SETTINGS']:
            mode = parser['SETTINGS']['MODE'].strip()
            return (mode.lower() == 'paper')
        return False

    def _get_headers(self, api_id: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, str]:
        """기본 요청 헤더 생성"""
        access_token = self.token_manager.get_token()
        if not access_token:
            raise ConnectionError("Access Token 발급 실패: 토큰 파일(token.json)을 확인하세요.")
            
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": api_id,
            "authorization": f"Bearer {access_token}",
        }
        if cont_yn:
            headers["cont-yn"] = cont_yn
        if next_key:
            headers["next-key"] = next_key
        return headers

    def _call_api(self, api_id: str, url_path: str, method: str = "POST", 
                  body: Optional[Dict[str, Any]] = None, 
                  cont_yn: Optional[str] = None, 
                  next_key: Optional[str] = None) -> Dict[str, Any]:
        """HTTP 요청 전송"""
        full_url = self.base_url + url_path
        
        try:
            headers = self._get_headers(api_id, cont_yn, next_key)
        except ConnectionError as e:
            return {"return_code": -999, "return_msg": str(e)}

        # 디버깅용 로그 (필요시 주석 해제)
        # print(f"[{api_id}] Request: {full_url}")
        
        try:
            response = requests.request(
                method, 
                full_url, 
                headers=headers, 
                data=json.dumps(body) if body else None,
                timeout=5 # 5초 타임아웃
            )
            
            # 응답 코드 확인
            if response.status_code != 200:
                return {"return_code": -response.status_code, "return_msg": f"HTTP Error: {response.text}"}
            
            response_data = response.json()
            
            # 연속 조회용 헤더 처리
            response_data['response_headers'] = {
                'cont-yn': response.headers.get('cont-yn'),
                'next-key': response.headers.get('next-key')
            }
            return response_data
            
        except requests.exceptions.RequestException as e:
            print(f"API Request Failed for {api_id}: {e}")
            return {"return_code": -999, "return_msg": f"Network Error: {e}"}
        except json.JSONDecodeError:
            return {"return_code": -998, "return_msg": "응답이 JSON 형식이 아닙니다."}

    # ==========================================================
    # I. 국내주식 시세 조회 (ka...)
    # ==========================================================
    
    def get_stock_daily_chart_continuous(self, stk_cd: str, base_dt: str, upd_stkpc_tp: str, target_days: int) -> Dict[str, Any]:
        """[ka10081] 일봉 차트 연속 조회"""
        api_id = "ka10081"
        url_path = "/api/dostk/chart"
        all_chart_data = []
        next_key = None
        
        for i in range(1, 20): 
            time.sleep(0.2) 
            body = {"stk_cd": stk_cd, "base_dt": base_dt, "upd_stkpc_tp": upd_stkpc_tp}
            cont_yn = "Y" if i > 1 and next_key else None
            
            response = self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)
            
            if str(response.get('return_code')) != '0':
                break
            
            chart_data = response.get('output', []) # output 필드명 확인 필요 (문서마다 다를 수 있음)
            if not chart_data and 'chart' in response: # 어떤 API는 chart, 어떤건 output
                chart_data = response['chart']
                
            all_chart_data.extend(chart_data)

            cont_header = response.get('response_headers', {})
            cont_yn_next = cont_header.get('cont-yn')
            next_key = cont_header.get('next-key')

            if len(all_chart_data) >= target_days:
                break
            if cont_yn_next != 'Y' or not next_key:
                break
        
        return {
            'return_code': 0,
            'return_msg': f'성공 ({len(all_chart_data)}건)',
            'output': all_chart_data
        }

    # ==========================================================
    # II. 계좌 및 주문 (kt...)
    # ==========================================================
    
    def get_deposit_details(self, qry_tp: str) -> Dict[str, Any]:
        """[kt00001] 예수금상세현황요청"""
        api_id = "kt00001"
        url_path = "/api/dostk/acnt"
        return self._call_api(api_id, url_path, body={"qry_tp": qry_tp})

    def get_account_balance(self, qry_tp: str, dmst_stex_tp: str) -> Dict[str, Any]:
        """[kt00018] 계좌평가잔고내역요청"""
        api_id = "kt00018"
        url_path = "/api/dostk/acnt"
        return self._call_api(api_id, url_path, body={"qry_tp": qry_tp, "dmst_stex_tp": dmst_stex_tp})

    def buy_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: str, trde_tp: str, cond_uv: str = "") -> Dict[str, Any]:
        """[kt10000] 매수 주문"""
        api_id = "kt10000"
        url_path = "/api/dostk/ordr"
        body = {
            "dmst_stex_tp": dmst_stex_tp, 
            "stk_cd": stk_cd, 
            "ord_qty": ord_qty, 
            "ord_uv": ord_uv, 
            "trde_tp": trde_tp, 
            "cond_uv": cond_uv
        }
        return self._call_api(api_id, url_path, body=body)

    def sell_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: str, trde_tp: str, cond_uv: str = "") -> Dict[str, Any]:
        """[kt10001] 매도 주문"""
        api_id = "kt10001"
        url_path = "/api/dostk/ordr"
        body = {
            "dmst_stex_tp": dmst_stex_tp, 
            "stk_cd": stk_cd, 
            "ord_qty": ord_qty, 
            "ord_uv": ord_uv, 
            "trde_tp": trde_tp, 
            "cond_uv": cond_uv
        }
        return self._call_api(api_id, url_path, body=body)