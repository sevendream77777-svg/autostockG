# kiwoom_api.py - V34 프로젝트 마스터 API 클라이언트 (최종본 - 오류 수정 완료)

import json
import requests
import time
from typing import Optional, Dict, Any, List
import configparser
from datetime import datetime

# 주의: 실제 프로젝트 구조에 맞게 경로를 수정해야 할 수 있습니다.
try:
    from token_manager import KiwoomTokenManager
except ImportError:
    # 이 오류는 token_manager.py가 없거나 클래스명이 다를 때 발생합니다.
    raise ImportError("KiwoomTokenManager를 import 할 수 없습니다. token_manager.py 파일을 확인하세요.")

class KiwoomRestApi:
    
    def __init__(self):
        # config.ini에서 설정값을 읽어옵니다.
        def _read_config():
            config_parser = configparser.ConfigParser()
            config_file_path = 'config.ini'
            if not config_parser.read(config_file_path, encoding='utf-8'):
                raise FileNotFoundError(f"설정 파일({config_file_path})을 찾을 수 없습니다.")

            mode = config_parser['SETTINGS']['MODE'].strip()
            is_mock_mode = (mode.lower() == 'paper')
            
            return is_mock_mode

        self.mock_mode = _read_config()
        self.base_url = "https://api.kiwoom.com"
        if self.mock_mode:
            self.base_url = "https://mockapi.kiwoom.com" 

        # TokenManager 초기화 (config.ini에서 정보 읽어옴)
        self.token_manager = KiwoomTokenManager(config_file='config.ini', token_file='token.json')


    def _get_headers(self, api_id: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, str]:
        """기본 요청 헤더를 생성합니다."""
        access_token = self.token_manager.get_token()
        if not access_token:
            raise ConnectionError("Access Token 발급/로드에 실패했습니다.")
            
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
        """실제 HTTP 요청을 실행하고 응답을 처리하는 코어 메서드입니다."""
        full_url = self.base_url + url_path
        
        try:
            headers = self._get_headers(api_id, cont_yn, next_key)
        except ConnectionError as e:
            return {"return_code": -999, "return_msg": str(e)}

        print(f"[{api_id}] Calling API: {full_url} (Cont: {cont_yn}, NextKey: {next_key})")
        
        try:
            response = requests.request(
                method, 
                full_url, 
                headers=headers, 
                data=json.dumps(body) if body else None
            )
            response.raise_for_status()
            
            response_data = response.json()
            response_data['response_headers'] = {
                'cont-yn': response.headers.get('cont-yn'),
                'next-key': response.headers.get('next-key')
            }
            return response_data
            
        except requests.exceptions.RequestException as e:
            print(f"API Request Failed for {api_id}: {e}")
            
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    return error_data
                except json.JSONDecodeError:
                    return {"return_code": -998, "return_msg": f"API 서버 응답 파싱 실패: {e.response.text}"}
            
            return {"return_code": -999, "return_msg": f"API Request Failed: {e}"}


    # ==========================================================
    # I. 국내주식 API (ka...): 시세/조회
    # ==========================================================
    
    def get_stock_daily_chart_continuous(self, stk_cd: str, base_dt: str, upd_stkpc_tp: str, target_days: int) -> Dict[str, Any]:
        """[ka10081] 주식일봉차트조회요청 연속 조회 (데이터 복원 로직 없음)"""
        api_id = "ka10081"
        url_path = "/api/dostk/chart"
        all_chart_data: List[Dict[str, str]] = []
        next_key: Optional[str] = None
        
        # ----------------------------------------------------
        # 💡 [핵심]: 로직 검증용 가상 데이터 주입 (Mock Mode, 3일 테스트 시)
        # ----------------------------------------------------
        if self.mock_mode and target_days == 3:
            print(f"[{api_id}] {stk_cd} **데이터 엔진 우회**: 목표 3일치 가상 데이터 강제 주입.")
            
            # 가상 데이터 (골든 크로스 발생 조건)
            virtual_chart_data = [
                {"dt": "20251111", "prc": "+70000", "open": "+58000", "high": "+70000", "low": "+57000", "vol": "1000000"},
                {"dt": "20251110", "prc": "+30000", "open": "+54000", "high": "+55500", "low": "+30000", "vol": "900000"},
                {"dt": "20251109", "prc": "+50000", "open": "+51000", "high": "+51000", "low": "+49500", "vol": "800000"}
            ]
            final_response = {
                'return_code': 0,
                'return_msg': f'연속 조회 성공 (최종 {len(virtual_chart_data)}일 확보 - 가상 데이터)',
                'chart': virtual_chart_data
            }
            return final_response
        # ----------------------------------------------------
        
        if target_days == 3:
            print(f"[{api_id}] {stk_cd} **실제 API 연결 테스트**: 목표 **3일**만 조회 시도.")
        else:
            print(f"[{api_id}] {stk_cd} 장기 데이터 연속 조회 시작 (목표: {target_days}일)")
        
        
        for i in range(1, 20): # 최대 20번 반복 (안전 상한선)
            
            time.sleep(0.5) 
            
            body = {"stk_cd": stk_cd, "base_dt": base_dt, "upd_stkpc_tp": upd_stkpc_tp}

            cont_yn = "Y" if i > 1 and next_key else None
            
            print(f"[{api_id}] {stk_cd} :: {i}차 요청 (누적 일봉: {len(all_chart_data)} / 목표: {target_days})")

            response = self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)
            
            if str(response.get('return_code')) != '0':
                print(f"❌ 연속 조회 중단: API 오류 발생 ({response.get('return_msg')})")
                break
            
            chart_data = response.get('chart', [])
            all_chart_data.extend(chart_data)

            cont_header = response.get('response_headers', {})
            cont_yn_next = cont_header.get('cont-yn')
            next_key = cont_header.get('next-key')

            if len(all_chart_data) >= target_days:
                print(f"✅ 연속 조회 종료: 목표 일수({target_days}일) 달성.")
                break
                
            if cont_yn_next != 'Y' or not next_key:
                print(f"✅ 연속 조회 종료: 서버에서 더 이상 데이터가 없습니다. (최종 누적: {len(all_chart_data)}일)")
                break
        
        final_response = {
            'return_code': 0,
            'return_msg': f'연속 조회 성공 (최종 {len(all_chart_data)}일 확보)',
            'chart': all_chart_data
        }
        return final_response


    # ==========================================================
    # II. 국내주식 API (kt...): 계좌 및 주문
    # ==========================================================
    
    def get_account_balance_details(self, qry_tp: str, dmst_stex_tp: str) -> Dict[str, Any]:
        """[kt00018] 계좌평가잔고내역요청 (잔고 조회에 사용)"""
        api_id = "kt00018"
        url_path = "/api/dostk/acnt"
        body = {"qry_tp": qry_tp, "dmst_stex_tp": dmst_stex_tp}
        return self._call_api(api_id, url_path, body=body, method="POST")

    def get_deposit_details(self, qry_tp: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt00001] 예수금상세현황요청"""
        api_id = "kt00001"
        url_path = "/api/dostk/acnt"
        body = {"qry_tp": qry_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def buy_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, cond_uv: Optional[str] = None, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10000] 주식 매수주문"""
        api_id = "kt10000"
        url_path = "/api/dostk/ordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp, "cond_uv": cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def sell_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, cond_uv: Optional[str] = None, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10001] 주식 매도주문"""
        api_id = "kt10001"
        url_path = "/api/dostk/ordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp, "cond_uv": cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def correct_order(self, dmst_stex_tp: str, orig_ord_no: str, stk_cd: str, mdfy_qty: str, mdfy_uv: str, mdfy_cond_uv: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10002] 주식 정정주문"""
        api_id = "kt10002"
        url_path = "/api/dostk/ordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "orig_ord_no": orig_ord_no, "stk_cd": stk_cd, "mdfy_qty": mdfy_qty, "mdfy_uv": mdfy_uv, "mdfy_cond_uv": mdfy_cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def cancel_order(self, dmst_stex_tp: str, orig_ord_no: str, stk_cd: str, cncl_qty: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10003] 주식 취소주문"""
        api_id = "kt10003"
        url_path = "/api/dostk/ordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "orig_ord_no": orig_ord_no, "stk_cd": stk_cd, "cncl_qty": cncl_qty}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def credit_buy_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, cond_uv: Optional[str] = None, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10006] 신용 매수주문"""
        api_id = "kt10006"
        url_path = "/api/dostk/crdordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp, "cond_uv": cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def credit_sell_order(self, dmst_stex_tp: str, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, crd_deal_tp: str, crd_loan_dt: Optional[str], cond_uv: Optional[str] = None, cont_yn: Optional[str] = None) -> Dict[str, Any]:
        """[kt10007] 신용 매도주문"""
        api_id = "kt10007"
        url_path = "/api/dostk/crdordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp, "crd_deal_tp": crd_deal_tp, "crd_loan_dt": crd_loan_dt, "cond_uv": cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def credit_correct_order(self, dmst_stex_tp: str, orig_ord_no: str, stk_cd: str, mdfy_qty: str, mdfy_uv: str, mdfy_cond_uv: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10008] 신용 정정주문"""
        api_id = "kt10008"
        url_path = "/api/dostk/crdordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "orig_ord_no": orig_ord_no, "stk_cd": stk_cd, "mdfy_qty": mdfy_qty, "mdfy_uv": mdfy_uv, "mdfy_cond_uv": mdfy_cond_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def credit_cancel_order(self, dmst_stex_tp: str, orig_ord_no: str, stk_cd: str, cncl_qty: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt10009] 신용 취소주문"""
        api_id = "kt10009"
        url_path = "/api/dostk/crdordr"
        body = {"dmst_stex_tp": dmst_stex_tp, "orig_ord_no": orig_ord_no, "stk_cd": stk_cd, "cncl_qty": cncl_qty}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    # 신용융자/대주 가능 종목 조회 API
    def get_credit_loan_possible_items(self, crd_stk_grde_tp: Optional[str], mrkt_deal_tp: str, stk_cd: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt20016] 신용융자 가능종목요청"""
        api_id = "kt20016"
        url_path = "/api/dostk/stkinfo"
        body = {"crd_stk_grde_tp": crd_stk_grde_tp, "mrkt_deal_tp": mrkt_deal_tp, "stk_cd": stk_cd}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_credit_loan_possible_inquiry(self, stk_cd: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt20017] 신용융자 가능문의"""
        api_id = "kt20017"
        url_path = "/api/dostk/stkinfo"
        body = {"stk_cd": stk_cd}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    # 금현물 주문/계좌 API
    def gold_buy_order(self, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50000] 금현물 매수주문"""
        api_id = "kt50000"
        url_path = "/api/dostk/ordr"
        body = {"stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def gold_sell_order(self, stk_cd: str, ord_qty: str, ord_uv: Optional[str], trde_tp: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50001] 금현물 매도주문"""
        api_id = "kt50001"
        url_path = "/api/dostk/ordr"
        body = {"stk_cd": stk_cd, "ord_qty": ord_qty, "ord_uv": ord_uv, "trde_tp": trde_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def gold_correct_order(self, stk_cd: str, orig_ord_no: str, mdfy_qty: str, mdfy_uv: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50002] 금현물 정정주문"""
        api_id = "kt50002"
        url_path = "/api/dostk/ordr"
        body = {"stk_cd": stk_cd, "orig_ord_no": orig_ord_no, "mdfy_qty": mdfy_qty, "mdfy_uv": mdfy_uv}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def gold_cancel_order(self, orig_ord_no: str, stk_cd: str, cncl_qty: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50003] 금현물 취소주문"""
        api_id = "kt50003"
        url_path = "/api/dostk/ordr"
        body = {"orig_ord_no": orig_ord_no, "stk_cd": stk_cd, "cncl_qty": cncl_qty}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_balance(self, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50020] 금현물 잔고확인"""
        api_id = "kt50020"
        url_path = "/api/dostk/acnt"
        body = {}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_deposit(self, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50021] 금현물 예수금"""
        api_id = "kt50021"
        url_path = "/api/dostk/acnt"
        body = {}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_order_conclusion_all(self, ord_dt: str, qry_tp: Optional[str], mrkt_deal_tp: str, stk_bond_tp: str, slby_tp: str, stk_cd: Optional[str], fr_ord_no: Optional[str], dmst_stex_tp: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50030] 금현물 주문체결전체조회"""
        api_id = "kt50030"
        url_path = "/api/dostk/acnt"
        body = {"ord_dt": ord_dt, "qry_tp": qry_tp, "mrkt_deal_tp": mrkt_deal_tp, "stk_bond_tp": stk_bond_tp, "slby_tp": slby_tp, "stk_cd": stk_cd, "fr_ord_no": fr_ord_no, "dmst_stex_tp": dmst_stex_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_order_conclusion(self, ord_dt: str, qry_tp: str, stk_bond_tp: str, sell_tp: str, stk_cd: Optional[str], fr_ord_no: Optional[str], dmst_stex_tp: str, cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50031] 금현물 주문체결조회"""
        api_id = "kt50031"
        url_path = "/api/dostk/acnt"
        body = {"ord_dt": ord_dt, "qry_tp": qry_tp, "stk_bond_tp": stk_bond_tp, "sell_tp": sell_tp, "stk_cd": stk_cd, "fr_ord_no": fr_ord_no, "dmst_stex_tp": dmst_stex_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_trade_details(self, strt_dt: Optional[str], end_dt: Optional[str], tp: Optional[str], stk_cd: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50032] 금현물 거래내역조회"""
        api_id = "kt50032"
        url_path = "/api/dostk/acnt"
        body = {"strt_dt": strt_dt, "end_dt": end_dt, "tp": tp, "stk_cd": stk_cd}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)

    def get_gold_unconcluded_orders(self, ord_dt: str, qry_tp: Optional[str], mrkt_deal_tp: str, stk_bond_tp: str, sell_tp: str, stk_cd: Optional[str], fr_ord_no: Optional[str], dmst_stex_tp: Optional[str], cont_yn: Optional[str] = None, next_key: Optional[str] = None) -> Dict[str, Any]:
        """[kt50075] 금현물 미체결조회"""
        api_id = "kt50075"
        url_path = "/api/dostk/acnt"
        body = {"ord_dt": ord_dt, "qry_tp": qry_tp, "mrkt_deal_tp": mrkt_deal_tp, "stk_bond_tp": stk_bond_tp, "sell_tp": sell_tp, "stk_cd": stk_cd, "fr_ord_no": fr_ord_no, "dmst_stex_tp": dmst_stex_tp}
        return self._call_api(api_id, url_path, body=body, cont_yn=cont_yn, next_key=next_key)