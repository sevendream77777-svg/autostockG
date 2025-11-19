# data_manager.py (전체 코드)

import json
import os
import configparser
from typing import Dict, Any, List, Optional 
from datetime import datetime, timedelta

class DataManager:
    """
    과거 차트 데이터 등을 파일 시스템에 저장하고 불러오는 역할을 전담합니다.
    (데이터 백업 및 복원 시스템)
    """

    def __init__(self, target_folder: Optional[str] = None):
        if target_folder:
            self.data_path = target_folder
        else:
            # config.ini에서 DATA_PATH 설정값 읽기
            config = configparser.ConfigParser()
            config_file_path = 'config.ini'
            # config.ini 파일이 존재하지 않을 경우를 대비하여 fallback 처리
            if os.path.exists(config_file_path):
                 config.read(config_file_path, encoding='utf-8') 
            
            # config.ini에 DATA_PATH가 없으면 기본값 './data/' 사용
            self.data_path = config.get('SETTINGS', 'DATA_PATH', fallback='./data/').strip()

        # 데이터 저장 폴더가 없으면 생성
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            print(f"✅ DataManager: 저장 폴더 생성 완료 ({self.data_path})")

    # --- 파일 경로 함수 ---
    
    def _get_chart_file_path(self, stock_code: str) -> str:
        """종목 코드를 기반으로 저장될 (차트) 파일 경로를 반환합니다."""
        return os.path.join(self.data_path, f"{stock_code}_chart_data.json")

    def _get_finance_file_path(self, stock_code: str) -> str:
        """종목 코드를 기반으로 저장될 재무 데이터 파일 경로를 반환합니다. (추가)"""
        return os.path.join(self.data_path, f"{stock_code}_finance_data.json")

    # --- 기존 차트 데이터 함수 (유지) ---
    
    def save_chart_data(self, stock_code: str, chart_data: List[Dict[str, str]]):
        """API에서 받은 일봉 차트 데이터를 파일에 저장합니다."""
        file_path = self._get_chart_file_path(stock_code)
        
        data_to_save = {
            "saved_at": datetime.now().strftime("%Y%m%d %H:%M:%S"),
            "data": chart_data
        }
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4)
            print(f"✅ DataManager: {stock_code}의 차트 데이터 저장 완료.")
        except Exception as e:
            print(f"❌ DataManager: {stock_code} 차트 데이터 저장 실패: {e}")

    def load_chart_data(self, stock_code: str, max_age_days: int = 1) -> Optional[List[Dict[str, str]]]:
        """저장된 차트 데이터를 불러오고, 파일이 너무 오래되었으면(max_age_days) None을 반환합니다."""
        file_path = self._get_chart_file_path(stock_code)
        
        if not os.path.exists(file_path):
            print(f"🟡 DataManager: {stock_code}의 저장된 차트 파일이 없습니다.")
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data_loaded = json.load(f)
            
            saved_time_str = data_loaded.get('saved_at', '20000101 00:00:00')
            saved_time = datetime.strptime(saved_time_str, "%Y%m%d %H:%M:%S")
            
            if datetime.now() - saved_time > timedelta(days=max_age_days):
                print(f"⚠️ DataManager: {stock_code} 차트 데이터가 {max_age_days}일 이상되어 만료되었습니다. 새로 수집합니다.")
                return None

            print(f"✅ DataManager: {stock_code}의 차트 데이터 로드 성공 (저장 시간: {saved_time_str})")
            return data_loaded.get('data', [])

        except Exception as e:
            print(f"❌ DataManager: {stock_code} 차트 데이터 로드/파싱 실패: {e}. 새로 수집합니다.")
            return None
    
    # --- 재무 데이터 저장 함수 (추가) ---
    
    def save_finance_data(self, stock_code: str, finance_data: List[Dict[str, Any]]):
        """API에서 받은 재무 데이터를 파일에 저장합니다."""
        file_path = self._get_finance_file_path(stock_code)
        
        data_to_save = {
            "saved_at": datetime.now().strftime("%Y%m%d %H:%M:%S"),
            "data": finance_data
        }
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=4)
            print(f"✅ DataManager: {stock_code}의 재무 데이터 저장 완료. ({len(finance_data)}일치)")
        except Exception as e:
            print(f"❌ DataManager: {stock_code} 재무 데이터 저장 실패: {e}")

    # --- 재무 데이터 로드 함수 (추가) ---
    
    def load_finance_data(self, stock_code: str, max_age_days: int = 3) -> Optional[List[Dict[str, Any]]]:
        """
        저장된 재무 데이터를 불러오고, 파일이 너무 오래되었으면(max_age_days) None을 반환합니다.
        테스트를 위해 max_age_days를 3일로 설정합니다.
        """
        file_path = self._get_finance_file_path(stock_code)
        
        if not os.path.exists(file_path):
            print(f"🟡 DataManager: {stock_code}의 저장된 재무 파일이 없습니다. 새로 수집합니다.")
            return None

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data_loaded = json.load(f)
            
            saved_time_str = data_loaded.get('saved_at', '20000101 00:00:00')
            saved_time = datetime.strptime(saved_time_str, "%Y%m%d %H:%M:%S")
            
            if datetime.now() - saved_time > timedelta(days=max_age_days):
                print(f"⚠️ DataManager: {stock_code} 재무 데이터가 {max_age_days}일 이상되어 만료되었습니다. 새로 수집합니다.")
                return None

            print(f"✅ DataManager: {stock_code}의 재무 데이터 로드 성공 (저장 시간: {saved_time_str}, {len(data_loaded.get('data', []))}일치)")
            return data_loaded.get('data', [])

        except Exception as e:
            print(f"❌ DataManager: {stock_code} 재무 데이터 로드/파싱 실패: {e}. 새로 수집합니다.")
            return None


if __name__ == '__main__':
    # 🌟 [추가된 부분] DataManager가 단독 실행될 때 실행되는 코드 (테스트용)
    print("--- DataManager 독립 실행 모드 ---")
    try:
        manager = DataManager()
        print(f"✅ DataManager: 기본 준비 완료. 데이터 경로는 {manager.data_path} 입니다.")
        
    except Exception as e:
        print(f"❌ DataManager: 초기화 중 오류 발생: {e}")