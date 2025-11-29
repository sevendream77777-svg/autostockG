"""
Compatibility shim for moved Kakao notifier module.
Imports KakaoNotifier from api.kakao_api.kakao_notifier.
"""
from api.kakao_api.kakao_notifier import KakaoNotifier  # re-export

__all__ = ["KakaoNotifier"]
