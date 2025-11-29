# Compatibility shim so imports like `from kiwoom_api import KiwoomRestApi` keep working.
from api.kiwoom_rest.kiwoom_api import KiwoomRestApi  # re-export

__all__ = ["KiwoomRestApi"]
