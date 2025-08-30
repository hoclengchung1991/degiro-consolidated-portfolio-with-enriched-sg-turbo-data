from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials
from repos.utils.shared import DegiroSettings
# .company_info import DegiroSettings

settings = DegiroSettings()  # type: ignore
credentials = build_credentials(    
        override={
            "username": settings.USERNAME,
            "password": settings.PASSWORD,
            "int_account": settings.INT_ACCOUNT,
            "totp_secret_key": settings.TOTP_SECRET_KEY,
        },
    )
class Degiro():

    def __init__(self):
        trading_api = TradingAPI(credentials=credentials)
        trading_api.connect()        
        self.degiro_connector = trading_api
        
    
    