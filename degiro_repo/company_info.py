from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials
from pydantic_settings import BaseSettings, SettingsConfigDict
class Settings(BaseSettings):
    USERNAME:str
    PASSWORD:str
    TOTP_SECRET_KEY:str
    USER_TOKEN:str
    INT_ACCOUNT:str
    
    model_config = SettingsConfigDict(env_file="./.env")

settings = Settings()  # type: ignore
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
        
    
    