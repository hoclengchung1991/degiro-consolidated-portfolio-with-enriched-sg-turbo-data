import polars as pl
from pydantic_settings import BaseSettings, SettingsConfigDict
INITIAL_COLS = [    
    "ISIN",
    "BROKER",
    "SECURITY_TYPE",
    "HEFBOOM",
    "STOPLOSS",
    "Aantal",
    "Koers",
    "Valuta",
    "INITIELE_WAARDE",
    "Waarde",
    "GAK",
    "W/V\xa0€",
    "W/V %",
    "ONGEREALISEERDE_WV_EUR",
    "ONGEREALISEERDE_WV_PCT",
]

CASH_SCHEMA = {   
    "ISIN": pl.Utf8, 
    "BROKER": pl.Utf8,
    "SECURITY_TYPE": pl.Utf8,
    "HEFBOOM": pl.Float64,
    "STOPLOSS": pl.Float64,
    "Aantal": pl.Float64,
    "Koers": pl.Utf8,
    "Valuta": pl.Utf8,
    "INITIELE_WAARDE": pl.Float64,
    "Waarde": pl.Float64,
    "GAK": pl.Float64,
    "W/V\xa0€": pl.Float64,
    "W/V %": pl.Utf8,
    "ONGEREALISEERDE_WV_EUR": pl.Float64,
    "ONGEREALISEERDE_WV_PCT": pl.Float64,
    "UNDERLYING_ISIN": pl.Utf8,
    "UNDERLYING": pl.Utf8,
    "SECTOR": pl.Utf8,
    "INDUSTRY": pl.Utf8,
}


class DegiroSettings(BaseSettings):
    USERNAME: str
    PASSWORD: str
    TOTP_SECRET_KEY: str
    USER_TOKEN: str
    INT_ACCOUNT: str
    USERNAME_DE: str
    PASSWORD_DE:str
    TOTP_SECRET_KEY_DE:str

    model_config = SettingsConfigDict(env_file=".env")