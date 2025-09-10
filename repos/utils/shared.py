import polars as pl
from pydantic_settings import BaseSettings, SettingsConfigDict
import requests
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

    

def get_tradingview_chart_url_by_isin(isin: str) -> str:
    url = (
        "https://symbol-search.tradingview.com/symbol_search/v3/"
        f"?text={isin}&hl=1&exchange=&lang=en&search_type=undefined"
        "&domain=production&sort_by_country=US&promo=true"
    )

    headers = {
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.text}")

    data = response.json()

    if "symbols" not in data or len(data["symbols"]) == 0:
        raise Exception(f"No symbols found for ISIN: {isin}")

    # Try to find primary listing
    primary = next((s for s in data["symbols"] if s.get("is_primary_listing")), None)
    symbol_data = primary or data["symbols"][0]

    exchange = symbol_data["exchange"].upper()
    symbol = symbol_data["symbol"].upper()

    return f"https://www.tradingview.com/chart/?symbol={exchange}:{symbol}"