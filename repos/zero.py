import polars as pl

import logging
import json

from repos.company_info import Degiro
from repos.german_turbo_info import fetch_turbo_data_parallel
from repos.utils.shared import CASH_SCHEMA, INITIAL_COLS

# Create and configure logger
logging.basicConfig(
    format="%(asctime)s %(message)s",
)

# Creating an object
logger = logging.getLogger()

# pl.read_csv()
import polars as pl
import os
from datetime import date
import requests
import json
import time

base_path = r"C:\Users\Admin\Downloads\finanzen_zero"


class ZeroRepo:
    zero_portfolio: pl.DataFrame
    isin_to_underlying_isin_zero: dict[str, str]

    def __init__(self):
        files: list[str] = os.listdir(base_path)
        number_regex = r"(\d+)"
        df_files = pl.DataFrame({"file_name": files}).with_columns(
            number=pl.when(pl.col("file_name").str.extract(number_regex, 1).is_null())
            .then(0)
            .otherwise(pl.col("file_name").str.extract(number_regex, 1).cast(pl.Int64))
        )
        csv_files: pl.DataFrame = df_files.filter(pl.col("file_name").str.ends_with(".csv"))
        df_most_recent_portfolio_csv_file: pl.DataFrame = (
            csv_files.sort("number", descending=True).select("file_name").limit(1)
        )
        most_recent_portfolio_csv_file_name: str = df_most_recent_portfolio_csv_file.select(
            "file_name"
        ).item()

        zero_df: pl.DataFrame = pl.read_csv(
            source=f"{base_path}\\{most_recent_portfolio_csv_file_name}", separator=";"
        )

        isin_to_lev = {}
        isin_to_stoploss = {}
        for isin in zero_df.select("ISIN").unique().to_numpy():
            isin_to_lookup: str = isin[0]
            response: requests.Response = requests.get(
                f"https://service.ivestor.de/api/instrument/isin/{isin_to_lookup}"
            )
            while response.status_code != 200:
                time.sleep(1)
                response = requests.get(
                    f"https://service.ivestor.de/api/instrument/isin/{isin_to_lookup}"
                )
            response_decode = json.loads(response.content.decode())
            if "figures" in response_decode:
                isin_to_lev[isin_to_lookup] = 1
                isin_to_stoploss[isin_to_lookup] = 0
            elif "figuresDerivatives" in response_decode:
                isin_to_lev[isin_to_lookup] = response_decode["figuresDerivatives"][
                "leverage"]["value"]
                isin_to_stoploss[isin_to_lookup] = response_decode["koBarrier"]["value"]
            else:
                isin_to_lev[isin_to_lookup]=0
                isin_to_stoploss[isin_to_lookup]=0
                # raise Exception("Nothing found in ivestor?")
            
        turbo_isin_to_underlying_isin, _ = fetch_turbo_data_parallel(zero_df.filter(pl.col("Art") == "DERIVAT")["ISIN"].to_list())
        zero_portfolio_0: pl.DataFrame = zero_df.with_columns(
            BROKER=pl.lit("Zero"),
            SECURITY_TYPE=pl.when(pl.col("Art") == "DERIVAT")
            .then(pl.lit("Turbo"))
            .otherwise(pl.lit("STOCK")),
            HEFBOOM=pl.col("ISIN").replace(isin_to_lev).cast(pl.Float64),
            STOPLOSS=pl.col("ISIN").replace(isin_to_stoploss).cast(pl.Float64),
            Aantal=pl.col("Anzahl").cast(pl.Float64),
            Koers=pl.col("Kurs"),
            Valuta=pl.lit("EUR"),
            INITIELE_WAARDE=pl.col("Kaufwert")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64),
            Waarde=pl.col("Wert")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64),
            GAK=pl.col("Kaufkurs")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64),
            ONGEREALISEERDE_WV_EUR=pl.col("Erfolg [EUR]")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64),
            ONGEREALISEERDE_WV_PCT=pl.col("Erfolg [%]")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64),
            UNDERLYING_ISIN=pl.col("ISIN").replace(turbo_isin_to_underlying_isin),
        ).with_columns(
            pl.col("Erfolg [EUR]")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64)
            .alias("W/V €"),
            pl.col("Erfolg [%]")
            .str.replace_all(r"\.", "")
            .str.replace_all(",", ".")
            .cast(pl.Utf8)
            .alias("W/V %"),
        )
        self.zero_portfolio = zero_portfolio_0.select(INITIAL_COLS)

        self.isin_to_underlying_isin_zero = turbo_isin_to_underlying_isin
        
        
        # Add cash
        json_files: pl.DataFrame = df_files.filter(pl.col("file_name").str.ends_with(".json"))
        df_most_recent_portfolio_cash_json_file: pl.DataFrame = (
            json_files.sort("number", descending=True).select("file_name").limit(1)
        )
        most_recent_portfolio_json_cash_file_name: str = df_most_recent_portfolio_cash_json_file.select(
            "file_name"
        ).item()

        with open(f"{base_path}\\{most_recent_portfolio_json_cash_file_name}", "r", encoding="utf-8") as f:
            cash_amount:float =json.load(f)["cash"]
            
        
        zero_cash_eur_df = pl.DataFrame(
            {                    
                "ISIN": ["EUR"],
                "BROKER": ["ZERO"],
                "SECURITY_TYPE": ["CASH"],
                "HEFBOOM": [1.0],
                "STOPLOSS": [0.0],
                "Aantal": [1.0],
                "Koers": ["1.0"],
                "Valuta": ["EUR"],
                "INITIELE_WAARDE": [cash_amount],
                "Waarde": [cash_amount],
                "GAK": [1.0],
                "W/V\xa0€": [0.0],
                "W/V %": ["0%"],
                "ONGEREALISEERDE_WV_EUR": [0.0],
                "ONGEREALISEERDE_WV_PCT": [0.0],                    
                "UNDERLYING_ISIN": ["EUR"],
                "UNDERLYING": ["EUR"],
                "SECTOR": ["CASH"],
                "INDUSTRY": ["CASH"],
            },
            schema_overrides=CASH_SCHEMA
        )
        self.zero_cash_eur_df = zero_cash_eur_df

