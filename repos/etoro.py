import polars as pl

import logging

from repos.company_info import Degiro
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


base_path = r"C:\Users\Admin\Downloads\etoro"


class EtoroRepo:
    etoro_portfolio_ex_cash: pl.DataFrame
    etoro_portfolio_cash: pl.DataFrame
    isin_to_underlying_isin_etoro: dict[str, str]

    def __init__(self):

        files: list[str] = os.listdir(base_path)
        number_regex = r"(\d+)"
        df_files = pl.DataFrame({"file_name": files}).with_columns(
            number=pl.when(pl.col("file_name").str.extract(number_regex, 1).is_null())
            .then(0)
            .otherwise(pl.col("file_name").str.extract(number_regex, 1).cast(pl.Int64))
        )
        df_today_files_most_recent = (
            df_files.sort("number", descending=True).select("file_name").limit(1)
        )
        relevant_file_name: str = df_today_files_most_recent.select("file_name").item()
        etoro_df: pl.DataFrame = pl.read_csv(
            source=f"{base_path}\\{relevant_file_name}"
        ).with_columns(
            [
                pl.col("change")
                .str.extract(r"(-*\d+\.\d{2}) \((-*\d+\.\d{2}\%)", 1)
                .cast(pl.Float64)
                .alias("W/V €"),
                pl.col("change")
                .str.extract(r"(-*\d+\.\d{2}) \((-*\d+\.\d{2}\%)", 2)
                .alias("W/V %"),
            ]
        )

        ticker_to_isin_for_etoro_df: pl.DataFrame = pl.read_csv(
            source=r"C:\Users\Admin\Documents\degiro\pw\repos\ticker_to_isin_for_etoro.csv"
        )
        etoro_portfolio_df: pl.DataFrame = etoro_df.join(
            ticker_to_isin_for_etoro_df, left_on="ticker", right_on="TICKER", how="left"
        ).with_columns(
            [
                pl.lit("ETORO").alias("Beurs"),
                pl.when(pl.col("underlying") == "EUR")
                .then(pl.lit("CASH"))
                .otherwise(pl.lit("CFD"))
                .alias("SECURITY_TYPE"),
                pl.col("leverage").alias("HEFBOOM"),
                pl.when(pl.col("underlying") == "EUR")
                .then(0.0).otherwise(pl.col("avgOpen") * 0.5).alias("STOPLOSS"),
                pl.col("units").alias("Aantal"),
                pl.col("price").cast(pl.Utf8).alias("Koers"),
                pl.when(pl.col("underlying") == "EUR")
                .then(pl.lit("EUR"))
                .otherwise(pl.lit("*"))
                .alias("Valuta"),
                pl.col("invested").alias("INITIELE_WAARDE"),
                pl.col("netValue").alias("Waarde"),
                pl.col("avgOpen").alias("GAK"),
                (pl.col("netValue") - pl.col("invested"))
                .round(2)
                .alias("ONGEREALISEERDE_WV_EUR"),
                ((pl.col("netValue") - pl.col("invested")) / pl.col("invested") * 100)
                .round(2)
                .alias("ONGEREALISEERDE_WV_PCT"),
            ]
        )

        self.etoro_portfolio_ex_cash = etoro_portfolio_df.select(INITIAL_COLS).filter(
            pl.col("SECURITY_TYPE") == "CFD"
        )
        self.isin_to_underlying_isin_etoro = {
            val: val for val in self.etoro_portfolio_ex_cash["ISIN"].to_list()
        }

        # Add cash
        self.etoro_portfolio_cash = (
            etoro_portfolio_df.select(INITIAL_COLS)
            .filter(pl.col("SECURITY_TYPE") == "CASH")
            .with_columns(
                UNDERLYING_ISIN=pl.lit("EUR"),
                UNDERLYING=pl.lit("EUR"),
                SECTOR=pl.lit("CASH"),
                INDUSTRY=pl.lit("CASH"),
            )
        )
