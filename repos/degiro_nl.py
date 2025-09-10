from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import pyotp
import pandas as pd
import polars as pl
from io import StringIO
from repos.nl_sg_turbo_info import fetch_sg_turbo_data_parallel
from repos.utils.shared import DegiroSettings
import logging

from repos.utils.shared import CASH_SCHEMA, INITIAL_COLS

# Create and configure logger
logging.basicConfig(
    format="%(asctime)s %(message)s",
)
import time

# Creating an object
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DegiroRepoNL:
    consolidated_degiro_initial_df: pl.DataFrame
    degiro_cash_eur_df: pl.DataFrame
    isin_to_underlying_isin_degiro_nl:dict
    

    def __init__(self):
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()
            page.goto("https://trader.degiro.nl/login/nl#/login")
            page.get_by_text("Accept all").click()
            print(page.title())
            logger.info("Opening Degiro Site...")
            settings = DegiroSettings()  # type: ignore

            page.get_by_label("Gebruikersnaam").fill(settings.USERNAME)
            page.get_by_label("Wachtwoord").fill(settings.PASSWORD)
            page.get_by_text("Inloggen", exact=True).click()
            one_time_password = str(pyotp.TOTP(settings.TOTP_SECRET_KEY).now())
            page.get_by_placeholder("012345").fill(one_time_password)
            page.get_by_text("Bevestig", exact=True).click()
            logger.info("Login Succesfull")
            page.locator("a[data-name='portfolioMenuItem']").click()
            page.wait_for_selector("span[data-name='stock']")
            time.sleep(3)

            # Format stocks df
            ongerealiseerde_wv_regex = r"([+-]\d+,\d{2})\xA0\(([+-]\d+,\d{2})%\)"
            negative_values = True
            # while negative_values:
            content = StringIO(page.content())
            initial_read_tables = pd.read_html(content, thousands="", decimal=".")
            df_stocks: pl.DataFrame = (
                pl.from_pandas(initial_read_tables[0])
                .with_columns(
                    Product=pl.col("Product")
                    .str.replace("KV", "")
                    .str.replace(r".$", ""),
                    ONGEREALISEERDE_WV_EUR=pl.col("Ongerealiseerde W/V\xa0€")
                    .str.extract(ongerealiseerde_wv_regex, 1)
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    ONGEREALISEERDE_WV_PCT=pl.col("Ongerealiseerde W/V\xa0€")
                    .str.extract(ongerealiseerde_wv_regex, 2)
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    Waarde=(
                        pl.col("Waarde")
                        .str.replace_all(r"\.", "")
                        .str.replace_all(",", ".")
                        .cast(pl.Float64)
                    ).round(2),
                    GAK=pl.col("GAK").str.replace_all(",", ".").cast(pl.Float64),
                    SECURITY_TYPE=pl.lit("STOCK"),
                    HEFBOOM=pl.lit(1.0),
                    STOPLOSS=pl.lit(0.0),
                )
                .with_columns(
                    pl.col("Totale W/V€")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    pl.col("W/V\xa0€")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    (pl.col("Waarde") - pl.col("ONGEREALISEERDE_WV_EUR")).alias(
                        "INITIELE_WAARDE"
                    ),
                )
                .with_columns(
                    ISIN=pl.when(
                        pl.col("Symbool | ISIN").str.split("|").list.len() == 1
                    )
                    .then(pl.col("Symbool | ISIN"))
                    .otherwise(
                        pl.col("Symbool | ISIN").str.split("|").list.get(index=1)
                    ),
                    BROKER=pl.lit("Degiro NL")
                )
            ).select(INITIAL_COLS)
            

            # Format turbos df
            df_turbos: pl.DataFrame = (
                pl.from_pandas(initial_read_tables[1])
                .with_columns(
                    Product=pl.col("Product").str.replace("KV", "").str.slice(2),
                    # STOCK_NAME=pl.col("Product")
                    # .str.replace(r"Factor|Classic|BEST|Warrant", ";")
                    # .str.split(";")
                    # .list[0]
                    # .str.split(" ")
                    # .list.slice(1)
                    # .list.join(" "),
                    SECURITY_TYPE=pl.col("Product").str.extract(
                        r"(Factor|Classic|BEST|Warrant)"
                    ),
                    STOPLOSS=pl.when(
                        pl.col("Product")
                        .str.extract(pattern=r"SL (\d+\.\d+)")
                        .is_not_null()
                    )
                    .then(
                        pl.col("Product")
                        .str.extract(pattern=r"SL (\d+\.\d+)")
                        .cast(pl.Float64)
                    )
                    .otherwise(pl.lit(0.0)),
                    ONGEREALISEERDE_WV_EUR=pl.col("Ongerealiseerde W/V\xa0€")
                    .str.extract(ongerealiseerde_wv_regex, 1)
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    ONGEREALISEERDE_WV_PCT=pl.col("Ongerealiseerde W/V\xa0€")
                    .str.extract(ongerealiseerde_wv_regex, 2)
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    HEFBOOM=(
                        pl.col("Product").str.extract(r"(LE*V )(\d+)(\.\d+)*", 2)
                        + pl.col("Product")
                        .str.extract(r"(LE*V )(\d+)(\.\d+)*", 3)
                        .fill_null("")
                    ).cast(pl.Float64),
                    Waarde=(
                        pl.col("Waarde")
                        .str.replace_all(r"\.", "")
                        .str.replace_all(",", ".")
                        .cast(pl.Float64)
                    ).round(2),
                    GAK=pl.col("GAK").str.replace_all(",", ".").cast(pl.Float64),
                )
                .with_columns(
                    pl.col("Totale W/V€")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    pl.col("W/V\xa0€")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float64),
                    (pl.col("Waarde") - pl.col("ONGEREALISEERDE_WV_EUR")).alias(
                        "INITIELE_WAARDE"
                    ),
                )
                .with_columns(
                    ISIN=pl.when(
                        pl.col("Symbool | ISIN").str.split("|").list.len() == 1
                    )
                    .then(pl.col("Symbool | ISIN"))
                    .otherwise(
                        pl.col("Symbool | ISIN").str.split("|").list.get(index=1).str.strip()
                    ),
                    BROKER=pl.lit("Degiro NL")
                )
                .select(INITIAL_COLS)
            )
            unioned: pl.DataFrame = pl.concat([df_turbos, df_stocks])
            self.consolidated_degiro_initial_df = unioned.with_columns(
                Aantal=pl.col("Aantal").cast(pl.Float64),
                ISIN=pl.col("ISIN").str.strip_chars()                
            )

            # Cash
            total_cash_eur_degiro: float = float(
                page.locator('[data-field="totalCash"]')
                .inner_text()
                .replace("€", "")
                .replace("\xa0", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            )

            degiro_cash_eur_df = pl.DataFrame(
                {
                    "ISIN": ["EUR"],
                    "BROKER": ["DEGIRO_NL"],
                    "SECURITY_TYPE": ["CASH"],
                    "HEFBOOM": [1.0],
                    "STOPLOSS": [0.0],
                    "Aantal": [1.0],
                    "Koers": ["1.0"],
                    "Valuta": ["EUR"],
                    "INITIELE_WAARDE": [total_cash_eur_degiro],
                    "Waarde": [total_cash_eur_degiro],
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
                schema_overrides=CASH_SCHEMA,
            )
            self.degiro_cash_eur_df = degiro_cash_eur_df
            
            self.isin_to_underlying_isin_degiro_nl, _ = fetch_sg_turbo_data_parallel(df_turbos.filter(pl.col("SECURITY_TYPE") != "STOCK")["ISIN"].to_list(), max_workers=10)
