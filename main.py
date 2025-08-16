from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import pyotp
import pandas as pd
import polars as pl
import requests, json
from io import StringIO
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
class Settings(BaseSettings):
    USERNAME:str
    PASSWORD:str
    TOTP_SECRET_KEY:str
    
    model_config = SettingsConfigDict(env_file=".env")


STOCK_SELECT_COLS = [
    "Product",
    "Symbool | ISIN",
    "Beurs",
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
    "Totale W/V€",
]

OUTPUT_COLS = [
    "Product",
    "Symbool | ISIN",
    "Beurs",
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
    "Totale W/V€",
    "ISIN",
    "UNDERLYING_ISIN",
    "UNDERLYING",
]

with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    page = context.new_page()
    page.goto("https://trader.degiro.nl/login/nl#/login")
    page.get_by_text("Accept all").click()
    print(page.title())
    settings = Settings() # type: ignore

    page.get_by_label("Gebruikersnaam").fill(settings.USERNAME)
    page.get_by_label("Wachtwoord").fill(settings.PASSWORD)
    page.get_by_text("Inloggen", exact=True).click()
    one_time_password = str(pyotp.TOTP(settings.TOTP_SECRET_KEY).now())
    page.get_by_placeholder("012345").fill(one_time_password)
    page.get_by_text("Bevestig", exact=True).click()
    page.locator("a[data-name='portfolioMenuItem']").click()
    page.wait_for_selector("span[data-name='stock']")

    # Format stocks df    
    ongerealiseerde_wv_regex = r"([+-]\d+,\d{2})\xA0\(([+-]\d+,\d{2})%\)"
    negative_values = True
    # while negative_values:
    content = StringIO(page.content())
    initial_read_tables = pd.read_html(content, thousands="", decimal=".")
    df_stocks: pl.DataFrame = (
        pl.from_pandas(initial_read_tables[0])
        .with_columns(
            Product=pl.col("Product").str.replace("KV", "").str.head(-1),
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
    ).select(STOCK_SELECT_COLS)
    negative_values:bool = df_stocks.select("Waarde").limit(1).item() <= 0
        

    # Format turbos df
    df_turbos: pl.DataFrame = (
        pl.from_pandas(initial_read_tables[1])
        .with_columns(
            Product=pl.col("Product").str.replace("KV", "").str.slice(2),
            STOCK_NAME=pl.col("Product")
            .str.replace(r"Factor|Classic|BEST|Warrant", ";")
            .str.split(";")
            .list[0]
            .str.split(" ")
            .list.slice(1)
            .list.join(" "),
            SECURITY_TYPE=pl.col("Product").str.extract(
                r"(Factor|Classic|BEST|Warrant)"
            ),
            STOPLOSS=pl.when(
                pl.col("Product").str.extract(pattern=r"SL (\d+\.\d+)").is_not_null()
            )
            .then(
                pl.col("Product").str.extract(pattern=r"SL (\d+\.\d+)").cast(pl.Float64)
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
        .select(STOCK_SELECT_COLS)
    )
    unioned: pl.DataFrame = pl.concat([df_turbos, df_stocks])
    with_isin = unioned.with_columns(
        ISIN=pl.when(pl.col("Symbool | ISIN").str.split("|").list.len() == 1)
        .then(pl.col("Symbool | ISIN"))
        .otherwise(
            pl.col("Symbool | ISIN").str.split("|").list.get(index=1, null_on_oob=True)
        )
    )
    # .filter(pl.col("Symbool | ISIN")=="0YU | US98850P1093")

    isins: list[str] = [
        isin[0]
        for isin in with_isin.filter(pl.col("SECURITY_TYPE") != "STOCK")
        .select("ISIN")
        .to_numpy()
    ]
    sg_isin_lookup_code = {
        i: json.loads(
            requests.get(
                f"https://sgbeurs.nl/quicksearch/quicksearch?term={i}"
            ).content.decode()
        )["products"][0]["code"]
        for i in isins
    }
    # sg_code_lookup = [json.loads(requests.get(f"https://sgbeurs.nl/quicksearch/quicksearch?term={i}").content.decode()) for i in sg_isin_lookup_code]
    sg_pid_lookup = {
        v: json.loads(
            requests.get(
                f"https://sgbeurs.nl/EmcWebApi/api/Products?code={v}"
            ).content.decode()
        )["Id"]
        for _, v in sg_isin_lookup_code.items()
    }
    import time

    isin_to_underlying = {}
    isin_to_underlying_isin = {}
    for k, v in sg_pid_lookup.items():
        response = requests.get(
            f"https://sgbeurs.nl/EmcWebApi/api/Products/AllProperties/{v}"
        )
        content = response.content.decode()
        while "exceeded" in content:
            time.sleep(1)
            response = requests.get(
                f"https://sgbeurs.nl/EmcWebApi/api/Products/AllProperties/{v}"
            )
            content = response.content.decode()
        underlying = [
            element
            for element in json.loads(content)
            if element["Label"] == "Onderliggende waarde"
        ][0]["Value"]
        underlying_isin = [
            element
            for element in json.loads(content)
            if element["Label"] == "ISIN code onderliggende waarde"
        ][0]["Value"]
        isin_to_underlying_isin[k] = underlying_isin
        isin_to_underlying[k] = underlying

    isin_to_onderliggende_waarde = {
        k: isin_to_underlying[v] for k, v in sg_isin_lookup_code.items()
    }
    isin_to_onderliggende_waarde_isin = {
        k: isin_to_underlying_isin[v] for k, v in sg_isin_lookup_code.items()
    }
    df_unioned_turbos_with_underlying = with_isin.with_columns(
        UNDERLYING_ISIN=pl.when(pl.col("SECURITY_TYPE") != "STOCK")
        .then(pl.col("ISIN").replace(isin_to_onderliggende_waarde_isin))
        .otherwise(pl.col("ISIN")),
        UNDERLYING=pl.when(pl.col("SECURITY_TYPE") != "STOCK")
        .then(pl.col("ISIN").replace(isin_to_onderliggende_waarde))
        .otherwise(pl.col("Product")),
    ).with_columns(UNDERLYING_ISIN=pl.col("UNDERLYING_ISIN").str.strip_chars())
    underlying_isin_to_underlying_final = df_unioned_turbos_with_underlying.group_by(
        pl.col("UNDERLYING_ISIN").str.strip_chars()
    ).agg(pl.first("UNDERLYING").alias("UNDERLYING_RIGHT"))
    df_unioned_turbos_final = (
        df_unioned_turbos_with_underlying.join(
            underlying_isin_to_underlying_final, on="UNDERLYING_ISIN", how="left"
        )
        .with_columns(UNDERLYING=pl.col("UNDERLYING_RIGHT"))
        .select(OUTPUT_COLS)
    )
    df_unioned_turbos_final.write_excel(column_totals=True, autofit=True)
    browser.close()
os.startfile("pivot_table.xlsx")
