from typing import Any
from degiro_connector.trading.api import API
import gspread
import polars as pl
import pandas as pd
from repos.company_info import Degiro
from repos.degiro_nl import DegiroRepoNL
from repos.degiro_de import DegiroRepoDE
from repos.etoro import EtoroRepo
from repos.google import get_tickers_from_isins
from repos.nl_sg_turbo_info import fetch_sg_turbo_data_parallel
from repos.sg_uisins import lookup_underlying_isin_from_sg_for_sg_turbos
from repos.utils.shared import get_tradingview_chart_url_by_isin
from repos.zero import ZeroRepo


degiro_repo_de = DegiroRepoDE()
degiro_portfolio_de: pl.DataFrame = degiro_repo_de.consolidated_degiro_initial_df
isin_to_underlying_isin_degiro_de: dict[str, str] = (
    degiro_repo_de.isin_to_underlying_isin_degiro_de
)

degiro_repo_nl = DegiroRepoNL()
degiro_portfolio_nl: pl.DataFrame = degiro_repo_nl.consolidated_degiro_initial_df
isin_to_underlying_isin_degiro_nl: dict[str, str] = (
    degiro_repo_nl.isin_to_underlying_isin_degiro_nl
)

etoro_repo = EtoroRepo()
etoro_portfolio_ex_cash: pl.DataFrame = etoro_repo.etoro_portfolio_ex_cash
isin_to_underlying_isin_etoro: dict[str, str] = etoro_repo.isin_to_underlying_isin_etoro

zero_repo = ZeroRepo()
zero_portfolio: pl.DataFrame = zero_repo.zero_portfolio
isin_to_underlying_isin_zero: dict[str, str] = zero_repo.isin_to_underlying_isin_zero


unioned: pl.DataFrame = pl.concat(
    [degiro_portfolio_de, degiro_portfolio_nl, etoro_portfolio_ex_cash, zero_portfolio]
)

degiro_unioned: pl.DataFrame = pl.concat([degiro_portfolio_de, degiro_portfolio_nl])


# Lookup underlying isin Degiro
stock_isins: list[str] = [
    i[0]
    for i in degiro_unioned.filter(pl.col("SECURITY_TYPE") == "STOCK")
    .select("ISIN")
    .unique()
    .to_numpy()
]
stock_isin_underlying_isin: dict[str, str] = {isin: isin for isin in stock_isins}


isin_to_underlying_isin: dict[str, str] = (
    stock_isin_underlying_isin
    | isin_to_underlying_isin_degiro_nl
    | isin_to_underlying_isin_degiro_de
    | isin_to_underlying_isin_zero
    | isin_to_underlying_isin_etoro
)

with_underlying_isin: pl.DataFrame = unioned.with_columns(
    UNDERLYING_ISIN=pl.col("ISIN").replace(isin_to_underlying_isin)
).filter(pl.col("UNDERLYING_ISIN").is_not_null())

# lookup underlying name, sector,
# Enrich by company info
isin_to_company_sector = {}
isin_to_company_industry = {}
isin_to_name = {}
degiro: API = Degiro().degiro_connector

for isin in with_underlying_isin["UNDERLYING_ISIN"].to_list():
    company_profile = degiro.get_company_profile(product_isin=isin)
    if isin is None:
        raise Exception(
            "Most likely forgot to add underlying to the notes of finanzen Zero"
        )
    isin_to_company_sector[isin] = company_profile.data["sector"]
    isin_to_company_industry[isin] = company_profile.data["industry"]
    isin_to_name[isin] = company_profile.data["contacts"]["NAME"]

with_underlying_info = with_underlying_isin.with_columns(
    UNDERLYING=pl.col("UNDERLYING_ISIN").replace(isin_to_name),
    SECTOR=pl.col("UNDERLYING_ISIN").replace(isin_to_company_sector),
    INDUSTRY=pl.col("UNDERLYING_ISIN").replace(isin_to_company_industry),
)

# Add cash
degiro_de_cash_eur_df: pl.DataFrame = degiro_repo_de.degiro_cash_eur_df
degiro_nl_cash_eur_df: pl.DataFrame = degiro_repo_nl.degiro_cash_eur_df
etoro_cash_eur_df: pl.DataFrame = etoro_repo.etoro_portfolio_cash
zero_cash_eur_df: pl.DataFrame = zero_repo.zero_cash_eur_df

cash_consolidated: pl.DataFrame = pl.concat(
    [degiro_de_cash_eur_df, degiro_nl_cash_eur_df, etoro_cash_eur_df, zero_cash_eur_df]
)

final_consolidated_portfolio: pl.DataFrame = pl.concat(
    [with_underlying_info, cash_consolidated]
)

final_consolidated_portfolio.write_excel(column_totals=True, autofit=True)
underlying_isin_list = final_consolidated_portfolio.select("UNDERLYING_ISIN").unique().filter(pl.col("UNDERLYING_ISIN") != "EUR")["UNDERLYING_ISIN"].to_list()
isin_to_gf_ticker = get_tickers_from_isins(underlying_isin_list)
tv_to_gf_exchange_mapping = {"EURONEXT": "AMS", "SIX":"SWX", "LSE":"LON", "OMXCOP":"CPH", "XETRA": "ETR"}
holdings: pl.DataFrame = (
    final_consolidated_portfolio.select("UNDERLYING_ISIN")
    .unique()
    .filter(pl.col("UNDERLYING_ISIN") != "EUR")
    .with_columns(
        tradingview_link=pl.col("UNDERLYING_ISIN").map_elements(
            get_tradingview_chart_url_by_isin, return_dtype=pl.Utf8
        )
    )
    .with_columns(
        exchange=pl.col("tradingview_link").str.extract(r"symbol=(\w*):(\w*)", 1),
        ticker=pl.col("tradingview_link").str.extract(r"symbol=(\w*):(\w*)", 2),
        price=pl.col("UNDERLYING_ISIN").replace(isin_to_gf_ticker)
    )
    .select("UNDERLYING_ISIN", "exchange", "ticker", "tradingview_link")
)
holdings_pd: pd.DataFrame = holdings.to_pandas()
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# Authenticate and create the client
client = gspread.oauth(
    scopes=scope,
    credentials_filename=r"C:\Users\Admin\Documents\degiro\pw\client_secret_353715690187-m3re7sf7gil3emin7bhur5gmd0kancs1.apps.googleusercontent.com.json",
)

# Open the spreadsheet
sheet = client.open("Lookup Turbo quotes").worksheet("Holdings")
sheet.clear()
sheet.update([holdings_pd.columns.values.tolist()] + holdings_pd.values.tolist())

open("pivot_table.xlsx")
