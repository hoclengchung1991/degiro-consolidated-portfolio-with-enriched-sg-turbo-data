from typing import Any
from degiro_connector.trading.api import API
import gspread
import polars as pl
import pandas as pd
from repos.enrich.company_info import Degiro
from repos.brokers.degiro_nl import DegiroRepoNL
from repos.brokers.degiro_de import DegiroRepoDE
from repos.brokers.etoro import EtoroRepo
from repos.enrich.ticker import get_google_tickers_from_isins, get_yahoo_tickers_from_isins
from repos.utils.shared import get_tradingview_chart_url_by_keyword
from repos.brokers.zero import ZeroRepo
import os
from datetime import datetime

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

output_dir = "portfolio_snapshots"
os.makedirs(output_dir, exist_ok=True)

# Generate filename with today's date
today_str = datetime.now().strftime("%Y_%m_%d")
filename = f"snapshot_{today_str}.xlsx"

# Full path
output_path = os.path.join(output_dir, filename)

final_consolidated_portfolio.write_excel(output_path,column_totals=True, autofit=True)
print("Finished Writing to excel. Creating holdings...")
# os.startfile("pivot_table.xlsx")
# underlying_isin_list = final_consolidated_portfolio.select("UNDERLYING_ISIN").unique().filter(pl.col("UNDERLYING_ISIN") != "EUR")["UNDERLYING_ISIN"].to_list()

# with open("current_holding_isins.csv", "w", newline="") as csvfile:
#     writer = csv.writer(csvfile)
#     for item in underlying_isin_list:
#         writer.writerow([item])  # each item becomes one row