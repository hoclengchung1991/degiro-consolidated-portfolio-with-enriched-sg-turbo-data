from typing import Any
from degiro_connector.trading.api import API
import polars as pl
from repos.company_info import Degiro
from repos.degiro_nl import DegiroRepoNL
from repos.degiro_de import DegiroRepoDE
from repos.etoro import EtoroRepo
from repos.sg_uisins import lookup_underlying_isin_from_sg_for_sg_turbos
from repos.zero import ZeroRepo


degiro_repo_de = DegiroRepoDE()
degiro_portfolio_de: pl.DataFrame = degiro_repo_de.consolidated_degiro_initial_df

degiro_repo_nl = DegiroRepoNL()
degiro_portfolio_nl: pl.DataFrame = degiro_repo_nl.consolidated_degiro_initial_df

etoro_repo = EtoroRepo()
etoro_portfolio_ex_cash: pl.DataFrame = etoro_repo.etoro_portfolio_ex_cash
isin_to_underlying_isin_etoro: dict[str, str] = etoro_repo.isin_to_underlying_isin_etoro

zero_repo = ZeroRepo()
zero_portfolio: pl.DataFrame = zero_repo.zero_portfolio
isin_to_underlying_isin_zero: dict[str, str] = zero_repo.isin_to_underlying_isin_zero



unioned: pl.DataFrame = pl.concat(
    [degiro_portfolio_de, degiro_portfolio_nl, etoro_portfolio_ex_cash, zero_portfolio]
)

degiro_unioned: pl.DataFrame = pl.concat(
    [degiro_portfolio_de, degiro_portfolio_nl]
)


# Lookup underlying isin Degiro
sg_tb_isins: list[str] = [
    i[0]
    for i in degiro_unioned.filter(pl.col("SECURITY_TYPE") != "STOCK")
    .select("ISIN")
    .unique()
    .to_numpy()
]
stock_isins: list[str] = [
    i[0]
    for i in degiro_unioned.filter(pl.col("SECURITY_TYPE") == "STOCK")
    .select("ISIN")
    .unique()
    .to_numpy()
]
stock_isin_underlying_isin: dict[str, str] = {isin: isin for isin in stock_isins}
looked_up_sg_tb_underlying_isins: dict[str, str] = (
    lookup_underlying_isin_from_sg_for_sg_turbos(sg_tb_isins)
)
isin_to_underlying_isin: dict[str, str] = (
    stock_isin_underlying_isin
    | looked_up_sg_tb_underlying_isins
    | isin_to_underlying_isin_zero
    | isin_to_underlying_isin_etoro
)

with_underlying_isin: pl.DataFrame = unioned.with_columns(
    UNDERLYING_ISIN=pl.col("ISIN").replace(isin_to_underlying_isin)
)

# lookup underlying name, sector,
# Enrich by company info
isin_to_company_sector = {}
isin_to_company_industry = {}
isin_to_name = {}
degiro: API = Degiro().degiro_connector

for isin in with_underlying_isin["UNDERLYING_ISIN"].to_list():
    company_profile = degiro.get_company_profile(product_isin=isin)
    if isin is None:
        raise Exception("Most likely forgot to add underlying to the notes of finanzen Zero")
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

cash_consolidated: pl.DataFrame = pl.concat([degiro_de_cash_eur_df, degiro_nl_cash_eur_df, etoro_cash_eur_df, zero_cash_eur_df])

final_consolidated_portfolio: pl.DataFrame = pl.concat([with_underlying_info, cash_consolidated])

final_consolidated_portfolio.write_excel(column_totals=True, autofit=True)
