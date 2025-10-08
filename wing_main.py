"""
xlwings Lite allows you to define
automation scripts and custom functions
with Python instead of VBA/Office Scripts.

Select a function in the green dropdown
above and run it by clicking the button or
or by pressing F5. One of the sample
scripts will insert custom functions,
which are defined using the @func
decorator. You can add, delete, or edit
functions in this file."""

import datetime as dt
import numpy as np
import pandas as pd
import seaborn as sns
import xlwings as xw
from xlwings import func, script, ret
import requests
import re
from dataclasses import dataclass, asdict

cors_proxy = "http://localhost:8080/"
cors_proxy_stockwits = "http://localhost:8081/"


@dataclass
class HttpRequestsCounter:
    """Class for keeping track of an item in inventory."""
    onvista: int = 0
    tradingview: int = 0
    finanzen_net: int = 0
    yahoo_finance:int = 0 
http_requests_counter = HttpRequestsCounter()

@script
def hello_world(book: xw.Book):
    # Scripts require the @script decorator and the type-hinted
    # book argument (book: xw.Book)
    cell = book.sheets.active["A1"]
    cell.value = "Hello World!"
    cell.color = "#FFFF00"  # yellow


@script
def seaborn_sample(book: xw.Book):
    # Create a pandas DataFrame from a CSV on GitHub and print its info
    df = pd.read_csv(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"
    )
    print(df.info())

    # Add a new sheet, write the DataFrame out, and format it as Table
    sheet = book.sheets.add()

    # Add a styled title
    title_cell = sheet["A1"]
    title_cell.value = "The Penguin Dataset"
    title_cell.font.bold = True
    title_cell.font.size = 22
    title_cell.font.color = "#156082"
    title_cell.font.name = "Comic Sans MS"

    # Write out the DataFrame
    sheet["A3"].options(index=False).value = df
    sheet.tables.add(sheet["A3"].resize(len(df) + 1, len(df.columns)))

    # Add a Seaborn plot as picture
    plot = sns.jointplot(
        data=df, x="flipper_length_mm", y="bill_length_mm", hue="species"
    )
    sheet.pictures.add(plot.fig, anchor=sheet["B10"])

    # Activate the new sheet
    sheet.activate()


@script
def insert_custom_functions(book: xw.Book):
    # This script inserts the custom functions below
    # so you can try them out easily
    sheet = book.sheets.add()
    sheet["A1"].value = "This sheet shows the usage of custom functions"
    sheet["A3"].value = '=HELLO("xlwings")'
    sheet["A5"].value = "=STANDARD_NORMAL(3, 4)"
    sheet["A10"].value = "=CORREL2(A5#)"
    sheet.activate()


@func
def hello(name: str):
    # This is the easiest custom function
    return f"Hello {name}!"


@func
def standard_normal(rows, cols):
    # Returns an array of standard normally distributed pseudo random numbers
    rng = np.random.default_rng()
    matrix = rng.standard_normal(size=(rows, cols))
    date_rng = pd.date_range(start=dt.datetime(2025, 6, 15), periods=rows, freq="D")
    df = pd.DataFrame(
        matrix, columns=[f"col{i + 1}" for i in range(matrix.shape[1])], index=date_rng
    )
    return df


@func
def correl2(df: pd.DataFrame):
    # Like CORREL, but it works on whole matrices instead of just 2 arrays.
    # The type hint converts the values of the range into a pandas DataFrame.
    # Use this function on the output of the standard_normal function from above.
    return df.corr()





@func
def get_isin_from_name(keyword: str = "") -> str:
    if not keyword:
        return ""

    url = (
        "https://www.finanzen.net/suggest/finde/jsonv2"
        "?max_results=25&Keywords_mode=APPROX"
        f"&Keywords={requests.utils.quote(keyword)}"
        f"&query={requests.utils.quote(keyword)}"
        "&bias=100"
    )

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(cors_proxy + url, headers=headers)
    global http_requests_counter
    http_requests_counter.finanzen_net+=1
    
    response.raise_for_status()

    json_data = response.json()

    # Find the "Aktien" section
    aktien_section = next(
        (
            section
            for section in json_data.get("it", [])
            if section.get("n") == "Aktien"
        ),
        None,
    )

    if not aktien_section or not aktien_section.get("il"):
        return ""

    first_hit = aktien_section["il"][0]
    return first_hit.get("isin", "")


@func
def get_tradingview_ticker_by_keyword(isin: str) -> str:
    url = (
        "https://symbol-search.tradingview.com/symbol_search/v3/"
        f"?text={isin}&hl=1&exchange=&lang=en&search_type=undefined"
        "&domain=production&sort_by_country=US&promo=true"
    )

    headers = {
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "User-Agent": "Mozilla/5.0",
    }
    global http_requests_counter
    response = requests.get(cors_proxy + url, headers=headers)
    http_requests_counter.tradingview+=1
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.text}")

    data = response.json()

    if "symbols" not in data or len(data["symbols"]) == 0:
        raise Exception(f"No symbols found for ISIN: {isin}")

    # Try to find primary listing
    primary = next((s for s in data["symbols"] if s.get("is_primary_listing")), None)
    symbol_data = primary or data["symbols"][0]

    source_id = symbol_data["source_id"].upper()
    symbol = symbol_data["symbol"].upper()
    symbol_clean = re.sub(r"<.*?>", "", symbol).upper()

    return f"{source_id}:{symbol_clean}"

@func
def get_yahoo_ticker_currency(ticker:str) -> str:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1m", "range": "1d"}
    global http_requests_counter
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    resp = requests.get(cors_proxy + url, params=params, headers=headers)
    http_requests_counter.yahoo_finance+=1
    
    data = resp.json()["chart"]["result"][0]["meta"]["currency"]

    return data

@func
def get_yahoo_ticker(isin: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        )
    }

    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={requests.utils.quote(isin)}"

    response = requests.get(cors_proxy + url, headers=headers)
    global http_requests_counter
    http_requests_counter.yahoo_finance+=1
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.text}")
    
    response_dict = response.json()

    if "quotes" in response_dict and len(response_dict["quotes"]) > 0:
        return str(response_dict["quotes"][0]["symbol"])
    else:
        return ""


@func
def get_yahoo_close(
    stock: str, interval: str = "1h", range_: str = "1mo"
) -> pd.DataFrame:
    """
    Fetch Yahoo Finance close prices.

    :param stock: Ticker symbol
    :param interval: Data interval (e.g. '1m', '5m', '1h', '1d')
    :param range_: Range of data (e.g. '1d', '5d', '1mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
    :return: Pandas Series indexed by timestamp with close prices
    """
    global http_requests_counter
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock}"
    params = {"interval": interval, "range": range_}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    resp = requests.get(cors_proxy + url, params=params, headers=headers)
    http_requests_counter.yahoo_finance+=1
    data = resp.json()["chart"]["result"][0]

    timestamps = data["timestamp"]
    closes = data["indicators"]["quote"][0]["close"]

    df = pd.DataFrame({"timestamp": timestamps, "close": closes})
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(
        "Europe/Amsterdam"
    )

    return df

@func
def get_yahoo_previous_day_changepct(ticker: str):
    """
    Fetch Yahoo Finance close prices.

    :param stock: Ticker symbol
    :param interval: Data interval (e.g. '1m', '5m', '1h', '1d')
    :param range_: Range of data (e.g. '1d', '5d', '1mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
    :return: Pandas Series indexed by timestamp with close prices
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1m", "range": "1d"}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    
    resp = requests.get(cors_proxy + url, params=params, headers=headers)    
    global http_requests_counter
    http_requests_counter.yahoo_finance+=1
    previous_close = resp.json()["chart"]["result"][0]["meta"]["previousClose"]
    current_close = get_yahoo_close(ticker, "1m", "1d").ffill().tail(1)["close"].iloc[0]

    return round((current_close - previous_close)/previous_close*100,2)

@func
def get_yahoo_latest_price(ticker: str):
    """
    Fetch Yahoo Finance close prices.

    :param stock: Ticker symbol
    :param interval: Data interval (e.g. '1m', '5m', '1h', '1d')
    :param range_: Range of data (e.g. '1d', '5d', '1mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
    :return: Pandas Series indexed by timestamp with close prices
    """

    return round(get_yahoo_close(ticker, "1m", "1d").ffill().tail(1)["close"].iloc[0], 2)


@func
def calculate_ema_table(ticker: str):
    intervals = {
        "30M": {"period": "1mo", "interval": "30m"},
        "1H": {"period": "3mo", "interval": "1h"},
        "4H": {"period": "1y", "interval": "4h"},
        "D": {"period": "2y", "interval": "1d"},
    }

    result = {}

    for name, params in intervals.items():
        df = get_yahoo_close(
            ticker, interval=params["interval"], range_=params["period"]
        )

        if df.empty or "close" not in df.columns:
            raise ValueError(f"No close data for interval {name}")

        close_series = df["close"]
        

        result[name] = {
            "EMA20": close_series.ewm(span=20, adjust=False).mean().iloc[-1],
            "EMA50": close_series.ewm(span=50, adjust=False).mean().iloc[-1],
            "EMA200": close_series.ewm(span=200, adjust=False).mean().iloc[-1],
        }
        
    return pd.DataFrame(result).T


from io import StringIO


def lookup_wkn_isin(wkn):
    API_URL = "https://api.onvista.de/api/v1/main/search?limit=5&searchValue="
    global http_requests_counter
    try:
        resp = requests.get(API_URL + str(wkn))
        http_requests_counter.onvista+=1
        resp.raise_for_status()
        data = resp.json()
        isin = data["instrumentList"]["list"][0]["isin"]
    except Exception:
        isin = None

    return isin

@func
@ret(index=False)
def import_html(url: str):
    global http_requests_counter
    try:
        # Download page
        # print(url)
        resp = requests.get(cors_proxy + url)
        http_requests_counter.finanzen_net+=1
        resp.raise_for_status()

        # Parse HTML tables
        resp_str = StringIO(resp.text)
        # tables_with_link = pd.read_html(resp_str, extract_links="all", thousands=".", decimal=".")
 
        tables = pd.read_html(resp_str, thousands=".", decimal=",")
        if not tables:
            return [["No table found"]]
        returned_table = tables[0].drop(0).iloc[: , 1:].drop(columns=["Unnamed: 4"])
        # returned_table["ISIN"] = "https://mein.finanzen-zero.net/instrument/DE000" + returned_table["WKN"]
     
        return returned_table

    except Exception as e:
        return [[f"Error: {str(e)}"]]


@func
def get_finanzen_id_by_isin(isin):
    url = "https://www.finanzen.net/ajax/UnderlyingsByInput"

    payload = {
        "input": isin
    }

    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.finanzen.net",
        "Referer": "https://www.finanzen.net/knockouts/suche",
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(cors_proxy+ url, data=payload, headers=headers)
    global http_requests_counter
    http_requests_counter.finanzen_net+=1
    response.raise_for_status()

    html = response.text

    # Find all value="NUMBER"
    matches = re.findall(r'value="(\d+)"', html)
    if matches:
        # print(f"Underlying IDs: {', '.join(matches)}")
        return matches

    return []

def ema_trend(row):
    ema20 = row["EMA20"]
    ema50 = row["EMA50"]
    ema200 = row["EMA200"]

    # Condition thresholds
    if abs(ema20 - ema50) / ema50 < 0.002:
        return "Potential 20/50 Crossover"
    if abs(ema50 - ema200) / ema200 < 0.002:
        return "Potential 50/200 Crossover"
    if abs(ema20 - ema200) / ema200 < 0.002:
        return "Potential 20/200 Crossover"

    if ema20 > ema50 > ema200:
        return "EMA20 > EMA50 > EMA200 → strong uptrend"
    if ema20 > ema200 > ema50:
        return "EMA20 > EMA200 > EMA50 → moderate uptrend"
    if ema20 < ema50 and ema20 > ema200:
        return "EMA20 < EMA50 AND EMA20 > EMA200 → moderate uptrend, short-term weakness"
    if ema20 < ema200 and ema20 > ema50:
        return "EMA20 < EMA200 AND EMA20 > EMA50 → moderate downtrend, short-term strength"
    if ema20 < ema200 and ema200 < ema50:
        return "EMA20 < EMA200 < EMA50 → moderate downtrend, short-term retracement"
    if ema20 < ema50 < ema200:
        return "EMA20 < EMA50 < EMA200 → strong downtrend"

    return "No clear signal"
@func
@ret(index=False, header=False)
def trend_summary(ticker:str):
    ema_df = calculate_ema_table(ticker)
    ema_df["Signal"] = ema_df.apply(ema_trend, axis=1)    
    signals_transposed = ema_df["Signal"].to_frame().T    
    return signals_transposed

@func
def get_isin_from_wkn(wkn: str) -> str:
    global http_requests_counter

    url = f"https://api.onvista.de/api/v1/main/search?limit=5&searchValue={wkn}"
    resp = requests.get(url)
    http_requests_counter.onvista+=1
    resp.raise_for_status()  # raise error if bad request
    
    data = resp.json()

    isin = data["instrumentList"]["list"][0]["isin"]
    return isin
    
@func
def get_turbo_details_link_from_wkn(wkn: str) -> str:
    global http_requests_counter
    url = f"https://api.onvista.de/api/v1/main/search?limit=5&searchValue={wkn}"
    resp = requests.get(url)
    http_requests_counter.onvista+=1
    resp.raise_for_status()  # raise error if bad request
    
    data = resp.json()

    isin = data["instrumentList"]["list"][0]["isin"]
    return isin

@func
def get_current_http_request_counter():
    global http_requests_counter
    return asdict(http_requests_counter)

@func
def get_stocktwits_url_from_yahoo_ticker(yahoo_ticker: str) -> str:
    """
    Search Stocktwits and return the deeplink URL of the first 'Stocks & ETFs' result.

    Example:
        >>> get_stocktwits_url_from_yahoo_ticker("evolution gaming")
        'https://stocktwits.com/symbol/EVO.ST'
    """
    yahoo_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
    params = {"interval": "1m", "range": "1d"}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        resp = requests.get(cors_proxy + yahoo_url, params=params, headers=headers)    
        global http_requests_counter
        http_requests_counter.yahoo_finance+=1
        long_name:str = resp.json()["chart"]["result"][0]["meta"]["longName"]

        stockwits_query_url = "https://api.stocktwits.com/api/2/search/v2/grouped_search.json"
        params = {
            "q": long_name,
            "regions": "ALL"
        }
        headers = {"User-Agent": "Mozilla/5.0"}

        resp = requests.get(cors_proxy_stockwits + stockwits_query_url, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        # Find "Stocks & ETFs" section
        stocks_section = next(
            (section for section in data.get("results", [])
            if section.get("title") == "Stocks & ETFs"),
            None
        )

        if not stocks_section or not stocks_section.get("items"):
            return ""

        first_hit = stocks_section["items"][0]
        deeplink = first_hit.get("deeplink")

        if not deeplink:
            return ""

        return f"https://stocktwits.com/{deeplink}"
    except Exception as e:
        print(e)
        raise e