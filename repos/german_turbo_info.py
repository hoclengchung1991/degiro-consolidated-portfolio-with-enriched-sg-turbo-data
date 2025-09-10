import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_turbo_data_parallel(turbo_isins, max_workers=10):
    """
    Fetch underlying ISINs and mid leverage for a list of turbo ISINs in parallel.

    Args:
        turbo_isins (list): List of turbo ISINs.
        max_workers (int): Number of threads for parallel execution.

    Returns:
        tuple: (turbo_isin_to_underlying_isin, turbo_isin_to_leverage_mid)
    """
    
    turbo_isin_to_underlying_isin = {}
    turbo_isin_to_leverage_mid = {}

    def fetch_turbo_data(turbo_isin):
        try:
            # Step 1: Get entityValue
            url = f"https://api.onvista.de/api/v1/main/search?limit=1&searchValue={turbo_isin}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            entity_value = data['instrumentList']['list'][0]['entityValue']

            # Step 2: Get derivative snapshot
            url = f"https://api.onvista.de/api/v1/derivatives/{entity_value}/snapshot"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Step 3: Extract underlying ISIN
            isin_underlying = data.get("finderUnderlying", {}).get("isin")

            # Step 4: Extract gearing and compute mid leverage
            gearing_ask = data.get("derivativesFigure", {}).get("gearingAsk")
            gearing_bid = data.get("derivativesFigure", {}).get("gearingBid")
            leverage_mid = None
            if gearing_ask is not None and gearing_bid is not None:
                leverage_mid = (gearing_bid + gearing_ask) / 2

            return turbo_isin, isin_underlying, leverage_mid

        except Exception as e:
            print(f"Error fetching {turbo_isin}: {e}")
            return turbo_isin, None, None

    # Parallel execution
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_turbo_data, isin) for isin in turbo_isins]
        for future in as_completed(futures):
            turbo_isin, isin_underlying, leverage_mid = future.result()
            turbo_isin_to_underlying_isin[turbo_isin] = isin_underlying
            turbo_isin_to_leverage_mid[turbo_isin] = leverage_mid

    return turbo_isin_to_underlying_isin, turbo_isin_to_leverage_mid