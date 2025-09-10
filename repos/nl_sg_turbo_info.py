import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_single_sg_turbo_data(turbo_isin):
    turbo_isin_to_underlying_isin = {}
    turbo_isin_to_leverage = {}

    try:
        # Step 1: Get code
        url = f"https://sgbeurs.nl/QuickSearch/QuickSearch?term={turbo_isin}"
        response = requests.get(url)
        data = response.json()
        code = data['products'][0]['code']

        # Step 2: Get product Id
        product_url = f"https://sgbeurs.nl/EmcWebApi/api/Products?code={code}"
        product_response = requests.get(product_url)
        product_data = product_response.json()
        product_id = product_data['Id']

        # Step 3: Get all properties with retry on 429
        while True:
            properties_url = f"https://sgbeurs.nl/EmcWebApi/api/Products/AllProperties/{product_id}"
            properties_response = requests.get(properties_url)
            if properties_response.status_code == 429:
                print(f"429 Too Many Requests for {turbo_isin}, retrying in 2 sec...")
                time.sleep(2)
                continue
            properties_data = properties_response.json()
            break

        # Step 4: Extract Gearing and Underlying ISIN
        gearing_value = None
        underlying_isin = None
        for prop in properties_data:
            if prop.get("Name") == "Gearing":
                gearing_value = prop.get("Value")
            if prop.get("Name") == "AssetIsin":
                underlying_isin = prop.get("Value")

        turbo_isin_to_leverage[turbo_isin] = gearing_value
        turbo_isin_to_underlying_isin[turbo_isin] = underlying_isin

    except Exception as e:
        print(f"Error processing {turbo_isin}: {e}")

    return turbo_isin_to_underlying_isin, turbo_isin_to_leverage

def fetch_sg_turbo_data_parallel(isin_list, max_workers=5):
    turbo_isin_to_underlying_isin = {}
    turbo_isin_to_leverage = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single_sg_turbo_data, isin): isin for isin in isin_list}

        for future in as_completed(futures):
            result_underlying, result_leverage = future.result()
            turbo_isin_to_underlying_isin.update(result_underlying)
            turbo_isin_to_leverage.update(result_leverage)

    return turbo_isin_to_underlying_isin, turbo_isin_to_leverage