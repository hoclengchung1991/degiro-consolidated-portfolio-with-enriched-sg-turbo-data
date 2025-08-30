import json
import requests
import os
import csv

def lookup_underlying_isin_from_sg_for_sg_turbos(isins: list[str]):
    path = r"C:\Users\Admin\Documents\degiro\pw\repos\sg_turbo_isin_to_underlying_isin_underlying.csv"
    with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)            
            isin_to_onderliggende_waarde_isin = {row[0]: row[1] for row in reader}

    # if os.path.exists(path):
    #     with open(path, newline="", encoding="utf-8") as f:
    #         reader = csv.reader(f)            
    #         cached_isin_to_onderliggende_waarde_isin = {row[0]: row[1] for row in reader}
        
    # else:
    #     print("CSV does not exist yet, returning empty DataFrame")
    #     cached_isin_to_onderliggende_waarde_isin = {}
    

    # isin_to_onderliggende_waarde_isin:dict[str,str]
    # lookup_turbo_isins: set[str] = set(isins)
    # cached_turbo_isins: set[str] = set(cached_isin_to_onderliggende_waarde_isin.keys())
    # actual_turbo_lookup_isins: set[str] = lookup_turbo_isins - cached_turbo_isins
    # if actual_turbo_lookup_isins:
    #     sg_isin_lookup_code = {
    #             i: json.loads(
    #                 requests.get(
    #                     f"https://sgbeurs.nl/quicksearch/quicksearch?term={i}"
    #                 ).content.decode()
    #             )["products"][0]["code"]
    #             for i in actual_turbo_lookup_isins
    #         }
            

    #     sg_pid_lookup = {
    #         v: json.loads(
    #             requests.get(
    #                 f"https://sgbeurs.nl/EmcWebApi/api/Products?code={v}"
    #             ).content.decode()
    #         )["Id"]
    #         for _, v in sg_isin_lookup_code.items()
    #     }
    #     import time

    #     isin_to_underlying = {}
    #     isin_to_underlying_isin = {}
    #     for k, v in sg_pid_lookup.items():
    #         response = requests.get(
    #             f"https://sgbeurs.nl/EmcWebApi/api/Products/AllProperties/{v}"
    #         )
    #         content = response.content.decode()
    #         while "exceeded" in content:
    #             time.sleep(1)
    #             response = requests.get(
    #                 f"https://sgbeurs.nl/EmcWebApi/api/Products/AllProperties/{v}"
    #             )
    #             content = response.content.decode()
    #         underlying = [
    #             element
    #             for element in json.loads(content)
    #             if element["Label"] == "Onderliggende waarde"
    #         ][0]["Value"]
    #         underlying_isin = [
    #             element
    #             for element in json.loads(content)
    #             if element["Label"] == "ISIN code onderliggende waarde"
    #         ][0]["Value"]
    #         isin_to_underlying_isin[k] = underlying_isin
    #         isin_to_underlying[k] = underlying

    #     isin_to_onderliggende_waarde_isin = {
    #         k: isin_to_underlying_isin[v] for k, v in sg_isin_lookup_code.items()
    #     } | cached_isin_to_onderliggende_waarde_isin
    #     with open(path, "w", newline="") as f:
    #         writer = csv.writer(f)
    #         for k, v in isin_to_onderliggende_waarde_isin.items():
    #             writer.writerow([k, v])
        
    # else:
    #     isin_to_onderliggende_waarde_isin = cached_isin_to_onderliggende_waarde_isin
    # Write local for caching. 
    return isin_to_onderliggende_waarde_isin