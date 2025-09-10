import re
import time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

def get_tickers_from_isins(isin_list):
    tickers = {}
    
    start_time = time.time()  # start timer
    
    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True, args=["--start-maximized"])
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        page.goto("https://www.google.com/finance/")
        page.get_by_label("Accept all", exact=True).first.click()
        
        for isin in isin_list:
            try:
                search_input = page.get_by_role("combobox", name="Search for stocks, ETFs & more")
                search_input.fill(isin)
                # Wait for dropdown and select first option
                page.locator("div[role='option']").first.click()
                
                pattern = r"quote\/(\w*):(\w*)"
                match = re.search(pattern, page.url)                
                if match:
                    tickers[isin] = f"{match.group(2)}:{match.group(1)}"
                else:
                    tickers[isin] = None
            except Exception as e:
                tickers[isin] = None
                print(f"Error processing {isin}: {e}")
        
        browser.close()
    
    end_time = time.time()  # end timer
    print(f"Processed {len(isin_list)} ISINs in {end_time - start_time:.2f} seconds")
    
    return tickers

# Example usage
# isins = ["US37637K1088", "US03027X1000", "LU1673108939", "US91324P1021", "US6792951054", 
#          "VGG320891077", "CH0009002962", "NL0010273215", "SE0012673267", "US6541061031", 
#          "DK0062498333", "US7134481081", "US29414B1044", "US21036P1084", "FR0000121014", 
#          "NL0012969182"]

# result = get_tickers_from_isins(isins)
# print(result)
