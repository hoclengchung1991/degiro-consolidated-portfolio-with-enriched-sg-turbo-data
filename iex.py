from temp_mail_so import TempMailSo
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
import random,re,time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
import sys
import logging

# Create and configure logger
logging.basicConfig(
                    format='%(asctime)s %(message)s',
                    )

# Creating an object
logger = logging.getLogger()



num: int = random.randint(1_000_001, 10_000_000)
pattern = r'https://seekingalpha\.com/auth/registrations/validate/[^\s"\']*&open_reset_password=true\b'

iex_register_url = "https://www.iex.nl/account/signup.aspx"
# seeking_alpha_url = sys.argv[1]

class Settings(BaseSettings):
    TEMP_MAIL_TOKEN:str
    RAPID_API_KEY:str
    
    model_config = SettingsConfigDict(env_file=".env2")
settings = Settings() # type: ignore
# Initialize client
client = TempMailSo(
    rapid_api_key=settings.RAPID_API_KEY,
    auth_token=settings.TEMP_MAIL_TOKEN
)

# Get current domains
# domains = client.list_domains()
# first_listed_domain = domains["data"][1]["domain"]
# Create a temporary inbox
# temp_email_prefix = f"test{num}"

# inbox = client.create_inbox(
#     address=temp_email_prefix,  # Email prefix
#     domain=first_listed_domain,  # Domain
#     lifespan=300  # Inbox lifespan in seconds (0 for permanent)
# )
logger.setLevel(logging.INFO)
#list available inboxes



# Gen email
import requests

url = "https://free-gmail-api.p.rapidapi.com/generate-email"

payload = { "email": ["Gmail"] }
headers = {
	"x-rapidapi-key": "ea50ebb4demsh5ef98aa0d87e9b2p1d27f0jsnff6e4c0d267b",
	"x-rapidapi-host": "free-gmail-api.p.rapidapi.com",
	"Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)
generated_email = response.json()["email"]


with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=False,args=["--start-maximized ","--disable-blink-features=AutomationControlled", "--no-sandbox"])
    context = browser.new_context(no_viewport=True)
    page = context.new_page()
    # page.goto("https://app.adguard-mail.com/#/aliases")
    # page.get_by_placeholder("example@mail.com", exact=True).fill("hoclengchung@gmail.com")
    # page.get_by_text("Continue",exact=True).last.click()
    
    # page.get_by_placeholder("********", exact=True).fill("adguard1991!")
    # page.get_by_text("Continue",exact=True).last.click()
    # page.goto("https://app.adguard-mail.com/#/aliases")
    
    
    

    page.goto(iex_register_url)
    page.get_by_text("Akkoord",exact=True).last.click()
    page.check("#ctl00_ctl00_content_contentMain_ctl00_rblGeslachtMan")
    page.get_by_placeholder("Voorbeeld", exact=True).fill("Voornaam")
    page.get_by_placeholder("van der Voorbeeld", exact=True).fill("Achternaam")
    page.get_by_placeholder("voorbeeld@domein.com", exact=True).fill(generated_email)
    page.get_by_text("Word lid",exact=True).last.click()
    message_id = ""
    while not message_id:
        url_get_messages = "https://free-gmail-api.p.rapidapi.com/message-list"

        payload_get_msg = { "email": response.json()["email"] }
        headers_get_msg = {
            "x-rapidapi-key": "ea50ebb4demsh5ef98aa0d87e9b2p1d27f0jsnff6e4c0d267b",
            "x-rapidapi-host": "free-gmail-api.p.rapidapi.com",
            "Content-Type": "application/json"
        }

        response2 = requests.post(url_get_messages, json=payload_get_msg, headers=headers_get_msg)
    
        for m in response2.json()["messages"]:
            if m["from"] =="klantenservice@iex.nl":
                message_id = m["messageID"]
        print("listening to emails...")
        time.sleep(1)


    url_details = "https://free-gmail-api.p.rapidapi.com/message-details"

    payload_details = {
        "email": response.json()["email"],
        "message_id": message_id
    }
    headers_details = {
        "x-rapidapi-key": "ea50ebb4demsh5ef98aa0d87e9b2p1d27f0jsnff6e4c0d267b",
        "x-rapidapi-host": "free-gmail-api.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response3 = requests.post(url_details, json=payload_details, headers=headers_details)
    
    refined_content = response3.json()["refined_content"]

    import re

    text = """
    Some text with a link: https://www.iex.nl/account/28S839BT9T/confirm.aspx
    and maybe other stuff
    """

    pattern = r"https://www\.iex\.nl/account/([^/]+)/confirm\.aspx"
    match = re.search(pattern, refined_content)

    if match:
        url = match.group(0)        # full URL
        token = match.group(1)      # dynamic part (e.g., 28S839BT9T)
        print("URL:", url)
        print("Token:", token)
        page.goto(url)
    else:
        print("No match found")
    while True:
        pass
    
    # emails = client.list_emails(inbox_id=inbox_id)
    # while not emails["data"]:
    #     emails = client.list_emails(inbox_id=inbox_id)
    #     logger.info(f"Listing emails...")
    #     if emails["data"]:
    #         email_id = emails["data"][0]["id"]
    #         email = client.get_email(inbox_id=inbox_id,email_id=email_id)
    #         match = re.search(pattern, email["data"]["htmlContent"])
    #         if match:
    #             url = match.group(0)                        
    #             logger.info(f"received mail uri: {url}")
    #             page.goto(url)                        
    #             page.get_by_text("Cancel", exact=True).click()
    #             page.get_by_text("Accept All Cookies",exact=True).click()
    #             try:
    #                 while True:
    #                     time.sleep(60)
    #             except KeyboardInterrupt:
    #                     browser.close()
                
            
    #     else:
            
    #         time.sleep(1)
