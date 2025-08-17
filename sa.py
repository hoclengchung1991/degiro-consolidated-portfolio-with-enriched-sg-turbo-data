from temp_mail_so import TempMailSo
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
import random,re,time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup
# importing module
import logging

# Create and configure logger
logging.basicConfig(
                    format='%(asctime)s %(message)s',
                    )

# Creating an object
logger = logging.getLogger()



num: int = random.randint(1_000_001, 10_000_000)
pattern = r'https://seekingalpha\.com/auth/registrations/validate/[^\s"\']*&open_reset_password=true\b'
seeking_alpha_url = input("Enter SA article URL: ")
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
domains = client.list_domains()
first_listed_domain = domains["data"][0]["domain"]
# Create a temporary inbox
temp_email_prefix = f"test{num}"
generated_email = f"{temp_email_prefix}@{first_listed_domain}"
inbox = client.create_inbox(
    address=temp_email_prefix,  # Email prefix
    domain=first_listed_domain,  # Domain
    lifespan=300  # Inbox lifespan in seconds (0 for permanent)
)
logger.setLevel(logging.INFO)
#list available inboxes
inboxes = client.list_inboxes()
for inbox in inboxes['data']:
    if temp_email_prefix in inbox["name"]:
        inbox_id = inbox["id"]
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()
            page.goto(seeking_alpha_url)
            page.get_by_placeholder("Enter email address").fill(generated_email)
            page.get_by_text("Create free account",exact=True).last.click()
            emails = client.list_emails(inbox_id=inbox_id)
            while not emails["data"]:
                emails = client.list_emails(inbox_id=inbox_id)
                logger.info(f"Listing emails...")
                if emails["data"]:
                    email_id = emails["data"][0]["id"]
                    email = client.get_email(inbox_id=inbox_id,email_id=email_id)
                    match = re.search(pattern, email["data"]["htmlContent"])
                    if match:
                        url = match.group(0)                        
                        logger.info(f"received mail uri: {url}")
                        page.goto(url)                        
                        page.get_by_text("Cancel", exact=True).click()
                        page.get_by_text("Accept All Cookies",exact=True).click()
                        try:
                            while True:
                                time.sleep(2)
                        except KeyboardInterrupt:
                                browser.close()
                        
                    
                else:
                    
                    time.sleep(1)