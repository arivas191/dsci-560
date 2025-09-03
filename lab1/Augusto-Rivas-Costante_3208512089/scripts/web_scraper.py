import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

url = "https://www.cnbc.com/world/?region=world"

# Data folder outside the scripts folder
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.abspath(os.path.join(script_dir, "..", "data"))
raw_dir = os.path.join(data_dir, "raw_data")
processed_dir = os.path.join(data_dir, "processed_data")
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)

# Configure Chromium for proper headless execution
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.binary_location = "/usr/bin/chromium-browser"

driver = webdriver.Chrome(
    service=Service("/usr/bin/chromedriver"),
    options=options
)

driver.get(url)

# Wait until Market banner data has loaded
WebDriverWait(driver, 20).until(
    EC.presence_of_element_located((By.CLASS_NAME, "MarketCard-row"))
)

html = driver.page_source
driver.quit()

# Parse HTML
soup = BeautifulSoup(html, "html.parser")

market_banner = soup.find(id="HomePageInternational-MarketsBanner-1")
latest_news = soup.find(class_="LatestNews-isHomePage")

# Save extracted sections
raw_file_path = os.path.join(raw_dir, "web_data.html")
with open(raw_file_path, "w", encoding="utf-8") as f:
    if market_banner:
        f.write(market_banner.prettify() + "\n")
    if latest_news:
        f.write(latest_news.prettify() + "\n")
