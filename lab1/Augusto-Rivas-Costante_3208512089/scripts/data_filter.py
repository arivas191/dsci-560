import os
import csv
from bs4 import BeautifulSoup

# Paths
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.abspath(os.path.join(script_dir, "..", "data"))
raw_dir = os.path.join(data_dir, "raw_data")
processed_dir = os.path.join(data_dir, "processed_data")

raw_file_path = os.path.join(raw_dir, "web_data.html")
market_csv = os.path.join(processed_dir, "market_data.csv")
news_csv = os.path.join(processed_dir, "news_data.csv")

print("Reading web_data.html...")
with open(raw_file_path, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

print("Filtering fields...")

# Market data
market_data = []
banner = soup.find(id="HomePageInternational-MarketsBanner-1")
if banner:
    for market_link in banner.find_all("a"):
        symbol = market_link.select_one(".MarketCard-symbol")
        position = market_link.select_one(".MarketCard-stockPosition")
        change_pct = market_link.select_one(".MarketCard-changesPct")
        if symbol and position and change_pct:
            # strip any quotes or extra characters
            market_data.append([
                symbol.get_text(strip=True),
                position.get_text(strip=True).replace('"', ''),
                change_pct.get_text(strip=True),
            ])

# News data
news_data = []
news_section = soup.find(class_="LatestNews-isHomePage")
if news_section:
    for item in news_section.find_all("li"):
        headline_el = item.select_one(".LatestNews-headline")
        timestamp = item.select_one(".LatestNews-timestamp")
        if headline_el and timestamp:
            news_data.append([
                timestamp.get_text(strip=True),
                headline_el.get_text(strip=True),
                headline_el.get("href")
            ])

# Save Market data
print("Storing market data...")
with open(market_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["Symbol", "StockPosition", "ChangePct"])
    writer.writerows(market_data)
print("CSV created:", market_csv)

# Save News data
print("Storing news data...")
with open(news_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["Timestamp", "Headline", "Link"])
    writer.writerows(news_data)
print("CSV created:", news_csv)
