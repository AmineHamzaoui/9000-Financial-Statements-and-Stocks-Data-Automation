import os
import time
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = uc.ChromeOptions()
# Avoid headless for bypassing Cloudflare
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")

# Load tickers


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


tickers = get_ticker_list()
if not tickers:
    exit()

driver = uc.Chrome(options=options)

for ticker in tickers:
    print(f"Processing ticker: {ticker}")
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/stock-price-history"
    driver.get(url)

    try:
        # Wait for table (or fail if blocked by Cloudflare)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "historical_data_table"))
        )
        print(f"[✓] Page loaded for {ticker}")
    except TimeoutException:
        print(f"[⚠️] Timeout or Cloudflare challenge detected for {ticker}")

    # Save the full HTML either way
    html_source = driver.page_source
    output_path = os.path.join(os.getcwd(), f"{ticker}_macrotrends.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_source)
    print(f"[💾] Saved HTML to {output_path}")

driver.quit()
