import os
import time
import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")
options.add_argument('--headless=new')

# === CREATE OUTPUT FOLDER ===
output_dir = "market_cap"
os.makedirs(output_dir, exist_ok=True)

# === FUNCTION TO LOAD TICKERS ===


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


# === LOAD TICKERS AND FILTER ===
tickers = get_ticker_list()
if not tickers:
    print("No tickers found. Exiting.")
    exit()

# Filter to start from 'HSPO'
if 'RDN' not in tickers:
    print("RDN not found in the ticker list. Exiting.")
    exit()

# Skip all tickers before 'HSPO'
start_index = tickers.index('RDN')
tickers = tickers[start_index:]

# Filter out tickers whose files already exist
tickers = [ticker for ticker in tickers if not os.path.exists(
    os.path.join(output_dir, f"{ticker}_market_cap.json"))]
if not tickers:
    print("All target tickers already processed. Nothing to do.")
    exit()

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)

# === MARKET CAP URL ===
base_url = "https://www.macrotrends.net/assets/php/market_cap.php?t={}"

# === LOOP THROUGH TICKERS ===
for ticker in tickers:
    print(f"\n➡️ Processing market cap for ticker: {ticker}")
    try:
        url = base_url.format(ticker.upper())
        print(f"🌐 Loading URL: {url}")
        driver.get(url)
        time.sleep(3)  # Wait for JavaScript data to load

        try:
            data_json = driver.execute_script(
                "return JSON.stringify(window.chartData);")
            if data_json:
                output_path = os.path.join(
                    output_dir, f"{ticker}_market_cap.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(data_json)
                print(f"💾 Saved market cap data to {output_path}")
            else:
                print(f"⚠️ No market cap data found for {ticker}")
        except WebDriverException as e:
            print(f"❌ JS extraction failed for {ticker}: {e}")

    except Exception as e:
        print(f"❌ Failed to fetch market cap for {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()
