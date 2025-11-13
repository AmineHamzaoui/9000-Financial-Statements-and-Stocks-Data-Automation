import os
import time
import json
import undetected_chromedriver as uc
from selenium.common.exceptions import WebDriverException

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")
options.add_argument('--headless=new')

# === CREATE OUTPUT FOLDERS ===
output_dirs = {
    "stock_data": "stock_data",
    "stock_splits": "stock_splits"
}
for folder in output_dirs.values():
    os.makedirs(folder, exist_ok=True)

# === FUNCTION TO LOAD TICKERS ===
def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []

# === LOAD TICKERS ===
tickers = get_ticker_list()
if not tickers:
    print("No tickers found. Exiting.")
    exit()

# === START FROM SPECIFIC TICKER ===
start_from = "EDBL"
if start_from in tickers:
    start_index = tickers.index(start_from)
    tickers = tickers[start_index:]
    print(f"⏩ Starting from ticker '{start_from}' ({start_index + 1}/{start_index + len(tickers)})")
else:
    print(f"Ticker '{start_from}' not found in list.")
    exit()

# === FILTER: SKIP TICKERS IF FILES ALREADY EXIST ===
def is_already_processed(ticker):
    for category in output_dirs:
        expected_path = os.path.join(output_dirs[category], f"{ticker}_{category}.json")
        if not os.path.exists(expected_path):
            return False  # At least one missing → needs download
    return True  # All files exist → skip

tickers = [t for t in tickers if not is_already_processed(t)]
if not tickers:
    print("✅ All tickers already processed. Nothing to do.")
    exit()

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)

# === URL TYPES TO FETCH ===
url_map = {
    "stock_data": "https://www.macrotrends.net/assets/php/stock_price_history.php?t={}",
    "stock_splits": "https://www.macrotrends.net/assets/php/stock_splits.php?t={}"
}

# === LOOP THROUGH TICKERS ===
for ticker in tickers:
    print(f"\n➡️ Processing ticker: {ticker}")
    for category, base_url in url_map.items():
        try:
            url = base_url.format(ticker.upper())
            print(f"🌐 Loading URL for {category}: {url}")
            driver.get(url)
            time.sleep(3)  # Wait for JavaScript data to load

            try:
                data_json = driver.execute_script("return JSON.stringify(window.dataDaily);")
                if data_json:
                    output_path = os.path.join(output_dirs[category], f"{ticker}_{category}.json")
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(data_json)
                    print(f"💾 Saved {category} data to {output_path}")
                else:
                    print(f"⚠️ No {category} data found for {ticker}")
            except WebDriverException as e:
                print(f"❌ JS extraction failed for {category} - {ticker}: {e}")

        except Exception as e:
            print(f"❌ Failed to fetch {category} for {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()
# === FINISHED ===