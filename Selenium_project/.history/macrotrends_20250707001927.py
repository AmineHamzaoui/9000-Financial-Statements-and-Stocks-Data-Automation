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
# Avoid headless for better JS loading
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")

# === FUNCTION TO LOAD TICKERS ===

url = "https://www.macrotrends.net/assets/php/stock_splits.php?t=AAPL
"https://www.macrotrends.net/assets/php/market_cap.php?t=AAPL"

https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=revenue&statement=income-statement&freq=Q --> var chartData 
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=gross-profit&statement=income-statement&freq=Q --> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=operating-income&statement=income-statement&freq=Q --> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=ebitda&statement=income-statement&freq=Q --> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=net-income&statement=income-statement&freq=Q--> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=eps-earnings-per-share-diluted&statement=income-statement&freq=Q--> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=shares-outstanding&statement=income-statement&freq=Q--> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=total-assets&statement=balance-sheet&freq=Q --> var chartData
https://www.macrotrends.net/assets/php/fundamental_iframe.php?t=AAPL&type=cash-on-hand&statement=balance-sheet&freq=Q --> var chartData

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

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)

# === LOOP THROUGH TICKERS ===
for ticker in tickers:
    print(f"\n➡️ Processing ticker: {ticker}")

    try:
        # Load the JS data page
        url = f"https://www.macrotrends.net/assets/php/stock_price_history.php?t={ticker.upper()}"
        driver.get(url)
        time.sleep(3)  # Allow time for JS variable to be set

        # Extract the JS variable `window.dataDaily`
        try:
            data_json = driver.execute_script(
                "return JSON.stringify(window.dataDaily);")
            if data_json:
                output_path = os.path.join(
                    os.getcwd(), f"{ticker}_stock_data.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(data_json)
                print(f"💾 Saved JSON data to {output_path}")
            else:
                print(f"⚠️ No data found for ticker: {ticker}")
        except WebDriverException as e:
            print(f"❌ JavaScript execution failed for {ticker}: {e}")

    except Exception as e:
        print(f"❌ Failed to process ticker {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()
