import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up options
options = uc.ChromeOptions()
# Comment out headless for debugging; headless mode is often blocked
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--disable-gpu')
options.add_argument("--window-size=1280,800")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

# Load ticker list


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

# Open driver
driver = uc.Chrome(options=options)

for ticker in tickers:
    print(f"Processing ticker: {ticker}")
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/stock-price-history"
    driver.get(url)

    try:
        # Wait for the table to load — may vary depending on structure
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "historical_data_table"))
        )
        # Optional: sleep to allow JavaScript full execution
        time.sleep(2)

        # If Macrotrends uses JS variable like `dataDaily`, try extracting
        result = driver.execute_script(
            "return JSON.stringify(window.dataDaily);")
        # Print partial to avoid overload
        print(f"Data for {ticker}: {result[:300]}...")

    except Exception as e:
        print(f"Error for {ticker}: {e}")

driver.quit()
