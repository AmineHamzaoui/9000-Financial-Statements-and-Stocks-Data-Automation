import os
import time
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
# Avoid headless for better Cloudflare bypass
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")

# === LOAD TICKERS ===


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

# === START DRIVER ===
driver = uc.Chrome(options=options)

for ticker in tickers:
    print(f"\n➡️ Processing ticker: {ticker}")
    url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/stock-price-history"
    driver.get(url)

    # === STEP 1: Handle GDPR Consent ===
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Tout accepter')]"))
        ).click()
        print("✅ Consent accepted")
        time.sleep(1)
    except TimeoutException:
        print("⚠️ No consent popup detected (likely already accepted or skipped)")

    # === STEP 2: Check for valid page ===
    if "Page Not Found" in driver.title or "404" in driver.title:
        print(f"❌ Invalid or missing ticker page for: {ticker}")
        continue

    # === STEP 3: Wait for historical data table to load ===
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "historical_data_table"))
        )
        print(f"✅ Table loaded for {ticker}")
    except TimeoutException:
        print(f"⚠️ Table not found — possibly Cloudflare blocking or malformed page")

    # === STEP 4: Save full rendered HTML ===
    html_source = driver.page_source
    output_path = os.path.join(os.getcwd(), f"{ticker}_macrotrends.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_source)
    print(f"💾 Saved HTML to {output_path}")

driver.quit()
