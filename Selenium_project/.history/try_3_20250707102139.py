import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
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
        print("❌ Ticker list file not found!")
        return []


tickers = get_ticker_list()
if not tickers:
    print("❌ No tickers found. Exiting.")
    exit()

# === SETUP DRIVER ===
driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 10)

for ticker in tickers[:10]:  # limit for testing
    print(f"\n➡️ Processing: {ticker}")
    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement"
        driver.get(url)

        # ❌ Skip error pages (HTTP 500 or "page not working")
        if "Cette page ne fonctionne pas" in driver.page_source:
            print("🚫 Skipped — Page not working (HTTP 500).")
            continue

        # ✅ Close ads (X icon)
        try:
            ad_closers = driver.find_elements(
                By.XPATH, "//div[@style='cursor: pointer;']//*[name()='svg']")
            for ad in ad_closers:
                driver.execute_script("arguments[0].click();", ad)
            if ad_closers:
                print(f"✅ Closed {len(ad_closers)} ad(s).")
        except:
            pass

        # ✅ Accept cookies
        try:
            accept_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            driver.execute_script("arguments[0].click();", accept_btn)
            print("✅ Cookie accepted.")
        except:
            print("⚠️ No cookie banner or already accepted.")

        # ✅ Select Quarterly format
        try:
            dropdown = wait.until(EC.element_to_be_clickable(
                (By.CLASS_NAME, "select2-selection")))
            driver.execute_script("arguments[0].click();", dropdown)
            quarterly = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//li[contains(text(), 'Format: Quarterly')]")))
            driver.execute_script("arguments[0].click();", quarterly)
            print("✅ Quarterly format selected.")
        except Exception as e:
            print(f"❌ Dropdown issue: {e}")
            continue

        # ✅ Wait for quarterly data to load
        try:
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//th[contains(text(), 'Q1') or contains(text(), 'Q2')]")))
            print("✅ Quarterly data loaded.")
        except:
            print("⚠️ Quarterly headers not found, skipping.")
            continue

        # ✅ Save HTML
        html = driver.page_source
        filename = f"{ticker}_quarterly_dropdown_selected.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"💾 Saved: {filename}")

    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")

# === DONE ===
driver.quit()
print("\n✅ Finished.")
