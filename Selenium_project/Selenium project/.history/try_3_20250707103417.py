import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === Chrome setup ===
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")


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

driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 15)

for ticker in tickers[:10]:  # test limit
    print(f"\n➡️ Processing: {ticker}")
    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement"
        driver.get(url)

        # ❌ Skip broken pages
        if "Cette page ne fonctionne pas" in driver.page_source:
            print("🚫 Skipping — HTTP 500.")
            continue

        # ✅ Accept cookie
        try:
            accept_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            accept_btn.click()
            print("✅ Cookie accepted.")
        except:
            print("⚠️ Cookie already accepted or not shown.")

        # ✅ Close known ad popup if found
        try:
            close_ad_btns = driver.find_elements(
                By.XPATH, "//div[contains(@class, 'ns-fnt3e-e-17')]//span[text()='Close']")
            for btn in close_ad_btns:
                driver.execute_script("arguments[0].click();", btn)
            if close_ad_btns:
                print(f"✅ Closed {len(close_ad_btns)} ad(s).")
        except:
            pass

        # ✅ Select "Quarterly"
        try:
            dropdown = wait.until(EC.element_to_be_clickable(
                (By.CLASS_NAME, "select2-selection")))
            dropdown.click()
            print("✅ Dropdown opened.")

            quarterly = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//li[contains(text(), 'Format: Quarterly')]")))
            quarterly.click()
            print("✅ 'Format: Quarterly' selected.")

            time.sleep(2)  # allow page content to update
        except Exception as e:
            print(f"❌ Failed dropdown selection: {e}")
            continue

        # ✅ Save HTML regardless of table presence
        html = driver.page_source
        filename = f"{ticker}_quarterly_dropdown_selected.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"💾 Saved: {filename}")

    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")

driver.quit()
print("\n✅ Done.")
