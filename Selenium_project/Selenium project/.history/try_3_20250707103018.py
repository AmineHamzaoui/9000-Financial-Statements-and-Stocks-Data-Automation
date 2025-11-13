import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")

# === LOAD TICKERS FROM FILE ===


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

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 10)

for ticker in tickers[:10]:  # limit for testing
    print(f"\n➡️ Processing ticker: {ticker}")
    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement"
        driver.get(url)

        # ❌ Skip HTTP 500 error pages
        if "Cette page ne fonctionne pas" in driver.page_source:
            print("🚫 Skipped — HTTP 500 error.")
            continue

        # ✅ Accept cookie popup if present
        try:
            accept_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            accept_button.click()
            print("✅ Cookie consent accepted.")
        except:
            print("⚠️ No cookie popup or already accepted.")

        # ✅ Close 'X' ads (SVG-based)
        try:
            ad_svgs = driver.find_elements(
                By.XPATH, "//div[@style='cursor: pointer;']//*[name()='svg']")
            for ad in ad_svgs:
                try:
                    driver.execute_script("arguments[0].click();", ad)
                except:
                    pass
            if ad_svgs:
                print(f"✅ Closed {len(ad_svgs)} SVG ad(s).")
        except:
            pass

        # ✅ Close ads with "Close" label
        try:
            close_buttons = driver.find_elements(
                By.XPATH, "//div[contains(@class,'ns-fnt3e-e-17')]//span[text()='Close']")
            for btn in close_buttons:
                try:
                    driver.execute_script("arguments[0].click();", btn)
                except:
                    pass
            if close_buttons:
                print(f"✅ Closed {len(close_buttons)} text ads.")
        except:
            pass

        # ✅ Select 'Quarterly' format
        try:
            dropdown = wait.until(EC.element_to_be_clickable(
                (By.CLASS_NAME, "select2-selection")))
            dropdown.click()
            print("✅ Dropdown opened.")

            quarterly_option = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//li[contains(text(), 'Format: Quarterly')]")))
            quarterly_option.click()
            print("✅ 'Format: Quarterly' selected.")

            # Wait for table to update
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//th[contains(text(), 'Q1') or contains(text(), 'Q2')]")))
            print("✅ Quarterly table loaded.")
        except Exception as e:
            print(f"❌ Failed dropdown selection: {e}")
            continue

        # ✅ Save HTML
        html = driver.page_source
        filename = f"{ticker}_quarterly_dropdown_selected.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"💾 Saved HTML to {filename}")

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")

# === DONE ===
driver.quit()
print("\n✅ All done.")
