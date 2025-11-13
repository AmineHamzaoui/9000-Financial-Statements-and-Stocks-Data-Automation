import os
import time
import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === Chrome setup ===
options = uc.ChromeOptions()
options.add_argument('--headless=new')
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")
options.add_argument('--host-resolver-rules=MAP localhost 127.0.0.1')

# Add uBlock adblocker
adblock_path = os.path.join(os.getcwd(), 'D:/Slenuim_projec/uBlock.crx')
if os.path.exists(adblock_path):
    options.add_extension(adblock_path)
else:
    print("⚠️ Adblock extension not found.")

# === Load ticker list ===


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

print(f"✅ Loaded {len(tickers)} tickers to process.")

# === Setup financial sections ===
financial_sections = {
    "income-statement": "income-statement",
    "balance-sheet": "balance-sheet",
    "cash-flow-statement": "cash-flow-statement",
    "financial-ratios": "financial-ratios"
}

# === Create output folders ===
base_dir = os.getcwd()
for section in financial_sections:
    os.makedirs(os.path.join(base_dir, section), exist_ok=True)

# === Start browser ===
driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 15)

for ticker in tickers[1552:]:
    print(f"\n➡️ Processing ticker: {ticker}")
    for section_name, section_url in financial_sections.items():
        save_path = os.path.join(base_dir, section_name, f"{ticker}.json")
        if os.path.exists(save_path):
            print(f"⏩ Skipping {ticker} in {section_name} — already exists.")
            continue

        print(f"\n📄 Fetching section: {section_name} for {ticker}")
        try:
            url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/{section_url}?freq=Q"
            driver.get(url)
            time.sleep(2)

            page_source = driver.page_source
            if "Cette page ne fonctionne pas" in page_source or "Oops! We can't find that page." in page_source:
                print("🚫 Skipping — broken or missing page.")
                continue

            # Accept cookie popup
            try:
                accept_btn = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
                accept_btn.click()
                print("✅ Cookie accepted.")
            except:
                print("⚠️ Cookie already accepted or not shown.")

            # Close ad popups
            try:
                close_ad_btns = driver.find_elements(
                    By.XPATH, "//div[contains(@class, 'ns-fnt3e-e-17')]//span[text()='Close']")
                for btn in close_ad_btns:
                    driver.execute_script("arguments[0].click();", btn)
                if close_ad_btns:
                    print(f"✅ Closed {len(close_ad_btns)} ad(s).")
            except:
                pass

            # Select "Quarterly" view
            try:
                dropdown = wait.until(EC.element_to_be_clickable(
                    (By.CLASS_NAME, "select2-selection")))
                dropdown.click()
                quarterly = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//li[contains(text(), 'Format: Quarterly')]")))
                quarterly.click()
                print("✅ 'Format: Quarterly' selected.")
                time.sleep(2)
            except Exception as e:
                print(f"❌ Dropdown issue: {e}")
                continue

            # Extract originalData
            try:
                json_data = driver.execute_script(
                    "return JSON.stringify(originalData);")
                if json_data and json_data != "undefined":
                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(json_data)
                    print(f"💾 Saved: {save_path}")
                else:
                    print(
                        f"⚠️ No originalData found for {ticker} in {section_name}. Skipping.")
            except Exception as e:
                print(f"❌ Could not extract originalData: {e}")

        except Exception as e:
            print(f"❌ General error with {ticker} in {section_name}: {e}")

driver.quit()
print("\n✅ Done.")
