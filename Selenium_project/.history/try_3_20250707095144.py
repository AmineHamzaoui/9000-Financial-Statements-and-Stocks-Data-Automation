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
        print("Ticker list file not found!")
        return []

tickers = get_ticker_list()
if not tickers:
    print("No tickers found. Exiting.")
    exit()

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)

for ticker in tickers[:10]:  # limit for testing
    print(f"\n➡️ Processing ticker: {ticker}")
    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement"
        driver.get(url)
        wait = WebDriverWait(driver, 15)

        # ✅ Accept cookie popup if it exists
        try:
            accept_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            accept_button.click()
            print("✅ Cookie consent accepted.")
        except:
            print("⚠️ No cookie banner detected or already accepted.")

        # ✅ Click the format dropdown
        try:
            dropdown = wait.until(EC.element_to_be_clickable(
                (By.CLASS_NAME, "select2-selection")))
            dropdown.click()
            print("✅ Dropdown opened.")

            # Click the "Format: Quarterly" option
            quarterly_option = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//li[contains(text(), 'Format: Quarterly')]")))
            quarterly_option.click()
            print("✅ 'Format: Quarterly' selected.")

            # Wait for the table to update (watch for any Q1/Q2/Q3 headers)
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//th[contains(text(), 'Q1') or contains(text(), 'Q2')]")))
            print("✅ Quarterly table loaded.")

        except Exception as e:
            print(f"❌ Failed to select Quarterly: {e}")

        # ✅ Save HTML after dropdown change
        html = driver.page_source
        output_path = os.path.join(os.getcwd(), f"{ticker}_quarterly_dropdown_selected.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"💾 Saved HTML to {output_path}")

    except Exception as e:
        print(f"❌ Error processing ticker {ticker}: {e}")

driver.quit()
