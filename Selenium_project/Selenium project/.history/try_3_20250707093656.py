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
    print("No tickers found. Exiting.")
    exit()

# === START SELENIUM DRIVER ===
driver = uc.Chrome(options=options)

for ticker in tickers[:1]:  # Only the first ticker for testing
    print(f"\n➡️ Processing ticker: {ticker}")
    try:
        # Force quarterly via URL
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement?freq=Q"
        driver.get(url)

        wait = WebDriverWait(driver, 20)

        # ✅ Accept cookie popup if it exists
        try:
            accept_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            accept_button.click()
            print("✅ Cookie consent accepted.")
        except:
            print("⚠️ No cookie banner detected or already accepted.")

        # ✅ Wait for quarterly-specific indicator (e.g., Q1/Q2) to appear in table headers
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//th[contains(text(), 'Q1') or contains(text(), 'Q2') or contains(text(), 'Q3') or contains(text(), 'Q4')]")))
        print("✅ Quarterly data detected.")

        # ✅ Save the rendered HTML
        html_content = driver.page_source
        output_path = os.path.join(
            os.getcwd(), f"{ticker}_quarterly_income_statement.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 Saved HTML to {output_path}")

    except Exception as e:
        print(f"❌ Failed to process ticker {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()
