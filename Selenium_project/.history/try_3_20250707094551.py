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

# === LOOP THROUGH TICKERS ===
for ticker in tickers:
    print(f"\n➡️ Processing ticker: {ticker}")

    try:
        # === 1. FORCE QUARTERLY INCOME STATEMENT URL ===
        income_url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement?freq=Q"
        driver.get(income_url)
        wait = WebDriverWait(driver, 15)

        # Accept cookie popup if it exists
        try:
            accept_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
            accept_button.click()
            print("✅ Cookie consent accepted.")
        except:
            print("⚠️ No cookie banner detected or already accepted.")

        # Check if still on the right URL
        current_url = driver.current_url
        if current_url != income_url:
            print(f"❌ Redirected to: {current_url}")
        else:
            print("✅ Still on the forced quarterly URL.")

        # Save income statement page
        income_html = driver.page_source
        income_path = os.path.join(os.getcwd(), f"{ticker}_quarterly_income_statement.html")
        with open(income_path, "w", encoding="utf-8") as f:
            f.write(income_html)
        print(f"💾 Saved income statement HTML to {income_path}")

        # === 2. LOAD STOCK PRICE HISTORY PAGE ===
        stock_price_url = f"https://www.macrotrends.net/assets/php/stock_price_history.php?t={ticker.upper()}"
        driver.get(stock_price_url)

        # Save the stock price JS data page
        js_html = driver.page_source
        js_path = os.path.join(os.getcwd(), f"{ticker}_stock_price_history.html")
        with open(js_path, "w", encoding="utf-8") as f:
            f.write(js_html)
        print(f"💾 Saved stock price history HTML to {js_path}")

    except Exception as e:
        print(f"❌ Error processing ticker {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()