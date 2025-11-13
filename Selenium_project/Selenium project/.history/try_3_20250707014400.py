import os
import time
import undetected_chromedriver as uc

# === SETUP OPTIONS ===
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
for ticker in tickers[:1]:  # Only the first ticker
    print(f"\n➡️ Processing ticker: {ticker}")
    try:
        url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/income-statement?freq=Q"
        driver.get(url)
        time.sleep(5)  # Wait for JS rendering to finish

        # Save full rendered HTML
        html_content = driver.page_source
        output_path = os.path.join(os.getcwd(), f"{ticker}_rendered_page.html")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 Saved HTML to {output_path}")

    except Exception as e:
        print(f"❌ Failed to process ticker {ticker}: {e}")

# === CLOSE BROWSER ===
driver.quit()
