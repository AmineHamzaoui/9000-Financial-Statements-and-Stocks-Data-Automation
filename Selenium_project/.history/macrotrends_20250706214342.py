import os
import undetected_chromedriver as uc

# Set up Chrome options
options = uc.ChromeOptions()
options.add_argument('--headless')  # run in headless mode
options.add_argument('--disable-gpu')  # optional for headless
options.add_argument('--no-sandbox')  # recommended for some environments

# Load tickers from file


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


# Main execution
tickers = get_ticker_list()
if not tickers:
    exit()

# Launch driver once
driver = uc.Chrome(options=options)

for ticker in tickers:
    print(f"Processing ticker: {ticker}")
    url = f'https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/stock-price-history'
    driver.get(url)

    # Optional wait: can use WebDriverWait if needed for dynamic loading
    driver.implicitly_wait(5)

    try:
        result = driver.execute_script(
            "return JSON.stringify(window.dataDaily);")
        print(f"Data for {ticker}: {result}")
    except Exception as e:
        print(f"Error extracting data for {ticker}: {e}")

# Clean up
driver.quit()
