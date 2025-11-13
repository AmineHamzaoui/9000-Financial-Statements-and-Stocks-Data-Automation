
import undetected_chromedriver as uc
options = uc.ChromeOptions()
options.add_argument('--headless')  # Optional



def get_ticker_list(self):
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.logger.error("Ticker list file not found!")

driver = uc.Chrome(options=options)
for ticker in tickers:
    print(f"Processing ticker: {ticker}")
driver.get(f'https://www.macrotrends.net/stocks/charts/{ticker}/{ticker}.lower()/stock-price-history')
# Run JavaScript code
result = driver.execute_script("return document.title;")
print("Page title is:", result)

# Another example: scroll down
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

driver.quit()
