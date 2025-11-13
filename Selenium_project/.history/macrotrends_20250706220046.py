# <- use seleniumwire's wrapper
from seleniumwire import undetected_chromedriver as uc
import os
import time

options = uc.ChromeOptions()
# options.add_argument('--headless')  # you can toggle
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")

driver = uc.Chrome(options=options)

ticker = "AAPL"
url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker.lower()}/stock-price-history"
driver.get(url)
time.sleep(5)  # let JS load

# Save HTML
html_path = f"{ticker}_macrotrends.html"
with open(html_path, "w", encoding="utf-8") as f:
    f.write(driver.page_source)

# Save all JS files referenced
js_dir = "js_files"
os.makedirs(js_dir, exist_ok=True)

for request in driver.requests:
    if request.response and request.path.endswith(".js"):
        try:
            js_response = request.response.body.decode(
                "utf-8", errors="ignore")
            js_file = os.path.join(
                js_dir, os.path.basename(request.path.split("?")[0]))
            with open(js_file, "w", encoding="utf-8") as f:
                f.write(js_response)
            print(f"[💾] Saved JS file: {js_file}")
        except Exception as e:
            print(f"[!] Failed to save JS from {request.url}: {e}")

driver.quit()
