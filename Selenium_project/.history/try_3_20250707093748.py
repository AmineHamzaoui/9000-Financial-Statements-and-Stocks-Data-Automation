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

# === FORCE THIS EXACT TICKER AND URL ===
ticker = "AA"
ticker_name = "alcoa"
forced_url = f"https://www.macrotrends.net/stocks/charts/{ticker}/{ticker_name}/income-statement?freq=Q"

# === START DRIVER ===
driver = uc.Chrome(options=options)
print(f"➡️ Navigating to: {forced_url}")
driver.get(forced_url)

try:
    wait = WebDriverWait(driver, 15)

    # ✅ Accept cookie popup if it exists
    try:
        accept_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(text(),'Tout accepter')]")))
        accept_button.click()
        print("✅ Cookie consent accepted.")
    except:
        print("⚠️ No cookie banner detected or already accepted.")

    # ✅ Confirm page is still on the forced URL
    current_url = driver.current_url
    if current_url != forced_url:
        print(f"❌ Redirected to: {current_url}")
    else:
        print("✅ Still on the forced quarterly URL.")

    # ✅ Save HTML as-is
    html = driver.page_source
    file_path = os.path.join(os.getcwd(), f"{ticker}_forced_quarterly.html")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"💾 HTML saved to {file_path}")

except Exception as e:
    print(f"❌ Failed to save HTML: {e}")

driver.quit()
