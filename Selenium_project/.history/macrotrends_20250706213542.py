
import undetected_chromedriver as uc
options = uc.ChromeOptions()
options.add_argument('--headless')  # Optional

driver = uc.Chrome(options=options)
driver.get("https://example.com")