
import undetected_chromedriver as uc
options = uc.ChromeOptions()
options.add_argument('--headless')  # Optional

driver = uc.Chrome(options=options)
driver.get("https://www.macrotrends.net/stocks/charts/{ticke-RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR }/apple/stock-price-history)
# Run JavaScript code
result = driver.execute_script("return document.title;")
print("Page title is:", result)

# Another example: scroll down
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

driver.quit()
