import os
import time
import requests
import undetected_chromedriver as uc
from urllib.parse import urlparse, parse_qs
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# === DIRECT API KEY (yours) ===
API_2CAPTCHA_KEY = "88967ec637ae4c768ef7d2181d3c90a3"

# === SETUP OPTIONS ===
options = uc.ChromeOptions()
options.add_argument('--no-sandbox')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--window-size=1280,800")
# <-- run visible? remove this line if you want to see Chrome
options.add_argument('--headless=new')

# === DOWNLOAD DIRECTORY ===
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === 2Captcha Helpers ===


def send_2captcha_recaptcha(sitekey, pageurl, api_key, timeout=120):
    """Submit reCAPTCHA to 2Captcha and poll for solution."""
    print(f"[2Captcha] Submitting CAPTCHA for {pageurl}")
    s = requests.Session()
    resp = s.post("http://2captcha.com/in.php", data={
        "key": api_key,
        "method": "userrecaptcha",
        "googlekey": sitekey,
        "pageurl": pageurl,
        "json": 1
    }).json()

    if resp.get("status") != 1:
        print("[2Captcha] Failed to submit:", resp)
        return None

    job_id = resp["request"]
    print(f"[2Captcha] Job submitted: {job_id}")

    poll_url = "http://2captcha.com/res.php"
    for _ in range(int(timeout / 5)):
        time.sleep(5)
        r = s.get(poll_url, params={
            "key": api_key,
            "action": "get",
            "id": job_id,
            "json": 1
        }).json()
        if r.get("status") == 1:
            print("[2Captcha] Solved successfully.")
            return r["request"]
        elif r.get("request") != "CAPCHA_NOT_READY":
            print("[2Captcha] Error:", r)
            return None

    print("[2Captcha] Timeout waiting for solution.")
    return None


def inject_token_and_submit(driver, token):
    """Inject solved token into the form and submit it."""
    js = """
    (token) => {
      const el = document.getElementById('g-recaptcha-response');
      if (el) {
         el.value = token;
         el.style.display = 'block';
      }
      const forms = document.getElementsByTagName('form');
      if (forms.length) forms[0].submit();
    }
    """
    driver.execute_script("(" + js + ")(arguments[0]);", token)
    print("[inject_token_and_submit] Token injected and form submitted.")


# === MAIN WORKFLOW ===
def main():
    url = "https://turbobit.net/download/free/rqociccbdhpe?asgtbndr=1"
    print(f"[INIT] Launching TurboBit page: {url}")

    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 25)

    try:
        driver.get(url)
        print("[STEP] Page loaded.")
        time.sleep(3)

        # Handle cookie consent
        for txt in ["Accept", "I agree", "J'accepte"]:
            try:
                btn = driver.find_element(
                    By.XPATH, f"//button[contains(text(), '{txt}')]")
                btn.click()
                print(f"[STEP] Clicked cookie button: {txt}")
                break
            except Exception:
                continue

        # Wait for the reCAPTCHA iframe
        print("[STEP] Waiting for reCAPTCHA iframe...")
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "iframe[src*='recaptcha/api2/anchor']")))
        iframe = driver.find_element(
            By.CSS_SELECTOR, "iframe[src*='recaptcha/api2/anchor']")
        driver.switch_to.frame(iframe)
        print("[STEP] Switched into iframe.")

        # Try clicking the checkbox
        clicked = False
        try:
            checkbox = wait.until(EC.element_to_be_clickable(
                (By.ID, "recaptcha-anchor")))
            ActionChains(driver).move_to_element(checkbox).click().perform()
            time.sleep(2)
            cls = checkbox.get_attribute("class") or ""
            if "recaptcha-checkbox-checked" in cls:
                print("[STEP] Checkbox clicked successfully.")
                clicked = True
        except Exception as e:
            print("[ERROR] Checkbox click failed:", e)

        driver.switch_to.default_content()

        # If not clicked, use 2Captcha
        if not clicked:
            print("[STEP] Using 2Captcha to solve challenge.")
            src = iframe.get_attribute("src")
            qs = parse_qs(urlparse(src).query)
            sitekey = qs.get("k", [""])[0]
            if not sitekey:
                print("[ERROR] Sitekey not found.")
                return
            token = send_2captcha_recaptcha(sitekey, url, API_2CAPTCHA_KEY)
            if not token:
                print("[ERROR] 2Captcha failed to solve.")
                return
            inject_token_and_submit(driver, token)
            time.sleep(5)

        # Wait for submit button
        print("[STEP] Waiting for submit button...")
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#submit")))
        submit_btn = driver.find_element(By.CSS_SELECTOR, "#submit")
        submit_btn.click()
        print("[STEP] Submit clicked, waiting for download.")
        time.sleep(10)

    except TimeoutException as e:
        print("[TIMEOUT]", e)
    except WebDriverException as e:
        print("[EXCEPTION] WebDriver error:", e)
    except Exception as e:
        print("[EXCEPTION] Unexpected:", e)
    finally:
        print("[DONE] Leaving browser open for inspection.")
        # driver.quit()


if __name__ == "__main__":
    main()
