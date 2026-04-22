"""
msci_scraper.py  —  MSCI Research Papers  (v26)
------------------------------------------------
v26: Anchored paths; logs separated to gap/data/logs/MSCI/
  - OUTPUT_DIR : gap/data/pdfs/MSCI/
  - TEMP_DIR   : gap/data/pdfs/MSCI/_temp_downloads/  (download staging)
  - LOG_DIR    : gap/data/logs/MSCI/  (_done, _failed, _article_urls)
All v25 scraping logic unchanged.
"""

import re
import time
import random
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException,
    NoSuchElementException, InvalidSessionIdException, WebDriverException,
)
from urllib3.exceptions import MaxRetryError

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.msci.com"
LISTING_URL = (
    BASE_URL
    + "/research-and-insights"
    + "?sortCriteria=%40display_date%20descending"
    + "&f-research_format=Paper"
    + "&aq=(%40research_format%3D%3D(%22Blog%20post%22%2C%22Paper%22%2C%22Podcast%22"
    + "%2C%22Quick%20take%22%2C%22Video%22))"
    + "%20AND%20(NOT%20%40aem_filetype%20AND%20%40aem_page_path%2F%3D"
    + "%22%5E%2Fresearch-and-insights.*%22)"
)
GATE_URL = BASE_URL + "/research-and-insights/paper/carbon-credit-integrity-in-the-accu-market"

USER = {
    "first_name": "William",
    "last_name":  "Huang",
    "email":      "wh2@andrew.cmu.edu",
    "company":    "Carnegie Mellon University",
    "country":    "United States",
    "state":      "Pennsylvania",
}

# Script lives at gap/src/scrapers/ — three .parent calls reach gap/
REPO_ROOT  = Path(__file__).resolve().parent.parent.parent

OUTPUT_DIR = REPO_ROOT / "data" / "pdfs"  / "MSCI"
TEMP_DIR   = OUTPUT_DIR / "_temp_downloads"
LOG_DIR    = REPO_ROOT / "data" / "logs" / "MSCI"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = LOG_DIR / "_done.txt"
FAIL_FILE = LOG_DIR / "_failed.txt"
URL_CACHE = LOG_DIR / "_article_urls.txt"

MIN_DELAY = 2.0
MAX_DELAY = 4.5

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep(lo=None, hi=None):
    time.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))

def slugify(text: str, n: int = 80) -> str:
    s = re.sub(r"[^\w\s-]", "", text.lower())
    s = re.sub(r"[\s_-]+", "_", s).strip("_")
    return s[:n]

def load_done() -> set:
    return set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()

def mark_done(url: str):
    with open(LOG_FILE, "a") as f:
        f.write(url + "\n")

def mark_failed(url: str):
    with open(FAIL_FILE, "a") as f:
        f.write(url + "\n")

def load_url_cache() -> list[str]:
    return URL_CACHE.read_text().splitlines() if URL_CACHE.exists() else []

def save_url_cache(urls: list[str]):
    URL_CACHE.write_text("\n".join(urls))

def safe_pdf_url(url: str) -> str:
    parsed = urlparse(url)
    decoded_path = unquote(parsed.path)
    safe_path = "/".join(quote(seg, safe="") for seg in decoded_path.split("/"))
    return parsed._replace(path=safe_path).geturl()

def clear_msci_session(driver):
    try:
        driver.get(BASE_URL)
        time.sleep(1.0)
    except Exception:
        pass
    try:
        driver.delete_all_cookies()
    except Exception:
        pass
    try:
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except Exception:
        pass

# ── Driver ─────────────────────────────────────────────────────────────────────

def apply_cdp_download(driver):
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(TEMP_DIR.resolve()),
        })
    except Exception:
        pass

def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1280,900")
    opts.add_experimental_option("prefs", {
        "download.default_directory":        str(TEMP_DIR.resolve()),
        "download.prompt_for_download":       False,
        "download.directory_upgrade":         True,
        "plugins.always_open_pdf_externally": True,
        "plugins.plugins_disabled":           ["Chrome PDF Viewer"],
    })
    driver = webdriver.Chrome(options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    apply_cdp_download(driver)
    return driver

def dismiss_cookies(driver):
    try:
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 "//button[contains(normalize-space(text()),'Accept All') or "
                 "contains(normalize-space(text()),'Allow All') or "
                 "contains(normalize-space(text()),'Accept Cookies') or "
                 "contains(normalize-space(text()),'Accept all')]")
            )
        )
        btn.click()
        print("  [cookies] accepted")
        time.sleep(1)
    except TimeoutException:
        pass

def dismiss_overlays(driver):
    try:
        driver.execute_script(
            "document.querySelectorAll("
            "'.onetrust-pc-dark-filter, #onetrust-consent-sdk, '"
            " + '#onetrust-banner-sdk, [class*=\"onetrust\"], [id*=\"onetrust\"], '"
            " + '[class*=\"cookie\"], [id*=\"cookie\"]'"
            ").forEach(e => e.remove());"
            "document.body.style.overflow = '';"
            "document.body.style.position = '';"
        )
        time.sleep(0.3)
    except Exception:
        pass

def safe_click(driver, element):
    dismiss_overlays(driver)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.5)
    dismiss_overlays(driver)
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)

def react_fill(driver, element, value: str):
    element.click()
    element.clear()
    element.send_keys(value)
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('input',  {bubbles:true}));", element)
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", element)

# ── Auth ───────────────────────────────────────────────────────────────────────

def auth(driver, prompt: bool = True):
    print(f"[auth] Opening gate page: {GATE_URL}")
    driver.get(GATE_URL)
    sleep(3, 4)
    dismiss_cookies(driver)
    dismiss_overlays(driver)
    if prompt:
        print("\n>>> Submit the email form in the browser to unlock PDF access.")
        print(">>> Wait for the download to start or confirmation to appear.")
        print(">>> Then press ENTER.\n")
        input(">>> ")
    else:
        print("[auth] Auto-resuming after browser restart — waiting 10s...")
        time.sleep(10)
    apply_cdp_download(driver)
    print("[auth] ✓ Browser session authenticated\n")

def resurrect(old_driver) -> webdriver.Chrome:
    print("\n[resurrect] Browser died — relaunching...")
    try:
        proc = getattr(getattr(old_driver, "service", None), "process", None)
        if proc:
            proc.kill()
    except Exception:
        pass
    try:
        old_driver.quit()
    except Exception:
        pass
    for attempt in range(1, 4):
        try:
            time.sleep(5 * attempt)
            new_driver = build_driver()
            auth(new_driver, prompt=False)
            apply_cdp_download(new_driver)
            print("[resurrect] ✓ Browser restarted\n")
            return new_driver
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")
    raise RuntimeError("Could not resurrect browser after 3 attempts")

# ── Phase 1 ────────────────────────────────────────────────────────────────────

def extract_articles_from_page(driver) -> list[str]:
    soup = BeautifulSoup(driver.page_source, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.match(
            r"^(https://www\.msci\.com)?/research-and-insights/(paper|research-reports)/[^#?]+$",
            href
        ):
            full = BASE_URL + href if href.startswith("/") else href
            if full not in urls:
                urls.append(full)
    return urls

def collect_article_urls(driver) -> list[str]:
    print("[Phase 1] Loading listing page...")
    driver.get(LISTING_URL)
    sleep(4, 6)
    dismiss_overlays(driver)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, '/research-and-insights/paper/')]")
            )
        )
    except TimeoutException:
        print("  [warn] Initial results not found")

    all_urls, seen, page = [], set(), 1
    while True:
        sleep(2, 3)
        dismiss_overlays(driver)
        page_urls = extract_articles_from_page(driver)
        new = [u for u in page_urls if u not in seen]
        for u in new:
            seen.add(u)
            all_urls.append(u)
        print(f"  [page {page}] +{len(new)} articles (total: {len(all_urls)})")

        next_btn = None
        for xpath in [
            "//button[contains(@aria-label, 'Next')]",
            "//a[contains(@aria-label, 'Next')]",
            "//button[normalize-space(text())='Next']",
            "//*[contains(@class,'next') and not(contains(@class,'disabled'))]",
        ]:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                if btn.is_enabled() and btn.is_displayed():
                    next_btn = btn
                    break
            except NoSuchElementException:
                continue
        if next_btn is None:
            try:
                next_btn = driver.find_element(
                    By.XPATH,
                    f"//*[contains(@aria-label,'Page {page+1}') or normalize-space(text())='{page+1}']"
                )
            except NoSuchElementException:
                pass
        if next_btn is None or len(new) == 0:
            print(f"  [done] Pagination complete after page {page}")
            break
        try:
            safe_click(driver, next_btn)
            sleep(3, 4)
            page += 1
        except Exception as e:
            print(f"  [warn] Click failed: {e} — stopping")
            break
        if page > 100:
            break
    return all_urls

# ── Lock detection + PDF URL ───────────────────────────────────────────────────

def get_page_state(driver, article_url: str) -> tuple[bool, str | None, str | None]:
    driver.get(article_url)
    sleep(2, 3)
    dismiss_overlays(driver)
    apply_cdp_download(driver)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 "//a[contains(@href,'contact-us/insights') or "
                 "contains(@href,'/downloads/') or "
                 "contains(normalize-space(text()),'View paper') or "
                 "contains(normalize-space(text()),'Unlock') or "
                 "contains(normalize-space(text()),'Download')]")
            )
        )
    except TimeoutException:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    form_url = None
    pdf_url  = None

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "form_handler_type=gated-pdf" in href:
            parsed_href = urlparse(href if href.startswith("http") else BASE_URL + href)
            if "contact-us/insights" in parsed_href.path:
                form_url = BASE_URL + href if href.startswith("/") else href
        if "document=" in href and form_url is None or "document=" in href:
            try:
                params = parse_qs(urlparse(href).query)
                doc_path = params.get("document", [None])[0]
                if doc_path and doc_path.endswith(".pdf"):
                    pdf_url = doc_path_to_download_url(doc_path, article_url)
            except Exception:
                pass
        if "/downloads/" in href and ".pdf" in href and not pdf_url:
            full = BASE_URL + href if href.startswith("/") else href
            pdf_url = safe_pdf_url(full)

    is_locked = form_url is not None
    if not is_locked:
        try:
            el = driver.find_element(
                By.XPATH, "//a[contains(@href,'form_handler_type=gated-pdf')]")
            href = el.get_attribute("href") or ""
            parsed = urlparse(href if href.startswith("http") else BASE_URL + href)
            if "contact-us/insights" in parsed.path:
                form_url = href
                is_locked = True
        except NoSuchElementException:
            pass

    if is_locked and form_url and not pdf_url:
        params = parse_qs(urlparse(form_url).query)
        doc = params.get("document", [""])[0].strip()
        if not doc:
            return False, None, None

    return is_locked, form_url, pdf_url


def is_on_confirmation_page(driver) -> bool:
    if "confirmation=true" in driver.current_url:
        return True
    src = driver.page_source
    on_form = "STEP 1 OF" in src or "STEP 2 OF" in src or "STEP 3 OF" in src
    if on_form:
        return False
    return "Access your content" in src


def doc_path_to_download_url(doc_path: str, article_url: str) -> str:
    if "/content/dam/web/" in doc_path:
        dl = doc_path.replace("/content/dam/web/", "/downloads/web/")
    elif "/content/dam/documents/" in doc_path:
        dl = doc_path.replace("/content/dam/documents/", "/downloads/documents/")
    else:
        dl = doc_path
    full = BASE_URL + dl if dl.startswith("/") else dl
    return safe_pdf_url(full)


def get_visible_text_inputs(driver) -> list:
    return [i for i in driver.find_elements(By.TAG_NAME, "input")
            if i.is_displayed()
            and "ms-peer" in (i.get_attribute("class") or "")
            and "ms-cursor-pointer" not in (i.get_attribute("class") or "")
            and i.get_attribute("type") in ("text", "email")]

def get_visible_dropdowns(driver, max_count: int | None = None) -> list:
    res = [i for i in driver.find_elements(By.TAG_NAME, "input")
           if i.is_displayed()
           and "ms-cursor-pointer" in (i.get_attribute("class") or "")]
    return res[:max_count] if max_count else res

def get_headlessui_options(driver) -> list:
    try:
        panels = driver.find_elements(By.XPATH, "//div[starts-with(@id,'headlessui-listbox-options-')]")
        for panel in panels:
            if panel.is_displayed():
                lis = [li for li in panel.find_elements(By.TAG_NAME, "li")
                       if li.is_displayed() and li.text.strip()]
                if lis:
                    return lis
    except Exception:
        pass
    return []

def pick_dropdown(driver, dd, label, specific_text=None, retries=3):
    for attempt in range(1, retries+1):
        driver.execute_script("""
            var r = arguments[0].getBoundingClientRect();
            window.scrollTo({top: Math.max(0, window.scrollY + r.top - 150),
                             behavior: 'instant'});
        """, dd)
        time.sleep(0.4)
        dismiss_overlays(driver)
        driver.execute_script("arguments[0].click();", dd)

        deadline = time.time() + 2.5
        options = []
        while time.time() < deadline:
            time.sleep(0.3)
            options = get_headlessui_options(driver)
            if not options:
                options = [li for li in driver.find_elements(By.TAG_NAME, "li")
                           if li.is_displayed() and li.text.strip()]
            if options:
                break

        if not options:
            print(f"  [{label}] attempt {attempt}: no options appeared")
            try:
                dd.send_keys(Keys.ESCAPE)
            except Exception:
                pass
            time.sleep(0.8)
            continue

        if specific_text:
            matches = [o for o in options if specific_text in o.text]
            target = matches[0] if matches else options[0]
        else:
            target = options[min(random.randint(0, 1), len(options)-1)]

        driver.execute_script("arguments[0].scrollIntoView({block:'nearest'});", target)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", target)
        picked = target.text.strip()
        print(f"  [{label}] → {picked!r} (attempt {attempt})")

        deadline2 = time.time() + 2.0
        while time.time() < deadline2:
            time.sleep(0.2)
            if not get_headlessui_options(driver):
                break
        sleep(0.5, 0.8)
        return picked

    print(f"  [{label}] FAILED after {retries} attempts")
    return None

def click_button(driver, text):
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.is_displayed() and btn.text.strip() == text:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.3)
            dismiss_overlays(driver)
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            print(f"  [button] clicked {text!r}")
            return True
    print(f"  [button] {text!r} NOT FOUND")
    return False

def get_validation_errors(driver):
    errors = []
    phrases = ("is not selected","is required","not selected","please select")
    for el in driver.find_elements(By.XPATH,
            "//*[self::p or self::span or self::div][string-length(normalize-space(text()))>3]"):
        try:
            if el.is_displayed() and any(p in el.text.lower() for p in phrases):
                errors.append(el.text.strip())
        except Exception:
            pass
    return errors

def get_first_validation_error(driver) -> tuple[str, object] | None:
    errors = []
    phrases = ("is not selected", "is required", "not selected", "please select")
    for el in driver.find_elements(By.XPATH,
            "//*[self::p or self::span or self::div][string-length(normalize-space(text()))>3]"):
        try:
            if el.is_displayed() and any(p in el.text.lower() for p in phrases):
                errors.append(el.text.strip())
        except Exception:
            pass
    if not errors:
        return None
    dds = get_visible_dropdowns(driver, max_count=6)
    return (errors[0], dds[-1] if dds else None)

def submit_unlock_form(driver, form_url: str) -> bool:
    print(f"  [form] navigating directly to form URL...")
    clear_msci_session(driver)
    driver.get(form_url)
    time.sleep(4)
    dismiss_cookies(driver)
    dismiss_overlays(driver)
    print(f"  [form] URL: {driver.current_url}")

    print("\n=== STEP 1 ===")
    try:
        WebDriverWait(driver, 10).until(lambda d: len(get_visible_text_inputs(d)) >= 1)
    except TimeoutException:
        print("  [form] ERROR: no text inputs appeared")
        return False

    ti = get_visible_text_inputs(driver)
    print(f"  [form] Text inputs: {len(ti)}, Dropdowns: {len(get_visible_dropdowns(driver))}")

    for idx, (key, val) in enumerate([
            ("first_name", USER["first_name"]),
            ("last_name",  USER["last_name"]),
            ("email",      USER["email"])]):
        if idx < len(ti):
            react_fill(driver, ti[idx], val)
            actual = ti[idx].get_attribute("value")
            print(f"  [form input {idx}] {key}={val!r} actual={actual!r}")

    sleep(0.5, 0.8)

    dds = get_visible_dropdowns(driver)
    print(f"  [form] Dropdowns after text fill: {len(dds)}")
    if dds:
        pick_dropdown(driver, dds[0], "country", USER["country"])
        sleep(1.5, 2.0)

    dds = get_visible_dropdowns(driver)
    print(f"  [form] Dropdowns after country: {len(dds)}")
    if len(dds) >= 2:
        pick_dropdown(driver, dds[1], "state", USER["state"])
    elif len(dds) == 1:
        pick_dropdown(driver, dds[0], "state", USER["state"])

    errs = get_validation_errors(driver)
    if errs:
        print(f"  [form] Validation errors before Next: {errs}")

    click_button(driver, "Next")
    sleep(2, 3)
    print(f"  [form] URL after Step 1 Next: {driver.current_url}")

    print("\n=== STEP 2 ===")
    try:
        WebDriverWait(driver, 10).until(lambda d: len(get_visible_text_inputs(d)) >= 1)
    except TimeoutException:
        print("  [form] ERROR: Step 2 text inputs never appeared")
        return False

    ti = get_visible_text_inputs(driver)
    print(f"  [form] Text inputs: {len(ti)}, Dropdowns: {len(get_visible_dropdowns(driver))}")

    if ti:
        react_fill(driver, ti[0], USER["company"])
        print(f"  [form] company = {ti[0].get_attribute('value')!r}")

    sleep(0.5, 0.8)

    filled = 0
    for i in range(4):
        dds = get_visible_dropdowns(driver, max_count=4)
        print(f"  [form] Iteration {i}: {len(dds)} dropdowns visible, filled={filled}")
        if filled >= len(dds):
            print(f"  [form] No new dropdown appeared, stopping at {filled}")
            break
        dd = dds[filled]
        result = pick_dropdown(driver, dd, f"step2_dd{filled}")
        if result is None:
            print(f"  [form] dropdown {filled} failed; stopping optional fills")
            break
        filled += 1
        sleep(1.0, 1.5)

    print(f"  [form] Step 2: filled {filled} dropdowns")

    click_button(driver, "Next")
    time.sleep(1.5)
    errs = get_validation_errors(driver)
    if errs:
        print(f"  [form] Validation errors after Next: {errs}")
        for round_n in range(4):
            errs = get_validation_errors(driver)
            if not errs:
                break
            print(f"  [form] Fixing: {errs[0]!r}")
            dds = get_visible_dropdowns(driver, max_count=4)
            if dds:
                pick_dropdown(driver, dds[len(dds)-1], f"retry_dd{round_n}")
                time.sleep(1.0)
        click_button(driver, "Next")
        time.sleep(1.5)

    print(f"  [form] URL after Step 2 Next: {driver.current_url}")

    print("\n=== STEP 3 ===")
    try:
        WebDriverWait(driver, 10).until(lambda d: len(get_visible_text_inputs(d)) == 0)
    except TimeoutException:
        print("  [form] WARNING: company text input still visible — may still be on Step 2")

    try:
        WebDriverWait(driver, 10).until(
            lambda d: len(get_visible_dropdowns(d)) >= 1 or
                      len([c for c in d.find_elements(By.XPATH, "//input[@type='checkbox']")
                           if c.is_displayed()]) >= 1)
    except TimeoutException:
        print("  [form] ERROR: Step 3 controls never appeared")
        return False

    ti3 = get_visible_text_inputs(driver)
    dds3 = get_visible_dropdowns(driver, max_count=1)
    print(f"  [form] Text inputs: {len(ti3)}, Dropdowns: {len(dds3)}")

    if dds3:
        pick_dropdown(driver, dds3[0], "interest")

    sleep(0.5, 0.8)

    for name, should_check in [("newsletter", False), ("privacy", True)]:
        try:
            cb = driver.find_element(By.XPATH,
                f"//input[@type='checkbox' and (@name='{name}' or contains(@id,'{name}'))]")
            if cb.is_selected() != should_check:
                driver.execute_script("arguments[0].click();", cb)
                print(f"  [form] {name} → {'checked' if should_check else 'unchecked'}")
            else:
                print(f"  [form] {name} already {'checked' if should_check else 'unchecked'}")
        except NoSuchElementException:
            print(f"  [form] {name} not found")

    click_button(driver, "Submit")
    sleep(4, 6)

    if is_on_confirmation_page(driver):
        print("  [form] ✓ confirmation page reached")
        return True

    print(f"  [form] confirmation not found — {driver.current_url}")
    return False

def click_confirmation_pdf_action(driver) -> bool:
    xpaths = [
        "//a[contains(normalize-space(.), 'View PDF')]",
        "//button[contains(normalize-space(.), 'View PDF')]",
        "//a[contains(normalize-space(.), 'View paper')]",
        "//button[contains(normalize-space(.), 'View paper')]",
        "//a[contains(normalize-space(.), 'Download PDF')]",
        "//button[contains(normalize-space(.), 'Download PDF')]",
        "//a[contains(normalize-space(.), 'Download')]",
        "//button[contains(normalize-space(.), 'Download')]",
        "//a[contains(@href,'.pdf') and not(contains(@style,'display:none'))]",
    ]
    for xpath in xpaths:
        try:
            el = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.5)
            dismiss_overlays(driver)
            try:
                el.click()
            except Exception:
                driver.execute_script("arguments[0].click();", el)
            print(f"  [locked] clicked confirmation-page PDF action via: {xpath}")
            return True
        except Exception:
            continue
    print("  [locked] no clickable PDF action found on confirmation page")
    return False

def wait_for_download(existing_names: set, timeout: int = 90) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        current   = {f.name for f in TEMP_DIR.glob("*") if not f.name.endswith(".crdownload")}
        in_flight = list(TEMP_DIR.glob("*.crdownload"))
        new_files = current - existing_names
        if new_files and not in_flight:
            return list(new_files)[0]
    return None


def download_pdf(driver, pdf_url: str, article_url: str,
                 is_locked: bool, form_url: str | None) -> bool:
    slug  = article_url.rstrip("/").split("/")[-1]
    fname = f"msci_{slugify(slug)}.pdf"
    fpath = OUTPUT_DIR / fname

    if fpath.exists():
        print(f"  [skip] {fname}")
        return True

    try:
        if is_locked and form_url:
            if not submit_unlock_form(driver, form_url):
                print("  [error] form submission failed")
                mark_failed(article_url)
                return False

            apply_cdp_download(driver)
            snap = {f.name for f in TEMP_DIR.glob("*")}
            if not click_confirmation_pdf_action(driver):
                print("  [error] confirmation page had no usable PDF action")
                mark_failed(article_url)
                return False
            downloaded = wait_for_download(snap, timeout=60)
            if not downloaded:
                print(f"  [error] locked click did not download: {fname}")
                mark_failed(article_url)
                return False
        else:
            driver.get(article_url)
            sleep(2, 3)
            dismiss_overlays(driver)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH,
                        "//a[contains(@href,'/downloads/') or contains(@href,'.pdf')]")))
            except TimeoutException:
                pass
            apply_cdp_download(driver)
            snap = {f.name for f in TEMP_DIR.glob("*")}
            actual_url = None
            soup = BeautifulSoup(driver.page_source, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" in href and "/downloads/" in href:
                    actual_url = BASE_URL + href if href.startswith("/") else href
                    actual_url = safe_pdf_url(actual_url)
                    break
            if not actual_url and pdf_url:
                actual_url = safe_pdf_url(pdf_url)
            if not actual_url:
                print("  [error] no PDF URL found on article page")
                mark_failed(article_url)
                return False
            print(f"  [fetching] {unquote(actual_url.split('/')[-1].split('?')[0])}")
            driver.get(actual_url)
            downloaded = wait_for_download(snap, timeout=60)
            if not downloaded:
                print(f"  [error] download failed: {fname}")
                mark_failed(article_url)
                return False

        src = TEMP_DIR / downloaded
        with open(src, "rb") as f:
            if b"%PDF" not in f.read(10):
                print(f"  [error] not a PDF")
                try:
                    src.unlink()
                except Exception:
                    pass
                mark_failed(article_url)
                return False

        src.rename(fpath)
        mode = "locked-click" if is_locked and form_url else "direct"
        print(f"  [OK] {fname} ({fpath.stat().st_size//1024} KB) [{mode}]")
        return True

    except Exception as e:
        print(f"  [error] {fname}: {e}")
        mark_failed(article_url)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None)
    parser.add_argument("--skip-collect", action="store_true")
    args = parser.parse_args()

    if args.delay:
        global MIN_DELAY, MAX_DELAY
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done = load_done()
    print("=" * 60)
    print("MSCI Scraper v26  ·  msci.com  ·  No guessed locked URLs")
    print(f"PDFs   → {OUTPUT_DIR.resolve()}")
    print(f"Logs   → {LOG_DIR.resolve()}")
    print(f"Done   : {len(done)} articles already downloaded")
    print("=" * 60)

    driver = build_driver()
    auth(driver, prompt=True)

    if args.skip_collect:
        article_urls = load_url_cache()
        print(f"\n[Phase 1] Loaded {len(article_urls)} URLs from cache")
    else:
        cached = load_url_cache()
        if cached:
            print(f"\n[Phase 1] Using {len(cached)} cached article URLs")
            article_urls = cached
        else:
            article_urls = collect_article_urls(driver)
            save_url_cache(article_urls)
            print(f"\n[Phase 1] → {len(article_urls)} URLs saved to cache")

    print(f"\n[Phase 2] Processing {len(article_urls)} articles...\n")
    ok = fail = skip = no_pdf = 0

    for i, article_url in enumerate(article_urls, 1):
        if article_url in done:
            skip += 1
            continue

        slug = article_url.rstrip("/").split("/")[-1]
        print(f"[{i}/{len(article_urls)}] {slug}")

        is_locked = False
        form_url  = None
        pdf_url   = None

        for attempt in range(3):
            try:
                is_locked, form_url, pdf_url = get_page_state(driver, article_url)
                break
            except (InvalidSessionIdException, WebDriverException,
                    MaxRetryError, Exception) as e:
                print(f"  [browser crash] {type(e).__name__}: {str(e)[:80]}")
                if attempt < 2:
                    try:
                        driver = resurrect(driver)
                    except Exception as re_err:
                        print(f"  [resurrect failed] {re_err}")
                        break

        if not pdf_url and not form_url:
            print(f"  [no pdf]")
            no_pdf += 1
            mark_failed(article_url)
            sleep()
            continue

        status = "locked" if is_locked else "unlocked"
        display_pdf = unquote(pdf_url.split("/")[-1]) if pdf_url else "(from confirmation page)"
        print(f"  [{status}] → {display_pdf}")

        ok_ = download_pdf(driver, pdf_url or "", article_url, is_locked, form_url)
        if ok_:
            mark_done(article_url)
            ok += 1
        else:
            fail += 1

        sleep()

    driver.quit()
    n_pdf = len(list(OUTPUT_DIR.glob("*.pdf")))
    print("\n" + "=" * 60)
    print(f"Done.  Downloaded: {ok}   Failed: {fail}   No PDF: {no_pdf}   Skipped: {skip}")
    print(f"PDFs on disk : {n_pdf}")
    print(f"PDFs  → {OUTPUT_DIR.resolve()}")
    print(f"Logs  → {LOG_DIR.resolve()}")
    print("=" * 60)

if __name__ == "__main__":
    main()