"""
robeco_scraper.py  —  Robeco Insights  (v2)
--------------------------------------------
Platform: robeco.com/en-us
Auth: One-time disclaimer click (cookie persists for session)
Filter: Content Type → Research + Insight (188 articles)
Listing: infinite scroll via "Show more" button
PDF: direct requests download from files/docm/*.pdf href on article page

Changes from v1:
  - Filter clicks now target <label> elements (checkbox-based filter UI)
  - Content Type dropdown opener tries <legend> tag first
  - Show More XPath broadened

Usage:
  conda activate emi
  python src/robeco_scraper.py [--delay 2.0]
"""

import re
import time
import random
import argparse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    NoSuchElementException,
    InvalidSessionIdException,
    WebDriverException,
)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.robeco.com"
LISTING_URL = BASE_URL + "/en-us/insights/latest-insights"
DISCLAIMER  = BASE_URL + "/en-us"

OUTPUT_DIR = Path("src/data/pdfs/Robeco")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = OUTPUT_DIR / "_done.txt"
FAIL_FILE = OUTPUT_DIR / "_failed.txt"

MIN_DELAY = 2.0
MAX_DELAY = 4.5

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep(lo=None, hi=None):
    time.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))

def slugify(text: str, n: int = 60) -> str:
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

def sync_session(driver, session: requests.Session):
    for c in driver.get_cookies():
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

# ── Driver ─────────────────────────────────────────────────────────────────────

def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1280,900")
    driver = webdriver.Chrome(options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver

def dismiss_overlays(driver):
    try:
        driver.execute_script("""
            document.querySelectorAll(
                '.onetrust-pc-dark-filter, #onetrust-consent-sdk, ' +
                '#onetrust-banner-sdk, [class*="onetrust"], [id*="onetrust"], ' +
                '[class*="cookie"], [id*="cookie"]'
            ).forEach(e => e.remove());
            document.body.style.overflow = '';
            document.body.style.position = '';
        """)
        time.sleep(0.3)
    except Exception:
        pass

def safe_click(driver, element):
    dismiss_overlays(driver)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.4)
    dismiss_overlays(driver)
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)

# ── Browser resurrection ───────────────────────────────────────────────────────

def resurrect(old_driver, prompt: bool = False) -> tuple:
    print("\n[resurrect] Browser died — relaunching...")
    try:
        old_driver.quit()
    except Exception:
        pass
    for attempt in range(1, 4):
        try:
            time.sleep(5 * attempt)
            new_driver = build_driver()
            new_session = auth(new_driver, prompt=prompt)
            print("[resurrect] ✓ Browser restarted\n")
            return new_driver, new_session
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")
    raise RuntimeError("Could not resurrect browser after 3 attempts")

# ── Auth / disclaimer ─────────────────────────────────────────────────────────

def auth(driver, prompt: bool = True) -> requests.Session:
    print(f"[auth] Opening: {DISCLAIMER}")
    driver.get(DISCLAIMER)
    sleep(2, 3)

    # Click "I Agree" disclaimer if present
    for xpath in [
        "//button[contains(normalize-space(text()), 'I Agree')]",
        "//a[contains(normalize-space(text()), 'I Agree')]",
        "//*[contains(normalize-space(text()), 'I Agree')]",
    ]:
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            safe_click(driver, btn)
            print("[auth] ✓ Disclaimer accepted")
            sleep(1, 2)
            break
        except TimeoutException:
            continue

    if prompt:
        print("\n>>> If any additional login or region prompt appeared, handle it now.")
        print(">>> Press ENTER when the Robeco insights page is accessible.\n")
        input(">>> ")

    ua = driver.execute_script("return navigator.userAgent;")
    session = requests.Session()
    session.headers.update({
        "User-Agent": ua,
        "Referer":    BASE_URL,
        "Accept":     "application/pdf,*/*;q=0.8",
    })
    sync_session(driver, session)
    print("[auth] ✓ Session synced\n")
    return session

# ── Filter + collect all article URLs ─────────────────────────────────────────

def collect_article_urls(driver) -> list[str]:
    print(f"[listing] Loading: {LISTING_URL}")
    driver.get(LISTING_URL)
    sleep(3, 5)
    dismiss_overlays(driver)

    # ── Open Content Type filter ───────────────────────────────────────────────
    print("[listing] Opening Content Type filter...")
    opened = False
    for xpath in [
        "//legend[contains(normalize-space(.), 'Content type')]",
        "//legend[contains(normalize-space(.), 'CONTENT TYPE')]",
        "//legend[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'content type')]",
        "//button[contains(normalize-space(.), 'Content')]",
        "//button[contains(normalize-space(.), 'CONTENT')]",
        "//*[contains(@class,'filter') and contains(normalize-space(.), 'ontent')]",
    ]:
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            safe_click(driver, btn)
            sleep(1, 2)
            print("[listing] ✓ Content Type dropdown opened")
            opened = True
            break
        except TimeoutException:
            continue

    if not opened:
        print("[listing] [warn] Could not open Content Type filter — debug dump:")
        elems = driver.find_elements(
            By.XPATH,
            "//*[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'content')]"
        )
        for e in elems[:10]:
            print(f"  TAG={e.tag_name} TEXT={e.text!r} CLASS={e.get_attribute('class')!r}")

    # ── Select Research ────────────────────────────────────────────────────────
    print("[listing] Selecting Research...")
    try:
        research_label = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//label[contains(normalize-space(.), 'Research')]")
            )
        )
        safe_click(driver, research_label)
        sleep(1, 2)
        print("[listing] ✓ Research selected")
    except TimeoutException:
        print("[listing] [warn] Research label not found — debug dump:")
        elems = driver.find_elements(By.XPATH, "//label")
        for e in elems[:15]:
            print(f"  LABEL text={e.text!r} for={e.get_attribute('for')!r}")

    # ── Select Insight ─────────────────────────────────────────────────────────
    print("[listing] Selecting Insight...")
    try:
        insight_label = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//label[contains(normalize-space(.), 'Insight')]")
            )
        )
        safe_click(driver, insight_label)
        sleep(1, 2)
        print("[listing] ✓ Insight selected")
    except TimeoutException:
        print("[listing] [warn] Insight label not found")

    # Close dropdown by clicking body
    try:
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass
    sleep(2, 3)

    # ── Show More loop ─────────────────────────────────────────────────────────
    show_more_clicks = 0
    while True:
        try:
            show_more = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, (
                    "//button[contains(normalize-space(.), 'Show more')] | "
                    "//button[contains(normalize-space(.), 'show more')] | "
                    "//button[contains(normalize-space(.), 'Load more')] | "
                    "//*[contains(@class,'show-more')] | "
                    "//*[contains(@class,'load-more')]"
                )))
            )
            safe_click(driver, show_more)
            show_more_clicks += 1
            sleep(2, 3)
            print(f"  [show more] click {show_more_clicks}")
        except TimeoutException:
            print(f"  [show more] exhausted after {show_more_clicks} clicks")
            break

    # ── Collect article URLs ───────────────────────────────────────────────────
    soup = BeautifulSoup(driver.page_source, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.match(r"^/en-us/insights/\d{4}/\d{2}/.+", href):
            urls.add(BASE_URL + href)

    print(f"[listing] → {len(urls)} article URLs collected\n")
    return list(urls)

# ── Get PDF URL from article page ─────────────────────────────────────────────

def get_pdf_url(driver, article_url: str) -> str | None:
    driver.get(article_url)
    sleep(2, 3)
    dismiss_overlays(driver)

    # Wait for page to fully render JS content
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, '.pdf')]")
            )
        )
    except TimeoutException:
        pass  # No PDF link visible — will return None below

    # Parse fully rendered page source
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Primary: /files/docm/ pattern
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/files/docm/" in href and href.endswith(".pdf"):
            return BASE_URL + href if href.startswith("/") else href

    # Secondary: ctfassets CDN (Contentful)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "ctfassets.net" in href and href.endswith(".pdf"):
            return href

    # Fallback: any .pdf link
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".pdf"):
            return BASE_URL + href if href.startswith("/") else href

    return None
# ── PDF download ──────────────────────────────────────────────────────────────

def download_pdf(session: requests.Session, pdf_url: str,
                 title: str, retries: int = 3) -> bool:
    fname = f"robeco_{slugify(title)}.pdf"
    fpath = OUTPUT_DIR / fname

    if fpath.exists():
        print(f"    [skip] {fname}")
        return True

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                pdf_url, stream=True, timeout=60,
                headers={"Referer": BASE_URL},
            )
            resp.raise_for_status()
            content = b"".join(resp.iter_content(8192))

            if len(content) < 5000 or b"%PDF" not in content[:10]:
                print(f"    [warn] not a valid PDF ({len(content)} bytes): {fname}")
                return False

            fpath.write_bytes(content)
            print(f"    [✓] {fname} ({fpath.stat().st_size // 1024} KB)")
            return True

        except Exception as e:
            if attempt < retries:
                wait = 5 * attempt
                print(f"    [retry {attempt}/{retries}] {e} — waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"    [error] {fname}: {e}")
                mark_failed(pdf_url)
                return False

    return False

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None)
    args = parser.parse_args()

    if args.delay:
        global MIN_DELAY, MAX_DELAY
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done = load_done()

    print("=" * 60)
    print("Robeco Scraper v2  ·  robeco.com/en-us  ·  Research+Insight")
    print(f"{len(done)} articles already downloaded")
    print("=" * 60)

    driver  = build_driver()
    session = auth(driver, prompt=True)

    article_urls = collect_article_urls(driver)

    ok = fail = skip = 0

    for i, article_url in enumerate(article_urls, 1):
        print(f"\n[{i}/{len(article_urls)}] {article_url}")

        if article_url in done:
            skip += 1
            print("  [skip] already done")
            continue

        pdf_url = None
        for attempt in range(3):
            try:
                pdf_url = get_pdf_url(driver, article_url)
                break
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"  [browser crash] {e}")
                if attempt < 2:
                    driver, session = resurrect(driver)
                else:
                    break

        if not pdf_url:
            print("  [warn] no PDF link found — skipping")
            mark_failed(article_url)
            fail += 1
            sleep()
            continue

        print(f"  PDF: {pdf_url}")

        # Title from URL slug
        slug  = article_url.rstrip("/").split("/")[-1]
        title = slug.replace("-", " ")

        sync_session(driver, session)
        ok_ = download_pdf(session, pdf_url, title)
        if ok_:
            mark_done(article_url)
            ok += 1
        else:
            fail += 1

        sleep()

    driver.quit()

    print("\n" + "=" * 60)
    print(f"Done.  Downloaded: {ok}   Failed: {fail}   Skipped: {skip}")
    print(f"PDFs: {OUTPUT_DIR.resolve()}")
    print("=" * 60)

if __name__ == "__main__":
    main()