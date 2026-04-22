"""
dfa_scraper.py  —  Dimensional Fund Advisors  (v2)
---------------------------------------------------
Platform: dimensional.com/us-en
Auth: None required
Filter: Research tab only (91 items)
Listing: pagination via "More results" button
Output: .txt files saved to gap/data/Other_Corpus/DFA/

Script lives at gap/src/scrapers/dfa_scraper.py
  .parent       → gap/src/scrapers/
  .parent.parent → gap/src/
  .parent.parent.parent → gap/

Usage:
  conda activate emi
  cd Desktop/gap
  python src/scrapers/dfa_scraper.py [--delay 2.0]
"""

import re
import time
import random
import argparse
from pathlib import Path

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    InvalidSessionIdException,
    WebDriverException,
)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.dimensional.com"
LISTING_URL = BASE_URL + "/us-en/insights#t=catResearch&sort=@publishdate%20descending"

# Script lives at gap/src/scrapers/ — three .parent calls reach gap/
REPO_ROOT  = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_ROOT / "data" / "Other_Corpus" / "DFA"
LOG_DIR    = REPO_ROOT / "data" / "logs" / "DFA"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = LOG_DIR / "_done.txt"
FAIL_FILE = LOG_DIR / "_failed.txt"

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

def html_to_text(element) -> str:
    """Extract clean text from a BS4 element, preserving paragraph breaks."""
    for tag in element.find_all(["p", "h1", "h2", "h3", "h4", "h5", "li", "br"]):
        tag.insert_before("\n\n")
    text = element.get_text(separator="", strip=False)
    return re.sub(r"\n{3,}", "\n\n", text).strip()

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

def resurrect(old_driver) -> webdriver.Chrome:
    print("\n[resurrect] Browser died — relaunching...")
    try:
        old_driver.quit()
    except Exception:
        pass
    for attempt in range(1, 4):
        try:
            time.sleep(5 * attempt)
            new_driver = build_driver()
            print("[resurrect] ✓ Browser restarted\n")
            return new_driver
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")
    raise RuntimeError("Could not resurrect browser after 3 attempts")

# ── Collect all article URLs ───────────────────────────────────────────────────

def collect_article_urls(driver) -> list[str]:
    print(f"[listing] Loading: {LISTING_URL}")
    driver.get(LISTING_URL)
    sleep(3, 5)
    dismiss_overlays(driver)

    # Click Research tab
    print("[listing] Clicking Research tab...")
    try:
        research_tab = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[normalize-space(.)='Research'] | //button[normalize-space(.)='Research'] | //li[normalize-space(.)='Research']")
            )
        )
        safe_click(driver, research_tab)
        sleep(3, 5)
        print("[listing] ✓ Research tab clicked")
    except TimeoutException:
        print("[listing] [warn] Research tab not found")

    # More Results loop
    more_clicks = 0
    while True:
        try:
            more_btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.coveo-headless-results-show-more-button")
                )
            )
            safe_click(driver, more_btn)
            more_clicks += 1
            sleep(2, 3)
            print(f"  [more results] click {more_clicks}")
        except TimeoutException:
            print(f"  [more results] exhausted after {more_clicks} clicks")
            break

    # Collect article URLs
    soup = BeautifulSoup(driver.page_source, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.match(r"^https://www\.dimensional\.com/us-en/insights/[^#?]+$", href):
            urls.add(href)

    print(f"[listing] → {len(urls)} article URLs collected\n")
    return list(urls)

# ── Scrape one article → .txt ──────────────────────────────────────────────────

def scrape_article(driver, article_url: str) -> bool:
    slug  = article_url.rstrip("/").split("/")[-1]
    fname = f"dfa_{slugify(slug)}.txt"
    fpath = OUTPUT_DIR / fname

    if fpath.exists():
        print(f"    [skip] {fname}")
        return True

    try:
        driver.get(article_url)
        sleep(2, 3)
        dismiss_overlays(driver)

        # Wait for h1 to exist, then wait for at least one <p> inside the page
        # to confirm JS has finished rendering the article body.
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//h1"))
            )
        except TimeoutException:
            print(f"    [warn] h1 never appeared: {fname}")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.content-page-body p, div.rtf-container p")
                )
            )
        except TimeoutException:
            print(f"    [warn] no <p> in content-page-body after 15s: {fname}")

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Title
        title_el = soup.find("h1")
        title = title_el.get_text(strip=True) if title_el else slug

        # Probe confirmed: content lives in div.content-page-body;
        # strip disclaimer/disclosure sections before extracting text.
        content = soup.select_one("div.content-page-body")
        if not content:
            content = soup.select_one("div.content-column")   # one level up, fallback
        if not content:
            content = soup.body

        # Remove legal boilerplate that sits inside the content container
        for junk in content.select(
            "div.content-page-disclaimer, div.content-page-disclosures-content, footer"
        ):
            junk.decompose()

        body = html_to_text(content)
        print(f"    [{len(body):,} chars]")

        if len(body) < 100:
            print(f"    [warn] content too short ({len(body)} chars): {fname}")
            mark_failed(article_url)
            return False

        text_out = f"SOURCE: {article_url}\nTITLE: {title}\n\n{body}"
        fpath.write_text(text_out, encoding="utf-8")
        print(f"    [✓] {fname} | {title[:60]}")
        return True

    except Exception as e:
        print(f"    [error] {slug}: {e}")
        mark_failed(article_url)
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
    print("DFA Scraper v2  ·  dimensional.com  ·  TXT output")
    print(f"Output : {OUTPUT_DIR.resolve()}")
    print(f"Done   : {len(done)} articles already saved")
    print("=" * 60)

    driver = build_driver()
    ok = fail = skip = 0

    try:
        print("\n[1/2] Collecting all Research article URLs...")
        article_urls = collect_article_urls(driver)

        if not article_urls:
            print("[abort] No article URLs found.")
            return

        print(f"[2/2] Scraping {len(article_urls)} articles...\n")

        for i, article_url in enumerate(article_urls, 1):
            print(f"\n[{i}/{len(article_urls)}] {article_url}")

            if article_url in done:
                skip += 1
                print("  [skip] already done")
                continue

            ok_ = False
            for attempt in range(3):
                try:
                    ok_ = scrape_article(driver, article_url)
                    break
                except (InvalidSessionIdException, WebDriverException) as e:
                    print(f"  [browser crash] {e}")
                    if attempt < 2:
                        driver = resurrect(driver)
                    else:
                        mark_failed(article_url)
                        break

            if ok_:
                mark_done(article_url)
                ok += 1
            else:
                fail += 1

            sleep()

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    n_txt = len(list(OUTPUT_DIR.glob("*.txt")))
    print("\n" + "=" * 60)
    print(f"Done.  Saved: {ok}   Failed: {fail}   Skipped: {skip}")
    print(f"TXTs on disk : {n_txt}")
    print(f"Output : {OUTPUT_DIR.resolve()}")
    if fail:
        print(f"Failures : {FAIL_FILE.resolve()}")
    print("=" * 60)


if __name__ == "__main__":
    main()