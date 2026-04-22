"""
man_scraper.py  —  Man Institute / Man Group
---------------------------------------------
Scrapes research PDFs from man.com/maninstitute.

PDF URLs have opaque hash IDs and cannot be derived from article slugs,
so we must visit each article page to find the download link.

Pipeline:
  1. Selenium: load man.com/maninstitute, click "Show more" until exhausted
  2. Collect all article hrefs
  3. Per article: visit page, find a[href*="documents/download"] link
  4. Download PDF via requests

Note: Many articles are web-only (no PDF). Those are skipped gracefully.
The corpus may have sparse citations — include now, filter at analysis time.

Usage:
  conda activate emi
  python src/man_scraper.py [--delay 2.0]

Output: src/data/pdfs/MAN/man_{slug}.pdf
"""

import re
import time
import random
import argparse
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    InvalidSessionIdException,
    WebDriverException,
)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.man.com"
LISTING_URL = BASE_URL + "/maninstitute"

OUTPUT_DIR  = Path("src/data/pdfs/MAN")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE    = OUTPUT_DIR / "_done.txt"
FAIL_FILE   = OUTPUT_DIR / "_failed.txt"
NOPDF_FILE  = OUTPUT_DIR / "_no_pdf.txt"   # articles visited but no PDF found

MIN_DELAY = 2.0
MAX_DELAY = 4.0

# Article links: /insights/{slug} or /maninstitute/{slug}
ARTICLE_PATTERN = re.compile(
    r"^/(insights|maninstitute)/[a-z0-9][a-z0-9-]+$"
)

# PDF download links
PDF_PATTERN = re.compile(r"/documents/download/")

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep(lo=None, hi=None):
    time.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))

def slugify(text: str, n: int = 70) -> str:
    s = re.sub(r"[^\w-]", "_", text.lower()).strip("_")
    return s[:n]

def load_done() -> set:
    return set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()

def load_nopdf() -> set:
    return set(NOPDF_FILE.read_text().splitlines()) if NOPDF_FILE.exists() else set()

def mark_done(url: str):
    with open(LOG_FILE, "a") as f:
        f.write(url + "\n")

def mark_failed(url: str, reason: str = ""):
    with open(FAIL_FILE, "a") as f:
        f.write(f"{url}  # {reason}\n")

def mark_nopdf(url: str):
    with open(NOPDF_FILE, "a") as f:
        f.write(url + "\n")

def article_url_to_fname(article_url: str) -> str:
    slug = urlparse(article_url).path.rstrip("/").split("/")[-1]
    return f"man_{slugify(slug)}.pdf"

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
                '[class*="cookie"],[class*="consent"],[id*="cookie"],
                 [class*="onetrust"],[id*="onetrust"],[class*="modal"]'
            ).forEach(e => e.remove());
            document.body.style.overflow = '';
        """)
    except Exception:
        pass

def safe_click(driver, element):
    dismiss_overlays(driver)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.4)
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)

def sync_session(driver, session: requests.Session):
    for c in driver.get_cookies():
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

# ── Stage 1: collect article URLs via Show More clicks ────────────────────────

def get_all_article_urls(driver) -> list[str]:
    print(f"  Loading: {LISTING_URL}")
    driver.get(LISTING_URL)
    sleep(3, 5)
    dismiss_overlays(driver)

    seen = set()
    show_more_clicks = 0

    def harvest():
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Normalise to path
            if href.startswith("http"):
                path = urlparse(href).path
            else:
                path = href
            path = path.rstrip("/")
            if ARTICLE_PATTERN.match(path) and path not in seen:
                seen.add(path)
                new += 1
        return new

    # Initial harvest
    harvest()
    print(f"  Initial: {len(seen)} articles")

    # Click "Show more" until it disappears
    while True:
        show_more = None
        for xpath in [
            "//button[contains(translate(text(),'SHOWMRE','showmre'),'show more')]",
            "//a[contains(translate(text(),'SHOWMRE','showmre'),'show more')]",
            "//button[contains(@class,'load-more')]",
            "//button[contains(@class,'show-more')]",
            "//*[@data-action='load-more']",
        ]:
            try:
                show_more = driver.find_element(By.XPATH, xpath)
                break
            except NoSuchElementException:
                continue

        if show_more is None:
            print(f"  No 'Show more' button found — done ({len(seen)} total)")
            break

        safe_click(driver, show_more)
        show_more_clicks += 1
        sleep(2, 3)

        new = harvest()
        print(f"  [click {show_more_clicks}] +{new} articles (total: {len(seen)})")

        if new == 0:
            # Clicked but nothing new loaded — probably at the end
            print(f"  No new articles after click — stopping")
            break

        if show_more_clicks > 100:  # safety cap
            break

    return [urljoin(BASE_URL, p) for p in seen]

# ── Stage 2: find PDF download link on article page ───────────────────────────

def find_pdf_url(driver, article_url: str) -> str | None:
    """Visit article page and find the download link. Returns None if no PDF."""
    driver.get(article_url)
    sleep(1.5, 2.5)
    dismiss_overlays(driver)

    # Wait for page content
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "article"))
        )
    except TimeoutException:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Look for download links with /documents/download/ in href
    for a in soup.find_all("a", href=PDF_PATTERN):
        href = a["href"]
        if href.startswith("http"):
            return href
        return urljoin(BASE_URL, href)

    # Also check for links ending in .pdf
    for a in soup.find_all("a", href=re.compile(r"\.pdf($|\?)")):
        href = a["href"]
        if "man.com" in href or href.startswith("/"):
            return href if href.startswith("http") else urljoin(BASE_URL, href)

    return None

# ── Stage 3: download PDF ─────────────────────────────────────────────────────

def download_pdf(session: requests.Session, pdf_url: str,
                 article_url: str, retries: int = 3) -> bool:
    fname = article_url_to_fname(article_url)
    fpath = OUTPUT_DIR / fname

    if fpath.exists():
        print(f"    [skip] {fname}")
        return True

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                pdf_url, stream=True, timeout=60,
                headers={"Referer": article_url},
                allow_redirects=True,
            )
            resp.raise_for_status()
            content = b"".join(resp.iter_content(8192))

            if len(content) < 5000 or b"%PDF" not in content[:10]:
                print(f"    [warn] not a PDF ({len(content)} bytes): {fname}")
                mark_failed(pdf_url, "not a pdf")
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
                mark_failed(pdf_url, str(e))
                return False

    return False

# ── Browser resurrection ───────────────────────────────────────────────────────

def resurrect(old_driver) -> tuple:
    print("\n[resurrect] Browser died — relaunching...")
    try:
        old_driver.quit()
    except Exception:
        pass
    for attempt in range(1, 4):
        try:
            time.sleep(5 * attempt)
            new_driver = build_driver()
            new_driver.get(LISTING_URL)  # re-establish session
            time.sleep(3)
            print("[resurrect] ✓ Browser restarted\n")
            return new_driver
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")
    raise RuntimeError("Could not resurrect browser — rerun manually")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None)
    args = parser.parse_args()

    if args.delay:
        global MIN_DELAY, MAX_DELAY
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done   = load_done()
    nopdf  = load_nopdf()

    print("=" * 60)
    print("Man Institute Scraper  ·  man.com/maninstitute")
    print(f"{len(done)} PDFs already downloaded  |  "
          f"{len(nopdf)} articles confirmed no-PDF")
    print("=" * 60)

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": BASE_URL,
    })

    driver = build_driver()

    # Stage 1: collect all article URLs
    print("\n[1/3] Collecting article URLs...")
    article_urls = get_all_article_urls(driver)
    print(f"  → {len(article_urls)} articles found\n")

    # Stage 2 + 3: per-article — find PDF URL, download
    print("[2/3] Finding PDF links and downloading...")
    ok = fail = skip = nopdf_count = 0

    for i, article_url in enumerate(article_urls, 1):
        # Skip if already downloaded
        if article_url in done:
            skip += 1
            continue

        # Skip if we already know there's no PDF
        if article_url in nopdf:
            nopdf_count += 1
            continue

        print(f"\n[{i}/{len(article_urls)}] {article_url}")

        # Find PDF URL (with browser resurrection on crash)
        pdf_url = None
        for attempt in range(3):
            try:
                pdf_url = find_pdf_url(driver, article_url)
                break
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"  [browser crash] {e}")
                if attempt < 2:
                    driver = resurrect(driver)
                    sync_session(driver, session)
                else:
                    print("  [give up] skipping article")
                    break

        if pdf_url is None:
            print(f"  [no pdf] {article_url.split('/')[-1]}")
            mark_nopdf(article_url)
            nopdf_count += 1
            sleep(1.0, 2.0)  # shorter delay for no-PDF articles
            continue

        print(f"  PDF: {pdf_url[-80:]}")
        sync_session(driver, session)

        if download_pdf(session, pdf_url, article_url):
            mark_done(article_url)
            ok += 1
        else:
            fail += 1

        sleep()

    driver.quit()

    print("\n" + "=" * 60)
    print(f"Done.  Downloaded: {ok}   Failed: {fail}   "
          f"No PDF: {nopdf_count}   Skipped: {skip}")
    print(f"PDFs: {OUTPUT_DIR.resolve()}")
    print("=" * 60)

if __name__ == "__main__":
    main()