"""
ssga_scraper.py  (v3)
=====================
SSGA Factor + ESG research scraper.

Architecture (confirmed from DevTools inspection):
  1. Selenium loads the filtered listing page, clicks "Load more" exhausting
     all articles — using correct selector a[href*="/insights/"]
  2. For each article landing page (SSR): requests + BS4 extracts:
       <a aria-label="Download" href="/us/en/institutional/library-content/...pdf">
  3. Download PDF. URL prefix is /us/en/institutional/library-content/ (not /library-content/)

Filter URL scopes to: PDF type + Research + factor/ESG topics.
Saves article URLs to src/logs/ssga_article_urls.txt for review.

Run: python src/ssga_scraper.py
"""

import time
import re
import logging
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT        = Path(__file__).resolve().parent.parent
PDF_DIR     = ROOT / "src" / "data" / "pdfs" / "ssga"
LOG_DIR     = ROOT / "src" / "logs"
URLS_LOG    = LOG_DIR / "ssga_article_urls.txt"
RESUME_LOG  = LOG_DIR / "ssga_downloaded.txt"

for d in [PDF_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "ssga_scraper.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

def console(msg):
    print(msg)
    log.info(msg)

# ── Config ────────────────────────────────────────────────────────────────────

BASE = "https://www.ssga.com"

# Filter: PDF + Research type — scoped to factor/ESG/systematic/sustainable topics.
# 540 total with pdf~research; we'll filter at download time by topic.
# To keep manageable, scope to specific investment topics via the filter.
# "investment-topic/esg" "investment-topic/factor-investing" etc.
# Use the broader pdf~research filter and filter by topic at scrape time.
LISTING_URL = (
    BASE + "/us/en/institutional/insights"
    "?g=media-type%3Apdf~research"
)

# Topics to keep — checked against article URL slug or page content
TOPIC_KEYWORDS = [
    "factor", "esg", "sustainable", "systematic", "smart-beta",
    "quant", "stewardship", "climate", "rfactor", "r-factor",
    "green", "responsible", "fixed-income", "credit",
    "asset-allocation", "portfolio-construction", "emerging-market",
]

DELAY = 1.0

# ── Selenium: get article landing page URLs ───────────────────────────────────

def setup_driver():
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(
        options=opts,
        seleniumwire_options={"suppress_connection_errors": True}
    )
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

def harvest_article_urls() -> list[str]:
    driver = setup_driver()
    try:
        console(f"Loading listing: {LISTING_URL}")
        driver.get(LISTING_URL)
        time.sleep(5)

        # Handle popup
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                    "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                    "'abcdefghijklmnopqrstuvwxyz'),'institutional') "
                    "and (self::button or self::a or self::li or "
                    "self::div[@role='button'] or self::span)]"
                ))
            )
            btn.click()
            console("  Clicked institutional popup.")
            time.sleep(3)
        except TimeoutException:
            console("  No popup — injecting cookie.")
            driver.add_cookie({
                "name": "roleproduct", "value": "institutional",
                "domain": "www.ssga.com", "path": "/"
            })
            driver.refresh()
            time.sleep(4)

        # Wait for articles — correct selector: links inside results list
        console("Waiting for articles...")
        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "button.ssmp-load-more, li.resource-item, .results-list li")
                )
            )
            console("  Articles visible.")
        except TimeoutException:
            console("  Timeout — proceeding anyway.")

        # Click Load More until gone
        clicks = 0
        while True:
            count = driver.execute_script(
                "return document.querySelectorAll("
                "'.results-list a[href*=\"/insights/\"], "
                "a[aa-global-ctalink][href*=\"/insights/\"]').length;"
            )
            try:
                load_more = WebDriverWait(driver, 12).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.ssmp-load-more"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", load_more)
                clicks += 1
                console(f"  Load more #{clicks} — article links so far: {count}")
                time.sleep(4)
            except TimeoutException:
                console(f"  No more 'Load more'. Total clicks: {clicks}, final count: {count}")
                break

        # Extract all article landing page URLs
        time.sleep(2)
        urls = driver.execute_script("""
            var seen = new Set();
            var results = [];
            var selectors = [
                '.results-list a[href*="/insights/"]',
                'a[aa-global-ctalink][href*="/insights/"]',
                'a[href*="/us/en/institutional/insights/"]',
                '.result-item a',
                'li.resource-item a'
            ];
            selectors.forEach(function(sel) {
                document.querySelectorAll(sel).forEach(function(a) {
                    var h = a.getAttribute('href') || '';
                    // Must be an article page (not the listing itself)
                    if (h.includes('/insights/') &&
                        !h.includes('?g=') &&
                        !h.includes('#') &&
                        h.split('/insights/')[1] &&
                        h.split('/insights/')[1].length > 3 &&
                        !seen.has(h)) {
                        seen.add(h);
                        var full = h.startsWith('http') ? h : 'https://www.ssga.com' + h;
                        results.push(full);
                    }
                });
            });
            return results;
        """)

        console(f"\n  Article URLs found: {len(urls) if urls else 0}")
        return urls or []

    finally:
        driver.quit()

# ── Requests: extract PDF URL from landing page ───────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,*/*",
})

def extract_pdf_url(article_url: str) -> str | None:
    """Fetch SSR landing page, find the Download link href."""
    try:
        r = SESSION.get(article_url, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        # <a aria-label="Download" href="/us/en/institutional/library-content/...pdf">
        a = soup.find("a", {"aria-label": "Download"})
        if a and a.get("href", "").endswith(".pdf"):
            href = a["href"]
            return BASE + href if href.startswith("/") else href
        # Fallback: any link with library-content and .pdf
        for a in soup.find_all("a", href=True):
            if "library-content" in a["href"] and a["href"].endswith(".pdf"):
                href = a["href"]
                return BASE + href if href.startswith("/") else href
        return None
    except Exception:
        return None

# ── Download ──────────────────────────────────────────────────────────────────

DL_SESSION = requests.Session()
DL_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.ssga.com/us/en/institutional/insights",
    "Accept": "application/pdf,*/*",
})

def load_done() -> set:
    if not RESUME_LOG.exists():
        return set()
    return set(RESUME_LOG.read_text(encoding="utf-8").splitlines())

def mark_done(key: str):
    with open(RESUME_LOG, "a", encoding="utf-8") as f:
        f.write(key + "\n")

def is_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"

def safe_filename(url: str) -> str:
    parts = url.split("/")
    slug  = parts[-1]
    year  = next((p for p in parts if p.isdigit() and len(p) == 4), "")
    topic = parts[-3] if len(parts) >= 3 else ""
    prefix = f"{topic}_{year}_" if year and topic else ""
    return re.sub(r"[^\w\-.]", "_", prefix + slug)

def download_pdf(url: str) -> str:
    try:
        r = DL_SESSION.get(url, timeout=30)
        if r.status_code != 200:
            return f"error:HTTP{r.status_code}"
        if not is_pdf(r.content):
            return "not_pdf"
        fname = safe_filename(url)
        fpath = PDF_DIR / fname
        if fpath.exists():
            fpath = PDF_DIR / fname.replace(".pdf", f"_2.pdf")
        fpath.write_bytes(r.content)
        return "ok"
    except Exception as e:
        return f"error:{e}"

# ── Topic filter ──────────────────────────────────────────────────────────────

def is_in_scope(article_url: str, pdf_url: str) -> bool:
    """Keep only factor/ESG/systematic/sustainable content."""
    combined = (article_url + " " + (pdf_url or "")).lower()
    return any(kw in combined for kw in TOPIC_KEYWORDS)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = datetime.now()
    console(f"\n{'='*55}")
    console(f"SSGA Scraper v3 -- {start:%Y-%m-%d %H:%M}")
    console(f"PDFs -> {PDF_DIR}")
    console(f"{'='*55}\n")

    # Phase 1: get article URLs via Selenium
    article_urls = harvest_article_urls()

    if not article_urls:
        console("No article URLs found. Exiting.")
        return

    # Save for review
    URLS_LOG.write_text("\n".join(article_urls), encoding="utf-8")
    console(f"\nArticle URLs saved to: {URLS_LOG}")
    console(f"Total articles: {len(article_urls)}")

    # Phase 2: for each article, extract PDF URL and download
    done  = load_done()
    stats = {"ok": 0, "skip": 0, "out_of_scope": 0, "no_pdf": 0, "error": 0}

    for i, article_url in enumerate(article_urls, 1):
        if article_url in done:
            stats["skip"] += 1
            continue

        # Extract PDF URL from landing page
        pdf_url = extract_pdf_url(article_url)
        time.sleep(0.4)

        if not pdf_url:
            stats["no_pdf"] += 1
            mark_done(article_url)
            continue

        console(f"[{i}/{len(article_urls)}] {pdf_url.split('/')[-1]}")
        result = download_pdf(pdf_url)
        time.sleep(DELAY)

        if result == "ok":
            console(f"  [ok]")
            stats["ok"] += 1
            mark_done(article_url)
        elif result == "not_pdf":
            console(f"  [x] not PDF")
            stats["not_pdf"] += 1
            mark_done(article_url)
        else:
            console(f"  [x] {result}")
            stats["error"] += 1

    elapsed = datetime.now() - start
    console(f"\n{'='*55}")
    console(
        f"Done.  Downloaded: {stats['ok']}   "
        f"Out of scope: {stats['out_of_scope']}   "
        f"No PDF: {stats['no_pdf']}   "
        f"Failed: {stats['error']}   "
        f"Skipped: {stats['skip']}"
    )
    console(f"Elapsed: {elapsed}")
    console(f"PDFs: {PDF_DIR}")
    console(f"{'='*55}\n")

if __name__ == "__main__":
    main()