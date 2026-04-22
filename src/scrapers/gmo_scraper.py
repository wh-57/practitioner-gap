"""
scraper_gmo.py
==============
Scrapes GMO research library using the internal JSON API:
  GET /api/articles/getArticlesResearchLibrary?uid=...&isGmo=...&currentPage=N&type=ID

Target types and confirmed IDs:
  GMO Quarterly Letter : 3597
  White Papers         : 3598
  Market Commentary    : 3599  (inferred from sequential pattern)
  Insights             : 3600
  Viewpoints           : 3601

Output: src/data/pdfs/gmo/
Log:    src/data/pdfs/gmo/gmo_scrape_log.csv

Token note:
  uid and isGmo appear to be static site-wide keys (not per-session).
  They are hardcoded below. If requests start returning 401/403, refresh
  them from DevTools > Network > getArticlesResearchLibrary > Request URL.

Usage:
    python src/gmo_scraper.py                          # full corpus
    python src/gmo_scraper.py --no-headless            # visible browser
    python src/gmo_scraper.py --max-articles 3 --types whitepapers   # test
    python src/gmo_scraper.py --types insights,viewpoints            # subset
    python src/gmo_scraper.py --uid NEW --isgmo NEW                  # refresh tokens
"""

import os
import re
import csv
import time
import math
import json
import argparse
import hashlib
import logging
from datetime import datetime
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LIBRARY_URL = "https://www.gmo.com/americas/research-library/"
API_URL     = "https://www.gmo.com/api/articles/getArticlesResearchLibrary"
OUTPUT_DIR  = os.path.join("src", "data", "pdfs", "gmo")
LOG_FILE    = os.path.join(OUTPUT_DIR, "gmo_scrape_log.csv")

# Static site-wide tokens (refresh from DevTools if they expire)
DEFAULT_UID   = "IFvERLSOhMc7o8utLHtS48pSwOF2Anoemb86tJLOLYk="
DEFAULT_ISGMO = "9ObHuFINBcbr/IN1UQEB7jbuzBKzHVpYqfRZjBbE8Ks="

# Confirmed type IDs from browser URL bar
TYPE_MAP = {
    "quarterly":   ("GMO Quarterly Letter", 3597),
    "whitepapers": ("White Papers",         3598),
    "commentary":  ("Market Commentary",    3599),  # inferred; verify if 0 results
    "insights":    ("Insights",             3600),
    "viewpoints":  ("Viewpoints",           3601),
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gmo_scraper")

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    return webdriver.Chrome(options=opts)

# ---------------------------------------------------------------------------
# Overlay dismissal
# ---------------------------------------------------------------------------

def dismiss_overlays(driver):
    js = (
        "var els = document.querySelectorAll("
        + "'[class*=\"cookie\"],[class*=\"consent\"],[id*=\"cookie\"],[id*=\"overlay\"]'"
        + "); els.forEach(function(e){ e.style.display='none'; });"
    )
    try:
        driver.execute_script(js)
    except Exception:
        pass
    for sel in ["button[id*='accept']", "button[class*='accept']",
                "button[class*='agree']", "a[class*='accept']"]:
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            time.sleep(0.5)
        except (NoSuchElementException, ElementNotInteractableException):
            pass

# ---------------------------------------------------------------------------
# Warm-up: get session cookies
# ---------------------------------------------------------------------------

def warmup(driver) -> dict:
    log.info("Warming up — loading GMO homepage for session cookies...")
    driver.get("https://www.gmo.com/americas/")
    time.sleep(3)
    dismiss_overlays(driver)
    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    log.info("  %d cookies acquired.", len(cookies))
    return cookies

# ---------------------------------------------------------------------------
# Phase 2: API pagination
# ---------------------------------------------------------------------------

def fetch_all_articles(uid: str, isgmo: str, type_id: int, type_label: str,
                       session: requests.Session,
                       api_delay: float = 1.0) -> list[dict]:
    """Page through the API and return all unlocked article records."""
    articles = []
    page     = 1

    while True:
        try:
            r = session.get(API_URL, params={
                "uid": uid, "isGmo": isgmo,
                "currentPage": page, "type": type_id,
            }, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error("  API error on page %d: %s", page, e)
            break

        listing     = data.get("listing", [])
        total       = data.get("totalResults", 0)
        page_size   = data.get("pageSize", 12)
        total_pages = math.ceil(total / page_size) if page_size else 1

        log.info("  API page %d/%d — %d items (total: %d)",
                 page, total_pages, len(listing), total)

        if page == 1 and total == 0:
            log.warning(
                "  Zero results for type '%s' (ID %d). "
                "If this is Market Commentary, the ID 3599 may be wrong — "
                "check the URL bar in your browser and pass correct ID via --commentary-id.",
                type_label, type_id
            )
            break

        for item in listing:
            if item.get("Lock", False):
                log.info("    [LOCKED] %s", item.get("Title", ""))
                continue
            articles.append({
                "title":        item.get("Title",  "").strip(),
                "author":       item.get("Author", "").strip(),
                "date":         item.get("Date",   "").strip(),
                "content_type": item.get("Type",   "").strip(),
                "url":          urljoin("https://www.gmo.com", item.get("URL", "")),
            })

        if page >= total_pages or not listing:
            break
        page += 1
        time.sleep(api_delay)

    log.info("  Collected %d unlocked articles.", len(articles))
    return articles

# ---------------------------------------------------------------------------
# Phase 3: Article page -> PDF download URL
# ---------------------------------------------------------------------------

def find_download_url(driver, article_url: str) -> str | None:
    """Load article page and return the PDF download URL."""
    driver.get(article_url)
    time.sleep(2.5)
    dismiss_overlays(driver)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # 1. Direct .pdf hrefs in page HTML
    for a in soup.find_all("a", href=re.compile(r"\.pdf", re.I)):
        return urljoin(article_url, a["href"])

    # 2. <a download> attribute
    for a in soup.find_all("a", attrs={"download": True}):
        href = a.get("href", "")
        if href:
            return urljoin(article_url, href)

    # 3. href containing 'download'
    for a in soup.find_all("a", href=re.compile(r"download", re.I)):
        return urljoin(article_url, a["href"])

    # 4. Selenium element scan for Download button/link
    for xpath in [
        "//a[contains(translate(.,'DOWNLOAD','download'),'download')]",
        "//button[contains(translate(.,'DOWNLOAD','download'),'download')]",
        "//*[contains(@class,'download')]//a",
        "//*[contains(@class,'Download')]//a",
    ]:
        try:
            el   = driver.find_element(By.XPATH, xpath)
            href = el.get_attribute("href")
            if href and href.startswith("http"):
                return href
        except NoSuchElementException:
            continue

    # 5. data-url attributes
    for el in driver.find_elements(By.XPATH, "//*[@data-url]"):
        val = el.get_attribute("data-url") or ""
        if "pdf" in val.lower() or "download" in val.lower():
            return urljoin(article_url, val)

    # 6. Scan CDP performance log for any .pdf network request on this page
    try:
        for entry in driver.get_log("performance"):
            msg = json.loads(entry["message"])["message"]
            url = (msg.get("params", {})
                      .get("request", {})
                      .get("url", ""))
            if ".pdf" in url.lower():
                return url
    except Exception:
        pass

    return None

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_filename(title: str, url: str, ext: str = ".pdf") -> str:
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"[\s_-]+", "_", slug).strip("_")[:80]
    uid  = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"{slug}_{uid}{ext}"


def get_cookies(driver) -> dict:
    return {c["name"]: c["value"] for c in driver.get_cookies()}


def download_pdf(pdf_url: str, dest: str, session: requests.Session, delay: float) -> bool:
    time.sleep(delay)
    try:
        r = session.get(pdf_url, stream=True, timeout=30,
                        headers={"Referer": LIBRARY_URL,
                                 "Accept": "application/pdf,*/*"})
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "pdf" not in ct and not pdf_url.lower().endswith(".pdf"):
            log.warning("    Unexpected content-type '%s' — saving anyway", ct)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        log.info("    OK %.1f KB -> %s", os.path.getsize(dest) / 1024, os.path.basename(dest))
        return True
    except Exception as e:
        log.error("    FAIL %s -- %s", pdf_url, e)
        if os.path.exists(dest):
            os.remove(dest)
        return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape GMO research library PDFs")
    parser.add_argument(
        "--types", default="all",
        help="Comma-separated keys: all | quarterly,whitepapers,commentary,insights,viewpoints"
    )
    parser.add_argument("--max-articles", type=int, default=0,
                        help="Max articles per type (0=unlimited; use 2-3 for testing)")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds between PDF downloads (default 2.0)")
    parser.add_argument("--api-delay", type=float, default=1.0,
                        help="Seconds between API pagination calls (default 1.0)")
    parser.add_argument("--no-headless", dest="headless",
                        action="store_false", default=True)
    parser.add_argument("--uid",   default=DEFAULT_UID,
                        help="API uid token (default: hardcoded)")
    parser.add_argument("--isgmo", default=DEFAULT_ISGMO,
                        help="API isGmo token (default: hardcoded)")
    parser.add_argument("--commentary-id", type=int, default=3599,
                        help="Override type ID for Market Commentary if 3599 is wrong")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Apply commentary ID override
    TYPE_MAP["commentary"] = ("Market Commentary", args.commentary_id)

    if args.types.strip().lower() == "all":
        target_keys = list(TYPE_MAP.keys())
    else:
        target_keys = [t.strip() for t in args.types.split(",")
                       if t.strip() in TYPE_MAP]
    if not target_keys:
        log.error("No valid keys. Valid: %s", list(TYPE_MAP.keys()))
        return

    log.info("Target types  : %s", target_keys)
    log.info("Output dir    : %s", OUTPUT_DIR)
    log.info("Max articles  : %s", args.max_articles or "unlimited")
    log.info("Download delay: %.1f s", args.delay)

    # Resume support
    seen_urls  = set()
    log_exists = os.path.exists(LOG_FILE)
    if log_exists:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") in ("ok", "already_exists"):
                    seen_urls.add(row.get("article_url", ""))
        log.info("Resume: %d already-processed URLs loaded.", len(seen_urls))

    driver  = make_driver(headless=args.headless)
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Referer": LIBRARY_URL,
    })

    fieldnames = [
        "title", "author", "article_url", "pdf_url",
        "date", "content_type", "filename", "status", "scraped_at"
    ]

    total_dl = 0

    try:
        session.cookies.update(warmup(driver))

        with open(LOG_FILE, "a", newline="", encoding="utf-8") as log_fh:
            writer = csv.DictWriter(log_fh, fieldnames=fieldnames)
            if not log_exists:
                writer.writeheader()

            for type_key in target_keys:
                type_label, type_id = TYPE_MAP[type_key]

                log.info("=" * 60)
                log.info("TYPE: %s  (ID: %d)", type_label, type_id)
                log.info("=" * 60)

                # Phase 2: collect full article list via API (no Selenium needed)
                articles = fetch_all_articles(
                    args.uid, args.isgmo, type_id, type_label, session, args.api_delay
                )
                if args.max_articles:
                    articles = articles[:args.max_articles]
                    log.info("  Capped to %d (--max-articles).", len(articles))

                # Phase 3: visit article pages, find PDF, download
                for art in articles:
                    url = art["url"]
                    if url in seen_urls:
                        log.info("  [SKIP] %s", art["title"][:60])
                        continue
                    seen_urls.add(url)

                    log.info("  -> %s", art["title"][:70])

                    session.cookies.update(get_cookies(driver))
                    pdf_url = find_download_url(driver, url)

                    if not pdf_url:
                        log.warning("    No PDF found.")
                        writer.writerow({
                            "title": art["title"], "author": art["author"],
                            "article_url": url, "pdf_url": "",
                            "date": art["date"], "content_type": art["content_type"],
                            "filename": "", "status": "no_pdf_found",
                            "scraped_at": datetime.now().isoformat(),
                        })
                        log_fh.flush()
                        continue

                    fname = safe_filename(art["title"], pdf_url)
                    dest  = os.path.join(OUTPUT_DIR, fname)

                    if os.path.exists(dest):
                        log.info("    [EXISTS] %s", fname)
                        writer.writerow({
                            "title": art["title"], "author": art["author"],
                            "article_url": url, "pdf_url": pdf_url,
                            "date": art["date"], "content_type": art["content_type"],
                            "filename": fname, "status": "already_exists",
                            "scraped_at": datetime.now().isoformat(),
                        })
                        log_fh.flush()
                        continue

                    session.cookies.update(get_cookies(driver))
                    ok     = download_pdf(pdf_url, dest, session, args.delay)
                    status = "ok" if ok else "download_failed"
                    if ok:
                        total_dl += 1

                    writer.writerow({
                        "title": art["title"], "author": art["author"],
                        "article_url": url, "pdf_url": pdf_url,
                        "date": art["date"], "content_type": art["content_type"],
                        "filename": fname if ok else "", "status": status,
                        "scraped_at": datetime.now().isoformat(),
                    })
                    log_fh.flush()

                log.info("Type '%s' done.", type_label)

    except KeyboardInterrupt:
        log.info("Interrupted — progress saved to log.")
    finally:
        driver.quit()
        log.info("=" * 60)
        log.info("DONE. PDFs downloaded this run: %d", total_dl)
        log.info("Log: %s", LOG_FILE)


if __name__ == "__main__":
    main()