"""
bii_scraper.py
BlackRock Investment Institute — long-form research scraper
Output: src/data/pdfs/blackrock_bii/

Strategy:
  Phase 1a — Selenium TWO-HOP:
      Step 1: Load publications index, accept cookie gate, exhaust
              scroll/load-more, harvest all *article page* URLs from cards.
      Step 2: Visit each article page, find the PDF download link in the DOM.
  Phase 1b — Candidate URL probe:
      HEAD-check known bii-*.pdf URL patterns directly (catches systematic
      series that may not appear in the current index).
  Phase 2 — requests: direct GET each PDF URL (no SSO required).
      Skip PDFs with <MIN_PAGES pages (proxy for short bulletins/weekly).

Usage:
  python bii_scraper.py                  # headed Chrome (good for first run)
  python bii_scraper.py --headless       # headless
  python bii_scraper.py --skip-selenium  # probe-only (fast fallback)
  python bii_scraper.py --skip-probe     # selenium-only
  python bii_scraper.py --min-pages 10   # stricter page filter
"""

import argparse
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

# ── Selenium ─────────────────────────────────────────────────────────────────
try:
    from selenium import webdriver
    from selenium.common.exceptions import (
        NoSuchElementException,
        TimeoutException,
        WebDriverException,
    )
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
except ImportError:
    sys.exit("selenium not found — pip install selenium")

# ── PDF page-count ────────────────────────────────────────────────────────────
try:
    import pypdf
    def pdf_page_count(data: bytes) -> int:
        import io
        reader = pypdf.PdfReader(io.BytesIO(data))
        return len(reader.pages)
except ImportError:
    try:
        import PyPDF2
        def pdf_page_count(data: bytes) -> int:
            import io
            reader = PyPDF2.PdfReader(io.BytesIO(data))
            return len(reader.pages)
    except ImportError:
        print("[WARN] Neither pypdf nor PyPDF2 found — page-count filter disabled.")
        def pdf_page_count(data: bytes) -> int:
            return 999


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BASE_SITE        = "https://www.blackrock.com"
PUBLICATIONS_URL = (
    "https://www.blackrock.com/corporate/insights/"
    "blackrock-investment-institute/publications"
)
BASE_PDF_URL = "https://www.blackrock.com/corporate/literature/whitepaper/"

ARTICLE_SUBPAGES = [
    "investment-perspective",
    "global-insights",
    "portfolio-design",
    "thematic-insights",
]

# Archives page uses a different base path
ARCHIVES_URL = (
    "https://www.blackrock.com/corporate/insights/"
    "blackrock-investment-institute/archives"
)

SHORT_FORM_PATTERNS = [
    "weekly-commentary",
    "weekly-market-commentary",
    "/weekly",
    "chart-of-the-week",
    "podcast",
    "bulletin",
    "snapshot",
    "market-take",
    "talking-points",
    "market-update",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
    "Referer": "https://www.blackrock.com/",
}

SCROLL_PAUSE      = 2.5
MAX_SCROLL_ITERS  = 120
ARTICLE_SLEEP     = 1.2
DOWNLOAD_SLEEP    = 1.5
COOKIE_WAIT       = 12
MAX_ARTICLE_FAILS = 10


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def slugify(text):
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:120]


def is_short_form(url):
    slug = url.lower()
    return any(p in slug for p in SHORT_FORM_PATTERNS)


def dismiss_overlays(driver):
    try:
        script = (
            "var btns = document.querySelectorAll("
            + "'button, a, input[type=\"button\"], input[type=\"submit\"]'"
            + ");"
            "for (var i = 0; i < btns.length; i++) {"
            "  var t = (btns[i].innerText || btns[i].value || '').toLowerCase();"
            "  if (t.indexOf('accept') !== -1 || t.indexOf('agree') !== -1 "
            "      || t.indexOf('confirm') !== -1) {"
            "    btns[i].click(); break;"
            "  }"
            "}"
        )
        driver.execute_script(script)
        time.sleep(2)
    except Exception as e:
        print(f"  [WARN] dismiss_overlays: {e}")


def scroll_once(driver):
    old_h = driver.execute_script("return document.body.scrollHeight")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(SCROLL_PAUSE)
    return driver.execute_script("return document.body.scrollHeight") > old_h


def click_load_more(driver):
    xpaths = [
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        " 'abcdefghijklmnopqrstuvwxyz'), 'load more')]",
        "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        " 'abcdefghijklmnopqrstuvwxyz'), 'show more')]",
        "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
        " 'abcdefghijklmnopqrstuvwxyz'), 'load more')]",
    ]
    for xp in xpaths:
        try:
            btn = driver.find_element(By.XPATH, xp)
            if btn.is_displayed() and btn.is_enabled():
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(SCROLL_PAUSE)
                return True
        except NoSuchElementException:
            pass
    return False


def build_driver(headless):
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver


# ─────────────────────────────────────────────────────────────────────────────
# Cookie harvest — accept gate, extract cookies into requests session
# ─────────────────────────────────────────────────────────────────────────────

def harvest_cookies(headless: bool, session: requests.Session) -> bool:
    """
    Spin up a minimal Selenium session, accept the BlackRock consent gate,
    and inject the resulting cookies into the requests session.
    Returns True if cookies were successfully harvested.
    """
    print("[COOKIES] Launching browser to accept consent gate...")
    driver = build_driver(headless)
    try:
        driver.get(PUBLICATIONS_URL)
        time.sleep(5)
        dismiss_overlays(driver)
        time.sleep(3)

        # Confirm the gate was accepted by checking the page has real content
        page_src = driver.page_source.lower()
        if "accept" in page_src and "legal" in page_src and len(page_src) < 20000:
            print("[COOKIES] Gate may not have been accepted — trying again...")
            dismiss_overlays(driver)
            time.sleep(3)

        selenium_cookies = driver.get_cookies()
        if not selenium_cookies:
            print("[COOKIES] No cookies found — gate may still be blocking.")
            return False

        for c in selenium_cookies:
            session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain", ".blackrock.com")
            )
        print(f"[COOKIES] Injected {len(selenium_cookies)} cookies into requests session.")
        return True

    except Exception as e:
        print(f"[COOKIES] Error: {e}")
        return False
    finally:
        driver.quit()


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1a — Selenium TWO-HOP
# ─────────────────────────────────────────────────────────────────────────────

def extract_article_urls(driver):
    """Pull article page hrefs from the current DOM (not PDFs, not bare index pages)."""
    MATCH_PATHS = [
        "/corporate/insights/blackrock-investment-institute/publications/",
        "/corporate/insights/blackrock-investment-institute/archives",
        "/corporate/insights/blackrock-investment-institute/global-insights/",
    ]
    # Category-level pages to exclude (not article pages themselves)
    CATEGORY_SLUGS = set(ARTICLE_SUBPAGES + [
        "publications", "archives", "global-insights",
        "investment-perspective", "portfolio-design", "thematic-insights",
    ])

    urls = []
    seen = set()
    for a in driver.find_elements(By.TAG_NAME, "a"):
        try:
            href = a.get_attribute("href") or ""
            if not href or href in seen:
                continue
            if href.lower().endswith(".pdf"):
                continue
            # Must match one of our known path prefixes
            if not any(p in href for p in MATCH_PATHS):
                continue
            # Exclude bare category index pages (path ends at a known category slug)
            tail = href.rstrip("/").split("/")[-1]
            if tail in CATEGORY_SLUGS or href.rstrip("/").endswith("/publications"):
                continue
            # Exclude fragment-only links
            if href.endswith("#") or "/#" in href[-5:]:
                continue
            seen.add(href)
            urls.append(href)
        except Exception:
            pass
    return urls


def exhaust_page(driver, label):
    stale = 0
    for i in range(MAX_SCROLL_ITERS):
        before = len(extract_article_urls(driver))
        clicked = click_load_more(driver)
        if not clicked:
            grew = scroll_once(driver)
            stale = 0 if grew else stale + 1
        else:
            stale = 0

        after = len(extract_article_urls(driver))
        if i % 10 == 0:
            print(f"    [{label}] iter {i:3d} | article URLs: {after}")
        if stale >= 5 and after == before:
            print(f"    [{label}] No growth after {stale} iters — done.")
            break

    return extract_article_urls(driver)


def extract_pdf_from_article(driver):
    """Find a PDF link on a BII article page. Returns URL string or None."""
    # Strategy 1: direct .pdf href
    for a in driver.find_elements(By.TAG_NAME, "a"):
        try:
            href = a.get_attribute("href") or ""
            if href.lower().endswith(".pdf"):
                return href
        except Exception:
            pass

    # Strategy 2: download/PDF button text
    keywords = ["download", "pdf", "full report", "read the paper"]
    for a in driver.find_elements(By.TAG_NAME, "a"):
        try:
            text = (a.text or "").lower()
            if any(k in text for k in keywords):
                href = a.get_attribute("href") or ""
                if href:
                    return href
        except Exception:
            pass

    # Strategy 3: data-href / data-pdf attributes
    for tag in driver.find_elements(By.XPATH, "//*[@data-href or @data-pdf]"):
        try:
            val = tag.get_attribute("data-href") or tag.get_attribute("data-pdf") or ""
            if val:
                return urljoin(BASE_SITE, val)
        except Exception:
            pass

    return None


def scrape_article_pages(driver, article_urls):
    results = []
    consecutive_fails = 0
    total = len(article_urls)

    for idx, article_url in enumerate(article_urls, 1):
        if is_short_form(article_url):
            print(f"  [{idx:3d}/{total}] SKIP short-form: {article_url.split('/')[-1]}")
            continue
        try:
            driver.get(article_url)
            time.sleep(ARTICLE_SLEEP)
            dismiss_overlays(driver)
            pdf_url = extract_pdf_from_article(driver)
            title   = driver.title or ""
            if pdf_url:
                fname = Path(urlparse(pdf_url).path).name
                print(f"  [{idx:3d}/{total}] FOUND  {fname}")
                results.append({"title": title, "url": pdf_url})
                consecutive_fails = 0
            else:
                print(f"  [{idx:3d}/{total}] no PDF: {article_url.split('/')[-1]}")
                consecutive_fails += 1
        except Exception as e:
            print(f"  [{idx:3d}/{total}] ERROR: {e}")
            consecutive_fails += 1

        if consecutive_fails >= MAX_ARTICLE_FAILS:
            print(f"[WARN] {MAX_ARTICLE_FAILS} consecutive failures — stopping.")
            break

    return results


def harvest_links_selenium(headless):
    driver = build_driver(headless)
    all_pdf_links = []
    seen = set()

    # Only the two citation-bearing archive tabs — skip Weekly commentary
    ARCHIVE_TABS = [
        ARCHIVES_URL + "#macroeconomic-views",
        ARCHIVES_URL + "#investment-outlook",
    ]

    try:
        for tab_url in ARCHIVE_TABS:
            tab_label = tab_url.split("#")[-1]
            print(f"\n[HOP 1] Archives tab: {tab_label}")
            driver.get(tab_url)
            time.sleep(4)
            dismiss_overlays(driver)
            time.sleep(2)

            # Exhaust "Read more" / "Load more" pagination
            read_more_clicks = 0
            for _ in range(60):  # up to 60 pages
                # Try to find and click Read more
                clicked = False
                for btn in driver.find_elements(By.TAG_NAME, "button"):
                    txt = (btn.text or "").lower().strip()
                    if ("read more" in txt or "load more" in txt or "show more" in txt) and btn.is_displayed():
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2.5)
                        read_more_clicks += 1
                        clicked = True
                        break
                if not clicked:
                    # Also try <a> tags with read more text
                    for a in driver.find_elements(By.TAG_NAME, "a"):
                        txt = (a.text or "").lower().strip()
                        if ("read more" in txt or "load more" in txt) and a.is_displayed():
                            # Don't click article-level "read more" links — only pagination ones
                            href = a.get_attribute("href") or ""
                            if "archives" in href or not href:
                                driver.execute_script("arguments[0].click();", a)
                                time.sleep(2.5)
                                read_more_clicks += 1
                                clicked = True
                                break
                if not clicked:
                    break

            print(f"  Clicked 'Read more' {read_more_clicks} times")

            # Now extract all article links on this tab page
            # Each article card links to an article page or directly to a PDF
            article_urls = []
            for a in driver.find_elements(By.TAG_NAME, "a"):
                try:
                    href = a.get_attribute("href") or ""
                    if not href or href in seen:
                        continue
                    # Direct PDF links
                    if href.lower().endswith(".pdf") and "blackrock.com" in href:
                        seen.add(href)
                        all_pdf_links.append({"title": a.text or "", "url": href})
                        continue
                    # Article page links under publications or insights
                    if (
                        "blackrock-investment-institute" in href
                        and href not in seen
                        and not href.endswith("#" + tab_label)
                        and not any(href.rstrip("/").endswith(s) for s in [
                            "archives", "publications", "macroeconomic-views",
                            "investment-outlook", "weekly-commentary",
                        ])
                    ):
                        seen.add(href)
                        article_urls.append(href)
                except Exception:
                    pass

            print(f"  Found {len(article_urls)} article pages + {len(all_pdf_links)} direct PDFs so far")

            # Hop 2: visit each article page to extract PDF link
            for idx, article_url in enumerate(article_urls, 1):
                try:
                    driver.get(article_url)
                    time.sleep(ARTICLE_SLEEP)
                    dismiss_overlays(driver)
                    pdf_url = extract_pdf_from_article(driver)
                    if pdf_url and pdf_url not in seen:
                        seen.add(pdf_url)
                        fname = Path(urlparse(pdf_url).path).name
                        print(f"  [{idx:3d}/{len(article_urls)}] FOUND  {fname}")
                        all_pdf_links.append({"title": driver.title or "", "url": pdf_url})
                    else:
                        print(f"  [{idx:3d}/{len(article_urls)}] no PDF: {article_url.split('/')[-1]}")
                except Exception as e:
                    print(f"  [{idx:3d}/{len(article_urls)}] ERROR: {e}")
                    continue

        print(f"\n[HOP 2] Total PDF links extracted: {len(all_pdf_links)}")
        return all_pdf_links

    except WebDriverException as e:
        print(f"[ERROR] WebDriver: {e}")
        return []
    finally:
        driver.quit()


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1c — Wayback Machine CDX API
# ─────────────────────────────────────────────────────────────────────────────

CDX_API = "http://web.archive.org/cdx/search/cdx"

def harvest_wayback_slugs(session: requests.Session) -> list[dict]:
    """
    Query the Wayback Machine CDX index for all bii-*.pdf URLs ever crawled
    under blackrock.com/corporate/literature/whitepaper/.
    Returns list of {title, url} dicts pointing to the live BlackRock URL
    (not the Wayback cached copy).
    """
    print("\n[WAYBACK] Querying CDX API for bii-*.pdf slugs...")

    # Try two URL patterns — with and without www
    cdx_targets = [
        "www.blackrock.com/corporate/literature/whitepaper/bii-*",
        "blackrock.com/corporate/literature/whitepaper/bii-*",
    ]

    raw_urls = []
    for target in cdx_targets:
        params = {
            "url":      target,
            "matchType":"prefix",
            "output":   "json",
            "fl":       "original",
            "collapse": "original",   # deduplicate by exact URL (not urlkey)
            "limit":    "50000",
            # No statuscode filter — capture all historical URLs
        }
        try:
            resp = requests.get(CDX_API, params=params, timeout=60)
            resp.raise_for_status()
            rows = resp.json()
            if len(rows) > 1:
                raw_urls += [row[0] for row in rows[1:]]
                print(f"[WAYBACK]   {target}: {len(rows)-1} rows")
        except Exception as e:
            print(f"[WAYBACK] CDX request failed for {target}: {e}")

    print(f"[WAYBACK] CDX returned {len(raw_urls)} raw URLs total.")

    if not raw_urls:
        print("[WAYBACK] No results — BlackRock may block Wayback crawling of this path.")
        return []

    links = []
    seen = set()
    for url in raw_urls:
        # Normalise to https live URL (CDX may return http)
        url = re.sub(r"^http://", "https://", url)
        # Must end in .pdf and be a bii- paper
        if not url.lower().endswith(".pdf"):
            continue
        if "/whitepaper/bii-" not in url.lower():
            continue
        if url in seen:
            continue
        seen.add(url)
        links.append({"title": "", "url": url})

    print(f"[WAYBACK] {len(links)} unique bii-*.pdf slugs after filtering.")
    return links



_ALL_MONTHS = [
    "january","february","march","april","may","june",
    "july","august","september","october","november","december",
]
_QTR_MONTHS = ["january","april","july","october"]

KNOWN_SERIES = [
    # Core recurring series — try every month
    ("bii-macro-perspectives",           range(2016, 2027), _ALL_MONTHS),
    ("bii-investment-perspectives",      range(2016, 2027), _ALL_MONTHS),
    ("bii-portfolio-perspectives",       range(2016, 2027), _ALL_MONTHS),
    # Global macro outlook — semi-annual (Jan/Jul) + all months as fallback
    ("bii-global-macro-outlook",         range(2015, 2027), _ALL_MONTHS),
    # Midyear outlook variants
    ("bii-midyear-investment-outlook-us",range(2015, 2027), [""]),
    ("bii-midyear-investment-outlook",   range(2015, 2027), [""]),
    ("bii-2018-midyear-investment-outlook-us", [2018], [""]),
    # Sustainability / geopolitics themes — quarterly ish
    ("bii-investment-perspectives-sustainability", range(2020, 2027), _ALL_MONTHS),
    ("bii-investment-perspectives-geopolitics",    range(2020, 2027), _ALL_MONTHS),
    # Other known thematic slugs
    ("bii-investment-perspectives-alpha-and-factors", range(2018, 2024), _QTR_MONTHS),
    ("bii-investment-perspectives-portfolio-design",  range(2018, 2024), _QTR_MONTHS),
    ("bii-investment-perspectives-fixed-income",      range(2018, 2024), _QTR_MONTHS),
    ("bii-investment-perspectives-china",             range(2019, 2024), _QTR_MONTHS),
    ("bii-investment-perspectives-climate",           range(2020, 2024), _QTR_MONTHS),
    # Transition scenario
    ("bii-investment-perspectives-sustainability",    range(2020, 2027), _QTR_MONTHS),
    # Standalone whitepapers with known slugs
    ("bii-investment-perspectives-sustainability-july-2023", [], [""]),  # handled below
]
# Annual outlook — every year variant seen in the wild
for _y in range(2017, 2027):
    KNOWN_SERIES.append((f"bii-{_y+1}-global-outlook",       [_y],    [""]))
    KNOWN_SERIES.append((f"bii-{_y+1}-investment-outlook",   [_y],    [""]))
    KNOWN_SERIES.append((f"bii-{_y+1}-outlook",              [_y],    [""]))
    KNOWN_SERIES.append((f"bii-global-outlook-{_y+1}",       [_y],    [""]))
    KNOWN_SERIES.append((f"bii-investment-outlook-{_y+1}",   [_y],    [""]))

# Remove the placeholder empty entry added above
KNOWN_SERIES = [s for s in KNOWN_SERIES if s[1] != []]


def build_candidate_urls():
    candidates = []
    seen = set()
    for slug_base, years, months in KNOWN_SERIES:
        for year in years:
            for month in months:
                url = BASE_PDF_URL + (f"{slug_base}-{month}-{year}.pdf" if month else f"{slug_base}.pdf")
                if url not in seen:
                    seen.add(url)
                    candidates.append({"title": "", "url": url})
    return candidates


def probe_candidates(candidates, session):
    valid = []
    print(f"\n[PROBE] Checking {len(candidates)} candidate URLs...")
    for i, cand in enumerate(candidates, 1):
        try:
            r = session.head(cand["url"], headers=HEADERS, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                print(f"  [OK]  {Path(urlparse(cand['url']).path).name}")
                valid.append(cand)
        except requests.RequestException:
            pass
        if i % 50 == 0:
            print(f"  ... {i}/{len(candidates)} probed | {len(valid)} valid so far")
        time.sleep(0.3)
    print(f"[PROBE] {len(valid)} valid URLs confirmed.")
    return valid


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Filter + Download
# ─────────────────────────────────────────────────────────────────────────────

def classify(link):
    url = link["url"].lower()
    if is_short_form(url):
        return "skip_short"
    if not url.endswith(".pdf"):
        return "skip_no_pdf"
    return "keep"


def safe_filename(link, idx):
    stem = Path(urlparse(link["url"]).path).stem
    if stem:
        return stem + ".pdf"
    return slugify(link.get("title", f"bii-paper-{idx}")) + ".pdf"


def purge_fake_pdfs(out_dir: Path) -> int:
    """
    Delete any .pdf files in out_dir whose content is actually HTML
    (i.e. saved from a BlackRock gate redirect). Returns count deleted.
    """
    deleted = 0
    if not out_dir.exists():
        return 0
    for f in out_dir.glob("*.pdf"):
        try:
            magic = f.read_bytes()[:4]
            if magic != b"%PDF":
                print(f"  [PURGE] {f.name}  (not a real PDF — deleting)")
                f.unlink()
                deleted += 1
        except Exception as e:
            print(f"  [PURGE WARN] {f.name}: {e}")
    return deleted


def download_pdfs(links, out_dir, min_pages, session):
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = dict(downloaded=0, skipped_short=0, skipped_pages=0,
                 skipped_exists=0, skipped_no_pdf=0, failed=0, fake_pdf=0)

    seen = set()
    unique = []
    for lnk in links:
        if lnk["url"] not in seen:
            seen.add(lnk["url"])
            unique.append(lnk)

    print(f"\n[DOWNLOAD] {len(unique)} unique links.")

    for idx, link in enumerate(unique, 1):
        verdict  = classify(link)
        filename = safe_filename(link, idx)
        out_path = out_dir / filename

        if verdict == "skip_short":
            print(f"  [{idx:3d}] SKIP short    {filename}")
            stats["skipped_short"] += 1
            continue
        if verdict == "skip_no_pdf":
            print(f"  [{idx:3d}] SKIP no-pdf   {link['url']}")
            stats["skipped_no_pdf"] += 1
            continue
        if out_path.exists():
            print(f"  [{idx:3d}] EXISTS        {filename}")
            stats["skipped_exists"] += 1
            continue

        try:
            resp = session.get(link["url"], headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [{idx:3d}] FAIL          {filename}  ({e})")
            stats["failed"] += 1
            time.sleep(DOWNLOAD_SLEEP)
            continue

        # ── Validate it's actually a PDF, not an HTML gate page ──────────
        content_type = resp.headers.get("Content-Type", "")
        magic = resp.content[:5] if len(resp.content) >= 5 else b""
        if magic[:4] != b"%PDF" or "html" in content_type.lower():
            print(f"  [{idx:3d}] FAKE-PDF (HTML gate) {filename}")
            stats["fake_pdf"] += 1
            time.sleep(DOWNLOAD_SLEEP)
            continue

        try:
            n_pages = pdf_page_count(resp.content)
        except Exception:
            n_pages = 999

        if n_pages < min_pages:
            print(f"  [{idx:3d}] SKIP {n_pages}pp<{min_pages} {filename}")
            stats["skipped_pages"] += 1
            time.sleep(DOWNLOAD_SLEEP)
            continue

        out_path.write_bytes(resp.content)
        print(f"  [{idx:3d}] OK   {n_pages:3d}pp  {filename}")
        stats["downloaded"] += 1
        time.sleep(DOWNLOAD_SLEEP)

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="BlackRock BII scraper")
    p.add_argument("--headless",      action="store_true", default=False)
    p.add_argument("--min-pages",     type=int, default=6)
    p.add_argument("--out-dir",       default=os.path.join("src", "data", "pdfs", "blackrock_bii"))
    p.add_argument("--skip-selenium", action="store_true", default=False)
    p.add_argument("--skip-wayback",  action="store_true", default=False,
                   help="Skip Wayback Machine CDX slug harvest")
    p.add_argument("--skip-probe",    action="store_true", default=False)
    p.add_argument("--skip-cookies",  action="store_true", default=False,
                   help="Skip cookie harvest (use if you already have a warm session)")
    return p.parse_args()


def main():
    args    = parse_args()
    out_dir = Path(args.out_dir)

    print("=" * 60)
    print(f"[CONFIG] out_dir    = {out_dir}")
    print(f"[CONFIG] min_pages  = {args.min_pages}")
    print(f"[CONFIG] headless   = {args.headless}")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)
    all_links = []

    # ── Cookie harvest (always runs unless skipped) ───────────────────────
    if not args.skip_cookies:
        harvest_cookies(args.headless, session)
    else:
        print("[COOKIES] Skipped.")

    if not args.skip_selenium:
        sel_links = harvest_links_selenium(args.headless)
        print(f"\n[PHASE 1a] {len(sel_links)} PDF links from Selenium.")
        all_links.extend(sel_links)
    else:
        print("[PHASE 1a] Skipped.")

    if not args.skip_wayback:
        wb_links = harvest_wayback_slugs(session)
        existing = {l["url"] for l in all_links}
        new_wb   = [l for l in wb_links if l["url"] not in existing]
        print(f"[PHASE 1c] Wayback added {len(new_wb)} new URLs.")
        all_links.extend(new_wb)
    else:
        print("[PHASE 1c] Skipped.")

    if not args.skip_probe:
        candidates = build_candidate_urls()
        valid      = probe_candidates(candidates, session)
        existing   = {l["url"] for l in all_links}
        new        = [l for l in valid if l["url"] not in existing]
        print(f"[PHASE 1b] Probe added {len(new)} new URLs.")
        all_links.extend(new)
    else:
        print("[PHASE 1b] Skipped.")

    if not all_links:
        print("[ERROR] No links found.")
        sys.exit(1)

    print(f"\n[TOTAL]  {len(all_links)} links before dedup/download.")

    # Purge any corrupt HTML-gate files from prior runs
    purged = purge_fake_pdfs(out_dir)
    if purged:
        print(f"[PURGE]  Deleted {purged} corrupt (non-PDF) files from prior run.")

    stats = download_pdfs(all_links, out_dir, args.min_pages, session)

    pdf_count = len(list(out_dir.glob("*.pdf"))) if out_dir.exists() else 0
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Downloaded        : {stats['downloaded']}")
    print(f"  Already existed   : {stats['skipped_exists']}")
    print(f"  Skipped short-form: {stats['skipped_short']}")
    print(f"  Skipped <pages    : {stats['skipped_pages']}")
    print(f"  Skipped no PDF ext: {stats['skipped_no_pdf']}")
    print(f"  Fake PDFs (HTML)  : {stats['fake_pdf']}")
    print(f"  Failed            : {stats['failed']}")
    print(f"\n  PDFs on disk      : {pdf_count}  ({out_dir})")
    print("=" * 60)


if __name__ == "__main__":
    main()