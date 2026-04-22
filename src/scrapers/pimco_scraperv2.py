"""
pimco_scraper.py  v3
--------------------
Strategy: plain Selenium (no selenium-wire / no token interception).
  1. Load insights page, click target category checkboxes
  2. Click "Show more" until exhausted → collect all article URLs
  3. Per article: find PDF download link → download to data/pdfs/pimco/
                  no PDF → save clean .txt to data/Other_Corpus/pimco/

Script lives at gap/src/scrapers/pimco_scraper.py
  .parent              → gap/src/scrapers/
  .parent.parent       → gap/src/
  .parent.parent.parent → gap/

Target categories (chosen for citation density):
  Research, Cyclical Outlook, Asset Allocation Outlook,
  Secular Outlook, The Credit Market Lens

Output:
  gap/data/pdfs/pimco/
  gap/data/Other_Corpus/pimco/
  gap/data/logs/pimco/

Usage:
  conda activate emi
  cd Desktop/gap
  python src/scrapers/pimco_scraper.py [--delay 2.0] [--max-articles N]
"""

import re
import time
import random
import argparse
from pathlib import Path
from urllib.parse import urljoin

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

# ── Paths ──────────────────────────────────────────────────────────────────────
# Script lives at gap/src/scrapers/ — three .parent calls reach gap/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

PDF_DIR   = REPO_ROOT / "data" / "pdfs"         / "pimco"
OTHER_DIR = REPO_ROOT / "data" / "Other_Corpus" / "pimco"
LOG_DIR   = REPO_ROOT / "data" / "logs"         / "pimco"

for d in [PDF_DIR, OTHER_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOG_FILE  = LOG_DIR / "_done.txt"
FAIL_FILE = LOG_DIR / "_failed.txt"

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL     = "https://www.pimco.com"
INSIGHTS_URL = BASE_URL + "/us/en/insights"

MIN_DELAY = 2.0
MAX_DELAY = 4.5

TARGET_CATEGORIES = [
    "Research",
    "Cyclical Outlook",
    "Asset Allocation Outlook",
    "Secular Outlook",
    "The Credit Market Lens",
]

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

def sync_session(driver, session: requests.Session):
    for c in driver.get_cookies():
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

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
    opts.add_argument("--window-size=1440,900")
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
                 [class*="onetrust"],[id*="onetrust"],[class*="modal"],
                 [class*="overlay"],[class*="banner"]'
            ).forEach(e => {
                var s = e.style;
                s.display = 'none';
            });
            document.body.style.overflow = '';
        """)
        time.sleep(0.3)
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

# ── Phase 1: collect article URLs ──────────────────────────────────────────────

def collect_article_urls(driver) -> list[str]:
    print(f"[1/3] Loading insights page...")
    driver.get(INSIGHTS_URL)
    sleep(4, 6)
    dismiss_overlays(driver)

    # Handle investor gate
    print("[1/3] Checking for investor gate...")
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(text(),'Institutional Investor')]")
            )
        )
        inst = driver.find_element(By.XPATH, "//*[contains(text(),'Institutional Investor')]")
        driver.execute_script(
            "var el=arguments[0];"
            "var p=el.closest('button,a,[role=button],[class*=card],[class*=Card]');"
            "if(p){p.click();}else{el.click();}",
            inst
        )
        print("  clicked: Institutional Investor")
        sleep(0.8, 1.2)
        for cb in driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']"):
            if not cb.is_selected():
                driver.execute_script("arguments[0].click();", cb)
                print("  checked T&C")
                sleep(0.5, 0.8)
                break
        for xp in [
            "//button[contains(translate(text(),'ENTER','enter'),'enter')]",
            "//button[contains(translate(text(),'SUBMIT','submit'),'submit')]",
            "//button[@type='submit']",
        ]:
            try:
                btn = driver.find_element(By.XPATH, xp)
                if btn.is_displayed():
                    safe_click(driver, btn)
                    print(f"  clicked gate: {btn.text.strip()!r}")
                    sleep(2, 3)
                    break
            except NoSuchElementException:
                continue
    except TimeoutException:
        print("  no gate")

    # Wait for category checkboxes
    print("[1/3] Waiting for category filter...")
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='checkbox']"))
        )
    except TimeoutException:
        print("  [warn] no checkboxes found")
    sleep(1, 2)

    # Expand "See more" in category panel
    print("[1/3] Expanding category list...")
    driver.execute_script(
        "var els=document.querySelectorAll('button,a,span');"
        "for(var i=0;i<els.length;i++){"
        "  var t=(els[i].innerText||els[i].textContent||'').trim().toLowerCase();"
        "  if(t==='see more'||t==='show more'){els[i].click();break;}"
        "}"
    )
    sleep(1.5, 2)

    # Select categories via JS
    print(f"[1/3] Selecting: {TARGET_CATEGORIES}")
    for cat in TARGET_CATEGORIES:
        result = driver.execute_script(
            "var cat=arguments[0];"
            "var labels=document.querySelectorAll('label');"
            "for(var i=0;i<labels.length;i++){"
            "  var t=(labels[i].innerText||labels[i].textContent||'').trim();"
            "  if(t.indexOf(cat)===0){"
            "    var cb=labels[i].querySelector('input[type=checkbox]');"
            "    if(!cb){var f=labels[i].getAttribute('for');if(f)cb=document.getElementById(f);}"
            "    if(cb){cb.click();return t;}labels[i].click();return t;"
            "  }"
            "} return null;",
            cat
        )
        if result:
            print(f"  clicked: {result}")
        else:
            print(f"  [warn] not found: {cat}")
        time.sleep(random.uniform(1.5, 2.5))

    # Navigate to filtered URL for clean Coveo load
    current_url = driver.current_url
    print(f"[1/3] Filter URL: {current_url[-80:]}")
    if "#" in current_url and "category" in current_url:
        print("[1/3] Navigating to filtered URL for clean Coveo load...")
        driver.get(current_url)
        time.sleep(6)

    print("[1/3] Waiting for Coveo results to render...")
    import time as _time
    deadline = _time.time() + 20
    while _time.time() < deadline:
        anchors = driver.find_elements(By.TAG_NAME, "a")
        article_links = [
            a for a in anchors
            if "/us/en/insights/" in (a.get_attribute("href") or "")
            and not any(x in (a.get_attribute("href") or "")
                       for x in ["#", "podcasts", "videos", "education"])
        ]
        if len(article_links) >= 5:
            print(f"  results loaded: {len(article_links)} article links visible")
            break
        _time.sleep(1)
    else:
        print("  [warn] results may not have fully loaded after 20s")
    time.sleep(2)

    # Try to set 50 per page
    driver.execute_script(
        "var btns=document.querySelectorAll('button');"
        "for(var i=0;i<btns.length;i++){"
        "  if((btns[i].innerText||'').trim()==='50'){btns[i].click();break;}"
        "}"
    )
    time.sleep(3)

    # Paginate collecting article URLs
    EXCLUDE = {"podcasts", "videos", "video", "education", "webinars",
               "webcasts", "events", "insights"}
    all_urls = set()
    page = 1

    while True:
        time.sleep(2)
        anchors = driver.find_elements(By.TAG_NAME, "a")
        batch = set()
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                m = re.search(r"pimco\.com/us/en/insights/([^/#?]+)", href)
                if m:
                    slug = m.group(1).lower()
                    if slug not in EXCLUDE and not slug.startswith("tag"):
                        batch.add("https://www.pimco.com/us/en/insights/" + m.group(1))
            except Exception:
                continue
        new_urls = batch - all_urls
        all_urls.update(batch)
        print(f"  [page {page}] +{len(new_urls)} URLs (total: {len(all_urls)})")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        next_page = page + 1
        clicked = False
        for sel in [
            f"span[aria-label='Page {next_page}']",
            f"a[aria-label='Page {next_page}']",
            f"li.coveo-pager-list-item:nth-child({next_page}) span",
        ]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if el.is_displayed():
                    driver.execute_script("arguments[0].click();", el)
                    clicked = True
                    print(f"  [nav] page {next_page}")
                    driver.execute_script("window.scrollTo(0, 0);")
                    break
            except NoSuchElementException:
                continue

        if not clicked:
            print(f"  pagination done at page {page}")
            break
        page += 1
        time.sleep(3)
        if page > 20:
            break

    print(f"[1/3] -> {len(all_urls)} article URLs collected")
    return list(all_urls)


# ── PDF detection ──────────────────────────────────────────────────────────────

def find_pdf_url(soup: BeautifulSoup) -> str | None:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        cls  = " ".join(a.get("class", []))
        if "download" in cls and "/documents/" in href:
            return href if href.startswith("http") else urljoin(BASE_URL, href)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/documents/" in href and a.get("download") is not None:
            return href if href.startswith("http") else urljoin(BASE_URL, href)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.pdf($|\?)", href, re.I):
            return href if href.startswith("http") else urljoin(BASE_URL, href)
    return None


# ── Phase 3: download PDF or save .txt ────────────────────────────────────────

def process_article(session: requests.Session, article_url: str) -> str:
    """
    Fetch article, download PDF if available, else save clean .txt.
    Returns: 'pdf', 'txt', 'skip', 'fail'
    """
    slug = article_url.rstrip("/").split("/")[-1]

    try:
        resp = session.get(article_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"    [error] fetch failed: {e}")
        mark_failed(article_url)
        return "fail"

    soup = BeautifulSoup(resp.text, "html.parser")

    title_el   = soup.find("h1")
    title      = title_el.get_text(strip=True) if title_el else slug
    safe_title = slugify(title)

    # Try PDF first
    pdf_url = find_pdf_url(soup)
    if pdf_url:
        fname = f"pimco_{safe_title[:70]}.pdf"
        fpath = PDF_DIR / fname
        if fpath.exists():
            print(f"    [skip pdf] {fname}")
            return "skip"
        try:
            r = session.get(pdf_url, timeout=60, stream=True,
                            headers={"Referer": article_url})
            r.raise_for_status()
            content = r.content
            if len(content) < 5000 or b"%PDF" not in content[:10]:
                print(f"    [warn] not a valid PDF — falling back to .txt")
            else:
                fpath.write_bytes(content)
                print(f"    [✓ pdf] {fname} ({fpath.stat().st_size // 1024} KB)")
                return "pdf"
        except Exception as e:
            print(f"    [warn] PDF download failed ({e}) — saving .txt instead")

    # No PDF or download failed — save clean .txt
    fname = f"pimco_{safe_title[:70]}.txt"
    fpath = OTHER_DIR / fname
    if fpath.exists():
        print(f"    [skip txt] {fname}")
        return "skip"

    # Strip boilerplate tags before text extraction
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "aside", "iframe", "noscript"]):
        tag.decompose()

    # Find main content block
    content_el = None
    for sel in ["article", "main", "[class*='article']",
                "[class*='content']", "[role='main']"]:
        content_el = soup.select_one(sel)
        if content_el:
            break
    if not content_el:
        content_el = soup.body

    body = html_to_text(content_el)

    if len(body) < 100:
        print(f"    [warn] content too short ({len(body)} chars) — marking failed")
        mark_failed(article_url)
        return "fail"

    text_out = f"SOURCE: {article_url}\nTITLE: {title}\n\n{body}"
    fpath.write_text(text_out, encoding="utf-8")
    print(f"    [✓ txt] {fname} ({len(body):,} chars)")
    return "txt"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None)
    parser.add_argument("--max-articles", type=int, default=None)
    args = parser.parse_args()

    if args.delay:
        global MIN_DELAY, MAX_DELAY
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done = load_done()

    print("=" * 60)
    print("PIMCO Scraper v3  ·  category filter + PDF/TXT output")
    print(f"PDFs   → {PDF_DIR.resolve()}")
    print(f"TXTs   → {OTHER_DIR.resolve()}")
    print(f"Logs   → {LOG_DIR.resolve()}")
    print(f"Done   : {len(done)} articles already saved")
    print("=" * 60)

    driver = build_driver()

    # Phase 1: collect URLs via Selenium
    article_urls = collect_article_urls(driver)

    # Build requests session with browser cookies
    ua = driver.execute_script("return navigator.userAgent;")
    session = requests.Session()
    session.headers.update({
        "User-Agent": ua,
        "Referer": BASE_URL,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    })
    sync_session(driver, session)
    driver.quit()

    if args.max_articles:
        article_urls = article_urls[:args.max_articles]

    print(f"\n[2/3] Processing {len(article_urls)} articles...\n")
    counts = {"pdf": 0, "txt": 0, "skip": 0, "fail": 0}

    for i, url in enumerate(article_urls, 1):
        if url in done:
            counts["skip"] += 1
            continue

        print(f"[{i}/{len(article_urls)}] {url}")
        result = process_article(session, url)
        counts[result] += 1

        if result in ("pdf", "txt"):
            mark_done(url)

        sleep()

    n_pdf = len(list(PDF_DIR.glob("*.pdf")))
    n_txt = len(list(OTHER_DIR.glob("*.txt")))
    print("\n" + "=" * 60)
    print(f"Done.  PDFs: {counts['pdf']}  TXTs: {counts['txt']}  "
          f"Skipped: {counts['skip']}  Failed: {counts['fail']}")
    print(f"PDFs on disk : {n_pdf}  |  TXTs on disk : {n_txt}")
    print(f"Failures     : {FAIL_FILE.resolve()}")
    print("=" * 60)


if __name__ == "__main__":
    main()