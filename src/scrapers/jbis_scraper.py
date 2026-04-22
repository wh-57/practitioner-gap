"""
jbis_scraper.py  —  Journal of Beta Investment Strategies
----------------------------------------------------------
Same structure as jpm_scraper.py (pm-research.com, CMU SSO).
Journal slug: iijindinv
~68 issues (2010–2026), Vol 1–17, 4 issues/year.

Usage:
  conda activate emi
  python src/jbis_scraper.py [--delay 2.0]

Output: src/data/pdfs/JBIS/jbis_v{vol}_i{issue}_p{page}_{slug}.pdf
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

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL   = "https://pm-research.com"
ISSUES_URL = BASE_URL + "/content/iijindinv"
CMU_ENTRY  = ISSUES_URL + "?implicit-login=true"

OUTPUT_DIR = Path("src/data/pdfs/JBIS")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE   = OUTPUT_DIR / "_done.txt"
FAIL_FILE  = OUTPUT_DIR / "_failed.txt"

MIN_DELAY = 2.0
MAX_DELAY = 4.5

ISSUE_PATTERN   = re.compile(r"^/content/iijindinv/\d+/\d+$")
ARTICLE_PATTERN = re.compile(r"^/content/iijindinv/(\d+)/(\d+)/(\d+)$")

PREFIX = "jbis"

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep(lo=None, hi=None):
    time.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))

def slugify(text: str, n: int = 50) -> str:
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

def is_malformed(href: str) -> bool:
    return ":::" in href or "%3A%3A%3A" in href or "%3a%3a%3a" in href

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
                '#onetrust-banner-sdk, [class*="onetrust"], [id*="onetrust"]'
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
    time.sleep(0.3)
    dismiss_overlays(driver)
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)

# ── Auth ───────────────────────────────────────────────────────────────────────

def auth(driver, prompt: bool = True) -> requests.Session:
    print(f"[auth] Opening: {CMU_ENTRY}")
    driver.get(CMU_ENTRY)
    if prompt:
        print("\n>>> Complete CMU login if prompted.")
        print(">>> Wait until the journal page fully loads.")
        print(">>> Then press ENTER.\n")
        input(">>> ")
    else:
        print("[auth] Auto-resuming — waiting 5s...")
        time.sleep(5)
    ua = driver.execute_script("return navigator.userAgent;")
    session = requests.Session()
    session.headers.update({
        "User-Agent": ua,
        "Referer": BASE_URL,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    })
    sync_session(driver, session)
    print("[auth] ✓ Cookies synced\n")
    return session

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
            new_session = auth(new_driver, prompt=False)
            print("[resurrect] ✓ Browser restarted\n")
            return new_driver, new_session
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")

    raise RuntimeError("Could not resurrect browser after 3 attempts — rerun manually")

# ── Issue index crawl ─────────────────────────────────────────────────────────

def get_all_issue_urls(driver) -> list[str]:
    print(f"  Loading: {ISSUES_URL}")
    driver.get(ISSUES_URL)
    sleep(2, 4)

    print("  Clicking 'All issues' tab...")
    try:
        tab = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[normalize-space(text())='All issues']")
            )
        )
        safe_click(driver, tab)
        sleep(2, 3)
        print("  ✓ Tab clicked")
    except TimeoutException:
        print("  [warn] Tab not found — proceeding")

    issue_urls, seen, page = [], set(), 1

    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new_urls = []
        for a in soup.find_all("a", href=ISSUE_PATTERN):
            full = urljoin(BASE_URL, a["href"])
            if full not in seen:
                seen.add(full)
                new_urls.append(full)
        issue_urls.extend(new_urls)
        print(f"  [page {page}] +{len(new_urls)} issues (total: {len(issue_urls)})")

        next_btn = None
        for xpath in [
            "//a[normalize-space(text())='Next ›']",
            "//a[normalize-space(text())='Next']",
            "//a[contains(@class,'next')]",
            "//li[contains(@class,'next')]/a",
            "//a[contains(text(),'Next')]",
        ]:
            try:
                next_btn = driver.find_element(By.XPATH, xpath)
                break
            except NoSuchElementException:
                continue

        if next_btn is None:
            print("  No Next button — done paginating")
            break

        safe_click(driver, next_btn)
        sleep(2, 3)
        page += 1
        if page > 30:
            break

    return issue_urls

# ── Article listing ───────────────────────────────────────────────────────────

def get_articles(driver, issue_url: str) -> list[dict]:
    driver.get(issue_url)
    sleep()
    dismiss_overlays(driver)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href,'/content/iijindinv/')]")
            )
        )
    except TimeoutException:
        print("  [warn] timeout on issue page")

    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles, seen = [], set()

    for a in soup.find_all("a", href=ARTICLE_PATTERN):
        href = a["href"]
        if href in seen or is_malformed(href):
            continue
        seen.add(href)
        m = ARTICLE_PATTERN.match(href)
        vol, issue, startpage = m.group(1), m.group(2), m.group(3)
        title = a.get_text(strip=True) or f"article_{startpage}"
        article_url = urljoin(BASE_URL, href)
        articles.append({
            "url":       article_url,
            "pdf_url":   article_url + ".full.pdf",
            "title":     title,
            "vol":       vol,
            "issue":     issue,
            "startpage": startpage,
        })
    return articles

# ── PDF download ──────────────────────────────────────────────────────────────

def download_pdf(session: requests.Session, article: dict, retries: int = 3) -> bool:
    fname = (
        f"{PREFIX}_v{article['vol']}_i{article['issue']}"
        f"_p{article['startpage']}_{slugify(article['title'])}.pdf"
    )
    fpath = OUTPUT_DIR / fname

    if fpath.exists():
        print(f"    [skip] {fname}")
        return True

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(
                article["pdf_url"], stream=True, timeout=60,
                headers={"Referer": article["url"]},
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
                mark_failed(article["pdf_url"])
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
    print("JBIS Scraper  ·  pm-research.com  ·  CMU SSO")
    print(f"{len(done)} articles already downloaded")
    print("=" * 60)

    driver  = build_driver()
    session = auth(driver, prompt=True)

    print("[2/4] Collecting all issue URLs...")
    issue_urls = get_all_issue_urls(driver)
    print(f"  → {len(issue_urls)} issues\n")

    ok = fail = skip = 0

    for i, issue_url in enumerate(issue_urls, 1):
        print(f"\n[issue {i}/{len(issue_urls)}] {issue_url}")

        for attempt in range(3):
            try:
                articles = get_articles(driver, issue_url)
                break
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"  [browser crash] {e}")
                if attempt < 2:
                    driver, session = resurrect(driver)
                else:
                    print("  [give up] skipping issue after 3 crashes")
                    articles = []
                    break

        print(f"  {len(articles)} articles")
        sync_session(driver, session)

        for art in articles:
            if art["pdf_url"] in done:
                skip += 1
                print(f"    [skip] {art['title'][:55]}")
                continue

            ok_ = download_pdf(session, art)
            if ok_:
                mark_done(art["pdf_url"])
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