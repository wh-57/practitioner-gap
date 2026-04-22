"""
jacf_scraper.py  —  Journal of Applied Corporate Finance  (Wiley)  v4
----------------------------------------------------------------------
Platform : onlinelibrary.wiley.com
Auth     : CMU institutional access (must be on CMU network or VPN)
Driver   : undetected_chromedriver (bypasses Cloudflare)

Archive strategy (confirmed by probe2):
  1. Load /loi/17456622, expand decade accordions
  2. Collect all /loi/17456622/year/YYYY links from sidebar
  3. Navigate each year page → collect /toc/17456622/YYYY/VOL/ISSUE links
  4. Scrape each issue TOC for Original Articles only

Changes from v3:
  - get_articles no longer swallows WebDriver exceptions — lets them
    propagate so main() resurrection logic can trigger
  - PDF download uses /doi/pdfdirect/ instead of /doi/pdf/ (pdfdirect
    serves raw PDF bytes; pdf/ redirects through Wiley's viewer chain
    and never triggers Chrome's download handler)
  - download_pdf re-raises InvalidSessionIdException / WebDriverException
    so the main loop can resurrect the browser and retry
  - Main download loop now handles browser crashes and resurrects

Section filter — WHITELIST:
  KEEP only h3.toc__heading containing 'original article'

Output: gap/data/pdfs/JACF/  (run from gap/src/)

Usage:
  conda activate emi
  cd Desktop/gap/src
  python scrapers/jacf_scraper.py [--delay 2.0]
"""

import re
import time
import random
import argparse
from pathlib import Path

from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    InvalidSessionIdException,
    WebDriverException,
)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL       = "https://onlinelibrary.wiley.com"
ARCHIVE_URL    = BASE_URL + "/loi/17456622"
CHROME_VERSION = 146

OUTPUT_DIR = Path("../data/pdfs/JACF")
TEMP_DIR   = OUTPUT_DIR / "_temp_downloads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = OUTPUT_DIR / "_done.txt"
FAIL_FILE = OUTPUT_DIR / "_failed.txt"

MIN_DELAY = 2.5
MAX_DELAY = 5.0

KEEP_SECTION = "original article"

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep(lo=None, hi=None):
    time.sleep(random.uniform(lo or MIN_DELAY, hi or MAX_DELAY))

def load_done() -> set:
    return set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()

def mark_done(url: str):
    with open(LOG_FILE, "a") as f:
        f.write(url + "\n")

def mark_failed(url: str):
    with open(FAIL_FILE, "a") as f:
        f.write(url + "\n")

def slugify(text: str, n: int = 60) -> str:
    s = re.sub(r"[^\w\s-]", "", text.lower())
    s = re.sub(r"[\s_-]+", "_", s).strip("_")
    return s[:n]

# ── Driver ─────────────────────────────────────────────────────────────────────

def build_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--no-sandbox")
    opts.add_experimental_option("prefs", {
        "download.default_directory":        str(TEMP_DIR.resolve()),
        "download.prompt_for_download":       False,
        "download.directory_upgrade":         True,
        "plugins.always_open_pdf_externally": True,
    })
    return uc.Chrome(options=opts, version_main=CHROME_VERSION)

def dismiss_overlays(driver):
    try:
        driver.execute_script("""
            document.querySelectorAll(
                '[class*="cookie"],[id*="cookie"],[class*="consent"],
                [id*="consent"],[class*="onetrust"],[id*="onetrust"]'
            ).forEach(e => e.remove());
            document.body.style.overflow = '';
        """)
        time.sleep(0.3)
    except Exception:
        pass

def check_cloudflare(driver, url: str, wait: int = 30):
    phrases = ["verifying you are human", "checking your browser",
               "just a moment", "enable javascript and cookies"]
    text = BeautifulSoup(driver.page_source, "html.parser").get_text().lower()
    if not any(p in text for p in phrases):
        return
    print(f"\n  ⚠  Cloudflare on {url} — solve in browser, resuming in {wait}s max.")
    deadline = time.time() + wait
    while time.time() < deadline:
        time.sleep(2)
        text = BeautifulSoup(driver.page_source, "html.parser").get_text().lower()
        if not any(p in text for p in phrases):
            print("  ✓ Challenge cleared.")
            return
    print("  [warn] Cloudflare timeout — continuing.")

def resurrect(old_driver) -> uc.Chrome:
    print("\n[resurrect] Browser died — relaunching...")
    try:
        old_driver.quit()
    except Exception:
        pass
    for attempt in range(1, 4):
        try:
            time.sleep(5 * attempt)
            driver = build_driver()
            print("[resurrect] ✓ Done\n")
            return driver
        except Exception as e:
            print(f"[resurrect] attempt {attempt} failed: {e}")
    raise RuntimeError("Could not resurrect browser after 3 attempts")

# ── Step 1: collect year links ────────────────────────────────────────────────

def collect_year_links(driver) -> list[str]:
    print(f"  Loading archive: {ARCHIVE_URL}")
    driver.get(ARCHIVE_URL)
    sleep(3, 4)
    check_cloudflare(driver, ARCHIVE_URL)
    dismiss_overlays(driver)

    # Click each decade accordion exactly once (track by label text)
    clicked = set()
    for tag in ["li", "a", "button", "span"]:
        for el in driver.find_elements(By.CSS_SELECTOR, tag):
            txt = el.text.strip()
            if re.match(r"\d{4}\s*[-–]\s*\d{4}$", txt) and txt not in clicked:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    clicked.add(txt)
                    print(f"  expanded: {txt}")
                    time.sleep(1.0)
                except Exception:
                    pass

    sleep(2, 3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    seen  = set()
    links = []
    for a in soup.find_all("a", href=re.compile(r"/loi/17456622/year/\d{4}$")):
        href = a["href"]
        full = BASE_URL + href if href.startswith("/") else href
        if full not in seen:
            seen.add(full)
            links.append(full)
    print(f"\n  {len(links)} year links collected.")
    return links

# ── Step 2: collect issue links from each year page ───────────────────────────

def collect_issue_links(driver, year_url: str) -> list[tuple[str, str]]:
    driver.get(year_url)
    sleep(2, 3)
    check_cloudflare(driver, year_url)
    soup  = BeautifulSoup(driver.page_source, "html.parser")
    seen  = set()
    issues = []
    for a in soup.find_all("a", href=re.compile(r"/toc/\d+/\d+/\d+/\d+")):
        href  = a["href"]
        full  = BASE_URL + href if href.startswith("/") else href
        label = a.get_text(strip=True)
        if full not in seen and label:
            seen.add(full)
            issues.append((full, label))
    return issues

# ── Step 3: parse one issue TOC ───────────────────────────────────────────────

def get_articles(driver, toc_url: str, label: str) -> list[dict]:
    """
    Parse issue TOC. Whitelist: only PDF links under 'original article' section.
    NOTE: does NOT catch WebDriver exceptions — lets them propagate to main()
    so the resurrection loop can trigger.
    """
    driver.get(toc_url)
    sleep(2, 3)
    check_cloudflare(driver, toc_url)
    dismiss_overlays(driver)

    soup     = BeautifulSoup(driver.page_source, "html.parser")
    articles = []

    all_epdf = soup.find_all("a", href=re.compile(r"/doi/epdf/10\.", re.I))
    if not all_epdf:
        page_title = soup.title.get_text(strip=True) if soup.title else "no title"
        print(f"  [warn] 0 epdf links on {label} — page title: {page_title!r}")

    # Detect whether this issue uses section headers (modern) or not (pre-~2005)
    has_section_headers = bool(
        soup.find("h3", class_=re.compile(r"toc__heading"))
    )

    # Titles to always skip regardless of era (h2 text, lowercase substring)
    SKIP_TITLES = [
        "a message from the editor",
        "message from the editor",
        "message form the editor",
        "executive summar",
        "issue information",
    ]

    for pdf_link in all_epdf:
        href = pdf_link.get("href", "")
        if not href:
            continue

        title_tag = pdf_link.find_previous("h2")
        title     = title_tag.get_text(strip=True) if title_tag else "untitled"

        if has_section_headers:
            # Modern issues: whitelist by h3.toc__heading section name
            section_tag = pdf_link.find_previous("h3", class_=re.compile(r"toc__heading"))
            section     = section_tag.get_text(strip=True).lower() if section_tag else ""
            if KEEP_SECTION not in section:
                continue
        else:
            # Older issues: no section headers — skip known non-article titles
            title_lower = title.lower()
            if any(skip in title_lower for skip in SKIP_TITLES):
                continue

        epdf_url     = BASE_URL + href if href.startswith("/") else href
        # pdfdirect serves raw PDF bytes; pdf/ redirects through viewer chain
        pdfdirect_url = epdf_url.replace("/doi/epdf/", "/doi/pdfdirect/")

        articles.append({
            "title":        title,
            "pdf_url":      pdfdirect_url,
            "epdf_url":     epdf_url,
            "vol_label":    label,
        })

    return articles

# ── Step 4: download one PDF ──────────────────────────────────────────────────

def wait_for_download(existing: set, timeout: int = 90) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        current   = {f.name for f in TEMP_DIR.glob("*.pdf")}
        in_flight = list(TEMP_DIR.glob("*.crdownload"))
        new       = current - existing
        if new and not in_flight:
            return list(new)[0]
    return None

def download_pdf(driver, article: dict) -> bool:
    """
    Returns True on success, False on permanent failure.
    Re-raises InvalidSessionIdException / WebDriverException so the
    caller can resurrect the browser and retry.
    """
    url = article["pdf_url"]
    if url in load_done():
        return True

    existing = {f.name for f in TEMP_DIR.glob("*.pdf")}

    # Navigate — let browser crashes propagate to caller
    driver.get(url)
    sleep(2, 3)
    downloaded = wait_for_download(existing, timeout=90)

    if not downloaded:
        # Fallback: try epdf URL
        print(f"    [retry] pdfdirect timed out, trying epdf/...")
        existing2 = {f.name for f in TEMP_DIR.glob("*.pdf")}
        driver.get(article["epdf_url"])
        sleep(2, 3)
        downloaded = wait_for_download(existing2, timeout=60)

    if not downloaded:
        print(f"    [warn] both URLs timed out: {url}")
        mark_failed(url)
        return False

    src   = TEMP_DIR / downloaded
    slug  = slugify(article["title"])
    fname = f"jacf_{slug}.pdf"
    fpath = OUTPUT_DIR / fname
    counter = 1
    while fpath.exists():
        fpath = OUTPUT_DIR / f"jacf_{slug}_{counter}.pdf"
        counter += 1

    src.rename(fpath)
    print(f"    [✓] {fpath.name} ({fpath.stat().st_size // 1024} KB)")
    return True

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global MIN_DELAY, MAX_DELAY

    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None,
                        help="Base delay in seconds (default: 2.5)")
    args = parser.parse_args()

    if args.delay:
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done = load_done()

    print("=" * 60)
    print("JACF Scraper v4  ·  Wiley  ·  undetected-chromedriver")
    print(f"Output  : {OUTPUT_DIR.resolve()}")
    print(f"Keeping : sections containing '{KEEP_SECTION}'")
    print(f"Done    : {len(done)} articles already downloaded")
    print("=" * 60)
    print("\nChrome window will open. Solve any Cloudflare challenge.\n")

    driver = build_driver()
    ok = fail = skip = 0

    try:
        # ── 1. Collect year links ─────────────────────────────────────────────
        print("[1/3] Expanding archive and collecting year links...")
        year_links = collect_year_links(driver)
        if not year_links:
            print("[abort] No year links found. Check VPN / CMU access.")
            return

        # ── 2. Collect all issue TOC links ────────────────────────────────────
        print(f"\n[2/3] Collecting issue links from {len(year_links)} year pages...")
        all_issues = []
        seen_tocs  = set()
        for year_url in year_links:
            year = year_url.split("/")[-1]
            print(f"  {year} … ", end="", flush=True)
            try:
                issues = collect_issue_links(driver, year_url)
                new    = [(u, l) for u, l in issues if u not in seen_tocs]
                for u, l in new:
                    seen_tocs.add(u)
                all_issues.extend(new)
                print(f"{len(new)} issue(s)")
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"crash — {e}")
                driver = resurrect(driver)

        print(f"\n  → {len(all_issues)} total issues collected.")

        # ── 3. Scrape each issue ──────────────────────────────────────────────
        print(f"\n[3/3] Downloading Original Articles...\n")

        for i, (toc_url, label) in enumerate(all_issues, 1):
            print(f"\n[issue {i}/{len(all_issues)}] {label}")

            # get_articles: retry up to 3x, resurrect on browser crash
            articles = []
            for attempt in range(3):
                try:
                    articles = get_articles(driver, toc_url, label)
                    break
                except (InvalidSessionIdException, WebDriverException) as e:
                    print(f"  [crash during get_articles] {e}")
                    if attempt < 2:
                        driver = resurrect(driver)
                    else:
                        print("  [give up] skipping issue")

            new_arts  = [a for a in articles if a["pdf_url"] not in done]
            skip     += len(articles) - len(new_arts)
            print(f"  {len(articles)} original article(s) | "
                  f"{len(articles)-len(new_arts)} skip | {len(new_arts)} to download")

            for art in new_arts:
                print(f"    {art['title'][:65]}")
                # download_pdf: retry up to 3x on browser crash
                for attempt in range(3):
                    try:
                        success = download_pdf(driver, art)
                        if success:
                            mark_done(art["pdf_url"])
                            ok += 1
                        else:
                            fail += 1
                        break
                    except (InvalidSessionIdException, WebDriverException) as e:
                        print(f"    [crash during download] {e}")
                        if attempt < 2:
                            driver = resurrect(driver)
                        else:
                            print("    [give up] marking failed")
                            mark_failed(art["pdf_url"])
                            fail += 1
                            break
                sleep()

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print("\n" + "=" * 60)
    print(f"Done.  Downloaded: {ok}   Failed: {fail}   Skipped: {skip}")
    print(f"PDFs : {OUTPUT_DIR.resolve()}")
    if fail:
        print(f"Failures: {FAIL_FILE.resolve()}")
    print("=" * 60)


if __name__ == "__main__":
    main()