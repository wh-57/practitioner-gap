"""
jor_scraper.py  —  Journal of Risk  (v4)
-----------------------------------------
Platform: risk.net (Infopro Digital)
Auth: None required (open access browser session)
Navigation: dropdown option values are URL paths → direct navigation
PDF: Chrome auto-downloads on driver.get(pdf_url) — no button click needed

Changes from v3:
  - OUTPUT_DIR updated to gap/data/pdfs/JOR/

Usage:
  conda activate emi
  python src/jor_scraper.py [--delay 2.0]
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

BASE_URL    = "https://www.risk.net"
JOURNAL_URL = BASE_URL + "/journal-of-risk"

OUTPUT_DIR = Path("../data/pdfs/JOR")          # final PDFs land here
TEMP_DIR   = OUTPUT_DIR / "_temp_downloads"      # Chrome downloads here first
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = OUTPUT_DIR / "_done.txt"
FAIL_FILE = OUTPUT_DIR / "_failed.txt"

MIN_DELAY = 2.0
MAX_DELAY = 4.5

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

# ── Driver ─────────────────────────────────────────────────────────────────────

def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1280,900")
    opts.add_experimental_option("prefs", {
        "download.default_directory":         str(TEMP_DIR.resolve()),
        "download.prompt_for_download":        False,
        "download.directory_upgrade":          True,
        "plugins.always_open_pdf_externally":  True,  # force download, not viewer
    })
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
    time.sleep(0.3)
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
    raise RuntimeError("Could not resurrect browser after 3 attempts — rerun manually")

# ── Issue index ────────────────────────────────────────────────────────────────

def get_all_issue_values(driver) -> list[tuple[str, str]]:
    """Returns list of (url_path, label) for all issues in the dropdown."""
    print(f"  Loading: {JOURNAL_URL}")
    driver.get(JOURNAL_URL)
    sleep(2, 4)
    dismiss_overlays(driver)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
        )
    except TimeoutException:
        print("  [warn] dropdown not found")
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    select_el = soup.find("select")
    if not select_el:
        print("  [warn] no <select> found")
        return []

    options = []
    for opt in select_el.find_all("option"):
        val = opt.get("value", "").strip()
        label = opt.get_text(strip=True)
        if val:
            options.append((val, label))

    print(f"  Found {len(options)} issues. Sample values:")
    for v, l in options[:3]:
        print(f"    {l!r} -> {v!r}")

    return options

# ── Navigate to issue ─────────────────────────────────────────────────────────

def navigate_to_issue(driver, value: str, label: str) -> bool:
    try:
        if value.startswith("http"):
            target = value
        elif value.startswith("/"):
            target = BASE_URL + value
        else:
            target = f"{JOURNAL_URL}/{value}"

        print(f"  → {target}")
        driver.get(target)
        sleep(2, 4)
        dismiss_overlays(driver)
        return True

    except Exception as e:
        print(f"  [warn] navigation failed for {label}: {e}")
        return False

# ── Article listing ───────────────────────────────────────────────────────────

def get_articles(driver, label: str) -> list[dict]:
    sleep(1, 2)
    dismiss_overlays(driver)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles = []

    pdf_links = soup.find_all("a", string=re.compile(r"Download PDF", re.I))

    for link in pdf_links:
        href = link.get("href", "")
        if not href:
            continue

        # Walk up all parent levels until we find a non-section-header title
        title = "untitled"
        for parent in link.parents:
            if parent.name in ["div", "article", "li", "section"]:
                for heading in parent.find_all(["h1", "h2", "h3", "h4"]):
                    text = heading.get_text(strip=True)
                    if text and "papers in this issue" not in text.lower():
                        title = text
                        break
                if title != "untitled":
                    break

        if href.startswith("/"):
            pdf_url = BASE_URL + href
        elif href.startswith("http"):
            pdf_url = href
        else:
            pdf_url = BASE_URL + "/" + href

        articles.append({
            "title":     title,
            "pdf_url":   pdf_url,
            "vol_label": label,
        })

    return articles

# ── Download ──────────────────────────────────────────────────────────────────

def wait_for_download(existing_names: set, timeout: int = 60) -> str | None:
    """
    Wait for a new PDF to finish in TEMP_DIR.
    Compares against existing_names (set of filenames before navigation).
    Returns new filename or None on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        current   = {f.name for f in TEMP_DIR.glob("*.pdf")}
        in_flight = list(TEMP_DIR.glob("*.crdownload"))
        new_files = current - existing_names
        if new_files and not in_flight:
            return list(new_files)[0]
    return None


def download_pdf_selenium(driver, article: dict) -> bool:
    # Check if already done by pdf_url
    if article["pdf_url"] in load_done():
        print(f"    [skip] {article['pdf_url']}")
        return True

    try:
        existing_names = {f.name for f in TEMP_DIR.glob("*.pdf")}

        driver.get(article["pdf_url"])

        downloaded = wait_for_download(existing_names)
        if not downloaded:
            print(f"    [warn] download timed out: {article['pdf_url']}")
            mark_failed(article["pdf_url"])
            return False

        # Keep Chrome's original filename, just prefix jor_
        src = TEMP_DIR / downloaded
        fname = f"jor_{downloaded}" if not downloaded.startswith("jor_") else downloaded
        fpath = OUTPUT_DIR / fname

        # Handle collision (same Chrome filename across runs)
        if fpath.exists():
            fpath.unlink()

        src.rename(fpath)
        print(f"    [✓] {fname} ({fpath.stat().st_size // 1024} KB)")
        return True

    except Exception as e:
        print(f"    [error] {article['pdf_url']}: {e}")
        mark_failed(article["pdf_url"])
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
    print("JOR Scraper v4  ·  risk.net  ·  Selenium auto-download")
    print(f"{len(done)} articles already downloaded")
    print("=" * 60)

    driver = build_driver()

    print("\n[1/3] Collecting all issue dropdown options...")
    issue_values = get_all_issue_values(driver)
    print(f"  → {len(issue_values)} issues\n")

    ok = fail = skip = 0

    for i, (value, label) in enumerate(issue_values, 1):
        print(f"\n[issue {i}/{len(issue_values)}] {label}")

        for attempt in range(3):
            try:
                success = navigate_to_issue(driver, value, label)
                if not success:
                    articles = []
                    break
                articles = get_articles(driver, label)
                break
            except (InvalidSessionIdException, WebDriverException) as e:
                print(f"  [browser crash] {e}")
                if attempt < 2:
                    driver = resurrect(driver)
                else:
                    print("  [give up] skipping issue")
                    articles = []
                    break

        print(f"  {len(articles)} articles")

        for art in articles:
            if art["pdf_url"] in done:
                skip += 1
                print(f"    [skip] {art['title'][:55]}")
                continue

            ok_ = download_pdf_selenium(driver, art)
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