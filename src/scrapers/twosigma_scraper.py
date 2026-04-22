"""
twosigma_scraper.py  —  Two Sigma Venn Insights  v1
----------------------------------------------------
Platform  : venn.twosigma.com
Auth      : None (public)
Driver    : undetected_chromedriver (avoids bot detection)

Listing strategy (confirmed by probe):
  - Articles listed at /insights, /insights/page/2, /insights/page/3, ...
  - Stop when a page returns 0 article links
  - Article URLs match: /insights/<slug>

Content extraction:
  - Target div: class="post-text" (confirmed by probe, text_len ~8k)
  - Strip all HTML tags, save as clean .txt
  - References are inline numbered paragraphs inside post-text — no special handling needed

Output: gap/data/Other_Corpus/TwoSigma/  (run from gap/src/)
Format: {slug}.txt  (clean extracted text, no HTML)

Usage:
  conda activate emi
  cd Desktop/gap/src
  python scrapers/twosigma_scraper.py [--delay 2.0]
"""

import re
import time
import random
import argparse
from pathlib import Path

from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.common.exceptions import (
    InvalidSessionIdException,
    WebDriverException,
)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL       = "https://www.venn.twosigma.com"
LISTING_URL    = BASE_URL + "/insights"
CHROME_VERSION = 146

# scrapers/ -> src/ -> gap/ -> then data/
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "Other_Corpus" / "TwoSigma"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE  = OUTPUT_DIR / "_done.txt"
FAIL_FILE = OUTPUT_DIR / "_failed.txt"

MIN_DELAY = 2.0
MAX_DELAY = 4.0

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

def slug_from_url(url: str) -> str:
    return url.rstrip("/").split("/")[-1][:120]

# ── Driver ─────────────────────────────────────────────────────────────────────

def build_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--no-sandbox")
    return uc.Chrome(options=opts, version_main=CHROME_VERSION)

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

# ── Step 1: collect all article URLs via pagination ────────────────────────────

def collect_article_urls(driver) -> list[str]:
    """
    Iterate /insights, /insights/page/2, /insights/page/3, ...
    Stop when a page yields 0 new article links.
    """
    all_urls = []
    seen     = set()
    page     = 1

    while True:
        url = LISTING_URL if page == 1 else f"{LISTING_URL}/page/{page}"
        print(f"  page {page}: {url} … ", end="", flush=True)

        try:
            driver.get(url)
            sleep(2, 3)
        except (InvalidSessionIdException, WebDriverException) as e:
            print(f"crash — {e}")
            driver = resurrect(driver)
            continue

        soup  = BeautifulSoup(driver.page_source, "html.parser")
        links = _extract_article_links(soup)
        new   = [u for u in links if u not in seen]

        if not new:
            print(f"0 new — stopping at page {page}")
            break

        for u in new:
            seen.add(u)
        all_urls.extend(new)
        print(f"{len(new)} article(s)  [total: {len(all_urls)}]")
        page += 1
        sleep(1, 2)

    return all_urls

def _extract_article_links(soup) -> list[str]:
    links = []
    seen  = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Match /insights/<slug> but not /insights itself or /insights/page/N
        if re.match(r"^/insights/[a-z0-9][a-z0-9-]+$", href):
            full = BASE_URL + href
            if full not in seen:
                seen.add(full)
                links.append(full)
        elif re.match(r"^https://www\.venn\.twosigma\.com/insights/[a-z0-9][a-z0-9-]+$", href):
            if href not in seen:
                seen.add(href)
                links.append(href)
    return links

# ── Step 2: extract and save one article ──────────────────────────────────────

def extract_article(driver, url: str) -> bool:
    """
    Navigate to article, extract div.post-text content as clean text,
    save to OUTPUT_DIR/{slug}.txt. Returns True on success.
    """
    try:
        driver.get(url)
        sleep(2, 3)
    except (InvalidSessionIdException, WebDriverException):
        raise  # let caller handle

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Primary target: div.post-text
    content_div = soup.find("div", class_="post-text")

    # Fallback: largest div with post-related class
    if not content_div:
        for cls in ["post-main-col", "post-section", "body-wrapper"]:
            content_div = soup.find("div", class_=cls)
            if content_div:
                break

    if not content_div:
        print(f"    [warn] no content div found: {url}")
        mark_failed(url)
        return False

    # Extract clean text: use get_text on the whole div so no tags are missed
    # (e.g. "References" heading may be <p><strong>...</strong></p>, not <h3>)
    # Replace block-level tags with newlines before extracting to preserve structure
    for tag in content_div.find_all(["p", "h1", "h2", "h3", "h4", "h5", "li", "br"]):
        tag.insert_before("\n\n")
    clean_text = content_div.get_text(separator="", strip=False)
    # Normalise whitespace: collapse runs of 3+ newlines to two
    clean_text = re.sub(r"\n{3,}", "\n\n", clean_text).strip()

    if len(clean_text) < 100:
        print(f"    [warn] content too short ({len(clean_text)} chars): {url}")
        mark_failed(url)
        return False

    # Add source URL as header
    slug  = slug_from_url(url)
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else slug
    header = f"SOURCE: {url}\nTITLE: {title}\n\n"

    fpath = OUTPUT_DIR / f"{slug}.txt"
    fpath.write_text(header + clean_text, encoding="utf-8")
    print(f"    [✓] {fpath.name} ({len(clean_text):,} chars)")
    return True

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global MIN_DELAY, MAX_DELAY

    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=None,
                        help="Base delay in seconds (default: 2.0)")
    args = parser.parse_args()

    if args.delay:
        MIN_DELAY = args.delay
        MAX_DELAY = args.delay * 2.0

    done = load_done()

    print("=" * 60)
    print("Two Sigma Venn Scraper v1  ·  undetected-chromedriver")
    print(f"Output : {OUTPUT_DIR.resolve()}")
    print(f"Done   : {len(done)} articles already saved")
    print("=" * 60)

    driver = build_driver()
    ok = fail = skip = 0

    try:
        # ── 1. Collect all article URLs ───────────────────────────────────────
        print("\n[1/2] Collecting article URLs via pagination...\n")
        all_urls = collect_article_urls(driver)
        print(f"\n  → {len(all_urls)} total articles found.\n")

        if not all_urls:
            print("[abort] No articles found.")
            return

        # ── 2. Extract each article ───────────────────────────────────────────
        print("[2/2] Extracting articles...\n")

        for i, url in enumerate(all_urls, 1):
            slug = slug_from_url(url)
            print(f"\n[{i}/{len(all_urls)}] {slug[:65]}")

            if url in done:
                skip += 1
                print(f"    [skip]")
                continue

            for attempt in range(3):
                try:
                    success = extract_article(driver, url)
                    if success:
                        mark_done(url)
                        ok += 1
                    else:
                        fail += 1
                    break
                except (InvalidSessionIdException, WebDriverException) as e:
                    print(f"    [crash] {e}")
                    if attempt < 2:
                        driver = resurrect(driver)
                    else:
                        print("    [give up] marking failed")
                        mark_failed(url)
                        fail += 1
                        break

            sleep()

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print("\n" + "=" * 60)
    print(f"Done.  Saved: {ok}   Failed: {fail}   Skipped: {skip}")
    print(f"Files : {OUTPUT_DIR.resolve()}")
    if fail:
        print(f"Failures: {FAIL_FILE.resolve()}")
    print("=" * 60)


if __name__ == "__main__":
    main()