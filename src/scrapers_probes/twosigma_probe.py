"""
twosigma_probe.py  —  Two Sigma Venn Insights  v1
--------------------------------------------------
Goal: understand the article listing and content structure before scraping.

Checks:
  1. What happens on scroll / "load more" — XHR API or DOM pagination?
  2. How many articles are initially visible vs. after expansion
  3. What the article page HTML looks like (content div, references section)
  4. Whether selenium-wire can intercept a JSON API endpoint

Run from gap/src/:
  python scrapers/twosigma_probe.py
"""

import time
import re
import json
from pathlib import Path
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

INSIGHTS_URL   = "https://www.venn.twosigma.com/insights"
CHROME_VERSION = 146

def build_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--no-sandbox")
    return uc.Chrome(options=opts, version_main=CHROME_VERSION)

def save(path, content):
    Path(path).write_text(content, encoding="utf-8")
    print(f"  [saved] {Path(path).resolve()}")

def main():
    driver = build_driver()
    try:
        # ── 1. Load insights listing page ─────────────────────────────────────
        print(f"\nLoading: {INSIGHTS_URL}")
        driver.get(INSIGHTS_URL)
        time.sleep(4)
        save("ts_listing_before.html", driver.page_source)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Count article links before any interaction
        article_links_before = _find_article_links(soup)
        print(f"\n  Article links before scroll/load: {len(article_links_before)}")
        for url, title in article_links_before[:5]:
            print(f"    {title!r:50s}  {url}")

        # ── 2. Look for a "load more" button ──────────────────────────────────
        print("\n  Looking for 'load more' / pagination buttons:")
        for sel in ["button", "a"]:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                txt = el.text.strip().lower()
                if any(kw in txt for kw in ["load more", "show more", "next", "view more"]):
                    tag  = el.tag_name
                    cls  = el.get_attribute("class") or ""
                    href = el.get_attribute("href") or ""
                    print(f"    <{tag} class={cls!r}> text={el.text.strip()!r} href={href!r}")

        # ── 3. Scroll to bottom and check for new content ─────────────────────
        print("\n  Scrolling to bottom to trigger lazy load...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("    Page height stabilized — no more content loaded")
                break
            print(f"    Height changed: {last_height} → {new_height}")
            last_height = new_height

        save("ts_listing_after.html", driver.page_source)
        soup2 = BeautifulSoup(driver.page_source, "html.parser")
        article_links_after = _find_article_links(soup2)
        print(f"\n  Article links after scroll: {len(article_links_after)}")
        for url, title in article_links_after[:10]:
            print(f"    {title!r:50s}  {url}")

        # ── 4. Navigate to first article, inspect structure ───────────────────
        if article_links_after:
            art_url, art_title = article_links_after[0]
            print(f"\n  Navigating to first article: {art_url}")
            driver.get(art_url)
            time.sleep(3)
            save("ts_article.html", driver.page_source)

            art_soup = BeautifulSoup(driver.page_source, "html.parser")

            print("\n  [A] Main content divs (class names):")
            for div in art_soup.find_all("div"):
                cls = div.get("class", [])
                cls_str = " ".join(cls)
                if any(kw in cls_str.lower() for kw in ["content", "article", "body", "post", "insight", "text"]):
                    txt_len = len(div.get_text(strip=True))
                    if txt_len > 200:
                        print(f"    <div class={cls}> text_len={txt_len}")

            print("\n  [B] References section:")
            for tag in art_soup.find_all(["h2", "h3", "h4"]):
                if "reference" in tag.get_text(strip=True).lower():
                    print(f"    Found: <{tag.name}> {tag.get_text(strip=True)!r}")
                    # Print siblings after it
                    sib = tag.find_next_sibling()
                    for _ in range(5):
                        if sib:
                            print(f"      next sibling: <{sib.name}> {sib.get_text(strip=True)[:100]!r}")
                            sib = sib.find_next_sibling()

            print("\n  [C] Footnote / reference list items:")
            for el in art_soup.find_all(["ol", "ul", "p"]):
                txt = el.get_text(strip=True)
                if re.search(r"^\d+[\.\)]", txt) and len(txt) > 30:
                    print(f"    {txt[:120]!r}")
                    break

        input("\nPress Enter to close browser...")
    finally:
        driver.quit()

def _find_article_links(soup) -> list[tuple[str, str]]:
    """Find all insight article links on the listing page."""
    BASE = "https://www.venn.twosigma.com"
    links = []
    seen  = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Article URLs: /insights/some-slug (not just /insights itself)
        if re.match(r"^/insights/[a-z0-9-]+$", href) or \
           re.match(r"^https://www\.venn\.twosigma\.com/insights/[a-z0-9-]+$", href):
            full  = BASE + href if href.startswith("/") else href
            title = a.get_text(strip=True) or href
            if full not in seen and full != BASE + "/insights":
                seen.add(full)
                links.append((full, title))
    return links

if __name__ == "__main__":
    main()