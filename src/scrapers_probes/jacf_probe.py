"""
jacf_probe3.py  —  Inspect section heading structure in older JACF issues
-------------------------------------------------------------------------
Checks a Vol 17 issue to see what tags/classes Wiley uses for section
headers vs. what the current scraper expects (h3.toc__heading).

Run from gap/src/:
  python scrapers/jacf_probe3.py
"""

import time
import re
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

CHROME_VERSION = 146
TEST_URL = "https://onlinelibrary.wiley.com/toc/17456622/2005/17/2"

def build_driver():
    opts = uc.ChromeOptions()
    opts.add_argument("--window-size=1280,900")
    return uc.Chrome(options=opts, version_main=CHROME_VERSION)

def main():
    print(f"Probing: {TEST_URL}\n")
    driver = build_driver()
    try:
        driver.get(TEST_URL)
        time.sleep(4)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # A. All headings
        print("=== ALL HEADINGS (h2, h3, h4) ===")
        for tag in soup.find_all(["h2", "h3", "h4"]):
            text = tag.get_text(strip=True)
            cls  = tag.get("class", [])
            if text:
                print(f"  <{tag.name} class={cls}> {text!r}")

        # B. PDF links with nearest preceding headings
        print("\n=== PDF LINKS + NEAREST PRECEDING HEADINGS ===")
        pdf_links = soup.find_all("a", href=re.compile(r"/doi/epdf/10\.", re.I))
        print(f"  Total epdf links: {len(pdf_links)}")
        for link in pdf_links[:6]:
            href = link.get("href", "")
            h3 = link.find_previous("h3")
            h2 = link.find_previous("h2")
            h3_text = h3.get_text(strip=True) if h3 else None
            h3_cls  = h3.get("class") if h3 else None
            h2_text = h2.get_text(strip=True) if h2 else None
            h2_cls  = h2.get("class") if h2 else None
            print(f"\n  href    : {href}")
            print(f"  prev h3 : {h3_text!r} | class={h3_cls}")
            print(f"  prev h2 : {h2_text!r} | class={h2_cls}")

        input("\nPress Enter to close...")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()