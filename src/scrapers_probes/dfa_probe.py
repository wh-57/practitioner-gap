"""
dfa_probe.py  —  DOM structure probe for dimensional.com article pages
-----------------------------------------------------------------------
Loads one article, waits for JS to render, then dumps:
  1. The outer HTML of key candidate containers
  2. A flat list of all tags + char counts so we can see where text lives
  3. The full page source saved to dfa_probe_source.html for manual inspection

Usage:
  conda activate emi
  cd Desktop/gap
  python src/scrapers/dfa_probe.py
"""

import time
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

PROBE_URL = "https://www.dimensional.com/us-en/insights/is-22-trillion-a-tipping-point"
OUT_DIR   = Path(__file__).resolve().parent.parent.parent / "data" / "logs" / "DFA"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def build_driver():
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

def main():
    driver = build_driver()
    print(f"Loading: {PROBE_URL}")
    driver.get(PROBE_URL)

    # Wait up to 20s for h1
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//h1"))
        )
        print("✓ h1 found")
    except Exception:
        print("✗ h1 never appeared")

    # Extra sleep to let JS finish
    print("Waiting 5s for JS render...")
    time.sleep(5)

    source = driver.page_source
    driver.quit()

    # Save full source for manual inspection
    source_path = OUT_DIR / "dfa_probe_source.html"
    source_path.write_text(source, encoding="utf-8")
    print(f"\nFull source saved → {source_path}")

    soup = BeautifulSoup(source, "html.parser")

    # ── 1. Candidate containers ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("CANDIDATE CONTAINERS (tag / class / char count)")
    print("=" * 60)
    candidates = [
        ("main",    soup.find("main")),
        ("article", soup.find("article")),
        ("body",    soup.find("body")),
    ]
    for label, el in candidates:
        if el:
            text = el.get_text(strip=True)
            print(f"  <{label}>  :  {len(text):,} chars")
        else:
            print(f"  <{label}>  :  NOT FOUND")

    # ── 2. All divs with substantial text ────────────────────────────────────
    print("\n" + "=" * 60)
    print("DIVS WITH > 200 CHARS OF TEXT")
    print("=" * 60)
    for div in soup.find_all("div"):
        text = div.get_text(strip=True)
        if len(text) > 200:
            classes = " ".join(div.get("class", []))[:60]
            id_     = div.get("id", "")[:30]
            print(f"  [{len(text):>6,} chars]  class='{classes}'  id='{id_}'")

    # ── 3. All tags with > 100 chars (to find web components) ────────────────
    print("\n" + "=" * 60)
    print("ALL TAGS WITH > 100 CHARS (including web components)")
    print("=" * 60)
    seen = set()
    for el in soup.find_all(True):
        tag = el.name
        if tag in seen:
            continue
        text = el.get_text(strip=True)
        if len(text) > 100:
            seen.add(tag)
            print(f"  <{tag}>  :  {len(text):,} chars")

    # ── 4. First 500 chars of <main> innerHTML ────────────────────────────────
    print("\n" + "=" * 60)
    print("FIRST 500 CHARS OF <main> innerHTML")
    print("=" * 60)
    main_el = soup.find("main")
    if main_el:
        print(str(main_el)[:500])
    else:
        print("  <main> not found")

    print("\nDone. Open dfa_probe_source.html in a browser to inspect manually.")

if __name__ == "__main__":
    main()