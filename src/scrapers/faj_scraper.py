# %% Imports & Setup
"""
FAJ Scraper — Financial Analysts Journal (Taylor & Francis)
Uses Selenium to bypass Cloudflare, requests for PDF downloads.

Strategy: Navigate directly to treeId URLs to get issue lists.

Usage:
    python faj_scraper.py              # full run (visible browser)
    python faj_scraper.py --headless   # full run (background)
    python faj_scraper.py --test       # test mode (5 PDFs only)
"""

import os
import re
import time
import argparse
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import requests

os.chdir(r"C:\Users\willi\Desktop\gap\src")

# ── Config ─────────────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
PDF_DIR = DATA_DIR / "pdfs" / "FAJ"
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.tandfonline.com"
LOI_URL = f"{BASE_URL}/loi/ufaj20"

# Volume range: 49 (1993) to 82 (2026)
START_VOL = 49
END_VOL = 82

# Timing
CLOUDFLARE_WAIT = 8
PAGE_WAIT = 3
PDF_WAIT = 0.5


# %% Selenium setup
def create_driver(headless: bool = False) -> webdriver.Chrome:
    """Create Chrome driver with anti-detection settings."""
    options = Options()
    
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    
    return driver


def wait_for_cloudflare(driver: webdriver.Chrome, timeout: int = 15) -> bool:
    """Wait for Cloudflare challenge to resolve."""
    start = time.time()
    while time.time() - start < timeout:
        if "Just a moment" not in driver.title:
            return True
        time.sleep(0.5)
    return False


def get_cookies_and_headers(driver: webdriver.Chrome) -> tuple[dict, dict]:
    """Extract cookies and headers from Selenium for requests."""
    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    headers = {
        "User-Agent": driver.execute_script("return navigator.userAgent"),
        "Referer": BASE_URL,
        "Accept": "application/pdf,*/*",
    }
    return cookies, headers


# %% Scraping functions
def get_issues_for_volume(driver: webdriver.Chrome, vol_num: int) -> list[str]:
    """
    Navigate to volume's treeId URL and get issue links.
    """
    tree_url = f"{LOI_URL}?treeId=vufaj20-{vol_num}"
    driver.get(tree_url)
    time.sleep(PAGE_WAIT)
    
    if not wait_for_cloudflare(driver):
        print(f"    Cloudflare timeout")
        return []
    
    # Find issue links
    issue_links = driver.find_elements(
        By.CSS_SELECTOR, f"a[href*='/toc/ufaj20/{vol_num}/']"
    )
    
    urls = []
    for link in issue_links:
        href = link.get_attribute("href")
        if href and "nav=tocList" in href and href not in urls:
            urls.append(href)
    
    return urls


def get_pdf_links_from_issue(driver: webdriver.Chrome, issue_url: str) -> list[dict]:
    """
    Navigate to an issue page and extract PDF links.
    """
    driver.get(issue_url)
    time.sleep(PAGE_WAIT)
    
    if not wait_for_cloudflare(driver):
        print("    Cloudflare timeout")
        return []
    
    articles = []
    seen_dois = set()
    
    # Find all epdf links
    pdf_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/doi/epdf/']")
    
    for link in pdf_links:
        href = link.get_attribute("href")
        if not href:
            continue
        
        # Extract DOI
        match = re.search(r"/doi/epdf/(.+?)(?:\?|$)", href)
        if not match:
            continue
        
        doi = match.group(1)
        if doi in seen_dois:
            continue
        seen_dois.add(doi)
        
        # Convert epdf → pdf with download flag
        pdf_url = f"{BASE_URL}/doi/pdf/{doi}?download=true"
        
        articles.append({
            "doi": doi,
            "pdf_url": pdf_url,
        })
    
    return articles


def download_pdf(article: dict, output_dir: Path, cookies: dict, headers: dict) -> str:
    """
    Download a PDF using requests.
    Returns: "ok", "skip", or "fail"
    """
    # Create safe filename from DOI
    safe_name = re.sub(r'[<>:"/\\|?*]', "_", article["doi"]) + ".pdf"
    pdf_path = output_dir / safe_name
    
    # Skip if exists and valid
    if pdf_path.exists() and pdf_path.stat().st_size > 10000:
        return "skip"
    
    try:
        resp = requests.get(
            article["pdf_url"],
            cookies=cookies,
            headers=headers,
            timeout=60,
            allow_redirects=True
        )
        
        if resp.status_code != 200:
            print(f"    FAIL ({resp.status_code}): {article['doi']}")
            return "fail"
        
        # Verify it's a PDF
        if resp.content[:5] != b"%PDF-":
            print(f"    FAIL (not PDF): {article['doi']}")
            return "fail"
        
        # Save
        with open(pdf_path, "wb") as f:
            f.write(resp.content)
        
        print(f"    OK: {safe_name}")
        return "ok"
        
    except Exception as e:
        print(f"    ERROR: {article['doi']} - {e}")
        return "fail"


# %% Main scraper
def scrape_faj(test_mode: bool = False, headless: bool = False):
    """Main scraper entry point."""
    print("=" * 60)
    print("FAJ Scraper — Financial Analysts Journal")
    print(f"Volumes: {START_VOL} (1993) to {END_VOL} (2026)")
    print(f"Output: {PDF_DIR.absolute()}")
    print(f"Mode: {'TEST (5 PDFs)' if test_mode else 'FULL'}")
    print(f"Browser: {'headless' if headless else 'visible'}")
    print("=" * 60)
    
    driver = create_driver(headless=headless)
    
    try:
        # Initial page load to establish session
        print("\nInitializing session...")
        driver.get(LOI_URL)
        time.sleep(CLOUDFLARE_WAIT)
        
        if not wait_for_cloudflare(driver):
            print("ERROR: Cloudflare timeout. Try without --headless")
            return
        
        # Accept cookies if banner appears
        try:
            cookie_btn = driver.find_element(By.ID, "onetrust-accept-btn-handler")
            cookie_btn.click()
            time.sleep(1)
            print("Accepted cookie banner")
        except:
            pass
        
        print("Session ready\n")
        
        # Get cookies for PDF downloads
        cookies, headers = get_cookies_and_headers(driver)
        
        total_ok = 0
        total_skip = 0
        total_fail = 0
        
        # Process volumes from newest to oldest
        for vol_num in range(END_VOL, START_VOL - 1, -1):
            print(f"\n{'='*40}")
            print(f"VOLUME {vol_num}")
            print(f"{'='*40}")
            
            # Get issues for this volume
            issues = get_issues_for_volume(driver, vol_num)
            print(f"  Found {len(issues)} issues")
            
            if not issues:
                continue
            
            for issue_url in issues:
                # Extract issue number
                issue_match = re.search(rf"/{vol_num}/(\d+)\?", issue_url)
                issue_num = issue_match.group(1) if issue_match else "?"
                print(f"\n  Issue {issue_num}:")
                
                # Get PDF links
                articles = get_pdf_links_from_issue(driver, issue_url)
                print(f"    Found {len(articles)} articles")
                
                # Refresh cookies
                cookies, headers = get_cookies_and_headers(driver)
                
                for article in articles:
                    time.sleep(PDF_WAIT)
                    result = download_pdf(article, PDF_DIR, cookies, headers)
                    
                    if result == "ok":
                        total_ok += 1
                    elif result == "skip":
                        total_skip += 1
                    else:
                        total_fail += 1
                    
                    # Test mode exit
                    if test_mode and total_ok >= 5:
                        print(f"\n\n{'='*60}")
                        print("TEST MODE COMPLETE")
                        print(f"  Downloaded: {total_ok}")
                        print(f"  Skipped: {total_skip}")
                        print(f"  Failed: {total_fail}")
                        print(f"{'='*60}")
                        return
        
        print(f"\n\n{'='*60}")
        print("SCRAPE COMPLETE")
        print(f"  Downloaded: {total_ok}")
        print(f"  Skipped: {total_skip}")
        print(f"  Failed: {total_fail}")
        print(f"  Output: {PDF_DIR.absolute()}")
        print(f"{'='*60}")
    
    finally:
        driver.quit()


# %% Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape FAJ PDFs")
    parser.add_argument("--test", action="store_true", help="Test mode (5 PDFs)")
    parser.add_argument("--headless", action="store_true", help="Run in background")
    args = parser.parse_args()
    
    scrape_faj(test_mode=args.test, headless=args.headless)