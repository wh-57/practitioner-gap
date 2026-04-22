"""
edhec_scraper.py  —  EDHEC  v1
================================
Two-track scraper:

TRACK 1 — EDHEC Climate Institute (live, 2022–present)
  URL: climateinstitute.edhec.edu/publications
  Method: Drupal URL params + requests + BeautifulSoup
  Categories:
    31 = Physical Risks
    32 = Transition Risks
    55 = Green Assets
    56 = Resilience & Transition Tech
    57 = Climate Scenarios
    58 = Climate Regulation and Policies
  Output: PDFs → data/pdfs/EDHEC/
          HTML  → src/data/Other_Corpus/EDHEC/

TRACK 2 — Legacy EDHEC-Risk Institute (Wayback Machine CDX API)
  Source: archived edhec-risk.com PDFs
  Method: CDX API to enumerate, then raw download via Wayback
  Output: PDFs → data/pdfs/EDHEC/

Path layout (REPO = gap/):
  REPO/data/pdfs/EDHEC/
  REPO/data/junk/EDHEC/
  REPO/src/data/Other_Corpus/EDHEC/

Usage:
  python src/edhec_scraper.py                  # both tracks
  python src/edhec_scraper.py --track1-only
  python src/edhec_scraper.py --track2-only
"""

import argparse
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = REPO_ROOT / "data"
SRC_DATA  = REPO_ROOT / "src" / "data"

PDF_DIR   = DATA_ROOT / "pdfs" / "EDHEC"
JUNK_DIR  = DATA_ROOT / "junk" / "EDHEC"
HTML_DIR  = SRC_DATA  / "Other_Corpus" / "EDHEC"

PDF_DONE  = PDF_DIR  / "_done.txt"
HTML_DONE = HTML_DIR / "_done.txt"

for d in [PDF_DIR, JUNK_DIR, HTML_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
SLEEP = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Track 1: Climate Institute
CLIMATE_BASE = "https://climateinstitute.edhec.edu"
CLIMATE_PUBS = f"{CLIMATE_BASE}/publications"

# Category IDs confirmed from URL bar inspection
CLIMATE_CATS = {
    31: "Physical-Risks",
    32: "Transition-Risks",
    55: "Green-Assets",
    56: "Resilience-Transition-Tech",
    57: "Climate-Scenarios",
    58: "Climate-Regulation-Policies",
}

# Track 2: Wayback CDX
CDX_API = "http://web.archive.org/cdx/search/cdx"
WAYBACK  = "https://web.archive.org/web"
EDHEC_DOMAINS = [
    "edhec-risk.com",
    "risk.edhec.edu",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def load_done(path: Path) -> set:
    return set(path.read_text(encoding="utf-8").splitlines()) if path.exists() else set()

def mark_done(path: Path, key: str):
    with path.open("a", encoding="utf-8") as f:
        f.write(key + "\n")

def safe_name(s: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", s)[:180]

def sleep():
    time.sleep(SLEEP)

def download_pdf(url: str, fname: str, done: set, done_path: Path) -> bool:
    """Download PDF to PDF_DIR. Returns True if newly downloaded."""
    key = fname.lower()
    if key in done:
        print(f"    [–] {fname}")
        return False
    out = PDF_DIR / fname
    try:
        r = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        r.raise_for_status()
        data = r.content
        if data[:4] != b"%PDF":
            print(f"    [✗] not a PDF: {fname}")
            return False
        out.write_bytes(data)
        mark_done(done_path, key)
        done.add(key)
        print(f"    [✓] {fname}")
        return True
    except Exception as e:
        print(f"    [✗] {fname} — {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# TRACK 1 — EDHEC Climate Institute (live Drupal site)
# ══════════════════════════════════════════════════════════════════════════════

def get_listing_page(cat_id: int, page: int = 0) -> BeautifulSoup:
    """Fetch one page of the publications listing for a given category."""
    url = (
        f"{CLIMATE_PUBS}"
        f"?field_research_programme_target_id={cat_id}"
        f"&field_year_value=&combine=&page={page}"
    )
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), url


def extract_pub_links(soup: BeautifulSoup) -> list:
    """Extract all publication detail page links from a listing page."""
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/publications/" in href or "/node/" in href:
            full = urljoin(CLIMATE_BASE, href)
            if full not in links:
                links.append(full)
    # Also grab any direct "SEE MORE" style buttons
    for a in soup.select("a.btn, a[class*='see-more'], a[class*='button']"):
        href = a.get("href", "")
        if href:
            full = urljoin(CLIMATE_BASE, href)
            if full not in links:
                links.append(full)
    return links


def scrape_pub_page(url: str) -> tuple:
    """
    Visit a publication detail page.
    Returns (title, date, pdf_url_or_None, html_content).
    """
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Title
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)

    # Date
    date = ""
    for sel in ["time", ".date", "[class*='date']", "span[class*='date']"]:
        el = soup.select_one(sel)
        if el:
            date = el.get_text(strip=True)[:10]
            break

    # PDF link
    pdf_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            pdf_url = urljoin(CLIMATE_BASE, href)
            break
    if not pdf_url:
        # Look for download buttons
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "download" in href.lower() or "pdf" in href.lower():
                pdf_url = urljoin(CLIMATE_BASE, href)
                break

    # HTML content (main body)
    content = ""
    for sel in ["article", "main", ".field--body", ".content", "#content"]:
        el = soup.select_one(sel)
        if el:
            content = str(el)
            break
    if not content:
        content = str(soup.body) if soup.body else ""

    return title, date, pdf_url, content


def save_html(url: str, title: str, date: str, content: str, done: set) -> bool:
    key = url
    if key in done:
        return False
    slug = safe_name(url.rstrip("/").split("/")[-1])
    fname = f"{date}_{slug}.html" if date else f"{slug}.html"
    html = (
        f"<html><head><meta charset='utf-8'><title>{title}</title></head><body>\n"
        f"<h1>{title}</h1>\n"
        f"<p><em>Source: <a href='{url}'>{url}</a> | Date: {date}</em></p><hr/>\n"
        f"{content}\n</body></html>"
    )
    (HTML_DIR / fname).write_text(html, encoding="utf-8")
    mark_done(HTML_DONE, key)
    done.add(key)
    return True


def run_track1():
    print("\n" + "=" * 60)
    print("TRACK 1 — EDHEC Climate Institute (live)")
    print("=" * 60)

    pdf_done  = load_done(PDF_DONE)
    html_done = load_done(HTML_DONE)
    counts = {"pdf": 0, "html": 0, "skip": 0, "fail": 0}

    for cat_id, cat_name in CLIMATE_CATS.items():
        print(f"\n  [{cat_name}] category id={cat_id}")
        page = 0
        pub_urls = set()

        while True:
            try:
                soup, listing_url = get_listing_page(cat_id, page)
            except Exception as e:
                print(f"    [warn] listing page {page} failed: {e}")
                break

            links = extract_pub_links(soup)
            new = [l for l in links if l not in pub_urls]
            pub_urls.update(new)

            # Check if there's a "next page" link
            next_link = soup.select_one("a[rel='next'], li.next a, .pager__item--next a")
            print(f"    listing page {page}: {len(links)} links found, "
                  f"{'next→' if next_link else 'last page'}")

            if not next_link or not new:
                break
            page += 1
            sleep()

        print(f"    Total unique pub URLs: {len(pub_urls)}")

        for url in sorted(pub_urls):
            try:
                title, date, pdf_url, content = scrape_pub_page(url)
                sleep()
            except Exception as e:
                print(f"    [✗] {url} — {e}")
                counts["fail"] += 1
                continue

            if pdf_url:
                slug = safe_name(pdf_url.split("/")[-1].split("?")[0])
                fname = slug if slug.lower().endswith(".pdf") else slug + ".pdf"
                if download_pdf(pdf_url, fname, pdf_done, PDF_DONE):
                    counts["pdf"] += 1
                else:
                    counts["skip"] += 1
                sleep()
            else:
                # No PDF — save HTML
                if save_html(url, title, date, content, html_done):
                    counts["html"] += 1
                    print(f"    [html] {title[:60]}")

    print(f"\n  Track 1 done — PDFs: {counts['pdf']}  "
          f"HTMLs: {counts['html']}  Skipped: {counts['skip']}  Failed: {counts['fail']}")


# ══════════════════════════════════════════════════════════════════════════════
# TRACK 2 — Legacy EDHEC-Risk via Wayback Machine CDX API
# ══════════════════════════════════════════════════════════════════════════════

def query_cdx(domain: str) -> list:
    """
    Query Wayback CDX API for all archived PDFs from a domain.
    Returns list of (timestamp, original_url) tuples.
    """
    params = {
        "url":    f"{domain}/*",
        "output": "json",
        "fl":     "timestamp,original",
        "filter": "mimetype:application/pdf",
        "filter2":"statuscode:200",
        "collapse":"original",   # one snapshot per unique URL
        "limit":  "5000",
    }
    # CDX API uses multiple filter params — requests doesn't handle duplicate
    # keys well so build manually
    qs = (
        f"url={domain}/*"
        f"&output=json"
        f"&fl=timestamp,original"
        f"&filter=mimetype:application/pdf"
        f"&filter=statuscode:200"
        f"&collapse=original"
        f"&limit=5000"
    )
    url = f"{CDX_API}?{qs}"
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    rows = r.json()
    # First row is headers ["timestamp", "original"]
    if not rows or rows[0] == ["timestamp", "original"]:
        rows = rows[1:]
    return [(row[0], row[1]) for row in rows]


def wayback_pdf_url(timestamp: str, original: str) -> str:
    """Construct raw Wayback URL (id_ suffix serves original without rewrites)."""
    return f"{WAYBACK}/{timestamp}id_/{original}"


def run_track2():
    print("\n" + "=" * 60)
    print("TRACK 2 — Legacy EDHEC-Risk (Wayback Machine CDX)")
    print("=" * 60)

    pdf_done = load_done(PDF_DONE)
    ok = skip = fail = 0

    for domain in EDHEC_DOMAINS:
        print(f"\n  Querying CDX for domain: {domain}")
        try:
            results = query_cdx(domain)
        except Exception as e:
            print(f"  [warn] CDX query failed: {e}")
            continue

        print(f"  Found {len(results)} archived PDFs")

        for timestamp, original in results:
            # Derive filename from original URL
            path = urlparse(original).path
            raw_name = path.rstrip("/").split("/")[-1]
            if not raw_name or not raw_name.lower().endswith(".pdf"):
                raw_name = safe_name(path.replace("/", "_")) + ".pdf"
            fname = safe_name(raw_name)
            if not fname.lower().endswith(".pdf"):
                fname += ".pdf"

            wb_url = wayback_pdf_url(timestamp, original)
            if download_pdf(wb_url, fname, pdf_done, PDF_DONE):
                ok += 1
            else:
                skip += 1
            sleep()

    print(f"\n  Track 2 done — Downloaded: {ok}  Skipped: {skip}  Failed: {fail}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--track1-only", action="store_true")
    parser.add_argument("--track2-only", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("EDHEC Scraper v1")
    print(f"  PDFs         → {PDF_DIR.resolve()}")
    print(f"  Other_Corpus → {HTML_DIR.resolve()}")
    print(f"  Junk         → {JUNK_DIR.resolve()}")
    print("=" * 60)

    if not args.track2_only:
        run_track1()

    if not args.track1_only:
        run_track2()

    n_pdf  = len(list(PDF_DIR.glob("*.pdf")))
    n_html = len(list(HTML_DIR.glob("*.html")))
    print("\n" + "=" * 60)
    print(f"  Research PDFs      : {n_pdf}")
    print(f"  Other_Corpus HTMLs : {n_html}")
    print("=" * 60)


if __name__ == "__main__":
    main()