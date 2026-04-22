"""
pgim_scraper.py  —  practitioner-gap project  (v5 - anchored paths, logs in data/logs/pgim/)

Strategy:
  1. For each of 5 asset-class pages, discover the AEM container ID from page HTML
  2. Call the AEM advancesearchresults.json API filtered to white-paper + outlook
  3. Paginate (resultsOffset += 10) until isLastPage=True
  4. Collect pageURLs, visit each detail page, extract PDF download link
  5. Download PDFs to data/pdfs/pgim/

Content types included: white-paper, outlook
Content types excluded: everything else

Usage:
    conda activate emi
    python src/scrapers/pgim_scraper.py [--delay 2]
"""

import re, time, random, logging, argparse
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# Script lives at gap/src/scrapers/ — three .parent calls reach gap/
REPO_ROOT    = Path(__file__).resolve().parent.parent.parent

OUT_DIR      = REPO_ROOT / "data" / "pdfs"         / "pgim"
OTHER_CORPUS = REPO_ROOT / "data" / "Other_Corpus" / "pgim"
LOG_DIR      = REPO_ROOT / "data" / "logs"         / "pgim"
LOG_FILE     = LOG_DIR   / "_done.txt"

ASSET_CLASS_PAGES = [
    "https://www.pgim.com/us/en/institutional/insights/asset-class/fixed-income",
    "https://www.pgim.com/us/en/institutional/insights/asset-class/alternatives",
    "https://www.pgim.com/us/en/institutional/insights/asset-class/equity",
    "https://www.pgim.com/us/en/institutional/insights/asset-class/multi-asset",
    "https://www.pgim.com/us/en/institutional/insights/asset-class/real-estate",
]

# Content type tags to include (from tagCounts in API response)
INCLUDE_TAGS = [
    "contentTags:content-type/white-paper",
    "contentTags:content-type/outlook",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.pgim.com/",
    "Accept": "application/json, text/html, */*",
}

# ── Resume log ────────────────────────────────────────────────────────────────

def load_done():
    return set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()

def mark_done(url):
    with open(LOG_FILE, "a") as f:
        f.write(url + "\n")

def slug_from_url(url):
    path = unquote(urlparse(url).path)
    return re.sub(r"[^\w\-.]", "_", Path(path).name)

# ── HTTP helpers ──────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def get_json(url, params=None):
    try:
        r = SESSION.get(url, params=params, timeout=20)
        if r.status_code == 400:
            log.info(f"  API not supported for this page (400) — skipping")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  JSON fetch failed: {e}  {url}")
        return None

def get_html(url):
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.warning(f"  HTML fetch failed: {e}  {url}")
        return None

def download_pdf(url, dest, delay):
    try:
        r = SESSION.get(url, timeout=30, stream=True)
        if r.status_code != 200:
            log.warning(f"  HTTP {r.status_code}  {url}")
            return False
        first, chunks = b"", []
        for chunk in r.iter_content(8192):
            if not first: first = chunk[:4]
            chunks.append(chunk)
        if not first.startswith(b"%PDF"):
            log.warning(f"  Not a PDF: {url}")
            return False
        dest.write_bytes(b"".join(chunks))
        log.info(f"  ✓ {dest.name}")
        time.sleep(delay + random.uniform(0, 0.8))
        return True
    except Exception as e:
        log.warning(f"  Download error: {e}")
        return False

# ── Step 1: Discover container ID from page HTML ──────────────────────────────

def discover_container_id(page_url):
    """
    Fetch the page HTML and find the AEM container ID used by the search component.
    Looks for pattern: advancesearchresults.json/jcr:content/root/container/{container_id}/
    or data-path attributes containing the container path.
    """
    html = get_html(page_url)
    if not html:
        return None

    # Pattern 1: direct reference in script/data attributes
    m = re.search(r'container/(container_\d+)', html)
    if m:
        log.info(f"  Container ID found in HTML: {m.group(1)}")
        return m.group(1)

    # Pattern 2: advancesearch path in any attribute
    m = re.search(r'advancesearchresults\.json/jcr:content/root/container/([^/"]+)', html)
    if m:
        log.info(f"  Container ID found via advancesearch ref: {m.group(1)}")
        return m.group(1)

    log.warning(f"  Could not find container ID in HTML for {page_url}")
    return None

# ── Step 2: Fetch all items for one content type tag ─────────────────────────

def fetch_all_items(page_url, container_id, tag):
    """Paginate through the AEM search API for one content-type tag."""
    asset_class = page_url.rstrip("/").split("/")[-1]
    api_base = (
        f"https://www.pgim.com/us/en/institutional/insights/asset-class/{asset_class}"
        f".advancesearchresults.json/jcr:content/root/container/{container_id}/searchresult"
    )

    items = []
    offset = 0
    while True:
        params = {
            "fulltext": "",
            "resultsOffset": offset,
            "orderby": "@jcr:content/datePublished",
            "sort": "desc",
            "tags": tag,
        }
        data = get_json(api_base, params=params)
        if not data:
            break

        batch = data.get("data", [])
        items.extend(batch)
        log.info(f"    offset={offset}: {len(batch)} items (total so far: {len(items)} / {data.get('totalRecords','?')})")

        if data.get("isLastPage", True) or not batch:
            break
        offset += 10
        time.sleep(0.5)

    return items

# ── Step 3: Extract PDF URL from detail page ──────────────────────────────────

def extract_pdf_from_detail(page_url_path):
    """
    Visit the article detail page and find the PDF download link.
    PGIM detail pages have a 'Download PDF' button linking to content/dam/.../...pdf
    """
    full_url = "https://www.pgim.com" + page_url_path
    full_url = full_url.replace("/content/pgim", "")

    html = get_html(full_url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf") and "content/dam" in href:
            if href.startswith("/"):
                href = "https://www.pgim.com" + href
            return href, html

    for el in soup.find_all(attrs={"data-href": True}):
        dh = el["data-href"]
        if dh.lower().endswith(".pdf"):
            if dh.startswith("/"):
                dh = "https://www.pgim.com" + dh
            return dh, html

    pdf_match = re.search(r'["\']([^"\']*content/dam[^"\']*\.pdf)["\']', html)
    if pdf_match:
        pdf = pdf_match.group(1)
        if pdf.startswith("/"):
            pdf = "https://www.pgim.com" + pdf
        return pdf, html

    return None, html


def extract_clean_text(html, title=""):
    """Strip nav/header/footer/scripts and return clean article body text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "aside", "noscript", "iframe", "form"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    if title:
        text = f"{title}\n{'='*len(title)}\n\n" + text
    return text

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=2.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OTHER_CORPUS.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Scrub stale log entries: keep only URLs that correspond to existing files
    raw_done = load_done()
    done = set()
    for entry in raw_done:
        if entry.startswith("http") and entry.endswith(".pdf"):
            fname = re.sub(r"[^\w\-.]", "_", Path(entry).name)
            if (OUT_DIR / fname).exists():
                done.add(entry)
        elif entry.startswith("/content/pgim"):
            slug = re.sub(r"[^\w\-]", "_", entry.rstrip("/").split("/")[-1].replace(".html", ""))
            if (OTHER_CORPUS / f"{slug}.txt").exists():
                done.add(entry)
    log.info(f"Resume log: {len(raw_done)} raw entries → {len(done)} valid")

    all_items = []
    for page_url in ASSET_CLASS_PAGES:
        asset = page_url.split("/")[-1]
        log.info(f"\n── {asset.upper()} ──────────────────────────────────────")

        container_id = discover_container_id(page_url)
        if not container_id:
            log.warning(f"  Skipping {asset} — no container ID found")
            continue

        for tag in INCLUDE_TAGS:
            ct = tag.split("/")[-1]
            log.info(f"  Fetching tag: {ct}")
            items = fetch_all_items(page_url, container_id, tag)
            log.info(f"  → {len(items)} items for {ct} on {asset}")
            for it in items:
                it["_content_type"] = ct
                it["_asset_class"] = asset
            all_items.extend(items)

    seen, unique = set(), []
    for it in all_items:
        pu = it.get("pageURL", "")
        if pu and pu not in seen:
            seen.add(pu)
            unique.append(it)

    JUNK_TITLES = {"white papers", "outlook", "outlooks", "insights", "hub",
                   "white paper", "research", "perspectives", "publications"}
    unique = [it for it in unique
              if it.get("title", "").strip().lower() not in JUNK_TITLES]

    log.info(f"\n=== Total unique items after junk filter: {len(unique)} ===")
    for it in unique:
        log.info(f"  [{it['_content_type']}] [{it['_asset_class']}] {it.get('title','')[:70]}")

    log.info(f"\n=== Downloading PDFs ===")
    n_ok = n_skip = n_fail = n_nopdf = 0

    for it in unique:
        page_path = it.get("pageURL", "")
        title = it.get("title", "")[:60]
        ct = it["_content_type"]

        if page_path in done:
            n_skip += 1
            continue

        log.info(f"  [{ct}] {title}")
        pdf_url, raw_html = extract_pdf_from_detail(page_path)

        if pdf_url:
            if pdf_url in done:
                n_skip += 1
                mark_done(page_path)
                continue
            fname = slug_from_url(pdf_url)
            dest = OUT_DIR / fname
            if dest.exists():
                mark_done(page_path)
                mark_done(pdf_url)
                n_skip += 1
                continue
            ok = download_pdf(pdf_url, dest, args.delay)
            if ok:
                mark_done(page_path)
                mark_done(pdf_url)
                n_ok += 1
            else:
                n_fail += 1
        else:
            if raw_html:
                slug = re.sub(r"[^\w\-]", "_", page_path.rstrip("/").split("/")[-1].replace(".html", ""))
                txt_path = OTHER_CORPUS / f"{slug}.txt"
                if not txt_path.exists():
                    clean = extract_clean_text(raw_html, title=it.get("title", ""))
                    txt_path.write_text(clean, encoding="utf-8")
                    log.info(f"  → saved text: {txt_path.name}")
                    n_ok += 1
                else:
                    n_skip += 1
            else:
                log.warning(f"  No PDF and no HTML for: {page_path}")
                n_nopdf += 1
            mark_done(page_path)

    log.info("=" * 60)
    log.info(f"Downloaded={n_ok}  Skipped={n_skip}  No-PDF={n_nopdf}  Failed={n_fail}")
    log.info(f"PDFs → {OUT_DIR.resolve()}")
    log.info(f"Text → {OTHER_CORPUS.resolve()}")
    log.info(f"Logs → {LOG_DIR.resolve()}")

if __name__ == "__main__":
    main()