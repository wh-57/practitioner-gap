"""
cfa_scraper.py
==============
CFA Institute Research Foundation scraper — unified script.

Combines the original scraper with post-run cleanup in one flow:

  Phase 1 — Scrape
    1. Enumerate sitemap.xml pages — stop when a page returns < 100 URLs
       (Drupal serves a 6-URL stub forever once past real content)
    2. For each /research/foundation/ landing page: requests + BS4 → extract PDF href
    3. Validate %PDF magic bytes → download to data/pdfs/cfa/
    4. Pages with no PDF link → save clean text to data/Other_Corpus/cfa/

  Phase 2 — Cleanup (runs automatically after scrape, or standalone via --cleanup-only)
    1. Delete chapter-level duplicate PDFs (full-book PDFs already present for
       Geo-Economics, AI in Asset Management, and AI/Big Data handbook)
    2. Fix ETF 2nd Edition: remove stale 2015 PDF saved under wrong name,
       fetch real 2025 Module 1 PDF from correct landing page

Run from project root:
    python src/cfa_scraper.py                  # full scrape + cleanup
    python src/cfa_scraper.py --scrape-only    # skip cleanup
    python src/cfa_scraper.py --cleanup-only   # skip scrape

Resume-safe: skips already-downloaded files. Logs to src/logs/cfa_scraper.log
"""

import argparse
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import time
import re
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT        = Path(__file__).resolve().parent.parent
PDF_DIR     = ROOT / "data" / "pdfs" / "cfa"
TEXT_DIR    = ROOT / "data" / "Other_Corpus" / "cfa"
LOG_DIR     = ROOT / "src" / "logs"
RESUME_LOG  = LOG_DIR / "cfa_downloaded.txt"

for d in [PDF_DIR, TEXT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=LOG_DIR / "cfa_scraper.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

def console(msg):
    print(msg)
    log.info(msg)

# ── Session ───────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

BASE_RPC  = "https://rpc.cfainstitute.org"
BASE_CFAI = "https://www.cfainstitute.org"

DELAY = 1.2   # seconds between requests

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — SCRAPE
# ══════════════════════════════════════════════════════════════════════════════

# ── Resume log ────────────────────────────────────────────────────────────────

def load_done() -> set:
    if not RESUME_LOG.exists():
        return set()
    return set(RESUME_LOG.read_text(encoding="utf-8").splitlines())

def mark_done(key: str):
    with open(RESUME_LOG, "a", encoding="utf-8") as f:
        f.write(key + "\n")

# ── Step 1: Sitemap enumeration ───────────────────────────────────────────────

SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
MIN_URLS   = 100   # pages with fewer URLs are Drupal stubs — stop

def fetch_sitemap_page(page_num: int) -> list[str]:
    url = f"{BASE_RPC}/sitemap.xml?page={page_num}"
    try:
        r = SESSION.get(url, timeout=15)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        return [loc.text for loc in root.findall(f".//{{{SITEMAP_NS}}}loc")]
    except Exception as e:
        console(f"  sitemap page {page_num} error: {e}")
        return []

def collect_foundation_urls() -> list[str]:
    """
    Walk sitemap pages, collecting /research/foundation/ URLs.
    Stop as soon as a page returns < MIN_URLS entries.
    """
    console("Collecting sitemap pages...")
    all_urls = []
    page = 1
    while True:
        urls = fetch_sitemap_page(page)
        total = len(urls)
        foundation = [u for u in urls if "/research/foundation/" in u]
        console(f"  page {page:>3}: {total:>5} URLs | {len(foundation):>3} /research/foundation/")
        all_urls.extend(foundation)

        if total < MIN_URLS:
            console(f"  -> page {page} has only {total} URLs (< {MIN_URLS}) — Drupal stub, stopping.")
            break

        page += 1
        time.sleep(0.4)

    unique = sorted(set(all_urls))
    console(f"Total unique /research/foundation/ landing URLs: {len(unique)}")
    return unique

# ── Step 2: Extract PDF link from landing page ────────────────────────────────

def extract_pdf_url(landing_url: str) -> tuple[str | None, str | None]:
    try:
        r = SESSION.get(landing_url, timeout=15)
        if r.status_code != 200:
            console(f"  landing {landing_url} -> HTTP {r.status_code}")
            return None, None
    except Exception as e:
        console(f"  landing fetch error: {e}")
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else urlparse(landing_url).path.split("/")[-1]

    pdf_href = None

    # Pass 1: prefer main book/monograph link, skip supplementary briefs
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not re.search(r"\.(pdf|ashx)(\?|$)", href, re.IGNORECASE):
            continue
        txt = a.get_text(strip=True).lower()
        if "brief" in txt or "support" in href.lower():
            continue
        pdf_href = href
        break

    # Pass 2: fallback — any pdf/ashx link
    if not pdf_href:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"\.(pdf|ashx)(\?|$)", href, re.IGNORECASE):
                pdf_href = href
                break

    if not pdf_href:
        return None, title

    # Resolve to absolute URL
    if pdf_href.startswith("http"):
        pdf_abs = pdf_href
    elif pdf_href.startswith("//"):
        pdf_abs = "https:" + pdf_href
    else:
        pdf_abs = BASE_RPC + pdf_href

    return pdf_abs, title

# ── Step 3: Download and validate PDF ────────────────────────────────────────

def is_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"

def safe_filename(title: str, url: str) -> str:
    slug = re.sub(r"[^\w\- ]", "", title).strip()
    slug = re.sub(r"\s+", "_", slug)[:120]
    if not slug:
        slug = urlparse(url).path.split("/")[-1].replace(".pdf", "").replace(".ashx", "")
    return slug + ".pdf"

def download_pdf(pdf_url: str, title: str) -> tuple[str, str]:
    try:
        r = SESSION.get(pdf_url, timeout=30, stream=True)
        if r.status_code != 200:
            return "error", f"HTTP {r.status_code}"
        content = r.content
    except Exception as e:
        return "error", str(e)

    if not is_pdf(content):
        return "not_pdf", ""

    fname = safe_filename(title, pdf_url)
    fpath = PDF_DIR / fname
    if fpath.exists():
        suffix = urlparse(pdf_url).path.split("/")[-1].replace(".pdf", "").replace(".ashx", "")
        fpath = PDF_DIR / (fname.replace(".pdf", f"_{suffix}.pdf"))

    fpath.write_bytes(content)
    return "ok", str(fpath)

# ── Step 4: Text fallback ─────────────────────────────────────────────────────

def save_text(landing_url: str, title: str) -> str:
    try:
        r = SESSION.get(landing_url, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        fname = safe_filename(title, landing_url).replace(".pdf", ".txt")
        fpath = TEXT_DIR / fname
        fpath.write_text(text, encoding="utf-8")
        return str(fpath)
    except Exception as e:
        return f"error: {e}"

# ── Scrape main ───────────────────────────────────────────────────────────────

def run_scrape():
    start = datetime.now()
    console(f"\n{'='*60}")
    console(f"CFA Research Foundation Scraper — {start:%Y-%m-%d %H:%M}")
    console(f"PDFs  -> {PDF_DIR}")
    console(f"Text  -> {TEXT_DIR}")
    console(f"{'='*60}\n")

    done = load_done()
    landing_urls = collect_foundation_urls()
    stats = {"downloaded": 0, "skipped": 0, "no_pdf": 0, "failed": 0}

    for i, url in enumerate(landing_urls, 1):
        if url in done:
            stats["skipped"] += 1
            continue

        console(f"[{i}/{len(landing_urls)}] {url}")

        pdf_url, title = extract_pdf_url(url)
        time.sleep(DELAY)

        if not pdf_url:
            console(f"  -> no PDF — saving text | {title}")
            txt_path = save_text(url, title or url.split("/")[-1])
            console(f"  -> {txt_path}")
            stats["no_pdf"] += 1
            mark_done(url)
            time.sleep(DELAY)
            continue

        console(f"  -> {pdf_url.split('/')[-1]}")
        status, result = download_pdf(pdf_url, title or "untitled")
        time.sleep(DELAY)

        if status == "ok":
            console(f"  [ok] {Path(result).name}")
            stats["downloaded"] += 1
            mark_done(url)
        elif status == "not_pdf":
            console(f"  [x] magic-byte fail — saving text fallback")
            save_text(url, title or url.split("/")[-1])
            stats["no_pdf"] += 1
            mark_done(url)
        else:
            console(f"  [x] {result}")
            stats["failed"] += 1

    elapsed = datetime.now() - start
    console(f"\n{'='*60}")
    console(
        f"Scrape done.  Downloaded: {stats['downloaded']}   "
        f"Failed: {stats['failed']}   "
        f"No PDF: {stats['no_pdf']}   "
        f"Skipped: {stats['skipped']}"
    )
    console(f"Elapsed: {elapsed}")
    console(f"{'='*60}\n")

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — CLEANUP
# ══════════════════════════════════════════════════════════════════════════════

# Chapter-level file prefixes to delete — full-book PDFs already present
CHAPTER_PATTERNS = [
    # Geo-Economics individual chapters + intro
    "Geo-Economics_Chapter_",
    "Geo-Economics_Introduction_",
    # AI in Asset Management individual chapters
    "Chapter_1_Unsupervised_Learning_I",
    "Chapter_2_Unsupervised_Learning_II",
    "Chapter_3_Support_Vector",
    "Chapter_4_Ensemble_Learning",
    "Chapter_5_Deep_Learning",
    "Chapter_6_Reinforcement_Learning",
    "Chapter_7_Natural_Language_Processing",
    "Chapter_8_Machine_Learning_in_Commodity",
    "Chapter_9_Quantum_Computing",
    "Chapter_10_Ethical_AI",
    # AI/Big Data handbook part files (full handbook already present)
    "Introductory_Material",
    "I_Machine_Learning_and_Data_Science",
    "II_Natural_Language_Understanding",
    "III_Trading_with_Machine_Learning",
    "IV_Chatbot_Knowledge_Graphs",
]

# ETF 2nd Edition — real 2025 landing page (the scraper landed on the 2015 PDF)
ETF_LANDING_2025  = BASE_RPC + "/research/foundation/2025/a-comprehensive-guide-to-etfs"
ETF_STALE_NAME    = "A_Comprehensive_Guide_to_ETFs_2nd_Edition.pdf"
ETF_CORRECT_NAME  = "A_Comprehensive_Guide_to_ETFs_2nd_Edition_Module1.pdf"


def fix_chapter_duplicates():
    console("\n── Cleanup 1: Removing chapter-level duplicate PDFs ──────────")
    deleted = 0
    for pdf in sorted(PDF_DIR.glob("*.pdf")):
        for pat in CHAPTER_PATTERNS:
            if pdf.name.startswith(pat):
                console(f"  del  {pdf.name}")
                pdf.unlink()
                deleted += 1
                break
    console(f"  Deleted {deleted} chapter-level files.")


def fix_etf_second_edition():
    console("\n── Cleanup 2: ETF 2nd Edition ────────────────────────────────")

    # Remove stale file (points at 2015 PDF)
    stale = PDF_DIR / ETF_STALE_NAME
    if stale.exists():
        console(f"  del  {stale.name}  (was pointing at 2015 PDF)")
        stale.unlink()

    # Skip if correct file already present
    correct = PDF_DIR / ETF_CORRECT_NAME
    if correct.exists():
        console(f"  already present: {ETF_CORRECT_NAME}")
        return

    console(f"  fetching: {ETF_LANDING_2025}")
    try:
        r = SESSION.get(ETF_LANDING_2025, timeout=15)
    except Exception as e:
        console(f"  ERROR fetching landing page: {e}")
        return

    if r.status_code != 200:
        console(f"  landing page returned HTTP {r.status_code} — skipping")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    pdf_hrefs = [
        a["href"] for a in soup.find_all("a", href=True)
        if re.search(r"\.pdf(\?|$)", a["href"], re.IGNORECASE)
    ]
    console(f"  PDF links found: {pdf_hrefs or 'NONE'}")

    if not pdf_hrefs:
        console("  No PDF link found on 2025 ETF landing page.")
        return

    href = pdf_hrefs[0]
    pdf_url = href if href.startswith("http") else BASE_RPC + href
    console(f"  downloading: {pdf_url.split('/')[-1]}")
    time.sleep(1)

    try:
        r2 = SESSION.get(pdf_url, timeout=30)
        if r2.status_code == 200 and r2.content[:4] == b"%PDF":
            correct.write_bytes(r2.content)
            console(f"  saved: {ETF_CORRECT_NAME}")
        else:
            console(f"  download failed or not a PDF (HTTP {r2.status_code}): {pdf_url}")
    except Exception as e:
        console(f"  fetch error: {e}")


def run_cleanup():
    console(f"\n{'='*60}")
    console("CFA Cleanup — chapter duplicates + ETF 2nd Edition")
    console(f"{'='*60}")

    fix_chapter_duplicates()
    fix_etf_second_edition()

    remaining = len(list(PDF_DIR.glob("*.pdf")))
    console(f"\nCleanup done. PDFs remaining: {remaining}")
    console(f"Dir: {PDF_DIR}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="CFA Research Foundation scraper + cleanup"
    )
    parser.add_argument(
        "--scrape-only", action="store_true",
        help="Run scrape phase only, skip cleanup"
    )
    parser.add_argument(
        "--cleanup-only", action="store_true",
        help="Run cleanup phase only, skip scrape"
    )
    args = parser.parse_args()

    if args.cleanup_only:
        run_cleanup()
    elif args.scrape_only:
        run_scrape()
    else:
        run_scrape()
        run_cleanup()

if __name__ == "__main__":
    main()
