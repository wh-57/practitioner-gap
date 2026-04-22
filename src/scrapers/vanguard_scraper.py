"""
vanguard_scraper.py
===================
Vanguard corporate research library scraper.

The full PDF corpus is embedded in the SSR HTML of a single page —
"Load more" is cosmetic JS only. Strategy:
  1. Single GET on research-library.html
  2. Extract all unique data-ctapath values ending in .pdf
  3. Validate %PDF magic bytes → download to src/data/pdfs/vanguard/

Run from project root:
    python src/vanguard_scraper.py

Resume-safe. Logs to src/logs/vanguard_scraper.log
"""

import requests
from bs4 import BeautifulSoup
import re
import logging
from pathlib import Path
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT       = Path(__file__).resolve().parent.parent
PDF_DIR    = ROOT / "src" / "data" / "pdfs" / "vanguard"
LOG_DIR    = ROOT / "src" / "logs"
RESUME_LOG = LOG_DIR / "vanguard_downloaded.txt"

for d in [PDF_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    filename=LOG_DIR / "vanguard_scraper.log",
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
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

BASE = "https://corporate.vanguard.com"
LIB  = BASE + "/content/corporatesite/us/en/corp/what-we-think/investing-insights/research-library.html"

DELAY = 1.0

# ── Resume ────────────────────────────────────────────────────────────────────

def load_done() -> set:
    if not RESUME_LOG.exists():
        return set()
    return set(RESUME_LOG.read_text(encoding="utf-8").splitlines())

def mark_done(key: str):
    with open(RESUME_LOG, "a", encoding="utf-8") as f:
        f.write(key + "\n")

# ── Step 1: Collect PDF URLs ──────────────────────────────────────────────────

def collect_pdf_urls() -> list[tuple[str, str]]:
    """
    Fetch the research library page and extract all unique PDF URLs
    from data-ctapath attributes. Returns list of (pdf_url, title).
    """
    console(f"Fetching research library: {LIB}")
    r = SESSION.get(LIB, timeout=20)
    if r.status_code != 200:
        console(f"ERROR: HTTP {r.status_code}")
        return []

    console(f"  Page size: {len(r.text):,} chars")
    soup = BeautifulSoup(r.text, "html.parser")

    seen = set()
    results = []

    for a in soup.find_all("a", attrs={"data-ctapath": True}):
        path = a.get("data-ctapath", "")
        if not path.lower().endswith(".pdf"):
            continue
        pdf_url = BASE + path if path.startswith("/") else path
        if pdf_url in seen:
            continue
        seen.add(pdf_url)

        # Title: derive from URL slug (always clean and descriptive)
        title = path.split("/")[-1].replace(".pdf", "")
        results.append((pdf_url, title))

    console(f"  Unique PDF URLs found: {len(results)}")
    return results

# ── Step 2: Download PDF ──────────────────────────────────────────────────────

def is_pdf(content: bytes) -> bool:
    return content[:4] == b"%PDF"

def safe_filename(title: str, url: str) -> str:
    slug = re.sub(r"[^\w\- ]", "", title).strip()
    slug = re.sub(r"\s+", "_", slug)[:120]
    if not slug:
        slug = url.split("/")[-1].replace(".pdf", "")
    return slug + ".pdf"

def download_pdf(pdf_url: str, title: str) -> tuple[str, str]:
    try:
        r = SESSION.get(pdf_url, timeout=30)
        if r.status_code != 200:
            return "error", f"HTTP {r.status_code}"
        content = r.content
    except Exception as e:
        return "error", str(e)

    if not is_pdf(content):
        return "not_pdf", ""

    fname  = safe_filename(title, pdf_url)
    fpath  = PDF_DIR / fname
    # Avoid collision: append URL slug if filename already exists
    if fpath.exists():
        url_slug = pdf_url.split("/")[-1].replace(".pdf", "")
        fpath = PDF_DIR / fname.replace(".pdf", f"_{url_slug}.pdf")

    fpath.write_bytes(content)
    return "ok", str(fpath)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = datetime.now()
    console(f"\n{'='*55}")
    console(f"Vanguard Research Scraper -- {start:%Y-%m-%d %H:%M}")
    console(f"PDFs -> {PDF_DIR}")
    console(f"{'='*55}\n")

    done   = load_done()
    papers = collect_pdf_urls()

    if not papers:
        console("No PDFs found — exiting.")
        return

    stats = {"downloaded": 0, "skipped": 0, "not_pdf": 0, "failed": 0}

    for i, (pdf_url, title) in enumerate(papers, 1):
        slug = pdf_url.split("/")[-1]

        if pdf_url in done:
            stats["skipped"] += 1
            continue

        console(f"[{i}/{len(papers)}] {slug}")

        status, result = download_pdf(pdf_url, title)

        if status == "ok":
            console(f"  [ok] {Path(result).name}")
            stats["downloaded"] += 1
            mark_done(pdf_url)
        elif status == "not_pdf":
            console(f"  [x] magic-byte fail — skipping")
            stats["not_pdf"] += 1
            mark_done(pdf_url)   # don't retry
        else:
            console(f"  [x] {result}")
            stats["failed"] += 1

        import time; time.sleep(DELAY)

    elapsed = datetime.now() - start
    console(f"\n{'='*55}")
    console(
        f"Done.  Downloaded: {stats['downloaded']}   "
        f"Failed: {stats['failed']}   "
        f"Not PDF: {stats['not_pdf']}   "
        f"Skipped: {stats['skipped']}"
    )
    console(f"Elapsed: {elapsed}")
    console(f"PDFs: {PDF_DIR}")
    console(f"{'='*55}\n")

if __name__ == "__main__":
    main()