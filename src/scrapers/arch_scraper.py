"""
arch_scraper.py  —  Alpha Architect  v7
========================================
v7: Patches from 2026-04-20 extraction_quality_scan audit that surfaced 9
externally-sourced PDFs still in denominator after v6:
  - 3 academic reprints with zero extractable text (Fama-French 1992,
    Goldberg 1968, Williams 1981 speech) now caught by JUNK_STEMS literals
    AND zero-text rule in is_finance_content()
  - 6 Democratize Quant 2022 conference decks now caught by loosened
    _CONFERENCE_SESSION regex + expanded JUNK_SUBS
  - Fixes v6 typo: "democquant" → "demquant" (actual AA filename pattern)
  - Log-path migration: resume logs now live in data/logs/AlphaArchitect/
    (_done_pdf.txt, _done_txt.txt). Closes "step 3 deferred" from the
    earlier reorganization. Old locations auto-cleaned on first v7 run.

v6: Adds content-based second-pass filter on downloaded PDFs. Catches
externally-linked non-finance papers (e.g. the Leli 1984 J. Clinical
Psychology paper) that slip past filename heuristics. See is_finance_content().

v5: Uses confirmed category IDs from wp-json/wp/v2/categories audit.

INCLUDE (AA's own original research):
  factor-investing          id=359   496 posts
  value-investing           id=124   307 posts
  tactical-asset-allocation id=232   298 posts
  momentum-investing        id=245   218 posts
  trend-following           id=360    91 posts
  low-volatility-investing  id=269    64 posts
  machine-learning          id=404    59 posts
  managed-futures-research  id=348    20 posts
  size-investing-research   id=358    32 posts

EXCLUDE (summaries of other people's papers, or non-research):
  academic-research-insight id=376   519 posts  — structured paper summaries
  key-research              id=314    51 posts  — curated paper summaries
  basilico                  id=375   383 posts  — Basilico paper summaries
  larry-swedroe             id=366   257 posts  — Swedroe paper summaries
  turnkey-behavioral-finance id=34   252 posts  — client education
  tool-updates              id=318   147 posts  — product updates
  index-updates             id=16746 119 posts  — product updates
  podcasts                  id=408   117 posts  — not research
  guest-posts               id=253   166 posts  — external authors
  business-updates          id=315    55 posts  — firm news
  media                     id=409    48 posts  — press
  mftf-training-series      id=377    20 posts  — trail race series
  1042-qrp-solutions        id=345    12 posts  — tax product

Path layout (REPO = gap/):
  REPO/data/pdfs/AlphaArchitect/             <- research PDFs
  REPO/data/junk/AlphaArchitect/             <- junk PDFs
  REPO/src/data/Other_Corpus/AlphaArchitect/ <- original AA text posts (.txt)

Usage:
  python src/arch_scraper.py
  python src/arch_scraper.py --pdfs-only
  python src/arch_scraper.py --text-only
"""

import argparse
import re
import shutil
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
# Script lives at gap/src/scrapers/arch_scraper.py
# .parent       → gap/src/scrapers/
# .parent.parent → gap/src/
# .parent.parent.parent → gap/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

PDF_DIR     = REPO_ROOT / "data" / "pdfs"         / "AlphaArchitect"
JUNK_DIR    = REPO_ROOT / "data" / "junk"          / "AlphaArchitect"
TEXT_DIR    = REPO_ROOT / "data" / "Other_Corpus"  / "AlphaArchitect"
LOG_DIR     = REPO_ROOT / "data" / "logs"          / "AlphaArchitect"
OLD_SRC_PDF = REPO_ROOT / "src"  / "data" / "pdfs" / "AlphaArchitect"  # legacy cleanup

# v7: Resume logs live in LOG_DIR per directory_structure.md rules
# ("Logs → data/logs/{source}/ — never co-located with PDFs or Other_Corpus")
PDF_DONE  = LOG_DIR / "_done_pdf.txt"
TEXT_DONE = LOG_DIR / "_done_txt.txt"

# Old log locations — auto-removed on startup (one-time migration)
OLD_PDF_DONE  = PDF_DIR  / "_done.txt"
OLD_TEXT_DONE = TEXT_DIR / "_done.txt"

for d in [PDF_DIR, JUNK_DIR, TEXT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# One-time log-path migration cleanup (v6 → v7)
for old in [OLD_PDF_DONE, OLD_TEXT_DONE]:
    if old.exists():
        print(f"  [migrate] removing old-location log: {old}")
        old.unlink()

# ── API ───────────────────────────────────────────────────────────────────────
API      = "https://alphaarchitect.com/wp-json/wp/v2"
PER_PAGE = 100
SLEEP    = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Confirmed IDs from category audit
INCLUDE_IDS = [359, 124, 232, 245, 360, 269, 404, 348, 358]
EXCLUDE_IDS = [376, 314, 375, 366, 34, 318, 16746, 408, 253, 315, 409, 377, 345]

# ── Junk PDF classification ───────────────────────────────────────────────────
JUNK_STEMS = {
    "xom","vz","t","pg","pfe","orcl","msft","mrk","mcd","kft","jnj","intc",
    "ibm","goog","ge","dow","dal","cvx","csco","cop","bmy","bby","axp",
    "apple","amzn",
    "mapdirections_marc-1","mftf-elevation-chart_final",
    "mftf-elevation-chart_final-1","2020-mftf-28-mile-in-lebanon-county",
    "household-client-manual","bcp_vfinal",
    "2017.03.28_robo-client-agreement_vfinal",
    "alpha-architect-brochure","alpha-architect-brochure-supplement",
    "2019.12.10-ad","1967.10.09","soapstone",
    "oct_2011_tka_newsletter","tka_pr_2011.09.14",
    "finance626_syllabus_spring2011","finance626_syllabus_spring20111",
    "fscore","fscore_short","gwscore","gwscore_short",
    "agscore","agscore_short","ltepscore","ltepscore_short",
    "pascore","pascore_short","pvscore","pvscore_short",
    "magicscore","magicscore_short",
    "aa-1042-qrp-rm","aa-1042-qrp","1042_qrp_factsheet_vf",
    "1042_qualified_replacement_property","postdata1",
    # v7: externally-sourced academic reprints (zero extractable text)
    "simple.models.68",
    "the_cross-section_of_expected_stock_returns",
    "williams-trying_too_hard",
}
JUNK_SUBS = [
    "parking","reimbursement","syllabus","brochure","client-manual",
    "client-agreement","factsheet","elevation-chart","28-mile",
    "newsletter","_pr_20","2017.10.24","vsb_mar-21","agenda",
    # Slide decks / presentations
    "ppt","slides","_deck","deck-","conference",
    "demquant","democratizing-quant","democratize-quant",  # v7: actual AA conf filename patterns
    # Fund product materials (tearsheets, risk reports, attribution, education, legal)
    "attribution","offering-document","firm_overview",
    "education_vf","10yr-nav","10yr_nav",
    "maguire-asset-management-letter",
]

# Matches numbered conference session prefixes.
# First alt: 1.1-, 3.A., 5.1. (nested sub-session form, from v6)
# Second alt: 3.demquant, 5.predictable, 2.State_of_ETFs (v7: simple N.word form)
_CONFERENCE_SESSION = re.compile(r"^\d+\.(?:[a-z0-9][-.]|[a-z_])", re.IGNORECASE)
# Matches fund tearsheet/risk-report suffixes: RAA-Agg-RM, QV-II-TM-1, IQV-RM-TM, etc.
_FUND_REPORT = re.compile(r"[-_](rm|tm)([-_]\d+)?$", re.IGNORECASE)

def is_junk_pdf(filename: str) -> bool:
    stem = Path(filename).stem.lower()
    if stem in JUNK_STEMS:
        return True
    if any(s in stem for s in JUNK_SUBS):
        return True
    if _CONFERENCE_SESSION.match(stem):
        return True
    if _FUND_REPORT.search(stem):
        return True
    return False


# ── Second-pass content filter (catches externally-linked non-finance papers) ─
# The WP Media API returns every PDF uploaded to AA's media library, including
# external papers that AA authors attached to blog posts. Filename heuristics
# can't catch these (e.g. Leli-Clinical-detection-of-intellectual-deterioration,
# a 1984 Journal of Clinical Psychology paper about WAIS brain-damage assessment
# that got uploaded to an AA post). Solution: after download, scan the first
# ~1500 words for finance vocabulary. Papers with fewer than MIN_FINANCE_HITS
# distinct finance terms get routed to junk.
FINANCE_TERMS = {
    "portfolio", "return", "returns", "volatility", "investor", "investors",
    "asset", "assets", "market", "markets", "equity", "equities",
    "bond", "bonds", "stock", "stocks", "etf", "fund", "funds",
    "factor", "factors", "alpha", "beta", "risk", "premium", "premia",
    "yield", "yields", "allocation", "hedge", "dividend", "dividends",
    "valuation", "sharpe", "variance", "covariance", "benchmark",
    "momentum", "trading", "investment", "investing", "capital",
    "financial", "finance", "economic", "economy", "earnings",
    "revenue", "inflation", "macroeconomic",
}
MIN_FINANCE_HITS   = 3      # distinct finance terms required to keep
CONTENT_CHECK_WORDS = 1500  # scan first N words

def is_finance_content(pdf_bytes: bytes) -> bool:
    """Return True if the PDF contains >= MIN_FINANCE_HITS distinct finance
    terms in its first CONTENT_CHECK_WORDS words. If text extraction fails,
    return True (keep the PDF — let the LLM decide rather than falsely junk)."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        chunks = []
        word_count = 0
        for page in doc:
            txt = page.get_text()
            chunks.append(txt)
            word_count += len(txt.split())
            if word_count >= CONTENT_CHECK_WORDS:
                break
        doc.close()
        # v7: Zero/near-zero extractable words => route to junk. AA publishes from
        # Word/LaTeX and always has searchable text. Image-only PDFs in AA's corpus
        # are reliably externally-sourced reprints (F-F 1992 scan, Williams 1981,
        # Goldberg 1968). PyMuPDF *exception* is still treated as keep (see except).
        if word_count < 20:
            print(f"    [content-check] zero/low-text PDF (words={word_count}) → junk")
            return False
        text = " ".join(chunks).lower()
        # Restrict to first N words for consistency
        text = " ".join(text.split()[:CONTENT_CHECK_WORDS])
        hits = sum(1 for term in FINANCE_TERMS if term in text)
        return hits >= MIN_FINANCE_HITS
    except Exception as e:
        print(f"    [content-check warn] {e}")
        return True  # extraction failed — keep it


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


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Original AA research posts → Other_Corpus TXT
# ══════════════════════════════════════════════════════════════════════════════

def fetch_posts() -> list:
    posts, page = [], 1
    while True:
        r = requests.get(
            f"{API}/posts",
            params={
                "categories":         ",".join(str(i) for i in INCLUDE_IDS),
                "categories_exclude": ",".join(str(i) for i in EXCLUDE_IDS),
                "per_page": PER_PAGE,
                "page":     page,
                "_fields":  "id,slug,link,date,title,content",
                "status":   "publish",
            },
            headers=HEADERS, timeout=30,
        )
        if r.status_code == 400:
            break
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        posts.extend(batch)
        print(f"  [page {page}/{total_pages}] +{len(batch)} posts (total {len(posts)})")
        if page >= total_pages:
            break
        page += 1
        sleep()
    return posts


def html_to_text(raw_html: str) -> str:
    """Strip HTML tags and normalise whitespace, preserving paragraph breaks."""
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "li", "br"]):
        tag.insert_before("\n\n")
    text = soup.get_text(separator="", strip=False)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def save_txt(post: dict, done: set) -> bool:
    pid = str(post["id"])
    if pid in done:
        return False
    title   = post["title"]["rendered"]
    content = post["content"]["rendered"]
    date    = post["date"][:10]
    link    = post["link"]
    slug    = link.rstrip("/").split("/")[-1]
    fname   = safe_name(f"{date}_{slug}") + ".txt"

    clean_title = BeautifulSoup(title, "html.parser").get_text(strip=True)
    body = html_to_text(content)
    text = f"SOURCE: {link}\nTITLE: {clean_title}\nDATE: {date}\n\n{body}"

    (TEXT_DIR / fname).write_text(text, encoding="utf-8")
    mark_done(TEXT_DONE, pid)
    return True


def run_text_phase():
    print("\n" + "=" * 60)
    print("PHASE 1 — Original AA posts → Other_Corpus TXT")
    print(f"  Include IDs : {INCLUDE_IDS}")
    print(f"  Exclude IDs : {EXCLUDE_IDS}")
    print("=" * 60)

    # Always wipe existing .txt files and done log — text phase is fast (API only,
    # no heavy downloads) so there is no meaningful resume benefit, and a stale
    # done log with missing files causes the entire phase to silently skip.
    # v7: skip _done.txt from previous pre-migration state (cleaned above at module load)
    stale = [f for f in TEXT_DIR.glob("*.txt") if not f.name.startswith("_done")]
    if stale:
        print(f"  Removing {len(stale)} stale TXT files...")
        for f in stale:
            f.unlink()
    if TEXT_DONE.exists():
        TEXT_DONE.unlink()
        print("  Cleared done log.")

    print("\n[1a] Fetching posts...")
    posts = fetch_posts()
    print(f"  {len(posts)} posts retrieved")

    print("\n[1b] Saving TXT...")
    done = load_done(TEXT_DONE)
    saved = skipped = failed = 0
    for post in posts:
        pid = str(post["id"])
        try:
            if save_txt(post, done):
                done.add(pid)
                saved += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  [error] post {pid}: {e}")
            failed += 1
    print(f"  Saved: {saved}  |  Already done: {skipped}  |  Failed: {failed}")


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — PDFs via WP Media API → pdfs/ or junk/
# ══════════════════════════════════════════════════════════════════════════════

def download_pdf(url: str, done: set) -> str:
    raw  = url.split("?")[0].rstrip("/").split("/")[-1]
    key  = raw.lower()
    if key in done:
        return "skip"
    fname = safe_name(raw)
    if not fname.lower().endswith(".pdf"):
        fname += ".pdf"
    # First-pass (filename): fast, catches known product/admin/fund-report PDFs
    dest = JUNK_DIR if is_junk_pdf(raw) else PDF_DIR
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        if r.content[:4] != b"%PDF":
            return "fail"
        # Second-pass (content): catches externally-linked non-finance papers
        # that passed the filename filter but have no finance vocabulary
        if dest == PDF_DIR and not is_finance_content(r.content):
            dest = JUNK_DIR
        (dest / fname).write_bytes(r.content)
        mark_done(PDF_DONE, key)
        done.add(key)
        return "junk" if dest == JUNK_DIR else "ok"
    except Exception as e:
        print(f"    [error] {e}")
        return "fail"


def run_pdf_phase():
    print("\n" + "=" * 60)
    print("PHASE 2 — Research PDFs via WP Media API")
    print("=" * 60)

    if OLD_SRC_PDF.exists():
        print(f"  Removing old src/data/pdfs/AlphaArchitect/...")
        shutil.rmtree(OLD_SRC_PDF)

    # Reset done file so PDFs re-download to correct locations
    if PDF_DONE.exists():
        PDF_DONE.unlink()

    done   = set()
    counts = {"ok": 0, "junk": 0, "skip": 0, "fail": 0}
    page   = 1

    while True:
        r = requests.get(
            f"{API}/media",
            params={"mime_type": "application/pdf", "per_page": 100,
                    "page": page, "_fields": "source_url"},
            headers=HEADERS, timeout=30,
        )
        if r.status_code in (400, 404):
            break
        if r.status_code != 200:
            print(f"  [warn] HTTP {r.status_code}")
            break
        batch = r.json()
        if not batch:
            break

        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        print(f"  [media page {page}/{total_pages}] {len(batch)} items")

        for item in batch:
            url = item.get("source_url", "")
            if not url:
                continue
            result = download_pdf(url, done)
            counts[result] += 1
            tag = {"ok": "✓", "junk": "J", "skip": "–", "fail": "✗"}[result]
            print(f"    [{tag}] {url.split('/')[-1]}")
            sleep()

        if page >= total_pages:
            break
        page += 1

    print(f"\n  Research: {counts['ok']}  |  Junk: {counts['junk']}  "
          f"|  Skip: {counts['skip']}  |  Fail: {counts['fail']}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdfs-only", action="store_true")
    parser.add_argument("--text-only", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("Alpha Architect Scraper v7")
    print(f"  Other_Corpus → {TEXT_DIR.resolve()}")
    print(f"  PDFs         → {PDF_DIR.resolve()}")
    print(f"  Junk         → {JUNK_DIR.resolve()}")
    print("=" * 60)

    if not args.pdfs_only:
        run_text_phase()

    if not args.text_only:
        run_pdf_phase()

    n_txt  = len([f for f in TEXT_DIR.glob("*.txt") if not f.name.startswith("_done")])
    n_pdf  = len(list(PDF_DIR.glob("*.pdf")))
    n_junk = len(list(JUNK_DIR.glob("*.pdf")))
    print("\n" + "=" * 60)
    print(f"  Other_Corpus TXTs  : {n_txt}")
    print(f"  Research PDFs      : {n_pdf}")
    print(f"  Junk PDFs          : {n_junk}")
    print("=" * 60)


if __name__ == "__main__":
    main()