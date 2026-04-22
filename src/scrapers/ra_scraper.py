"""
ra_scraper.py  —  Research Affiliates Publications  (v3)
---------------------------------------------------------
No Selenium needed. Fetches all publications from the public JSON API:
  https://www.researchaffiliates.com/content/dam/ra/datafiles/publications/publication-list.json

Filters for:
  - content_type = "Articles"  (RA's own practitioner research, purity ~0.85)
  - pdf_link pointing to /content/dam/ra/publications/pdf/ (direct RA PDF)
  - login_required = "false"

Skips:
  - "Journal Papers" (published in FAJ/SSRN — already in academic corpus)
  - "In the News" (press mentions, no reference lists)
  - External PDFs (Barron's, WSJ, SSRN links — not our target)

Usage:
  conda activate emi
  python src/ra_scraper.py

Output: src/data/pdfs/RA/ra_{id}-{slug}.pdf
"""

import re
import time
import random
import json
from pathlib import Path

import requests

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.researchaffiliates.com"
JSON_URL    = BASE_URL + "/content/dam/ra/datafiles/publications/publication-list.json"

OUTPUT_DIR  = Path("src/data/pdfs/RA")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE    = OUTPUT_DIR / "_done.txt"
FAIL_FILE   = OUTPUT_DIR / "_failed.txt"
SKIP_FILE   = OUTPUT_DIR / "_external.txt"  # log external/SSRN links for reference

MIN_DELAY = 1.0
MAX_DELAY = 2.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": BASE_URL,
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def load_done() -> set:
    return set(LOG_FILE.read_text().splitlines()) if LOG_FILE.exists() else set()

def mark_done(url: str):
    with open(LOG_FILE, "a") as f:
        f.write(url + "\n")

def mark_failed(url: str, reason: str = ""):
    with open(FAIL_FILE, "a") as f:
        f.write(f"{url}  # {reason}\n")

def log_external(entry: dict):
    with open(SKIP_FILE, "a") as f:
        f.write(f"{entry.get('title', '')} | {entry.get('pdf_link', '')} | {entry.get('external_publications', '')}\n")

def slugify_fname(s: str, n: int = 70) -> str:
    s = re.sub(r"[^\w-]", "_", s.lower()).strip("_")
    return s[:n]

# ── Fetch and filter publications ─────────────────────────────────────────────

def fetch_publications(session: requests.Session) -> tuple[list[dict], list[dict]]:
    """
    Returns (to_download, externals).
    to_download: Articles with direct RA PDF links.
    externals:   Articles with external links (SSRN, FAJ, etc.) — logged but skipped.
    """
    print(f"[1/2] Fetching publication list from JSON API...")
    resp = session.get(JSON_URL, timeout=30)
    resp.raise_for_status()
    all_pubs = resp.json()
    print(f"  Total entries in JSON: {len(all_pubs)}")

    to_download = []
    externals   = []
    skipped     = 0

    for entry in all_pubs:
        content_types = entry.get("content_type", [])
        pdf_link      = entry.get("pdf_link", "")
        login_req     = entry.get("login_required", "true")
        title         = entry.get("title", "")

        # Only want Articles (RA's own practitioner research)
        if "Articles" not in content_types:
            skipped += 1
            continue

        # Skip login-required
        if login_req == "true":
            skipped += 1
            continue

        # Skip empty PDF links
        if not pdf_link:
            skipped += 1
            continue

        # Classify: direct RA PDF vs external
        if pdf_link.startswith("/content/dam/ra/publications/pdf/"):
            full_url = BASE_URL + pdf_link
            slug = pdf_link.split("/")[-1].replace(".pdf", "")
            fname = f"ra_{slugify_fname(slug)}.pdf"
            to_download.append({
                "title":    title,
                "pdf_url":  full_url,
                "fname":    fname,
                "year":     entry.get("year", ""),
                "authors":  entry.get("authors", []),
            })
        else:
            # External link (SSRN, FAJ, Barron's, etc.)
            externals.append(entry)

    print(f"  Articles with direct RA PDFs: {len(to_download)}")
    print(f"  Articles with external links: {len(externals)}")
    print(f"  Skipped (non-articles/login/empty): {skipped}")

    return to_download, externals

# ── Download ──────────────────────────────────────────────────────────────────

def download_pdf(session: requests.Session, entry: dict, retries: int = 3) -> bool:
    fpath = OUTPUT_DIR / entry["fname"]

    if fpath.exists():
        print(f"  [skip] {entry['fname']}")
        return True

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(entry["pdf_url"], stream=True, timeout=60)
            resp.raise_for_status()
            content = b"".join(resp.iter_content(8192))

            if len(content) < 5000 or b"%PDF" not in content[:10]:
                print(f"  [warn] not a PDF ({len(content)} bytes): {entry['fname']}")
                mark_failed(entry["pdf_url"], "not a pdf")
                return False

            fpath.write_bytes(content)
            size_kb = fpath.stat().st_size // 1024
            print(f"  [✓] {entry['fname']} ({size_kb} KB)")
            return True

        except Exception as e:
            if attempt < retries:
                wait = 5 * attempt
                print(f"  [retry {attempt}/{retries}] {e} — waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"  [error] {entry['fname']}: {e}")
                mark_failed(entry["pdf_url"], str(e))
                return False

    return False

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    done = load_done()

    print("=" * 60)
    print("RA Scraper v3  ·  researchaffiliates.com  ·  JSON API")
    print(f"{len(done)} articles already downloaded")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Fetch and classify
    to_download, externals = fetch_publications(session)

    # Log external links for reference (these won't be downloaded)
    if externals:
        print(f"\n  Logging {len(externals)} external links to {SKIP_FILE.name}...")
        for e in externals:
            log_external(e)

    # Download
    print(f"\n[2/2] Downloading {len(to_download)} PDFs...")
    ok = fail = skip = 0

    for i, entry in enumerate(to_download, 1):
        if entry["pdf_url"] in done:
            skip += 1
            print(f"  [{i}/{len(to_download)}] [skip] {entry['title'][:55]}")
            continue

        print(f"  [{i}/{len(to_download)}] {entry['year']} — {entry['title'][:55]}")
        if download_pdf(session, entry):
            mark_done(entry["pdf_url"])
            ok += 1
        else:
            fail += 1

        sleep()

    print("\n" + "=" * 60)
    print(f"Done.  Downloaded: {ok}   Failed: {fail}   Skipped: {skip}")
    print(f"PDFs:  {OUTPUT_DIR.resolve()}")
    if externals:
        print(f"External links logged to: {SKIP_FILE.resolve()}")
    print("=" * 60)

if __name__ == "__main__":
    main()