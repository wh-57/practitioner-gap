"""
aa_v7_audit.py
==============
Checks every PDF currently in data/pdfs/AlphaArchitect/ against v7 filter
rules. Does NOT modify anything — read-only audit. Reports which files
would now be routed to junk under v7, so they can be manually moved.

Two checks:
  1. is_junk_pdf(filename) — filename-based (new JUNK_STEMS, JUNK_SUBS,
     loosened _CONFERENCE_SESSION regex)
  2. zero/low text content — the new v7 rule in is_finance_content()
     (PDFs with <20 extractable words route to junk)

Also reports the finance-vocab check for low-wpp PDFs so you can see which
ones v7's full pipeline would reject vs keep.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make arch_scraper importable
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "src" / "scrapers"))

try:
    from arch_scraper import (
        is_junk_pdf,
        FINANCE_TERMS,
        MIN_FINANCE_HITS,
        CONTENT_CHECK_WORDS,
    )
except ImportError as e:
    print(f"ERROR: cannot import from arch_scraper: {e}")
    sys.exit(1)

try:
    import fitz
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip install pymupdf")
    sys.exit(1)


PDF_DIR = REPO_ROOT / "data" / "pdfs" / "AlphaArchitect"


def content_audit(pdf_path: Path) -> tuple[str, int, int]:
    """
    Returns (verdict, word_count, finance_hits).
    verdict: "junk_zero_text" | "junk_low_finance" | "keep" | "probe_error"
    """
    try:
        with fitz.open(pdf_path) as doc:
            chunks = []
            word_count = 0
            for page in doc:
                txt = page.get_text() or ""
                chunks.append(txt)
                word_count += len(txt.split())
                if word_count >= CONTENT_CHECK_WORDS:
                    break
        if word_count < 20:
            return ("junk_zero_text", word_count, 0)
        text = " ".join(chunks).lower()
        text = " ".join(text.split()[:CONTENT_CHECK_WORDS])
        hits = sum(1 for term in FINANCE_TERMS if term in text)
        if hits < MIN_FINANCE_HITS:
            return ("junk_low_finance", word_count, hits)
        return ("keep", word_count, hits)
    except Exception as e:
        return (f"probe_error:{type(e).__name__}", 0, 0)


def main() -> None:
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    print(f"Auditing {len(pdfs)} PDFs in {PDF_DIR}\n")
    print("=" * 100)

    filename_junks = []
    content_junks_zero = []
    content_junks_low_finance = []
    probe_errors = []

    for i, pdf in enumerate(pdfs, 1):
        fname = pdf.name
        fn_junk = is_junk_pdf(fname)

        if fn_junk:
            filename_junks.append(fname)
            print(f"[{i:3d}/{len(pdfs)}]  FILENAME_JUNK  {fname}")
            continue

        # Content check only for files that passed filename filter
        verdict, wc, hits = content_audit(pdf)
        if verdict == "junk_zero_text":
            content_junks_zero.append((fname, wc))
            print(f"[{i:3d}/{len(pdfs)}]  ZERO_TEXT      {fname}  (words={wc})")
        elif verdict == "junk_low_finance":
            content_junks_low_finance.append((fname, wc, hits))
            print(f"[{i:3d}/{len(pdfs)}]  LOW_FINANCE    {fname}  (words={wc}, hits={hits}/{MIN_FINANCE_HITS})")
        elif verdict.startswith("probe_error"):
            probe_errors.append((fname, verdict))
            print(f"[{i:3d}/{len(pdfs)}]  {verdict}  {fname}")
        # "keep" files are not printed individually to reduce noise

    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"  Total PDFs audited                 : {len(pdfs)}")
    print(f"  Would be JUNK by filename (v7)     : {len(filename_junks)}")
    print(f"  Would be JUNK by zero-text (v7)    : {len(content_junks_zero)}")
    print(f"  Would be JUNK by low-finance (v6+) : {len(content_junks_low_finance)}")
    print(f"  Probe errors                       : {len(probe_errors)}")
    keep_n = len(pdfs) - len(filename_junks) - len(content_junks_zero) - len(content_junks_low_finance) - len(probe_errors)
    print(f"  Would KEEP                         : {keep_n}")

    if filename_junks or content_junks_zero or content_junks_low_finance:
        print("\nMOVE CANDIDATES (paste into junk-move script):")
        for f in filename_junks:
            print(f"    '{f}',  # filename rule")
        for f, wc in content_junks_zero:
            print(f"    '{f}',  # zero-text (words={wc})")
        for f, wc, hits in content_junks_low_finance:
            print(f"    '{f}',  # low finance vocab ({hits}/{MIN_FINANCE_HITS} hits)")
    else:
        print("\nNo files need to move. Current PDF_DIR is clean under v7 rules.")


if __name__ == "__main__":
    main()