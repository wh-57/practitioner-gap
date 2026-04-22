#!/usr/bin/env python
"""
canonical_sample.py — Sample PDFs for canonical pattern discovery.

Pulls a stratified random sample of PDFs from data/pdfs/{source}/ and
concatenates targeted excerpts (intro + references section) into a single
text file for manual review to identify canonical and metric-ambiguous
citation patterns.

Usage (PowerShell, from repo root):
    conda activate emi
    python src/post_processing/canonical_sample.py                  # default 60 docs
    python src/post_processing/canonical_sample.py --max-docs 150
    python src/post_processing/canonical_sample.py --max-docs 200 --seed 7

Output:
    output/canonical_sample/sample_{N}docs.txt           # concatenated excerpts
    output/canonical_sample/sample_{N}docs_manifest.txt  # per-doc ok/fail log

Design notes:
    - Stratified by source (equal target per source, capped at what's available)
    - Intro excerpt: first 5000 chars (captures abstract + intro where canonical
      name-drops cluster)
    - Refs excerpt: finds "References" / "Bibliography" heading, takes up to
      8000 chars from there. Fallback = last 8000 chars.
    - Skips docs with < 500 chars extracted (likely scanned / OCR-failed)
    - Uses pypdf; swap to pdfplumber if extraction quality is poor on a source
"""

import argparse
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

try:
    import pypdf
except ImportError:
    sys.exit("pypdf not installed. Run: pip install pypdf")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_ROOT = REPO_ROOT / "data" / "pdfs"
OUTPUT_DIR = REPO_ROOT / "output" / "canonical_sample"

INTRO_CHARS = 5000
REFS_MAX_CHARS = 8000
MIN_TEXT_CHARS = 500

# Ordered most-specific first so longer headings match before shorter ones
REFS_HEADINGS = [
    r"\n\s*reference\s+list\s*\n",
    r"\n\s*works\s+cited\s*\n",
    r"\n\s*bibliography\s*\n",
    r"\n\s*references\s*\n",
]


def extract_excerpts(pdf_path: Path):
    """Return ((intro, refs), None) on success or (None, error_msg) on failure."""
    try:
        reader = pypdf.PdfReader(str(pdf_path))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        text = "\n".join(parts)
    except Exception as e:
        return None, f"PDF read failed: {type(e).__name__}: {e}"

    if len(text.strip()) < MIN_TEXT_CHARS:
        return None, f"Too little text ({len(text.strip())} chars; likely scanned)"

    intro = text[:INTRO_CHARS]

    refs = ""
    lower = text.lower()
    for pattern in REFS_HEADINGS:
        m = re.search(pattern, lower)
        if m:
            refs = text[m.start(): m.start() + REFS_MAX_CHARS]
            break

    if not refs:
        # Fallback: last chunk (refs usually at end even if heading missed)
        refs = text[-REFS_MAX_CHARS:] if len(text) > INTRO_CHARS else text[INTRO_CHARS:]

    return (intro, refs), None


def stratified_sample(max_docs: int, seed: int):
    """Return list of (source, pdf_path) tuples, stratified across source folders."""
    random.seed(seed)

    if not PDF_ROOT.exists():
        sys.exit(f"PDF root not found: {PDF_ROOT}")

    source_pdfs = defaultdict(list)
    for source_dir in sorted(PDF_ROOT.iterdir()):
        if not source_dir.is_dir() or source_dir.name.startswith("_"):
            continue
        pdfs = [p for p in source_dir.rglob("*.pdf")
                if "_temp_downloads" not in p.parts]
        if pdfs:
            source_pdfs[source_dir.name] = pdfs

    if not source_pdfs:
        sys.exit(f"No PDFs found under {PDF_ROOT}")

    total = sum(len(v) for v in source_pdfs.values())
    num_sources = len(source_pdfs)
    print(f"Found {total} PDFs across {num_sources} sources")

    # Target: even per-source share, minimum 3 per source
    per_source = max(3, max_docs // num_sources)

    sample = []
    for source, pdfs in sorted(source_pdfs.items()):
        k = min(per_source, len(pdfs))
        picked = random.sample(pdfs, k)
        sample.extend((source, p) for p in picked)
        print(f"  {source:20s} {k:3d}/{len(pdfs)}")

    # Top up if under cap, drawing from sources that still have PDFs left
    if len(sample) < max_docs:
        already = {p for _, p in sample}
        leftover = [(s, p) for s, ps in source_pdfs.items()
                    for p in ps if p not in already]
        random.shuffle(leftover)
        need = max_docs - len(sample)
        sample.extend(leftover[:need])
        if need and leftover:
            print(f"  (topped up with {min(need, len(leftover))} extra docs)")

    # Trim if over cap (shouldn't happen given per_source math, but safe)
    if len(sample) > max_docs:
        random.shuffle(sample)
        sample = sample[:max_docs]

    return sample


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-docs", type=int, default=60,
                    help="Total PDFs to sample (default 60, hard cap 200)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    max_docs = min(args.max_docs, 200)
    if args.max_docs > 200:
        print(f"Capping max-docs at 200 (requested {args.max_docs})")

    sample = stratified_sample(max_docs, seed=args.seed)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"sample_{max_docs}docs.txt"
    man_path = OUTPUT_DIR / f"sample_{max_docs}docs_manifest.txt"

    ok = fail = 0
    with out_path.open("w", encoding="utf-8") as out, \
         man_path.open("w", encoding="utf-8") as manifest:
        manifest.write(
            f"# Canonical sample manifest\n"
            f"# max_docs={max_docs}, seed={args.seed}, "
            f"intro_chars={INTRO_CHARS}, refs_max={REFS_MAX_CHARS}\n\n"
        )
        for i, (source, pdf) in enumerate(sample, 1):
            result, err = extract_excerpts(pdf)
            if err:
                manifest.write(f"[FAIL] {source}/{pdf.name}  --  {err}\n")
                fail += 1
                continue

            intro, refs = result
            out.write(f"\n{'=' * 80}\n")
            out.write(f"DOC {i}/{len(sample)}  |  SOURCE: {source}  |  FILE: {pdf.name}\n")
            out.write(f"{'=' * 80}\n\n")
            out.write("--- INTRO / FIRST PAGES ---\n")
            out.write(intro.strip())
            out.write("\n\n--- REFERENCES / TAIL ---\n")
            out.write(refs.strip())
            out.write("\n")

            manifest.write(f"[OK]   {source}/{pdf.name}\n")
            ok += 1

    print(f"\nDone: {ok} ok, {fail} failed ({len(sample)} attempted)")
    print(f"Output:   {out_path}")
    print(f"Manifest: {man_path}")
    if out_path.exists():
        size_mb = out_path.stat().st_size / 1_000_000
        print(f"Size:     {size_mb:.1f} MB")


if __name__ == "__main__":
    main()