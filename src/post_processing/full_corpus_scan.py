#!/usr/bin/env python
"""
full_corpus_scan.py — Validate candidate canonical patterns across the full corpus.

Addresses the selection-bias critique: the 150-doc discovery sample is too small
to rule out low-frequency-but-important patterns. This script applies a list of
deferred candidate regexes across ALL ~9,400 extracted documents and reports
frequency + source spread for each, so the YAML can be locked on evidence.

Differences vs canonical_scraper.py:
    - Reads cached extracted text from output/extracted_text/{source}/{filename}.txt
      if present, otherwise extracts on the fly. Caching means a second run is fast.
    - Emits a SUMMARY report, not citation rows. No CSV of individual hits.
    - Designed to be run once, before locking patterns.yaml.

Usage:
    conda activate emi
    python src/post_processing/full_corpus_scan.py
    python src/post_processing/full_corpus_scan.py --min-docs 5  # inclusion threshold
    python src/post_processing/full_corpus_scan.py --extract-cache  # build text cache

Output:
    output/canonical_sample/full_corpus_candidate_scan.md    # human-readable
    output/canonical_sample/full_corpus_candidate_scan.csv   # per-candidate stats
"""

import argparse
import csv
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
TXT_ROOT = REPO_ROOT / "data" / "Other_Corpus"
CACHE_DIR = REPO_ROOT / "output" / "extracted_text"
OUT_DIR = REPO_ROOT / "output" / "canonical_sample"

MIN_TEXT_CHARS = 500

# ---------------------------------------------------------------------------
# Candidate patterns — all the ones reviewers flagged that I deferred.
# Format: (candidate_id, regex, suggested_bucket, reviewer_source)
# ---------------------------------------------------------------------------
CANDIDATES = [
    # Raised by Gemini
    ("grinold_1989_fundamental_law",
     r'\bgrinold\b|\bfundamental\s+law\s+of\s+active\s+management\b|\binformation\s+coefficient\b',
     "canonical_metric_ambiguous", "Gemini"),

    # Raised by Claude-3.5
    ("shleifer_vishny_1997_limits",
     r'\blimits\s+of\s+arbitrage\b|\bshleifer\s+(?:and\s+|&\s+)?vishny\s*\(?1997\)?',
     "canonical", "Claude-3.5"),

    # Raised by Reviewer 3 (the structured one)
    ("merton_1974_credit",
     r'\bmerton\b[^.]{0,30}?\b1974\b|\bstructural\s+(?:credit|default|debt)\s+model\b|\bmerton\s+distance[-\s]to[-\s]default\b',
     "canonical", "Reviewer-3"),
    ("fama_1981_inflation",
     r'\bfama\b[^.]{0,30}?\b1981\b',
     "canonical", "Reviewer-3"),
    ("campbell_1991_variance_decomp",
     r'\bcampbell\b[^.]{0,30}?\b1991\b|\bvariance\s+decomposition\b',
     "canonical_metric_ambiguous", "Reviewer-3"),
    ("cochrane_1996_investment_capm",
     r'\bcochrane\b[^.]{0,30}?\b1996\b|\binvestment\s+CAPM\b|\binvestment[-\s]based\s+asset\s+pricing\b',
     "canonical", "Reviewer-3"),

    # Raised by Reviewer 4
    ("pastor_stambaugh_2003_liquidity",
     r'\bp[aá]stor[-\s]+stambaugh\b|\bpastor[-\s]+stambaugh\b',
     "canonical", "Reviewer-4"),
    ("frazzini_pedersen_bab",
     r'\bfrazzini\b[^.]{0,60}?\bpedersen\b|\bbetting[-\s]+against[-\s]+beta\b|\bBAB\s+(?:factor|strategy|portfolio)\b',
     "canonical_metric_ambiguous", "Reviewer-4"),
    ("roll_1977_critique",
     r'\broll(?:\'s|\u2019s)?\s+critique\b|\broll\s*\(?1977\)?',
     "canonical", "Reviewer-4"),
    ("treynor_1965",
     r'\btreynor\s+ratio\b|\btreynor\s*\(?1965\)?',
     "canonical_metric_ambiguous", "Reviewer-4"),
    ("sortino_ratio",
     r'\bsortino\s+ratio\b',
     "canonical_metric_ambiguous", "Reviewer-4"),
    ("jorion_var",
     r'\bjorion\b\s*(?:\(?1996\)?|\(?1997\)?|\(?2007\)?)',
     "canonical_metric_ambiguous", "Reviewer-3"),
]


# ---------------------------------------------------------------------------
def iter_corpus():
    if PDF_ROOT.exists():
        for source_dir in sorted(PDF_ROOT.iterdir()):
            if not source_dir.is_dir() or source_dir.name.startswith("_"):
                continue
            for p in source_dir.rglob("*.pdf"):
                if "_temp_downloads" in p.parts:
                    continue
                yield source_dir.name, p, "pdf"
    if TXT_ROOT.exists():
        for source_dir in sorted(TXT_ROOT.iterdir()):
            if not source_dir.is_dir() or source_dir.name.startswith("_"):
                continue
            for p in source_dir.rglob("*.txt"):
                yield source_dir.name, p, "txt"


def extract_pdf(pdf_path: Path) -> str:
    try:
        reader = pypdf.PdfReader(str(pdf_path))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(parts)
    except Exception:
        return ""


def get_text(source: str, path: Path, kind: str, use_cache: bool) -> str:
    """Extract text, using a cache in output/extracted_text/{source}/ so
    repeated runs are fast. Returns empty string on failure."""
    cache_path = CACHE_DIR / source / (path.stem + ".txt")
    if use_cache and cache_path.exists():
        try:
            return cache_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pass

    if kind == "pdf":
        text = extract_pdf(path)
    else:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = ""

    if use_cache and text.strip():
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            cache_path.write_text(text, encoding="utf-8")
        except Exception:
            pass
    return text


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-docs", type=int, default=5,
                    help="Suggested inclusion threshold: appear in >=N docs")
    ap.add_argument("--min-sources", type=int, default=2,
                    help="Suggested inclusion threshold: appear in >=N sources")
    ap.add_argument("--no-cache", action="store_true",
                    help="Skip the text cache (slower but uses less disk)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after processing N documents (sanity run)")
    args = ap.parse_args()

    use_cache = not args.no_cache

    # Compile all candidate regexes
    compiled = []
    for cid, regex, bucket, source_reviewer in CANDIDATES:
        try:
            compiled.append((cid, re.compile(regex, re.IGNORECASE),
                             bucket, source_reviewer))
        except re.error as e:
            sys.exit(f"Bad regex for {cid}: {e}")

    # Stats per candidate
    hit_counts = defaultdict(int)          # total hits
    doc_counts = defaultdict(set)          # set of doc paths that hit
    source_counts = defaultdict(lambda: defaultdict(int))  # candidate -> source -> hits
    first_contexts = {}                    # candidate -> first context snippet

    total = 0
    scanned = 0
    skipped = 0

    for source, path, kind in iter_corpus():
        if args.limit and total >= args.limit:
            break
        total += 1

        text = get_text(source, path, kind, use_cache)
        if len(text.strip()) < MIN_TEXT_CHARS:
            skipped += 1
            continue
        scanned += 1

        key = f"{source}/{path.name}"
        for cid, rx, _bucket, _rev in compiled:
            matches = list(rx.finditer(text))
            if not matches:
                continue
            hit_counts[cid] += len(matches)
            doc_counts[cid].add(key)
            source_counts[cid][source] += len(matches)
            if cid not in first_contexts:
                m = matches[0]
                start = max(0, m.start() - 80)
                end = min(len(text), m.end() + 80)
                ctx = re.sub(r"\s+", " ", text[start:end]).strip()
                first_contexts[cid] = f"[{source}] ...{ctx}..."

        if scanned % 500 == 0:
            print(f"  scanned {scanned} docs (total iterated {total})")

    print(f"\nScanned {scanned} docs, skipped {skipped} (<{MIN_TEXT_CHARS} chars), total {total}")

    # Write reports
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUT_DIR / "full_corpus_candidate_scan.csv"
    md_path = OUT_DIR / "full_corpus_candidate_scan.md"

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["candidate_id", "suggested_bucket", "raised_by",
                    "total_hits", "n_docs", "n_sources",
                    "top_sources", "meets_threshold", "first_context"])
        for cid, rx, bucket, rev in compiled:
            hits = hit_counts.get(cid, 0)
            ndocs = len(doc_counts.get(cid, set()))
            srcs = source_counts.get(cid, {})
            nsrcs = len(srcs)
            top = "; ".join(
                f"{s}:{c}" for s, c in sorted(srcs.items(), key=lambda x: -x[1])[:5]
            )
            meets = (ndocs >= args.min_docs) and (nsrcs >= args.min_sources)
            w.writerow([cid, bucket, rev, hits, ndocs, nsrcs, top,
                        "YES" if meets else "no",
                        first_contexts.get(cid, "")])

    with md_path.open("w", encoding="utf-8") as f:
        f.write("# Full-corpus candidate scan\n\n")
        f.write(f"Scanned {scanned} documents. Threshold: >={args.min_docs} docs "
                f"across >={args.min_sources} sources.\n\n")
        f.write("| Candidate | Bucket | Raised by | Hits | Docs | Sources | Meets | Top sources |\n")
        f.write("|---|---|---|---:|---:|---:|---|---|\n")

        rows = []
        for cid, rx, bucket, rev in compiled:
            hits = hit_counts.get(cid, 0)
            ndocs = len(doc_counts.get(cid, set()))
            srcs = source_counts.get(cid, {})
            nsrcs = len(srcs)
            top = ", ".join(
                f"{s}({c})" for s, c in sorted(srcs.items(), key=lambda x: -x[1])[:4]
            )
            meets = (ndocs >= args.min_docs) and (nsrcs >= args.min_sources)
            rows.append((cid, bucket, rev, hits, ndocs, nsrcs, meets, top))
        rows.sort(key=lambda r: -r[3])
        for cid, bucket, rev, hits, ndocs, nsrcs, meets, top in rows:
            flag = "✅" if meets else "❌"
            f.write(f"| `{cid}` | {bucket} | {rev} | {hits} | {ndocs} | {nsrcs} | {flag} | {top} |\n")

        f.write("\n## First-match contexts (spot-check these)\n\n")
        for cid, ctx in first_contexts.items():
            f.write(f"### {cid}\n\n```\n{ctx}\n```\n\n")

    print(f"\nReports written:")
    print(f"  {md_path}")
    print(f"  {csv_path}")
    print(f"\nOpen the .md file and eyeball first-match contexts to spot false positives")
    print(f"before adding any YES-threshold candidate to patterns.yaml.")


if __name__ == "__main__":
    main()
