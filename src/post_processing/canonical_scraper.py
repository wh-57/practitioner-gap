#!/usr/bin/env python
"""
canonical_scraper.py — Mechanical regex scraper for canonical citations.

Reads patterns from patterns.yaml (source of truth). For each document in
data/pdfs/{source}/ and data/Other_Corpus/{source}/, applies every pattern
and emits one row per (doc_id, canonical_pattern_id) pair with the full
mention count and a representative context snippet.

Output schema (matches canonical_citations.csv v2):
    doc_id, source_file, citation_source, canonical_pattern_id,
    recovered_title, recovered_year, recovered_authors, recovered_journal,
    is_academic, is_canonical, academic_subfield,
    citation_function, citation_polarity,
    within_doc_mention_count, citation_context, confidence, schema_version

Key design decisions:
    - Dedup at emit time: one row per (doc_id, pattern_id). Previous per-mention
      output is replaced; mention count lives in within_doc_mention_count.
    - Pattern order matters in patterns.yaml. More specific patterns first
      (e.g. fama_french_2015 before fama_french_1993 before bare fama_french).
    - doc_id = first 12 hex chars of SHA-256 of file bytes. Stable across moves.
    - citation_context = first match ± 150 chars, whitespace-normalized.
    - is_academic=True, is_canonical=True for all rows (all patterns point to
      foundational academic papers by definition).
    - citation_function and citation_polarity are "unknown" — canonical
      scraper cannot determine these mechanically. 01b_merge.py can overwrite
      from matched LLM rows at merge time.

Usage:
    conda activate emi
    python src/scrapers/canonical_scraper.py              # incremental (resume)
    python src/scrapers/canonical_scraper.py --fresh      # ignore done log
    python src/scrapers/canonical_scraper.py --limit 50   # sanity run

IMPORTANT — RESUME LOG INVALIDATION AFTER OCR:
    The resume log (_done.txt) is keyed on "{source}/{filename}", NOT on the
    file's content hash. When OCR (e.g., ocr_jpm.py) replaces a PDF in-place,
    the path is unchanged but the bytes and doc_id are different. The resume
    log will skip such files, causing the scraper to miss freshly-searchable
    canonical mentions that only exist post-OCR.

    After ANY in-place OCR batch, re-run this scraper with --fresh to force
    a full rescan. Alternatively, delete the specific entries from
    data/logs/canonical/_done.txt that correspond to OCR'd files before
    running incrementally. Do NOT trust an incremental resume run to pick up
    OCR'd content.

    (A hash-based resume log is a backlog item; deferred for now because
    --fresh runs only cost ~minutes on the current corpus size.)
"""

import argparse
import csv
import hashlib
import re
import sys
from pathlib import Path
from typing import Iterator

try:
    import yaml
except ImportError:
    sys.exit("pyyaml not installed. Run: pip install pyyaml")

try:
    import pypdf
except ImportError:
    sys.exit("pypdf not installed. Run: pip install pypdf")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_ROOT = REPO_ROOT / "data" / "pdfs"
TXT_ROOT = REPO_ROOT / "data" / "Other_Corpus"
PATTERNS_YAML = REPO_ROOT / "src" / "post_processing" / "patterns.yaml"
OUTPUT_CSV = REPO_ROOT / "output" / "canonical_citations.csv"
LOG_DIR = REPO_ROOT / "data" / "logs" / "canonical"
DONE_LOG = LOG_DIR / "_done.txt"
FAIL_LOG = LOG_DIR / "_failed.txt"

CITATION_SOURCE = "canonical_scraper"
CONTEXT_WINDOW = 150  # chars on each side of first match
MIN_TEXT_CHARS = 500  # skip docs with less extracted text
SCHEMA_VERSION = "canonical_v2"

CSV_HEADER = [
    "doc_id", "source_file", "citation_source", "canonical_pattern_id",
    "recovered_title", "recovered_year", "recovered_authors",
    "recovered_journal", "is_academic", "is_canonical", "academic_subfield",
    "citation_function", "citation_polarity",
    "within_doc_mention_count", "citation_context",
    "confidence", "schema_version",
]


# ---------------------------------------------------------------------------
# Pattern loading
# ---------------------------------------------------------------------------
def load_patterns(path: Path) -> list[dict]:
    """Load and validate patterns.yaml. Returns list of pattern dicts with
    an added 'compiled' key holding the compiled regex."""
    if not path.exists():
        sys.exit(f"Patterns file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    patterns = data.get("patterns", [])
    if not patterns:
        sys.exit(f"No patterns in {path}")

    valid_buckets = {"canonical", "canonical_metric_ambiguous"}
    seen_ids = set()
    for p in patterns:
        pid = p.get("pattern_id")
        if not pid:
            sys.exit(f"Pattern missing pattern_id: {p}")
        if pid in seen_ids:
            sys.exit(f"Duplicate pattern_id: {pid}")
        seen_ids.add(pid)

        bucket = p.get("bucket")
        if bucket not in valid_buckets:
            sys.exit(f"{pid}: bucket must be one of {valid_buckets}, got {bucket!r}")

        regex = p.get("regex")
        if not regex:
            sys.exit(f"{pid}: missing regex")
        try:
            p["compiled"] = re.compile(regex, re.IGNORECASE)
        except re.error as e:
            sys.exit(f"{pid}: invalid regex: {e}")

        paper = p.get("paper", {})
        p["_authors"] = "; ".join(paper.get("authors", [])) or ""
        p["_year"] = paper.get("year", "")
        p["_title"] = paper.get("title", "")
        p["_journal"] = paper.get("journal", "") or ""
        p["_subfield"] = p.get("academic_subfield", "") or ""

    print(f"Loaded {len(patterns)} patterns from {path.name}")
    return patterns


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------
def extract_pdf(pdf_path: Path) -> str:
    """Return full extracted text. Empty string on failure."""
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


def extract_txt(txt_path: Path) -> str:
    try:
        return txt_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def doc_id_for(path: Path) -> str:
    """SHA-256 of file bytes, first 12 hex chars."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:12]


# ---------------------------------------------------------------------------
# Document iteration
# ---------------------------------------------------------------------------
def iter_corpus() -> Iterator[tuple[str, Path, str]]:
    """Yield (source_folder_name, path, kind) for every corpus file.
    kind is 'pdf' or 'txt'."""
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


# ---------------------------------------------------------------------------
# Pattern application
# ---------------------------------------------------------------------------
def find_hits(text: str, patterns: list[dict]) -> list[dict]:
    """Apply every pattern to text. Return one dict per pattern that
    matched, keyed by pattern_id, with hit count and first-match context."""
    hits = []
    for p in patterns:
        matches = list(p["compiled"].finditer(text))
        if not matches:
            continue
        m0 = matches[0]
        start = max(0, m0.start() - CONTEXT_WINDOW)
        end = min(len(text), m0.end() + CONTEXT_WINDOW)
        ctx = text[start:end]
        # Strip ASCII control chars (NUL, form feed, etc.) that PDFs sometimes
        # emit — they survive whitespace collapse and crash csv writers.
        ctx = ctx.translate({i: None for i in range(0x00, 0x20) if i not in (0x09, 0x0A, 0x0D)})
        ctx = re.sub(r"\s+", " ", ctx).strip()
        hits.append({
            "pattern": p,
            "count": len(matches),
            "context": ctx,
        })
    return hits


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def row_for(doc_id: str, source_file: str, hit: dict) -> dict:
    p = hit["pattern"]
    return {
        "doc_id": doc_id,
        "source_file": source_file,
        "citation_source": CITATION_SOURCE,
        "canonical_pattern_id": p["pattern_id"],
        "recovered_title": p["_title"],
        "recovered_year": p["_year"],
        "recovered_authors": p["_authors"],
        "recovered_journal": p["_journal"],
        "is_academic": True,
        "is_canonical": True,
        "academic_subfield": p["_subfield"],
        "citation_function": "unknown",
        "citation_polarity": "unknown",
        "within_doc_mention_count": hit["count"],
        "citation_context": hit["context"],
        "confidence": "high",  # string enum per fields.md ("high"/"medium"/"low")
        "schema_version": SCHEMA_VERSION,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--patterns", type=Path, default=PATTERNS_YAML)
    ap.add_argument("--output", type=Path, default=OUTPUT_CSV)
    ap.add_argument("--fresh", action="store_true",
                    help="Ignore resume log and overwrite output")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process at most N documents (sanity run)")
    args = ap.parse_args()

    patterns = load_patterns(args.patterns)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Resume log
    done = set()
    if DONE_LOG.exists() and not args.fresh:
        done = {line.strip() for line in DONE_LOG.read_text(encoding="utf-8").splitlines() if line.strip()}
        print(f"Resume: {len(done)} docs already processed")

    # Write mode: fresh → overwrite with header; incremental → append or
    # create with header if missing
    if args.fresh or not args.output.exists():
        f_out = args.output.open("w", encoding="utf-8", newline="")
        writer = csv.DictWriter(f_out, fieldnames=CSV_HEADER, quoting=csv.QUOTE_ALL)
        writer.writeheader()
    else:
        f_out = args.output.open("a", encoding="utf-8", newline="")
        writer = csv.DictWriter(f_out, fieldnames=CSV_HEADER, quoting=csv.QUOTE_ALL)

    f_done = DONE_LOG.open("w" if args.fresh else "a", encoding="utf-8")
    f_fail = FAIL_LOG.open("a", encoding="utf-8")

    processed = skipped = failed = row_count = 0
    try:
        for i, (source, path, kind) in enumerate(iter_corpus()):
            if args.limit and processed >= args.limit:
                break

            key = f"{source}/{path.name}"
            if key in done:
                skipped += 1
                continue

            text = extract_pdf(path) if kind == "pdf" else extract_txt(path)
            if len(text.strip()) < MIN_TEXT_CHARS:
                f_fail.write(f"{key}\tinsufficient_text\t{len(text.strip())}\n")
                f_fail.flush()
                failed += 1
                continue

            try:
                did = doc_id_for(path)
            except Exception as e:
                f_fail.write(f"{key}\thash_failed\t{e}\n")
                f_fail.flush()
                failed += 1
                continue

            hits = find_hits(text, patterns)
            rel_path = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
            for hit in hits:
                writer.writerow(row_for(did, rel_path, hit))
                row_count += 1

            f_done.write(f"{key}\n")
            f_done.flush()
            processed += 1

            if processed % 100 == 0:
                print(f"  [{processed}] last: {key}  ({len(hits)} patterns hit)")

    finally:
        f_out.close()
        f_done.close()
        f_fail.close()

    print(f"\nDone. Processed {processed}, skipped {skipped}, failed {failed}.")
    print(f"Rows written: {row_count}")
    print(f"Output:   {args.output}")
    print(f"Done log: {DONE_LOG}")
    if failed:
        print(f"Fail log: {FAIL_LOG}")


if __name__ == "__main__":
    main()