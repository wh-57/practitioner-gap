"""
01b_merge.py — Merge LLM and canonical citations into one file.

Reads:
    output/citations_deep.csv         (from 01_extract_deep.py, LLM)
    output/canonical_citations.csv    (from canonical_scraper.py, mechanical)
    output/documents_deep.csv         (for doc-level metadata on unmatched canonical rows)

Writes:
    output/citations_merged.csv       (combined, for 03_join.py to consume)

Logic:
    For each canonical row:
      1. Try to match it to an LLM row for the same doc (fuzzy title OR
         composite last-author+year match).
      2. If matched → drop the canonical row; transfer within_doc_mention_count
         and canonical_pattern_id onto the LLM row.
      3. If unmatched → keep the canonical row. If it's a METRIC_AMBIGUOUS
         pattern (sharpe_ratio, jensen_alpha, fama_macbeth), flag it with
         likely_metric_only=True. Otherwise treat it as a legitimate informal
         mention and keep likely_metric_only=False.

New columns added to the merged output:
    citation_source              "llm" or "mechanical_canonical"
    within_doc_mention_count     int (filled on canonical-matched rows)
    canonical_pattern_id         string (filled when canonical match found)
    likely_metric_only           bool (True only for unmatched metric-ambiguous canonicals)

Usage:
    python src/01b_merge.py
    python src/01b_merge.py --title-threshold 85 --year-window 1
"""
import argparse
import json
from pathlib import Path

import pandas as pd
import yaml
from rapidfuzz import fuzz


# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT     = Path(__file__).resolve().parent.parent   # gap/
OUT_DIR       = REPO_ROOT / "output"
LLM_PATH      = OUT_DIR / "citations_deep.csv"
CAN_PATH      = OUT_DIR / "canonical_citations.csv"
DOC_PATH      = OUT_DIR / "documents_deep.csv"
OUT_PATH      = OUT_DIR / "citations_merged.csv"
PATTERNS_YAML = REPO_ROOT / "src" / "post_processing" / "patterns.yaml"

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_TITLE_THRESHOLD = 85   # fuzzy ratio 0-100
DEFAULT_YEAR_WINDOW     = 1    # allow ±1 year drift on composite match


def load_metric_ambiguous_patterns(path: Path = PATTERNS_YAML) -> set[str]:
    """Load pattern_ids tagged canonical_metric_ambiguous in patterns.yaml.
    Single source of truth shared with canonical_scraper.py. Raises
    FileNotFoundError with a clear message if the yaml is missing."""
    if not path.exists():
        raise FileNotFoundError(
            f"patterns.yaml not found at {path}. "
            f"01b_merge.py depends on it for the metric-ambiguous pattern list. "
            f"Check that src/post_processing/patterns.yaml is present."
        )
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return {
        p["pattern_id"] for p in data["patterns"]
        if p["bucket"] == "canonical_metric_ambiguous"
    }


# Lazy-loaded at first use inside main(). Module-level load was crashing imports
# when patterns.yaml was missing (e.g., in tests or CI that only lint this file).
METRIC_AMBIGUOUS_PATTERNS: set[str] | None = None

# ── Helpers ───────────────────────────────────────────────────────────────────
def _parse_authors(val) -> list[str]:
    """recovered_authors is stored as JSON string list in both CSVs."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    try:
        parsed = json.loads(val)
        return parsed if isinstance(parsed, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _last_author_key(authors: list[str]) -> str:
    return authors[-1].strip().lower() if authors else ""


def _year_int(val):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _norm_title(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip().lower()


def find_match(canonical_row, llm_subset, title_threshold: int, year_window: int) -> int | None:
    """Return index of matching LLM row within llm_subset, or None."""
    can_authors = _parse_authors(canonical_row.get("recovered_authors"))
    can_last    = _last_author_key(can_authors)
    can_year    = _year_int(canonical_row.get("recovered_year"))
    can_title   = _norm_title(canonical_row.get("recovered_title"))

    # Pass 1: composite (last_author, year) match — tolerant of title variance
    if can_last and can_year is not None:
        for idx, llm_row in llm_subset.iterrows():
            llm_last = _last_author_key(_parse_authors(llm_row.get("recovered_authors")))
            llm_year = _year_int(llm_row.get("recovered_year"))
            if not llm_last or llm_year is None:
                continue
            if llm_last == can_last and abs(llm_year - can_year) <= year_window:
                return idx

    # Pass 2: fuzzy title match — catches cases where authors differ slightly
    if can_title:
        best_idx, best_score = None, 0
        for idx, llm_row in llm_subset.iterrows():
            llm_title = _norm_title(llm_row.get("recovered_title"))
            if not llm_title:
                continue
            score = fuzz.ratio(can_title, llm_title)
            if score > best_score:
                best_idx, best_score = idx, score
        if best_score >= title_threshold:
            return best_idx

    return None


# ── Main merge ────────────────────────────────────────────────────────────────
def build_canonical_row_for_merged(can_row: dict, doc_meta: dict | None, flag: bool) -> dict:
    """Promote a canonical row into the merged CSV schema by pulling in doc-level
    fields from documents_deep.csv (via doc_meta) and setting the new columns."""
    row = {
        # Doc-level fields from documents_deep (fall back to None if doc not found)
        "doc_id":                     can_row.get("doc_id"),
        "source_file":                can_row.get("source_file"),
        "source_year":                (doc_meta or {}).get("source_year"),
        "source_year_path":           (doc_meta or {}).get("source_year_path"),
        "source_title":               (doc_meta or {}).get("source_title"),
        "source_type":                (doc_meta or {}).get("source_type"),
        "source_institution":         (doc_meta or {}).get("source_institution"),
        "source_topic":               (doc_meta or {}).get("source_topic"),
        "source_academic_subfield":   (doc_meta or {}).get("source_academic_subfield"),
        "doc_has_bibliography":       (doc_meta or {}).get("doc_has_bibliography"),
        "doc_page_count":             (doc_meta or {}).get("doc_page_count"),
        "doc_word_count":             (doc_meta or {}).get("doc_word_count"),
        "doc_total_chars":            (doc_meta or {}).get("doc_total_chars"),
        "doc_text_truncated":         (doc_meta or {}).get("doc_text_truncated"),
        "doc_extraction_char_ratio":  (doc_meta or {}).get("doc_extraction_char_ratio"),
        "doc_text_strategy":          (doc_meta or {}).get("doc_text_strategy"),
        "doc_ref_section_char":       (doc_meta or {}).get("doc_ref_section_char"),
        "doc_citation_count":         (doc_meta or {}).get("doc_citation_count"),
        "doi_candidates":             (doc_meta or {}).get("doi_candidates"),
        "pdf_extraction_method":      (doc_meta or {}).get("pdf_extraction_method"),
        "schema_version":             can_row.get("schema_version"),
        # Citation-level fields — from the canonical row directly
        "recovered_authors":          can_row.get("recovered_authors"),
        "recovered_title":            can_row.get("recovered_title"),
        "recovered_year":             can_row.get("recovered_year"),
        "recovered_first_version_year": None,
        "recovered_journal":          can_row.get("recovered_journal"),
        "recovered_doi":              None,
        "citation_object":            None,
        "citation_context":           can_row.get("citation_context"),
        "citation_function":          can_row.get("citation_function"),
        "citation_polarity":          can_row.get("citation_polarity"),
        "is_canonical":               True,
        "citation_location":          None,
        "is_academic":                True,
        "is_self_citation":           False,
        # Merge-layer fields
        "citation_source":            "mechanical_canonical",
        "within_doc_mention_count":   can_row.get("within_doc_mention_count"),
        "canonical_pattern_id":       can_row.get("canonical_pattern_id"),
        "likely_metric_only":         flag,
    }
    return row


def merge(llm: pd.DataFrame, canonical: pd.DataFrame, doc_meta_by_id: dict,
          title_threshold: int, year_window: int) -> pd.DataFrame:
    # Pre-populate merge-layer columns on all LLM rows
    llm = llm.copy()
    llm["citation_source"]          = "llm"
    llm["within_doc_mention_count"] = pd.NA
    llm["canonical_pattern_id"]     = pd.NA
    llm["likely_metric_only"]       = False

    # Index LLM rows by doc_id for fast lookup
    llm_by_doc = {doc_id: grp for doc_id, grp in llm.groupby("doc_id", sort=False)}

    matched_canonical_count   = 0
    unmatched_flagged_count   = 0
    unmatched_unflagged_count = 0
    unmatched_rows_to_add     = []

    for _, can_row in canonical.iterrows():
        doc_id = can_row.get("doc_id")
        llm_subset = llm_by_doc.get(doc_id)

        matched_idx = None
        if llm_subset is not None and len(llm_subset) > 0:
            matched_idx = find_match(can_row, llm_subset, title_threshold, year_window)

        if matched_idx is not None:
            # Transfer mention count + pattern id onto the matched LLM row
            llm.at[matched_idx, "within_doc_mention_count"] = can_row.get("within_doc_mention_count")
            llm.at[matched_idx, "canonical_pattern_id"]     = can_row.get("canonical_pattern_id")
            matched_canonical_count += 1
        else:
            # Keep canonical as a new row in the merged output
            pattern_id = can_row.get("canonical_pattern_id")
            flag = pattern_id in METRIC_AMBIGUOUS_PATTERNS
            doc_meta = doc_meta_by_id.get(doc_id)
            unmatched_rows_to_add.append(
                build_canonical_row_for_merged(can_row.to_dict(), doc_meta, flag)
            )
            if flag:
                unmatched_flagged_count += 1
            else:
                unmatched_unflagged_count += 1

    merged = pd.concat([llm, pd.DataFrame(unmatched_rows_to_add)], ignore_index=True)

    print(f"\nMerge summary:")
    print(f"  LLM rows in                     : {len(llm)}")
    print(f"  Canonical rows in               : {len(canonical)}")
    print(f"  Canonical matched → merged in   : {matched_canonical_count}")
    print(f"  Canonical unmatched, kept (flag): {unmatched_flagged_count}")
    print(f"  Canonical unmatched, kept (ok)  : {unmatched_unflagged_count}")
    print(f"  Merged CSV total rows           : {len(merged)}")

    return merged


def main():
    global METRIC_AMBIGUOUS_PATTERNS
    parser = argparse.ArgumentParser(description="Merge LLM + canonical citation sources")
    parser.add_argument("--title-threshold", type=int, default=DEFAULT_TITLE_THRESHOLD,
                        help=f"Fuzzy title match threshold 0-100 (default {DEFAULT_TITLE_THRESHOLD})")
    parser.add_argument("--year-window", type=int, default=DEFAULT_YEAR_WINDOW,
                        help=f"Allowed year drift on composite match (default ±{DEFAULT_YEAR_WINDOW})")
    args = parser.parse_args()

    # Load metric-ambiguous pattern IDs now (not at import) so a missing
    # patterns.yaml surfaces as a clean runtime error, not an import crash.
    METRIC_AMBIGUOUS_PATTERNS = load_metric_ambiguous_patterns()
    print(f"Loaded {len(METRIC_AMBIGUOUS_PATTERNS)} metric-ambiguous pattern IDs from patterns.yaml")

    for path, name in [(LLM_PATH, "citations_deep.csv"),
                       (CAN_PATH, "canonical_citations.csv"),
                       (DOC_PATH, "documents_deep.csv")]:
        if not path.exists():
            raise FileNotFoundError(f"{path} not found. ({name} must exist.)")

    print(f"Reading {LLM_PATH.name}...")
    llm = pd.read_csv(LLM_PATH, low_memory=False)
    print(f"  {len(llm)} LLM citation rows across {llm['doc_id'].nunique()} docs")

    print(f"Reading {CAN_PATH.name}...")
    canonical = pd.read_csv(CAN_PATH, low_memory=False)
    print(f"  {len(canonical)} canonical rows across {canonical['doc_id'].nunique()} docs")

    print(f"Reading {DOC_PATH.name}...")
    documents = pd.read_csv(DOC_PATH, low_memory=False)
    doc_meta_by_id = documents.set_index("doc_id").to_dict(orient="index")
    print(f"  {len(documents)} document rows")

    merged = merge(llm, canonical, doc_meta_by_id,
                   args.title_threshold, args.year_window)

    merged.to_csv(OUT_PATH, index=False)
    print(f"\nWrote {OUT_PATH}")


if __name__ == "__main__":
    main()