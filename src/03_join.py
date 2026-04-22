# %% Imports & Setup
# Requires: pip install rapidfuzz pandas numpy
import argparse
import hashlib
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

# ── Paths (anchored to script location: gap/src/03_join.py) ────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent   # gap/
OUT_DIR   = REPO_ROOT / "output"                     # gap/output/

CITATIONS_PATH = OUT_DIR / "citations_merged.csv"
BENCHMARK_PATH = OUT_DIR / "openalex_benchmark.csv"
OUTPUT_PATH    = OUT_DIR / "citations_joined.csv"

# ── Config ─────────────────────────────────────────────────────────────────────
# Composite key (title + first author lastname): lower threshold ok since key is richer
COMPOSITE_THRESHOLD  = 88
# Title-only fallback (no author): stricter to avoid false positives
TITLE_ONLY_THRESHOLD = 93
# Year constraint: |recovered_year - bench_year| <= YEAR_WINDOW
YEAR_WINDOW          = 2
# Rapidfuzz candidate depth — walk top-K to find first that passes year constraint
TOP_K_CANDIDATES     = 5


# ── Text normalisation ─────────────────────────────────────────────────────────
def normalise(text) -> str:
    """Lowercase, strip leading articles, drop punctuation (except hyphens), collapse whitespace."""
    if not isinstance(text, str) or not text.strip():
        return ""
    t = text.lower().strip()
    t = re.sub(r"^(the|a|an)\s+", "", t)
    t = re.sub(r"[^\w\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalise_doi(doi) -> str:
    """Strip URL prefix and lowercase."""
    if not isinstance(doi, str) or not doi.strip():
        return ""
    d = doi.lower().strip()
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", d)
    return d


def parse_doi_list(val) -> set[str]:
    """Parse the JSON-encoded doi_candidates field into a set of normalised DOIs."""
    if not isinstance(val, str) or not val.strip():
        return set()
    try:
        items = json.loads(val)
        if not isinstance(items, list):
            return set()
        return {normalise_doi(d) for d in items if isinstance(d, str)}
    except (json.JSONDecodeError, TypeError):
        return set()


def extract_first_author(authors_field) -> str:
    """
    Extract first author last name from a recovered_authors field.
    Handles JSON list, plain string, or comma-separated string.
    """
    if authors_field is None or (isinstance(authors_field, float) and np.isnan(authors_field)):
        return ""
    if isinstance(authors_field, str):
        try:
            parsed = json.loads(authors_field)
            if isinstance(parsed, list) and parsed:
                return normalise(str(parsed[0]))
        except (json.JSONDecodeError, TypeError):
            pass
        first = re.split(r"[,;]", authors_field)[0].strip()
        return normalise(first)
    if isinstance(authors_field, list) and authors_field:
        return normalise(str(authors_field[0]))
    return ""


def make_composite_key(title, authors_field) -> str:
    """title + first author lastname, normalised. Falls back to title-only."""
    t = normalise(title)
    a = extract_first_author(authors_field)
    if t and a:
        return f"{t} {a}"
    return t


def stable_cluster_hash(text: str) -> str:
    """Deterministic 8-char hex for unmatched-citation cluster IDs."""
    return hashlib.sha256(text.encode()).hexdigest()[:8]


# ── Year constraint ───────────────────────────────────────────────────────────
def year_ok(cit_year, bench_year, window: int = YEAR_WINDOW) -> bool:
    """True iff both years known and |diff| <= window."""
    if pd.isna(cit_year) or pd.isna(bench_year):
        return False
    try:
        return abs(int(cit_year) - int(bench_year)) <= window
    except (ValueError, TypeError):
        return False


# ── Benchmark key construction ────────────────────────────────────────────────
def build_benchmark_keys(bench: pd.DataFrame) -> pd.DataFrame:
    """Build title-only, composite, and normalised-DOI keys for the benchmark."""
    bench = bench.copy()
    bench["key_title"] = bench["title"].apply(normalise)

    def bench_composite(row):
        first = ""
        try:
            lastnames = json.loads(row["author_lastnames"])
            if lastnames:
                first = normalise(str(lastnames[0]))
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
        t = normalise(row["title"])
        return f"{t} {first}" if (t and first) else t

    print("  Building benchmark composite keys...")
    bench["key_composite"] = bench.apply(bench_composite, axis=1)
    bench["doi_norm"]      = bench["doi"].apply(normalise_doi)
    return bench


# ── Load ──────────────────────────────────────────────────────────────────────
def load_data():
    print("Loading citations...")
    if not CITATIONS_PATH.exists():
        raise FileNotFoundError(
            f"{CITATIONS_PATH} not found. "
            "Run src/01_extract_deep.py → src/canonical_scraper.py → src/01b_merge.py first."
        )
    cit = pd.read_csv(CITATIONS_PATH, low_memory=False)
    n_docs = cit["doc_id"].nunique() if "doc_id" in cit.columns else "?"
    print(f"  {len(cit)} practitioner citations across {n_docs} documents")

    print("Loading benchmark...")
    if not BENCHMARK_PATH.exists():
        raise FileNotFoundError(
            f"{BENCHMARK_PATH} not found. Run src/02_openalex.py first."
        )
    bench = pd.read_csv(BENCHMARK_PATH, low_memory=False)
    print(f"  {len(bench)} academic benchmark papers")

    # Split on academic flag; low-confidence academic rows are excluded from
    # join attempts but kept in output for audit.
    has_conf = "confidence" in cit.columns
    academic_mask = cit["is_academic"] == True

    if has_conf:
        hi_mask = academic_mask & (cit["confidence"] != "low")
        lo_mask = academic_mask & (cit["confidence"] == "low")
    else:
        hi_mask = academic_mask
        lo_mask = pd.Series(False, index=cit.index)

    cit_academic = cit[hi_mask].copy()
    cit_low      = cit[lo_mask].copy()
    cit_other    = cit[~academic_mask].copy()

    print(f"  Academic (high/med confidence) — joining : {len(cit_academic)}")
    print(f"  Academic (low confidence)      — skipped : {len(cit_low)}")
    print(f"  Non-academic                   — skipped : {len(cit_other)}")

    return cit, cit_academic, cit_low, cit_other, bench


# ── Document-level DOI lookup (confirmation signal) ──────────────────────────
def build_doc_doi_lookup(cit: pd.DataFrame) -> dict[str, set[str]]:
    """
    Map doc_id -> set of DOIs regex-extracted from the source document body.
    Used as a non-binding cross-check on fuzzy matches (doi_confirmed flag).
    """
    if "doi_candidates" not in cit.columns or "doc_id" not in cit.columns:
        return {}
    doc_doi: dict[str, set[str]] = {}
    # doi_candidates is duplicated across every row of a given doc — take first.
    for doc_id, group in cit.groupby("doc_id"):
        first_val = group["doi_candidates"].iloc[0]
        doc_doi[doc_id] = parse_doi_list(first_val)
    return doc_doi


# ── Fuzzy match with year-constrained candidate walk ─────────────────────────
def fuzzy_match(
    unmatched:       pd.DataFrame,
    bench:           pd.DataFrame,
    doc_doi_lookup:  dict[str, set[str]],
    comp_threshold:  int = COMPOSITE_THRESHOLD,
    title_threshold: int = TITLE_ONLY_THRESHOLD,
    year_window:     int = YEAR_WINDOW,
    top_k:           int = TOP_K_CANDIDATES,
):
    """
    Two-stage fuzzy match, walking top-K candidates to find first that satisfies
    the year constraint |recovered_year - bench_year| <= year_window.
      Stage A: composite key (title + first author)  threshold=comp_threshold
      Stage B: title-only fallback                    threshold=title_threshold
    Matched rows get doi_confirmed=True when the benchmark paper's DOI appears
    in the source document's regex-extracted doi_candidates.
    """
    comp_keys   = bench["key_composite"].tolist()
    title_keys  = bench["key_title"].tolist()
    bench_years = bench["year"].tolist()
    bench_dois  = bench["doi_norm"].tolist()

    matched_rows:  list[dict] = []
    unmatched_idx: list = []

    def pick_from_candidates(candidates, cit_year, doi_set):
        """Return (pos, score, doi_confirmed) of first candidate passing year gate, else None."""
        for _, score, pos in candidates:
            if year_ok(cit_year, bench_years[pos], year_window):
                doi_conf = bench_dois[pos] in doi_set if (doi_set and bench_dois[pos]) else False
                return pos, score, doi_conf
        return None

    for idx, row in unmatched.iterrows():
        title    = row.get("recovered_title", "")
        authors  = row.get("recovered_authors", None)
        cit_year = row.get("recovered_year", None)
        doc_id   = row.get("doc_id", None)
        doi_set  = doc_doi_lookup.get(doc_id, set())

        query_composite = make_composite_key(title, authors)
        query_title     = normalise(title)

        if not query_title:
            unmatched_idx.append(idx)
            continue

        has_author = bool(extract_first_author(authors))
        matched_this_row = False

        # Stage A: composite key
        if has_author and query_composite:
            cands = process.extract(
                query_composite, comp_keys,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=comp_threshold,
                limit=top_k,
            )
            pick = pick_from_candidates(cands, cit_year, doi_set)
            if pick is not None:
                pos, score, doi_conf = pick
                bench_row = bench.iloc[pos]
                matched_rows.append({
                    **row.to_dict(),
                    **_bench_fields(bench_row),
                    "match_method":  "fuzzy_composite",
                    "match_score":   float(score),
                    "doi_confirmed": bool(doi_conf),
                })
                matched_this_row = True

        # Stage B: title-only fallback (attempted whenever Stage A missed)
        if not matched_this_row:
            cands = process.extract(
                query_title, title_keys,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=title_threshold,
                limit=top_k,
            )
            pick = pick_from_candidates(cands, cit_year, doi_set)
            if pick is not None:
                pos, score, doi_conf = pick
                bench_row = bench.iloc[pos]
                matched_rows.append({
                    **row.to_dict(),
                    **_bench_fields(bench_row),
                    "match_method":  "fuzzy_title_only",
                    "match_score":   float(score),
                    "doi_confirmed": bool(doi_conf),
                })
            else:
                unmatched_idx.append(idx)

    matched_df   = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
    unmatched_df = unmatched.loc[unmatched_idx].copy()

    if not matched_df.empty:
        comp_n  = int((matched_df["match_method"] == "fuzzy_composite").sum())
        title_n = int((matched_df["match_method"] == "fuzzy_title_only").sum())
        conf_n  = int(matched_df["doi_confirmed"].sum())
        print("\nFuzzy match results:")
        print(f"  Composite        : {comp_n}")
        print(f"  Title-only       : {title_n}")
        print(f"  DOI-confirmed    : {conf_n}  (cross-check, subset of matched)")
        print(f"  Unmatched        : {len(unmatched_df)}")
        print(f"  Avg match score  : {matched_df['match_score'].mean():.1f}")
        print(f"  Min match score  : {matched_df['match_score'].min():.1f}")
    else:
        print("\nFuzzy match results: 0 matches.")

    return matched_df, unmatched_df


# ── Benchmark fields attached to matched rows ─────────────────────────────────
def _bench_fields(bench_row) -> dict:
    return {
        "benchmark_openalex_id": bench_row.get("openalex_id", ""),
        "bench_doi":             bench_row.get("doi", ""),
        "bench_title":           bench_row.get("title", ""),
        "bench_year":            bench_row.get("year", None),
        "bench_journal":         bench_row.get("journal", ""),
        "bench_cited_by":        bench_row.get("cited_by_count", None),
    }


# ── paper_cluster_id ──────────────────────────────────────────────────────────
def assign_cluster_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stable ID for paper-level aggregation across the corpus:
      - matched rows:            cluster = benchmark_openalex_id
      - unmatched w/ usable key: cluster = 'unmatched_' + hash8(composite_key)
      - unmatched w/o title:     cluster = NaN
      - not_academic / low:      cluster = NaN

    NOTE: The unmatched clustering uses EXACT-match on the normalised composite
    key. Spelling variants will land in different clusters. Proper fuzzy
    cross-document clustering is left for post_processing/dedup.py.
    """
    df = df.copy()
    df["paper_cluster_id"] = np.nan

    matched_mask = df["match_method"].isin(["fuzzy_composite", "fuzzy_title_only"])
    df.loc[matched_mask, "paper_cluster_id"] = df.loc[matched_mask, "benchmark_openalex_id"]

    unmatched_mask = df["match_method"] == "unmatched"
    for idx, row in df[unmatched_mask].iterrows():
        key = make_composite_key(row.get("recovered_title", ""),
                                 row.get("recovered_authors", None))
        if key:
            df.at[idx, "paper_cluster_id"] = "unmatched_" + stable_cluster_hash(key)

    return df


# ── Combine + save ────────────────────────────────────────────────────────────
def combine_and_save(
    matched_fuzzy: pd.DataFrame,
    unmatched:     pd.DataFrame,
    cit_low:       pd.DataFrame,
    cit_other:     pd.DataFrame,
    output_path:   Path,
) -> pd.DataFrame:
    parts = []
    bench_cols = ["benchmark_openalex_id", "bench_doi", "bench_title",
                  "bench_year", "bench_journal", "bench_cited_by"]

    if not matched_fuzzy.empty:
        parts.append(matched_fuzzy)

    if not unmatched.empty:
        u = unmatched.copy()
        u["match_method"]  = "unmatched"
        u["match_score"]   = np.nan
        u["doi_confirmed"] = False
        for col in bench_cols:
            u[col] = np.nan
        parts.append(u)

    if not cit_low.empty:
        l = cit_low.copy()
        l["match_method"]  = "low_confidence"
        l["match_score"]   = np.nan
        l["doi_confirmed"] = False
        for col in bench_cols:
            l[col] = np.nan
        parts.append(l)

    if not cit_other.empty:
        o = cit_other.copy()
        o["match_method"]  = "not_academic"
        o["match_score"]   = np.nan
        o["doi_confirmed"] = False
        for col in bench_cols:
            o[col] = np.nan
        parts.append(o)

    df_out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # in_benchmark — clean boolean for downstream filters
    df_out["in_benchmark"] = df_out["match_method"].isin(
        ["fuzzy_composite", "fuzzy_title_only"]
    )

    # paper_cluster_id
    df_out = assign_cluster_ids(df_out)

    # Drop intermediate columns if they leaked through
    for col in ["doi_norm", "key_composite", "key_title"]:
        if col in df_out.columns:
            df_out.drop(columns=[col], inplace=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False)
    print(f"\nSaved to {output_path}")
    return df_out


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(df: pd.DataFrame):
    if df.empty:
        print("\n(No data.)")
        return

    academic       = df[df["is_academic"] == True]
    total_academic = len(academic)
    matched        = academic[academic["in_benchmark"] == True]
    unmatched      = academic[academic["match_method"] == "unmatched"]
    low_conf       = academic[academic["match_method"] == "low_confidence"]
    # Join rate denominator excludes low-confidence (they never entered the join attempt)
    eligible       = total_academic - len(low_conf)
    join_rate      = len(matched) / max(eligible, 1) * 100

    print("\n" + "=" * 55)
    print("JOIN SUMMARY")
    print("=" * 55)
    print(f"Total citation rows       : {len(df)}")
    print(f"Academic citations        : {total_academic}")
    print(f"  Matched (composite)     : {(academic['match_method']=='fuzzy_composite').sum()}")
    print(f"  Matched (title only)    : {(academic['match_method']=='fuzzy_title_only').sum()}")
    print(f"    of which DOI-confirmed: {int(matched['doi_confirmed'].sum())}")
    print(f"  Unmatched               : {len(unmatched)}")
    print(f"  Low-confidence skipped  : {len(low_conf)}")
    print(f"  Join rate (excl. low)   : {join_rate:.1f}%")

    if not matched.empty and "bench_journal" in matched.columns:
        print("\nMatched citations by benchmark journal:")
        print(matched["bench_journal"].value_counts().to_string())

    if not matched.empty and "match_score" in matched.columns:
        print("\nMatch-score distribution (matched rows):")
        print(matched["match_score"].describe().round(1).to_string())

    print("\nUnmatched — top 15 to spot-check:")
    sample = unmatched[unmatched["recovered_title"].notna()][
        ["recovered_title", "recovered_year", "recovered_journal", "recovered_authors"]
    ].head(15)
    for _, row in sample.iterrows():
        authors = str(row["recovered_authors"])[:30] if row["recovered_authors"] is not None else "?"
        year = row["recovered_year"] if pd.notna(row["recovered_year"]) else "?"
        journal = str(row["recovered_journal"])[:20] if row["recovered_journal"] is not None else "?"
        print(f"  [{year}] {str(row['recovered_title'])[:50]:<50}  — {authors}  [{journal}]")

    if not matched.empty and "bench_cited_by" in matched.columns:
        print("\nTop 15 matched papers by bench_cited_by:")
        top = matched.nlargest(15, "bench_cited_by")[
            ["recovered_title", "bench_journal", "bench_year", "bench_cited_by",
             "match_method", "match_score"]
        ]
        for _, row in top.iterrows():
            year  = int(row["bench_year"]) if pd.notna(row["bench_year"]) else "?"
            cites = int(row["bench_cited_by"]) if pd.notna(row["bench_cited_by"]) else "?"
            print(f"  [{year}] {str(row['recovered_title'])[:45]:<45} "
                  f"| {str(row['bench_journal'])[:6]:<6} | {cites:>5} cites "
                  f"[{row['match_method']} {row['match_score']:.0f}]")

    # Cluster coverage
    if "paper_cluster_id" in df.columns:
        n_matched_clusters   = df[df["in_benchmark"]]["paper_cluster_id"].nunique()
        n_unmatched_clusters = df[df["match_method"] == "unmatched"]["paper_cluster_id"].nunique()
        print(f"\nUnique paper clusters:")
        print(f"  Matched (= unique benchmark papers cited)   : {n_matched_clusters}")
        print(f"  Unmatched (exact-key cross-doc aggregation) : {n_unmatched_clusters}")
        print(f"  Note: unmatched clustering is exact-key only;")
        print(f"        dedup.py will refine with fuzzy clustering.")


# ──────────────────────────────────────────────────────────────────────────────
# Match strategy
# ──────────────────────────────────────────────────────────────────────────────
# Stage A: composite fuzzy key (normalise(title) + first_author_lastname)
#   threshold=88, year constraint |cit_year - bench_year| <= 2
# Stage B: title-only fallback
#   threshold=93 (stricter since key is weaker)
#
# Candidate walk: each stage retrieves top-5 rapidfuzz hits above threshold,
# then selects the first that passes the year constraint. Taking only the
# top-1 blindly can lock in a false positive when the correct match is rank 2.
#
# DOI confirmation signal: after a match, check whether the benchmark paper's
# DOI appears in the source document's regex-extracted doi_candidates list.
# This is NOT a match gate — it's a robustness filter for sensitivity analysis.
# Per-citation DOIs are not populated by the LLM; doi_candidates are extracted
# at the document level by 01_extract_deep.py.
#
# Output columns added per row:
#   benchmark_openalex_id, bench_doi, bench_title, bench_year, bench_journal,
#   bench_cited_by, match_method, match_score, doi_confirmed, in_benchmark,
#   paper_cluster_id
#
# match_method values:
#   fuzzy_composite   — composite match passed threshold + year gate
#   fuzzy_title_only  — title-only fallback passed threshold + year gate
#   unmatched         — academic citation, no benchmark match found
#   low_confidence    — academic but confidence=low, skipped from join attempt
#   not_academic      — non-academic citation, not attempted
# ──────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="GAP citation → benchmark fuzzy join")
    parser.add_argument(
        "--composite-threshold", type=int, default=COMPOSITE_THRESHOLD,
        metavar="T", help=f"threshold for composite-key match (default {COMPOSITE_THRESHOLD})",
    )
    parser.add_argument(
        "--title-threshold", type=int, default=TITLE_ONLY_THRESHOLD,
        metavar="T", help=f"threshold for title-only fallback (default {TITLE_ONLY_THRESHOLD})",
    )
    parser.add_argument(
        "--year-window", type=int, default=YEAR_WINDOW,
        metavar="W", help=f"allowed |cit_year - bench_year| (default {YEAR_WINDOW})",
    )
    args = parser.parse_args()

    cit, cit_academic, cit_low, cit_other, bench = load_data()

    print("\nBuilding benchmark keys...")
    bench = build_benchmark_keys(bench)

    print("\nBuilding per-document DOI lookup...")
    doc_doi = build_doc_doi_lookup(cit)
    n_docs_with_dois = sum(1 for v in doc_doi.values() if v)
    print(f"  Documents with regex-extracted DOIs: {n_docs_with_dois} / {len(doc_doi)}")

    matched_fuzzy, unmatched_final = fuzzy_match(
        cit_academic, bench, doc_doi,
        comp_threshold=args.composite_threshold,
        title_threshold=args.title_threshold,
        year_window=args.year_window,
    )

    df_out = combine_and_save(
        matched_fuzzy, unmatched_final, cit_low, cit_other, OUTPUT_PATH
    )

    print_summary(df_out)


if __name__ == "__main__":
    main()