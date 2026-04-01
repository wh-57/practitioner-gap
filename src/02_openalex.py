# %% Imports & Setup
import requests
import pandas as pd
import time
import os
import json
from pathlib import Path

os.chdir(r"C:\Users\willi\Desktop\gap\src")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
OPENALEX_EMAIL   = "your_email@pitt.edu"          # <-- fill in
OPENALEX_API_KEY = open("openalex_key.txt").read().strip()

START_YEAR = 2000
END_YEAR   = 2024
PER_PAGE   = 200

# ── Core finance journals (all papers included) ────────────────────────────────
# These define both the academic benchmark AND the finance-author seed set
FINANCE_JOURNALS = {
    "Journal of Finance":                           "issn:0022-1082",
    "Journal of Financial Economics":               "issn:0304-405X",
    "Review of Financial Studies":                  "issn:0893-9454",
    "Journal of Financial and Quantitative Analysis": "issn:0022-1090",
    "Review of Finance":                            "issn:1572-3097",
    "Review of Asset Pricing Studies":              "issn:2045-9920",
    "Review of Corporate Finance Studies":          "issn:2046-9128",
}

# Management Science — filtered by finance-author network (see below)
MGMT_SCIENCE_ISSN = "issn:0025-1909"


# ── Core fetch function ────────────────────────────────────────────────────────
def fetch_journal_papers(
    journal_name: str,
    issn_filter: str,
    start_year: int,
    end_year: int,
    email: str,
    api_key: str,
) -> list[dict]:
    """
    Pull all papers from a journal between start_year and end_year.
    Stores OpenAlex author IDs for use in the MS finance-author filter.
    Returns list of flat dicts.
    """
    base_url = "https://api.openalex.org/works"
    headers  = {
        "User-Agent":    f"mailto:{email}",
        "Authorization": f"Bearer {api_key}",
    }
    params = {
        "filter": (
            f"primary_location.source.{issn_filter},"
            f"publication_year:{start_year}-{end_year},"
            "type:article"
        ),
        "select": (
            "id,doi,title,publication_year,primary_location,"
            "authorships,cited_by_count,concepts,open_access"
        ),
        "per-page": PER_PAGE,
        "cursor":   "*",
    }

    all_results = []
    total       = None

    while True:
        resp = requests.get(base_url, params=params, headers=headers, timeout=30)
        if resp.status_code == 429:
            print(f"  Rate limited — waiting 60s...")
            time.sleep(60)
            continue
        resp.raise_for_status()

        data    = resp.json()
        results = data.get("results", [])
        meta    = data.get("meta", {})

        if total is None:
            total = meta.get("count", 0)
            print(f"  {journal_name}: {total} papers ({start_year}–{end_year})")

        all_results.extend(results)

        cursor = meta.get("next_cursor")
        if not cursor or not results:
            break
        params["cursor"] = cursor
        time.sleep(0.15)

    print(f"  Retrieved {len(all_results)} papers")
    return [flatten_work(w, journal_name) for w in all_results]


def flatten_work(work: dict, journal_name: str) -> dict:
    """Flatten an OpenAlex work object to a dict row."""
    authorships = work.get("authorships", [])

    # Full author names
    author_names = [
        a.get("author", {}).get("display_name", "")
        for a in authorships
    ]
    # Last names only
    author_lastnames = [
        name.split()[-1] if name else ""
        for name in author_names
    ]
    # OpenAlex author IDs — critical for MS filter
    author_ids = [
        a.get("author", {}).get("id", "").replace("https://openalex.org/", "")
        for a in authorships
        if a.get("author", {}).get("id")
    ]

    # Concepts
    concepts_sorted = sorted(
        work.get("concepts", []),
        key=lambda c: c.get("score", 0),
        reverse=True
    )
    top_concept   = concepts_sorted[0].get("display_name", "") if concepts_sorted else ""
    all_concepts  = [c.get("display_name", "") for c in concepts_sorted]

    return {
        "openalex_id":      work.get("id", "").replace("https://openalex.org/", ""),
        "doi":              work.get("doi", ""),
        "title":            work.get("title", ""),
        "year":             work.get("publication_year"),
        "journal":          journal_name,
        "cited_by_count":   work.get("cited_by_count", 0),
        "top_concept":      top_concept,
        "all_concepts":     json.dumps(all_concepts),
        "author_names":     json.dumps(author_names),
        "author_lastnames": json.dumps(author_lastnames),
        "author_ids":       json.dumps(author_ids),   # stored for MS filter
        "is_oa":            work.get("open_access", {}).get("is_oa", False),
    }


# ── Build finance-author seed set from core journals ──────────────────────────
def build_finance_author_set(df_core: pd.DataFrame) -> set:
    """
    Build the set of OpenAlex author IDs who have published
    in the core finance journals. This is the community membership filter.

    An author is 'finance-community' if they appear in the core journals
    at least once. Threshold can be raised to >=2 for stricter definition.
    """
    finance_authors = set()
    for author_ids_json in df_core["author_ids"].dropna():
        try:
            ids = json.loads(author_ids_json)
            finance_authors.update(ids)
        except (json.JSONDecodeError, TypeError):
            continue

    print(f"  Finance-author seed set: {len(finance_authors):,} unique author IDs")
    return finance_authors


# ── Fetch and filter Management Science ───────────────────────────────────────
def fetch_management_science(
    issn_filter: str,
    start_year: int,
    end_year: int,
    email: str,
    api_key: str,
    finance_authors: set,
    min_finance_authors: int = 1,
) -> list[dict]:
    """
    Pull all Management Science papers then keep only those with
    at least min_finance_authors authors in the finance-author seed set.

    min_finance_authors=1 is the broad definition (any finance-community author).
    min_finance_authors=2 is a stricter definition for robustness checks.
    """
    print(f"\nFetching: Management Science (all papers first)")
    all_papers = fetch_journal_papers(
        "Management Science", issn_filter,
        start_year, end_year, email, api_key
    )
    print(f"  Total MS papers pulled: {len(all_papers)}")

    # Filter by finance-author network
    finance_ms = []
    for paper in all_papers:
        try:
            author_ids = set(json.loads(paper.get("author_ids", "[]")))
        except (json.JSONDecodeError, TypeError):
            author_ids = set()

        n_finance = len(author_ids & finance_authors)

        if n_finance >= min_finance_authors:
            # Store how many finance-community authors the paper has
            paper["n_finance_community_authors"] = n_finance
            finance_ms.append(paper)

    pct = len(finance_ms) / max(len(all_papers), 1) * 100
    print(f"  After finance-author filter (>={min_finance_authors}): "
          f"{len(finance_ms)} papers ({pct:.1f}%)")

    # Also produce strict count for reporting
    strict = [p for p in finance_ms if p["n_finance_community_authors"] >= 2]
    print(f"  Strict filter (>=2 finance authors): {len(strict)} papers "
          f"({len(strict)/max(len(all_papers),1)*100:.1f}%)")

    # Tag papers with filter info
    for p in finance_ms:
        p["journal"] = "Management Science"

    return finance_ms


# ── Main ──────────────────────────────────────────────────────────────────────
def build_benchmark(
    finance_journals: dict,
    mgmt_science_issn: str,
    start_year: int,
    end_year: int,
    email: str,
    api_key: str,
    out_path: Path,
) -> pd.DataFrame:
    """
    Full pipeline:
      1. Pull core finance journal papers (defines benchmark + author seed set)
      2. Build finance-author set from those papers
      3. Pull MS, filter by author network, add to benchmark
    Supports resume — skips journals already in output file.
    MS is always re-filtered if core journals change, so it's not resumed.
    """
    # ── Resume: load already-done core journals ────────────────────────────
    done_journals = set()
    all_rows      = []

    if out_path.exists():
        existing      = pd.read_csv(out_path)
        # Only resume non-MS journals — MS is re-filtered from scratch
        core_existing = existing[existing["journal"] != "Management Science"]
        done_journals = set(core_existing["journal"].unique())
        all_rows      = core_existing.to_dict("records")
        print(f"Resuming — core journals already done: {done_journals}\n")

    # ── Step 1: Pull core finance journals ────────────────────────────────
    for journal_name, issn_filter in finance_journals.items():
        if journal_name in done_journals:
            print(f"Skipping {journal_name} — already done")
            continue

        print(f"\nFetching: {journal_name}")
        try:
            papers = fetch_journal_papers(
                journal_name, issn_filter,
                start_year, end_year, email, api_key
            )
            all_rows.extend(papers)
            pd.DataFrame(all_rows).to_csv(out_path, index=False)
            print(f"  Saved — running total: {len(all_rows)} papers")
        except Exception as e:
            print(f"  ERROR: {e}")
        time.sleep(1)

    # ── Step 2: Build finance-author seed set ─────────────────────────────
    print(f"\nBuilding finance-author seed set from core journals...")
    df_core        = pd.DataFrame(all_rows)
    finance_authors = build_finance_author_set(df_core)

    # ── Step 3: Fetch and filter Management Science ────────────────────────
    print(f"\nFetching Management Science with finance-author filter...")
    try:
        ms_papers = fetch_management_science(
            mgmt_science_issn,
            start_year, end_year,
            email, api_key,
            finance_authors,
            min_finance_authors=1,
        )
        all_rows.extend(ms_papers)
        pd.DataFrame(all_rows).to_csv(out_path, index=False)
        print(f"  Saved — final total: {len(all_rows)} papers")
    except Exception as e:
        print(f"  ERROR on Management Science: {e}")

    df = pd.DataFrame(all_rows)

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"Total papers in benchmark : {len(df)}")
    print(f"\nPapers per journal:")
    print(df["journal"].value_counts().to_string())
    print(f"\nPapers per year (last 10):")
    print(df.groupby("year").size().sort_index().tail(10).to_string())
    print(f"\nTop 15 most cited papers:")
    top = df.nlargest(15, "cited_by_count")[
        ["title", "journal", "year", "cited_by_count"]
    ]
    for _, row in top.iterrows():
        print(f"  [{int(row['year'])}] {str(row['title'])[:60]:<60} "
              f"| {str(row['journal'])[:6]} | {int(row['cited_by_count'])} cites")

    return df


# ── RUN ───────────────────────────────────────────────────────────────────────
# Notes:
#   - Core finance journals resume automatically if already pulled
#   - Management Science is always re-pulled and re-filtered from scratch
#     (ensures the author filter uses the latest seed set)
#   - Expected output: ~11,000-13,000 papers total
#     (~9,650 core + ~1,400-1,800 MS finance papers)
#   - n_finance_community_authors column lets you run robustness checks:
#     df[df.journal=="Management Science"][df.n_finance_community_authors >= 2]

out_path = DATA_DIR / "openalex_benchmark.csv"

df = build_benchmark(
    finance_journals   = FINANCE_JOURNALS,
    mgmt_science_issn  = MGMT_SCIENCE_ISSN,
    start_year         = START_YEAR,
    end_year           = END_YEAR,
    email              = OPENALEX_EMAIL,
    api_key            = OPENALEX_API_KEY,
    out_path           = out_path,
)