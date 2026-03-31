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

# ── Config ────────────────────────────────────────────────────────────────────
# Get a free API key at https://openalex.org/
# Without a key you get 100 requests/day. With a free key: 100,000/day.
# Add your email to get polite-pool access (recommended even without key).
OPENALEX_EMAIL = "wh2@andrew.cmu.edu"   # <-- fill in

# Target journals with their OpenAlex source IDs
# These are the ISSNs — OpenAlex resolves them automatically
JOURNALS = {
    "Journal of Finance":                      "issn:0022-1082",
    "Journal of Financial Economics":          "issn:0304-405X",
    "Review of Financial Studies":             "issn:0893-9454",
    "Journal of Financial and Quantitative Analysis": "issn:0022-1090",
    "Review of Finance":                       "issn:1572-3097",
}

START_YEAR = 2000
END_YEAR   = 2024
PER_PAGE   = 200   # OpenAlex max per page


# ── Core fetch function ───────────────────────────────────────────────────────
def fetch_journal_papers(
    journal_name: str,
    issn_filter: str,
    start_year: int,
    end_year: int,
    email: str,
) -> list[dict]:
    """
    Pull all papers from a journal between start_year and end_year
    using the OpenAlex API. Returns list of flat dicts.
    """
    base_url = "https://api.openalex.org/works"
    headers  = {"User-Agent": f"mailto:{email}"}

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

    papers  = []
    page    = 0
    total   = None

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
            print(f"  {journal_name}: {total} papers found "
                  f"({start_year}–{end_year})")

        for work in results:
            # Flatten authorships to last-name list
            authors = [
                a.get("author", {}).get("display_name", "")
                for a in work.get("authorships", [])
            ]
            author_lastnames = [
                name.split()[-1] if name else ""
                for name in authors
            ]

            # Primary concept (highest-scoring)
            concepts = work.get("concepts", [])
            top_concept = (
                concepts[0].get("display_name", "") if concepts else ""
            )

            papers.append({
                "openalex_id":      work.get("id", "").replace("https://openalex.org/", ""),
                "doi":              work.get("doi", ""),
                "title":            work.get("title", ""),
                "year":             work.get("publication_year"),
                "journal":          journal_name,
                "cited_by_count":   work.get("cited_by_count", 0),
                "top_concept":      top_concept,
                "author_lastnames": json.dumps(author_lastnames),
                "is_oa":            work.get("open_access", {}).get("is_oa", False),
            })

        page += 1
        cursor = meta.get("next_cursor")
        if not cursor or not results:
            break

        params["cursor"] = cursor
        time.sleep(0.2)   # be polite

    print(f"  Retrieved {len(papers)} papers")
    return papers


# ── Main ─────────────────────────────────────────────────────────────────────
def build_benchmark(
    journals: dict,
    start_year: int,
    end_year: int,
    email: str,
    out_path: Path,
) -> pd.DataFrame:
    """
    Pull papers from all target journals and save to CSV.
    Supports resume — skips journals already in the output file.
    """
    # Resume support
    done_journals = set()
    if out_path.exists():
        existing = pd.read_csv(out_path)
        done_journals = set(existing["journal"].unique())
        all_rows = existing.to_dict("records")
        print(f"Resuming — already have: {done_journals}\n")
    else:
        all_rows = []

    for journal_name, issn_filter in journals.items():
        if journal_name in done_journals:
            print(f"Skipping {journal_name} — already done")
            continue

        print(f"\nFetching: {journal_name}")
        try:
            papers = fetch_journal_papers(
                journal_name, issn_filter, start_year, end_year, email
            )
            all_rows.extend(papers)

            # Save after each journal
            pd.DataFrame(all_rows).to_csv(out_path, index=False)
            print(f"  Saved to {out_path}")

        except Exception as e:
            print(f"  ERROR on {journal_name}: {e}")
            continue

        time.sleep(1)

    df = pd.DataFrame(all_rows)
    print(f"\n{'='*55}")
    print(f"Total academic papers  : {len(df)}")
    print(f"Journals covered       : {df['journal'].nunique()}")
    print(f"\nPapers per journal:")
    print(df["journal"].value_counts().to_string())
    print(f"\nPapers per year (sample):")
    print(df.groupby("year").size().sort_index().tail(10).to_string())
    print(f"\nTop 10 most cited:")
    print(df.nlargest(10, "cited_by_count")[
        ["title", "journal", "year", "cited_by_count"]
    ].to_string(index=False))

    return df


# ── RUN ───────────────────────────────────────────────────────────────────────
# Step 1: fill in your email above
# Step 2: (optional but recommended) get free API key at openalex.org
#         and add to headers as: "Authorization": "Bearer YOUR_KEY"
# Step 3: run this script — takes ~10-20 minutes for all 5 journals

out_path = DATA_DIR / "openalex_benchmark.csv"

df = build_benchmark(
    journals    = JOURNALS,
    start_year  = START_YEAR,
    end_year    = END_YEAR,
    email       = OPENALEX_EMAIL,
    out_path    = out_path,
)
