# %% Imports & Setup
import fitz  # PyMuPDF
import anthropic
import pandas as pd
import json
import time
import re
import os
import threading
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

os.chdir(r"C:\Users\willi\Desktop\gap\src")

# %% Config
API_KEY  = open("../pilot/api_key.txt").read().strip()
DATA_DIR = Path("data")
PDF_DIR  = DATA_DIR / "pdfs"
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

client = anthropic.Anthropic(api_key=API_KEY)

MAX_TOKENS           = 16000
FULL_TEXT_CHAR_LIMIT = 60000
MAX_WORKERS          = 2    # concurrent PDFs — raise to 5 if no rate limit errors


# %% PDF text extraction
def extract_pdf_text(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    pages = [page.get_text() for page in doc]
    doc.close()
    full_text = "\n".join(pages)

    if len(full_text) <= FULL_TEXT_CHAR_LIMIT:
        return full_text, "full"

    ref_markers = [
        "references and further reading",
        "references\n",
        "bibliography\n",
        "further reading\n",
        "footnotes\n",
    ]
    ref_start = None
    lower = full_text.lower()
    for marker in ref_markers:
        idx = lower.rfind(marker)
        if idx != -1:
            ref_start = idx
            break

    if ref_start is not None:
        text = full_text[:6000] + "\n\n[...body truncated...]\n\n" + full_text[ref_start:]
        return text, f"refs@{ref_start}"
    else:
        text = full_text[:8000] + "\n\n[...middle truncated...]\n\n" + full_text[-4000:]
        return text, "first+last"


# %% Infer source_type from subfolder
def infer_source_type(pdf_path: Path) -> str:
    folder = pdf_path.parent.name.lower()
    if "aqr_alt" in folder or "alternative" in folder:
        return "aqr_alternative_thinking"
    if "faj" in folder:
        return "faj_article"
    if "jpm" in folder:
        return "jpm_article"
    if "aqr_white" in folder or "white" in folder:
        return "aqr_white_paper"
    return "other_practitioner"


# %% Extraction prompt
EXTRACTION_PROMPT = """You are a research assistant analyzing a practitioner finance document.

Return a single JSON object with two top-level keys: "source" and "citations".

════════════════════════════════════════
PART 1 — SOURCE DOCUMENT METADATA
════════════════════════════════════════
- title: full document title
- year: publication year as integer (null if not found)
- source_topic: primary investment topic — ONE of:
    "factor_investing", "portfolio_construction", "risk_management",
    "behavioral_finance", "macro_finance", "market_efficiency",
    "alternative_investments", "fixed_income", "derivatives", "other"
- source_academic_subfield: if this were an academic paper, which subfield? ONE of:
    "asset_pricing", "corporate_finance", "financial_intermediation",
    "behavioral_finance", "market_microstructure", "macro_finance",
    "other_academic", "not_academic"

════════════════════════════════════════
PART 2 — CITATIONS
════════════════════════════════════════
Identify EVERY reference. Include academic papers, books, practitioner pubs,
AQR internal publications, and informal/implicit mentions.
Do NOT include self-references to AQR Alternative Thinking editions.

For each citation:

IDENTIFICATION:
- raw_mention: exact text as it appears
- raw_authors: full author names as written (null if unavailable)
- recovered_authors: list of last names only (null if cannot recover)
- recovered_title: paper/book title (null if cannot recover)
- recovered_year: publication year as integer (null if cannot recover)
- recovered_journal: journal, publisher, or venue (null if cannot recover)
- recovered_doi: DOI only if it appears in the reference string or you are certain.
    NEVER guess. null if uncertain.
- recovered_venue_type: ONE of:
    "journal", "working_paper", "book", "practitioner_pub", "other"

CONTEXT:
- citation_context: 1-2 sentence verbatim snippet showing HOW this work is cited.
    null for purely implicit mentions or if no surrounding context is available.

QUALITY:
- confidence: "high", "medium", or "low"
- resolution_type: "formal_citation", "informal_named", "implicit", or "references_section"

FLAGS:
- is_academic: true if peer-reviewed journal or academic working paper
- is_aqr_internal: true if any AQR publication (white paper, working paper, etc.)

CLASSIFICATION:
- practitioner_topic: ONE of:
    "factor_investing", "portfolio_construction", "risk_management",
    "behavioral_finance", "macro_finance", "market_efficiency",
    "alternative_investments", "fixed_income", "derivatives", "other"
- academic_subfield: ONE of:
    "asset_pricing"            (risk premia, factors, return predictability, derivatives)
    "corporate_finance"        (capital structure, M&A, governance, payout)
    "financial_intermediation" (banks, funds, credit, liquidity, systemic risk)
    "behavioral_finance"       (psychology, biases, anomalies, sentiment)
    "market_microstructure"    (trading, liquidity, bid-ask, HFT, price discovery)
    "macro_finance"            (macro-financial linkages, term structure, consumption)
    "other_academic"           (international finance, household finance, fintech, econometrics)
    "not_academic"             (practitioner pubs, white papers, books, data sources)

GUIDANCE:
- ML return prediction → "asset_pricing"
- He-Krishnamurthy → "financial_intermediation"
- NBER/SSRN working papers → is_academic: true, venue_type: "working_paper"
- Ilmanen "Expected Returns" → "not_academic", venue_type: "book"
- DOI: only if you see it explicitly. Never fabricate.
- If the same work appears multiple times, include it only ONCE.

Output ONLY valid JSON. No preamble, no markdown fences.

{
  "source": {
    "title": "...",
    "year": 2024,
    "source_topic": "...",
    "source_academic_subfield": "..."
  },
  "citations": [
    {
      "raw_mention": "...",
      "raw_authors": ["..."],
      "recovered_authors": ["..."],
      "recovered_title": "...",
      "recovered_year": 2020,
      "recovered_journal": "...",
      "recovered_doi": null,
      "recovered_venue_type": "journal",
      "citation_context": "...",
      "confidence": "high",
      "resolution_type": "references_section",
      "is_academic": true,
      "is_aqr_internal": false,
      "practitioner_topic": "...",
      "academic_subfield": "..."
    }
  ]
}

Document text:
{text}"""


# %% Single PDF processing function (runs in thread)
def process_pdf(pdf_path: Path) -> tuple[str, dict, list[dict], str]:
    path_str    = str(pdf_path)
    source_type = infer_source_type(pdf_path)
    text, strategy = extract_pdf_text(pdf_path)
    prompt = EXTRACTION_PROMPT.replace("{text}", text)

    max_retries = 5
    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = message.content[0].text.strip()
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"\s*```$",     "", raw)

            result    = json.loads(raw)
            source    = result.get("source", {})
            citations = result.get("citations", [])

            if not source.get("title"):
                source["title"] = pdf_path.stem
            source["source_type"] = source_type

            status = (f"OK | {source.get('title', '')[:45]} ({source.get('year')}) "
                      f"| {len(citations)} citations | {strategy}")
            return path_str, source, citations, status

        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)   # 30s, 60s, 90s, 120s, 150s
            print(f"  Rate limit — waiting {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue

        except json.JSONDecodeError as e:
            source = {"title": pdf_path.stem, "year": None,
                      "source_type": source_type, "source_topic": None,
                      "source_academic_subfield": None}
            return path_str, source, [], f"JSON_ERROR: {e}"

        except Exception as e:
            source = {"title": pdf_path.stem, "year": None,
                      "source_type": source_type, "source_topic": None,
                      "source_academic_subfield": None}
            return path_str, source, [], f"API_ERROR: {e}"

    # All retries exhausted
    source = {"title": pdf_path.stem, "year": None,
              "source_type": source_type, "source_topic": None,
              "source_academic_subfield": None}
    return path_str, source, [], "FAILED: max retries exceeded"


# %% Main pipeline (parallel)
def run_pipeline(pdf_dir: Path, output_name: str = "citations.csv") -> pd.DataFrame:
    """
    Process all PDFs recursively under pdf_dir using a thread pool.
    MAX_WORKERS PDFs are processed concurrently.
    Saves incrementally after each completed batch.
    """
    pdf_files = sorted(pdf_dir.glob("**/*.pdf"))

    if not pdf_files:
        print(f"No PDFs found under {pdf_dir}")
        return pd.DataFrame()

    out_path = DATA_DIR / output_name

    # Resume: track already-processed file paths
    processed_paths = set()
    all_citations   = []
    save_lock       = threading.Lock()

    if out_path.exists():
        existing = pd.read_csv(out_path)
        if "source_file" in existing.columns:
            processed_paths = set(existing["source_file"].dropna().unique())
        all_citations = existing.to_dict("records")
        print(f"Resuming — {len(processed_paths)} files already processed")

    pending = [p for p in pdf_files if str(p) not in processed_paths]
    subfolders = sorted(set(p.parent.name for p in pdf_files))
    print(f"Found {len(pdf_files)} PDFs across subfolders: {subfolders}")
    print(f"Pending: {len(pending)} | Workers: {MAX_WORKERS}\n")

    if not pending:
        print("All PDFs already processed.")
        return pd.DataFrame(all_citations)

    completed = 0

    def save_result(path_str, source, citations):
        """Thread-safe save after each completed PDF."""
        rows = []
        for c in citations:
            row = {
                "source_file":              path_str,
                "source_year":              source.get("year"),
                "source_title":             source.get("title"),
                "source_type":              source.get("source_type"),
                "source_topic":             source.get("source_topic"),
                "source_academic_subfield": source.get("source_academic_subfield"),
            }
            row.update(c)
            rows.append(row)

        with save_lock:
            all_citations.extend(rows)
            pd.DataFrame(all_citations).to_csv(out_path, index=False)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_pdf, p): p for p in pending}

        for future in as_completed(futures):
            completed += 1
            pdf_path = futures[future]

            try:
                path_str, source, citations, status = future.result()
            except Exception as e:
                print(f"[{completed}/{len(pending)}] THREAD ERROR {pdf_path.name}: {e}")
                continue

            print(f"[{completed}/{len(pending)}] {pdf_path.name}")
            print(f"  {status}")

            save_result(path_str, source, citations)

    df = pd.DataFrame(all_citations)
    print(f"\n{'='*55}")
    print(f"Total PDFs : {len(pdf_files)} | Citations: {len(df)}")
    return df


# %% Summary stats
def print_summary(df: pd.DataFrame):
    if df.empty:
        print("No data.")
        return

    print("\n=== SUMMARY STATISTICS ===")
    print(f"Total citations      : {len(df)}")
    print(f"Academic             : {df['is_academic'].sum()}")
    print(f"Non-academic         : {(~df['is_academic']).sum()}")
    print(f"AQR internal         : {df['is_aqr_internal'].sum()}")
    print(f"Academic (excl. AQR) : {(df['is_academic'] & ~df['is_aqr_internal']).sum()}")
    print(f"DOI recovered        : {df['recovered_doi'].notna().sum()} "
          f"({df['recovered_doi'].notna().mean()*100:.0f}%)")
    print(f"Citation context     : {df['citation_context'].notna().sum()} "
          f"({df['citation_context'].notna().mean()*100:.0f}%)")

    print("\nConfidence:")
    print(f"  High   : {(df['confidence'] == 'high').sum()}")
    print(f"  Medium : {(df['confidence'] == 'medium').sum()}")
    print(f"  Low    : {(df['confidence'] == 'low').sum()}")

    print("\nVenue types:")
    print(df["recovered_venue_type"].value_counts().to_string())

    print("\nSource types:")
    print(df.groupby("source_type")["source_title"].nunique().to_string())

    print("\nTop 10 cited journals/venues:")
    print(df["recovered_journal"].value_counts().head(10).to_string())

    print("\nAcademic subfields (academic, excl. AQR internal):")
    mask = df["is_academic"] & ~df["is_aqr_internal"]
    print(df[mask]["academic_subfield"].value_counts().to_string())

    print("\nCitations per source year:")
    print(df.groupby("source_year").size().sort_index().to_string())

    print("\nSource type breakdown by year:")
    print(df.groupby(["source_year", "source_type"]).size()
            .unstack(fill_value=0).to_string())

    print("\nTop 15 cited authors:")
    authors = []
    for val in df["recovered_authors"].dropna():
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            if isinstance(parsed, list):
                authors.extend(parsed)
        except (json.JSONDecodeError, TypeError):
            continue
    for author, count in Counter(authors).most_common(15):
        print(f"  {author}: {count}")


# %% RUN
# ---------------------------------------------------------------
# Subfolder structure inside src/data/pdfs/:
#   AQR_alternative/   ← AQR Alternative Thinking PDFs
#   FAJ/               ← Financial Analysts Journal PDFs (summer)
#   JPM/               ← Journal of Portfolio Management PDFs (summer)
#   AQR_white/         ← AQR White Papers (optional)
#
# MAX_WORKERS=4 runs 4 PDFs simultaneously.
# If you hit rate limit errors, reduce to 3.
# For the full 200-400 paper corpus, consider the Anthropic Batch API instead.
# ---------------------------------------------------------------

df = run_pipeline(PDF_DIR, output_name="citations.csv")
if not df.empty:
    print_summary(df)

