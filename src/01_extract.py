# %% Imports & Setup
import fitz  # PyMuPDF
import anthropic
import pandas as pd
import json
import time
import re
import os
from pathlib import Path
from collections import Counter

os.chdir(r"C:\Users\willi\Desktop\gap\src")

# %% Config
API_KEY  = open("../pilot/api_key.txt").read().strip()
DATA_DIR = Path("data")
PDF_DIR  = DATA_DIR / "pdfs"
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

client = anthropic.Anthropic(api_key=API_KEY)

# Bump from 8000 to fix JSON truncation on citation-heavy papers
MAX_TOKENS = 16000

# Send full text for documents under this character count
FULL_TEXT_CHAR_LIMIT = 60000


# %% PDF text extraction
def extract_pdf_text(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    pages = [page.get_text() for page in doc]
    doc.close()
    full_text = "\n".join(pages)

    if len(full_text) <= FULL_TEXT_CHAR_LIMIT:
        print(f"  Full text ({len(full_text)} chars)")
        return full_text

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
        print(f"  Refs section at {ref_start} ({len(text)} chars total)")
    else:
        text = full_text[:8000] + "\n\n[...middle truncated...]\n\n" + full_text[-4000:]
        print(f"  No refs section — first+last ({len(text)} chars)")

    return text


# %% Extraction prompt
EXTRACTION_PROMPT = """You are a research assistant analyzing a practitioner finance document.

Return a single JSON object with two top-level keys: "source" and "citations".

════════════════════════════════════════
PART 1 — SOURCE DOCUMENT METADATA
════════════════════════════════════════
- title: full document title
- year: publication year as integer (null if not found)
- source_type: type of document — ONE of:
    "aqr_alternative_thinking", "faj_article", "jpm_article",
    "aqr_white_paper", "other_practitioner"
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
- DOI: only if you see it in the reference string. Never fabricate.
- If the same work appears multiple times, include it only ONCE.

Output ONLY valid JSON. No preamble, no markdown fences.

{
  "source": {
    "title": "...",
    "year": 2024,
    "source_type": "aqr_alternative_thinking",
    "source_topic": "...",
    "source_academic_subfield": "..."
  },
  "citations": [
    {
      "raw_mention": "...",
      "raw_authors": "...",
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


# %% LLM extraction
def extract_all(pdf_path: Path) -> tuple[dict, list[dict]]:
    """Single Claude call per PDF. Returns (source_meta, citations_list)."""
    text = extract_pdf_text(pdf_path)
    prompt = EXTRACTION_PROMPT.replace("{text}", text)

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

        return source, citations

    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw output (first 400 chars): {raw[:400]}")
        return {"title": pdf_path.stem, "year": None,
                "source_type": None, "source_topic": None,
                "source_academic_subfield": None}, []
    except Exception as e:
        print(f"  API error: {e}")
        return {"title": pdf_path.stem, "year": None,
                "source_type": None, "source_topic": None,
                "source_academic_subfield": None}, []


# %% Main pipeline
def run_pipeline(pdf_dir: Path, output_name: str = "citations.csv") -> pd.DataFrame:
    """
    Process all PDFs in pdf_dir.
    Saves incrementally to avoid losing progress on large runs.
    """
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"No PDFs found in {pdf_dir}")
        return pd.DataFrame()

    out_path = DATA_DIR / output_name

    # Resume support: skip already-processed files
    processed_titles = set()
    if out_path.exists():
        existing = pd.read_csv(out_path)
        processed_titles = set(existing["source_title"].dropna().unique())
        print(f"Resuming — {len(processed_titles)} source titles already processed")
        all_citations = existing.to_dict("records")
    else:
        all_citations = []

    print(f"Found {len(pdf_files)} PDFs total\n")

    for i, pdf_path in enumerate(pdf_files):
        source_check, _ = {"title": pdf_path.stem}, []

        # Quick title check — skip if already done
        # (We do a lightweight metadata-only check via filename stem first)
        print(f"[{i+1}/{len(pdf_files)}] {pdf_path.name}")

        source, citations = extract_all(pdf_path)

        if source.get("title") in processed_titles:
            print(f"  Skipping — already processed")
            continue

        print(f"  Source : {source.get('title')} ({source.get('year')})")
        print(f"  Type   : {source.get('source_type')}")
        print(f"  Topic  : {source.get('source_topic')} | "
              f"Subfield: {source.get('source_academic_subfield')}")
        print(f"  Found  : {len(citations)} citations")

        for c in citations:
            row = {
                "source_year":              source.get("year"),
                "source_title":             source.get("title"),
                "source_type":              source.get("source_type"),
                "source_topic":             source.get("source_topic"),
                "source_academic_subfield": source.get("source_academic_subfield"),
            }
            row.update(c)
            all_citations.append(row)

        # Save after every PDF — protects against interruptions
        pd.DataFrame(all_citations).to_csv(out_path, index=False)

        processed_titles.add(source.get("title"))
        time.sleep(1)

    df = pd.DataFrame(all_citations)
    print(f"\n{'='*55}")
    print(f"Total PDFs processed : {len(pdf_files)}")
    print(f"Total citations      : {len(df)}")
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

    print("\nAcademic vs non-academic by year (excl. AQR internal):")
    sub = df[~df["is_aqr_internal"]]
    print(sub.groupby(["source_year", "is_academic"]).size()
            .unstack(fill_value=0).to_string())


# %% RUN
# ---------------------------------------------------------------
# Place all practitioner PDFs in:  src/data/pdfs/
# Supports resume — safe to interrupt and restart.
# ---------------------------------------------------------------

df = run_pipeline(PDF_DIR, output_name="citations.csv")
if not df.empty:
    print_summary(df)
