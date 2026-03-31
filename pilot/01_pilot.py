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

os.chdir(r"C:\Users\willi\Desktop\gap\pilot")

# %% Config
API_KEY = open("api_key.txt").read().strip()
DATA_DIR = Path("data")
PDF_DIR  = DATA_DIR / "pdfs"
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

client = anthropic.Anthropic(api_key=API_KEY)

# Character limit below which we send full document text
FULL_TEXT_CHAR_LIMIT = 60000


# %% PDF text extraction
def extract_pdf_text(pdf_path: Path) -> str:
    """
    Extract text from a PDF, prioritizing the references section.
    If document is short enough, send full text.
    Otherwise: first 6000 chars + full references section.
    Fallback: first 8000 + last 4000 chars.
    """
    doc = fitz.open(pdf_path)
    pages = [page.get_text() for page in doc]
    doc.close()
    full_text = "\n".join(pages)

    if len(full_text) <= FULL_TEXT_CHAR_LIMIT:
        print(f"  Full text ({len(full_text)} chars) — sending entire document")
        return full_text

    ref_markers = [
        "references and further reading",
        "references\n",
        "bibliography\n",
        "further reading\n",
        "footnotes\n",
    ]

    ref_start = None
    lower_text = full_text.lower()
    for marker in ref_markers:
        idx = lower_text.rfind(marker)
        if idx != -1:
            ref_start = idx
            break

    if ref_start is not None:
        body = full_text[:6000]
        refs = full_text[ref_start:]
        text = body + "\n\n[...body truncated...]\n\n" + refs
        print(f"  References section at char {ref_start} "
              f"(refs: {len(refs)} chars, total: {len(text)} chars)")
    else:
        text = full_text[:8000] + "\n\n[...middle truncated...]\n\n" + full_text[-4000:]
        print(f"  No references section — using first+last strategy ({len(text)} chars)")

    return text


# %% Combined extraction prompt (metadata + citations in one call)
EXTRACTION_PROMPT = """You are a research assistant analyzing a practitioner finance document (AQR Alternative Thinking series).

Return a single JSON object with two top-level keys: "source" and "citations".

════════════════════════════════════════
PART 1 — SOURCE DOCUMENT METADATA
════════════════════════════════════════
Extract metadata about this document and return under the "source" key:

- title: full document title as it appears
- year: publication year as integer (null if not found)
- source_topic: the primary investment topic of this document — choose ONE from:
    "factor_investing", "portfolio_construction", "risk_management",
    "behavioral_finance", "macro_finance", "market_efficiency",
    "alternative_investments", "fixed_income", "derivatives", "other"
- source_academic_subfield: if this document were an academic paper, which subfield?
    Choose ONE from:
    "asset_pricing", "corporate_finance", "financial_intermediation",
    "behavioral_finance", "market_microstructure", "macro_finance",
    "other_academic", "not_academic"

════════════════════════════════════════
PART 2 — CITATIONS
════════════════════════════════════════
Identify EVERY reference in this text and return as array under the "citations" key.

Include:
- Academic papers (journal articles, working papers)
- Books (academic or practitioner)
- Practitioner publications (firm white papers, industry reports)
- AQR white papers and internal AQR references (mark is_aqr_internal: true)
- Informal named references (e.g., "the Fama-French three-factor model")
- Implicit references (e.g., "the capital asset pricing model")
- All entries in the references/further reading section

Do NOT include: self-references to AQR Alternative Thinking editions specifically.

For each citation provide ALL of these fields:

IDENTIFICATION:
- raw_mention: exact text as it appears in the document
- raw_authors: full author names as written (null if not available)
- recovered_authors: list of author last names only (null if cannot recover)
- recovered_title: title of the work (null if cannot recover)
- recovered_year: publication year as integer (null if cannot recover)
- recovered_journal: journal name, book publisher, or venue (null if cannot recover)
- recovered_doi: DOI if inferable from the reference string or your knowledge (null if not — do NOT guess)
- recovered_venue_type: type of publication — choose ONE from:
    "journal"           (peer-reviewed journal article)
    "working_paper"     (NBER, SSRN, university working paper)
    "book"              (academic or practitioner book)
    "practitioner_pub"  (white paper, industry report, firm publication)
    "other"             (anything else)

CONTEXT:
- citation_context: 1-2 sentences of surrounding text showing HOW this work is cited
    (e.g., cited as theoretical foundation, empirical support, methodological reference).
    Extract verbatim from the document where possible. null if implicit/informal mention.

QUALITY:
- confidence: "high" (certain), "medium" (likely correct), "low" (guessing)
- resolution_type: one of "formal_citation", "informal_named", "implicit", "references_section"

FLAGS:
- is_academic: true if published in peer-reviewed journal or as academic working paper
- is_aqr_internal: true if AQR white paper, AQR working paper, or other AQR internal publication

CLASSIFICATION:
- practitioner_topic: ONE from:
    "factor_investing", "portfolio_construction", "risk_management",
    "behavioral_finance", "macro_finance", "market_efficiency",
    "alternative_investments", "fixed_income", "derivatives", "other"
- academic_subfield: ONE from:
    "asset_pricing"            (risk premia, factor models, return predictability, derivatives)
    "corporate_finance"        (capital structure, M&A, governance, investment, payout)
    "financial_intermediation" (banks, funds, credit markets, liquidity, systemic risk)
    "behavioral_finance"       (investor psychology, biases, anomalies, sentiment)
    "market_microstructure"    (trading mechanics, liquidity, bid-ask spreads, HFT)
    "macro_finance"            (macro-financial linkages, term structure, consumption, disasters)
    "other_academic"           (international finance, household finance, fintech, econometrics)
    "not_academic"             (practitioner reports, white papers, books, data sources)

CLASSIFICATION GUIDANCE:
- ML papers predicting returns → "asset_pricing"
- He-Krishnamurthy intermediary pricing → "financial_intermediation"
- Behavioral biases in asset prices → "behavioral_finance"
- Ilmanen "Expected Returns" book → "not_academic", recovered_venue_type: "book"
- NBER/SSRN working papers → is_academic: true, recovered_venue_type: "working_paper"
- DOI: only include if you are certain (e.g., it appears in the reference string). Never guess.

RULES:
- Include ALL references — academic and non-academic alike
- If the same work appears multiple times, include it only ONCE
- For informal/implicit references, citation_context may be null

════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════
Output ONLY a valid JSON object. No preamble, no explanation, no markdown fences.

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


# %% Single LLM extraction function (metadata + citations in one call)
def extract_all(pdf_path: Path) -> tuple[dict, list[dict]]:
    """Single Claude call per PDF. Returns (source_meta, citations_list)."""
    text = extract_pdf_text(pdf_path)
    prompt = EXTRACTION_PROMPT.replace("{text}", text)

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()

        # Strip accidental markdown fences
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        result   = json.loads(raw)
        source   = result.get("source", {})
        citations = result.get("citations", [])

        if not source.get("title"):
            source["title"] = pdf_path.stem

        return source, citations

    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw output (first 400 chars): {raw[:400]}")
        return {"title": pdf_path.stem, "year": None,
                "source_topic": None, "source_academic_subfield": None}, []
    except Exception as e:
        print(f"  API error: {e}")
        return {"title": pdf_path.stem, "year": None,
                "source_topic": None, "source_academic_subfield": None}, []


# %% Main pipeline
def run_pipeline(pdf_dir: Path) -> pd.DataFrame:
    """Process all PDFs in pdf_dir. Returns DataFrame of all citations."""
    pdf_files = sorted(pdf_dir.glob("*.pdf"))

    if not pdf_files:
        print(f"No PDFs found in {pdf_dir}")
        return pd.DataFrame()

    print(f"Found {len(pdf_files)} PDFs\n")
    all_citations = []

    for i, pdf_path in enumerate(pdf_files):
        print(f"[{i+1}/{len(pdf_files)}] {pdf_path.name}")

        source, citations = extract_all(pdf_path)

        print(f"  Source : {source.get('title')} ({source.get('year')})")
        print(f"  Topic  : {source.get('source_topic')} | "
              f"Subfield: {source.get('source_academic_subfield')}")
        print(f"  Found  : {len(citations)} citations")

        # Source fields go first in every row
        for c in citations:
            row = {
                "source_year":              source.get("year"),
                "source_title":             source.get("title"),
                "source_topic":             source.get("source_topic"),
                "source_academic_subfield": source.get("source_academic_subfield"),
            }
            row.update(c)
            all_citations.append(row)

        time.sleep(1)

    print(f"\n{'='*55}")
    print(f"Total PDFs processed : {len(pdf_files)}")
    print(f"Total citations found: {len(all_citations)}")

    return pd.DataFrame(all_citations)


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

    print("\nConfidence breakdown:")
    print(f"  High   : {(df['confidence'] == 'high').sum()}")
    print(f"  Medium : {(df['confidence'] == 'medium').sum()}")
    print(f"  Low    : {(df['confidence'] == 'low').sum()}")

    print("\nVenue types:")
    print(df["recovered_venue_type"].value_counts().to_string())

    print("\nResolution types:")
    print(df["resolution_type"].value_counts().to_string())

    print("\nTop 10 cited journals/venues:")
    print(df["recovered_journal"].value_counts().head(10).to_string())

    print("\nPractitioner topics of citations:")
    print(df["practitioner_topic"].value_counts().to_string())

    print("\nAcademic subfields (academic papers only, excl. AQR internal):")
    mask = df["is_academic"] & ~df["is_aqr_internal"]
    print(df[mask]["academic_subfield"].value_counts().to_string())

    print("\nSource document topics:")
    print(df.groupby("source_topic")["source_title"].nunique()
            .sort_values(ascending=False).to_string())

    print("\nTop 15 cited authors:")
    authors = [
        a for sublist in df["recovered_authors"].dropna()
        for a in (sublist if isinstance(sublist, list) else [sublist])
    ]
    for author, count in Counter(authors).most_common(15):
        print(f"  {author}: {count}")

    print("\nCitations per source year:")
    print(df.groupby("source_year").size().sort_index().to_string())

    print("\nAcademic vs non-academic by source year (excl. AQR internal):")
    sub = df[~df["is_aqr_internal"]]
    print(sub.groupby(["source_year", "is_academic"]).size()
            .unstack(fill_value=0).to_string())


# %% RUN
# ---------------------------------------------------------------
# Place all AQR Alternative Thinking PDFs in:
#   Desktop/gap/pilot/data/pdfs/
# No need to rename files — title and year extracted from content.
# ---------------------------------------------------------------

df = run_pipeline(PDF_DIR)

if not df.empty:
    out_path = DATA_DIR / "citations_pilot.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")
    print_summary(df)