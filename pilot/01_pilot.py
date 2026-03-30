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
# Store your API key in a plain text file — never paste it directly in code
API_KEY = open("api_key.txt").read().strip()
DATA_DIR = Path("data")
PDF_DIR  = DATA_DIR / "pdfs"   # <-- put all manually downloaded PDFs here
DATA_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

client = anthropic.Anthropic(api_key=API_KEY)


# %% PDF text extraction
def extract_pdf_text(pdf_path: Path) -> str:
    """
    Extract text from a PDF, prioritizing the references section.
    Strategy: always include first 6000 chars (body) + full references section.
    If no references section found, take first 8000 + last 4000 chars.
    """
    doc = fitz.open(pdf_path)
    pages = [page.get_text() for page in doc]
    doc.close()
    full_text = "\n".join(pages)

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
        print(f"  References section found at char {ref_start} "
              f"(refs length: {len(refs)} chars)")
    else:
        text = full_text[:8000] + "\n\n[...middle truncated...]\n\n" + full_text[-4000:]
        print(f"  No references section found — using first+last strategy")

    return text


# %% Citation extraction prompt
EXTRACTION_PROMPT = """You are a research assistant extracting academic citations from a practitioner finance document (AQR Alternative Thinking series).

Your task has two parts:

1. Identify EVERY reference to academic research in this text, including:
   - Formal citations (e.g., "Fama and French (1993)")
   - Informal named references (e.g., "the Fama-French three-factor model", "Black-Scholes")
   - Implicit references to well-known results (e.g., "the capital asset pricing model", "efficient market hypothesis")
   - All entries in the references/further reading section at the end

2. For each reference, resolve it to a specific paper if possible:
   - raw_mention: exact text as it appears in the document
   - resolved_title: paper title (null if cannot resolve)
   - resolved_authors: list of author last names (null if cannot resolve)
   - resolved_year: publication year as integer (null if cannot resolve)
   - resolved_journal: journal, book, or venue (null if cannot resolve)
   - confidence: "high" (certain), "medium" (likely correct), "low" (guessing)
   - resolution_type: one of "formal_citation", "informal_named", "implicit", "references_section"

Rules:
- Do NOT include self-references to other AQR papers or AQR Alternative Thinking editions
- Do NOT include references to AQR data sets or AQR internal reports
- DO include books, working papers, and non-AQR practitioner publications
- If the same paper is cited multiple times, include it only ONCE

Output ONLY a valid JSON array. No preamble, no explanation, no markdown fences.

Document text:
{text}"""


# %% LLM extraction function
def extract_citations(text: str, paper_meta: dict) -> list[dict]:
    """Call Claude to extract and resolve citations from document text."""
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

        citations = json.loads(raw)

        # Attach source metadata
        for c in citations:
            c["source_title"] = paper_meta["title"]
            c["source_year"]  = paper_meta["year"]

        return citations

    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw output (first 300 chars): {raw[:300]}")
        return []
    except Exception as e:
        print(f"  API error: {e}")
        return []


# %% Main pipeline — reads from local PDF folder
def run_pipeline(pdf_dir: Path) -> pd.DataFrame:
    """
    Process all PDFs in pdf_dir.
    Expects filenames to contain a 4-digit year, e.g. '2018_Q1_CMA.pdf'
    """
    pdf_files = sorted(pdf_dir.glob("*.pdf"))

    if not pdf_files:
        print(f"No PDFs found in {pdf_dir}")
        print("Download AQR Alternative Thinking PDFs manually and place them there.")
        return pd.DataFrame()

    print(f"Found {len(pdf_files)} PDFs in {pdf_dir}\n")
    all_citations = []

    for i, pdf_path in enumerate(pdf_files):
        # Extract year from filename
        year_match = re.search(r"(20\d{2})", pdf_path.stem)
        year = int(year_match.group(1)) if year_match else None

        paper_meta = {
            "title": pdf_path.stem,
            "year":  year,
        }

        print(f"[{i+1}/{len(pdf_files)}] {pdf_path.name}  (year={year})")

        text = extract_pdf_text(pdf_path)
        print(f"  Text length: {len(text)} chars")

        print("  Calling Claude...")
        citations = extract_citations(text, paper_meta)
        print(f"  Extracted {len(citations)} citations")

        all_citations.extend(citations)
        time.sleep(1)  # rate limit courtesy

    print(f"\n{'='*55}")
    print(f"Total PDFs processed : {len(pdf_files)}")
    print(f"Total citations found: {len(all_citations)}")

    return pd.DataFrame(all_citations)


# %% Summary stats helper
def print_summary(df: pd.DataFrame):
    if df.empty:
        print("No data.")
        return

    print("\n=== SUMMARY STATISTICS ===")
    print(f"Total citations      : {len(df)}")
    print(f"High confidence      : {(df['confidence'] == 'high').sum()}")
    print(f"Medium confidence    : {(df['confidence'] == 'medium').sum()}")
    print(f"Low confidence       : {(df['confidence'] == 'low').sum()}")

    print("\nResolution types:")
    print(df["resolution_type"].value_counts().to_string())

    print("\nTop 10 cited journals:")
    print(df["resolved_journal"].value_counts().head(10).to_string())

    print("\nTop 15 cited authors:")
    authors = [
        a for sublist in df["resolved_authors"].dropna()
        for a in (sublist if isinstance(sublist, list) else [sublist])
    ]
    for author, count in Counter(authors).most_common(15):
        print(f"  {author}: {count}")

    print("\nCitations per source year:")
    print(df.groupby("source_year").size().sort_index().to_string())


# %% RUN
# ---------------------------------------------------------------
# Before running: place all AQR Alternative Thinking PDFs in:
#   Desktop/gap/pilot/data/pdfs/
# Name each file with the year somewhere in the filename, e.g.:
#   2018_Q1_Capital_Market_Assumptions.pdf
# ---------------------------------------------------------------

df = run_pipeline(PDF_DIR)

if not df.empty:
    out_path = DATA_DIR / "citations_pilot.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")
    print_summary(df)
