# %% Imports & Setup
import fitz  # PyMuPDF
import anthropic
import hashlib
import pandas as pd
import json
import time
import re
import argparse
import threading
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Paths (anchored to script location: gap/src/01_extract_deep.py) ─────────────
REPO_ROOT  = Path(__file__).resolve().parent.parent   # gap/
PDF_DIR    = REPO_ROOT / "data"   / "pdfs"            # gap/data/pdfs/
OTHER_DIR  = REPO_ROOT / "data"   / "Other_Corpus"    # gap/data/Other_Corpus/
OUT_DIR    = REPO_ROOT / "output"                     # gap/output/
OUT_PATH     = OUT_DIR / "citations_deep.csv"    # gap/output/citations_deep.csv — citation rows
DOC_OUT_PATH = OUT_DIR / "documents_deep.csv"   # gap/output/documents_deep.csv — one row per doc, always written

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
API_KEY     = (REPO_ROOT / "pilot" / "api_key.txt").read_text().strip()
client      = anthropic.Anthropic(api_key=API_KEY)

MAX_TOKENS           = 16000
FULL_TEXT_CHAR_LIMIT = 60000
MAX_WORKERS          = 2    # default; override with --workers flag

# ── Schema versioning — bump when prompt changes to track schema drift ──────────
# 2.2 (Phase 1): adds `extraction_note` doc field + `skipped_likely_scanned`
#                to the llm_status enum. Paired with 01c_resolve.py schema 2.2.
SCHEMA_VERSION = "2.2"


# ── Provenance (Phase 0) ────────────────────────────────────────────────────────
# Computed once at module load. If the prompt, patterns, or code change
# mid-run, rows written before and after will NOT share these hashes — that is
# deliberate and desired; the hashes record the script's intent at invocation.
# See fields.md Phase 0 entry and gap_structure.md "Schema Version Tracking".
def _load_patterns_hash() -> str:
    patterns_path = REPO_ROOT / "src" / "post_processing" / "patterns.yaml"
    if not patterns_path.exists():
        return "missing"
    return hashlib.sha256(patterns_path.read_bytes()).hexdigest()[:8]


def _get_git_hash() -> str:
    try:
        import subprocess
        result = subprocess.run(
            ["git", "rev-parse", "--short=8", "HEAD"],
            capture_output=True, text=True, cwd=REPO_ROOT, timeout=3,
        )
        if result.returncode != 0:
            return "no_git"
        commit = result.stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True, text=True, cwd=REPO_ROOT, timeout=3,
        )
        if status.stdout.strip():
            return f"{commit}-dirty"
        return commit
    except Exception:
        return "no_git"


# EXTRACTION_PROMPT is defined below; its hash is assigned immediately after
# the prompt block (search for "PROMPT_HASH = hashlib").
PATTERNS_HASH  = _load_patterns_hash()
CODE_VERSION   = _get_git_hash()


# %% Source type inference — keyed to actual folder names
SOURCE_TYPE_MAP = {
    "aqr":              "aqr_white_paper",
    "aqr_alternative":  "aqr_alternative_thinking",
    "alphaarchitect":   "alphaarchitect_post",
    "blackrock":        "blackrock_report",
    "blackrock_bii":    "blackrock_report",
    "cfa":              "cfa_monograph",
    "dfa":              "dfa_article",
    "edhec":            "edhec_report",
    "faj":              "faj_article",
    "gmo":              "gmo_report",
    "jacf":             "jacf_article",
    "jbis":             "jbis_article",
    "jfi":              "jfi_article",
    "jis":              "jis_article",
    "jor":              "jor_article",
    "jpm":              "jpm_article",
    "man":              "man_report",
    "msci":             "msci_report",
    "pgim":             "pgim_report",
    "pimco":            "pimco_report",
    "ra":               "ra_white_paper",
    "robeco":           "robeco_report",
    "ssga":             "ssga_report",
    "twosigma":         "twosigma_article",
    "vanguard":         "vanguard_report",
}

def infer_source_type(file_path: Path) -> str:
    folder = file_path.parent.name.lower()
    return SOURCE_TYPE_MAP.get(folder, "other_practitioner")


# Maps source_type → normalized institution name — computed in code, not by LLM
INSTITUTION_MAP = {
    "aqr_white_paper":           "AQR",
    "aqr_alternative_thinking":  "AQR",
    "alphaarchitect_post":       "Alpha Architect",
    "blackrock_report":          "BlackRock",
    "cfa_monograph":             "CFA Research Foundation",
    "dfa_article":               "DFA",
    "edhec_report":              "EDHEC",
    "faj_article":               "FAJ",
    "gmo_report":                "GMO",
    "jacf_article":              "JACF",
    "jbis_article":              "JBIS",
    "jfi_article":               "JFI",
    "jis_article":               "JIS",
    "jor_article":               "JOR",
    "jpm_article":               "JPM",
    "man_report":                "Man Institute",
    "msci_report":               "MSCI",
    "pgim_report":               "PGIM",
    "pimco_report":              "PIMCO",
    "ra_white_paper":            "Research Affiliates",
    "robeco_report":             "Robeco",
    "ssga_report":               "SSGA",
    "twosigma_article":          "Two Sigma",
    "vanguard_report":           "Vanguard",
}

def infer_institution(source_type: str) -> str:
    return INSTITUTION_MAP.get(source_type, "Unknown")


# %% Document identifier
def make_doc_id(file_path: Path) -> str:
    """
    Stable 12-character hex ID based on SHA-256 of raw file bytes.
    Tied to content, not path — stable across renames/moves.
    If content changes (re-scraped corrected PDF), ID changes correctly.
    NOTE: we hash raw bytes, not extracted text, so this is deterministic
    regardless of PDF library version or extraction settings.
    """
    try:
        content = file_path.read_bytes()
        return hashlib.sha256(content).hexdigest()[:12]
    except Exception:
        return hashlib.sha256(str(file_path).encode()).hexdigest()[:12]


# %% Text extraction
def extract_pdf_text(pdf_path: Path) -> tuple[str, str, int, int, str, bool, int, bool, int]:
    """
    Returns: (text, strategy, page_count, word_count, extraction_method,
              has_bibliography, ref_section_start_char, text_truncated, total_chars)
    has_bibliography: True if a references/bibliography header detected in last 40% of text
    extraction_method: "native_text" | "likely_scanned"
    ref_section_start_char: char index where references begin (-1 if not found)
    text_truncated: True if document was truncated before sending to LLM
    total_chars: full document character count before truncation
    """
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    pages = [page.get_text() for page in doc]
    doc.close()
    full_text = "\n".join(pages)
    word_count = len(full_text.split())

    words_per_page = word_count / max(page_count, 1)
    extraction_method = "likely_scanned" if words_per_page < 50 else "native_text"

    ref_markers = [
        "references and further reading",
        "references\n",
        "bibliography\n",
        "further reading\n",
        "footnotes\n",
    ]
    lower = full_text.lower()
    cutoff = int(len(lower) * 0.6)   # search last 40% only
    has_bibliography = any(m in lower[cutoff:] for m in ref_markers)

    total_chars = len(full_text)
    if total_chars <= FULL_TEXT_CHAR_LIMIT:
        return full_text, "full", page_count, word_count, extraction_method, has_bibliography, -1, False, total_chars

    ref_start = None
    for marker in ref_markers:
        idx = lower.rfind(marker)
        if idx != -1:
            ref_start = idx
            break

    if ref_start is not None:
        text = full_text[:6000] + "\n\n[...body truncated...]\n\n" + full_text[ref_start:]
        return text, f"refs@{ref_start}", page_count, word_count, extraction_method, has_bibliography, ref_start, True, total_chars
    else:
        text = full_text[:8000] + "\n\n[...middle truncated...]\n\n" + full_text[-4000:]
        return text, "first+last", page_count, word_count, extraction_method, has_bibliography, -1, True, total_chars


def extract_txt_text(txt_path: Path) -> tuple[str, str, int, int, str, bool, int, bool, int]:
    """Returns: (text, strategy, page_count, word_count, extraction_method,
                 has_bibliography, ref_section_start_char, text_truncated, total_chars)"""
    text = txt_path.read_text(encoding="utf-8", errors="ignore")
    total_chars = len(text)
    word_count = len(text.split())
    lower = text.lower()
    cutoff = int(len(lower) * 0.6)
    has_bibliography = any(m in lower[cutoff:] for m in [
        "references and further reading", "references\n", "bibliography\n",
        "further reading\n", "footnotes\n",
    ])
    if total_chars <= FULL_TEXT_CHAR_LIMIT:
        return text, "full", 0, word_count, "html_derived", has_bibliography, -1, False, total_chars
    text = text[:8000] + "\n\n[...middle truncated...]\n\n" + text[-4000:]
    return text, "first+last", 0, word_count, "html_derived", has_bibliography, -1, True, total_chars


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
and informal/implicit mentions. Do NOT include self-references to AQR Alternative
Thinking editions.

# NOTE: Within-document deduplication is handled in post-processing (dedup.py),
# not here. Extract all mentions; the post-processing script collapses them by
# (doc_id, fuzzy_paper_id) and aggregates within_doc_mention_count.

For each citation:

IDENTIFICATION:
- raw_mention: exact text as it appears (use the most complete mention)
- raw_authors: full author names as written (null if unavailable)
- recovered_authors: list of last names only (null if cannot recover)
- recovered_title: paper/book title (null if cannot recover)
- recovered_year: publication year as integer (null if cannot recover).
    Use the JOURNAL PUBLICATION year, not the working paper year.
- recovered_first_version_year: ONLY if a working-paper year is explicitly
    written in the citation string itself (e.g., "NBER WP 2018, forthcoming JF").
    Do NOT infer. Do NOT guess. This field is primarily populated via OpenAlex
    post-join lookup — only capture what is literally in the text. null otherwise.
- recovered_journal: journal, publisher, or venue (null if cannot recover)
- recovered_doi: null — DOI is extracted via regex in code; set this to null always.
CITATION OBJECT — what type of thing is being cited:
- citation_object: ONE of:
    "idea_theory"     — a theoretical framework, model, or concept
    "empirical_result"— an empirical finding or dataset-based result
    "methodology"     — a statistical or computational method
    "data_source"     — a database, index, or data provider (CRSP, Compustat, etc.)
    "background"      — general context, historical reference, or literature review
    "other"           — anything that doesn't fit above

CONTEXT:
- citation_context: 1-2 sentence verbatim snippet showing HOW this work is cited.
    null for purely implicit mentions or bibliography-only entries.

CITATION FUNCTION — classify how this citation is used in the document's argument:
- citation_function: ONE of:
    "method_input"         — the paper's model, method, or factor is directly
                             applied or implemented (NOT valid if is_canonical=true)
    "empirical_evidence"   — cited as evidence for a factual claim or result
    "investment_rationale" — cited to motivate or justify an investment strategy
    "background_historical"— cited as context, background, or historical reference
    "canonical_reference"  — a foundational paper cited because it is standard to
                             do so, not because it is freshly applied
    "critique"             — cited to contrast with, criticise, or disagree with
    "decorative"           — cited in passing with no substantive engagement

CONSTRAINT: if is_canonical=true, citation_function MUST be one of:
"background_historical", "canonical_reference", or "decorative".
A canonical paper cannot simultaneously be a fresh method input.

CITATION POLARITY — how the practitioner engages with the cited work:
- citation_polarity: ONE of:
    "positive_building"   — the practitioner builds on or endorses the work
    "neutral_exposition"  — cited descriptively without taking a position
    "critical_engaged"    — the practitioner challenges or qualifies the work
                            but engages substantively with it
    "dismissive"          — cited to reject or dismiss, minimal engagement
    "unclear"             — context insufficient to determine polarity

- is_canonical: true if this paper is a foundational/seminal work that would be
    expected to appear in virtually any paper on this topic (e.g., Fama-French 1993
    in a factor paper, Black-Scholes in a derivatives paper, Markowitz in a
    portfolio construction paper). false otherwise. Mark true SPARINGLY.
    Most citations should be false.

QUALITY:
- confidence: "high", "medium", or "low"
    Use "low" only when you are guessing — e.g., an author name with no title,
    or a title fragment with no year. Low-confidence citations will be dropped
    from main analysis.
- resolution_type: ONE of:
    "formal_citation"     — full bibliographic entry in references section
    "informal_named"      — named in body text with enough info to identify
    "implicit"            — inferred from context, no direct name/year
    "references_section"  — appears only in bibliography, no body-text mention

LOCATION — where in the document this citation appears:
- citation_location: ONE of:
    "abstract", "introduction", "literature_review", "methodology",
    "results", "conclusion", "references_section", "footnote", "unknown"
- location_subtype: ONE of:
    "main_text"     — substantive body paragraph
    "footnote"      — footnote or endnote
    "figure_caption"— caption of a chart or figure
    "table_note"    — note below a table
    "appendix"      — appendix or supplementary material
    "bibliography"  — reference list entry only (no body mention)
    "unknown"       — cannot determine

FLAGS:
- is_academic: true ONLY if this work was published in the academic finance
    literature. The criterion is COMMUNITY MEMBERSHIP, not peer review.

    is_academic = TRUE: JF, JFE, RFS, JFQA, RF, JCF, JFI (academic), RAPS, RCFS,
      Management Science (finance papers), AER, QJE, JPE, Econometrica, ReStud,
      NBER working papers, SSRN working papers by finance academics,
      university working papers by finance faculty.

    is_academic = FALSE (practitioner or hybrid venues, even if peer-reviewed):
      FAJ, JPM, Journal of Investment Management, JAI, JII, JSI, JFI (practitioner),
      AQR White Papers, AQR Alternative Thinking, any firm white paper or report,
      any book (including Ilmanen "Expected Returns"), CFA Institute publications,
      practitioner conference proceedings, policy institution reports.

    HARD CONSTRAINT: if the cited work is clearly a firm white paper,
    practitioner journal article, or policy report, then is_academic MUST be false.

- is_self_citation: true if the cited work was published by the same institution
    as the source document (e.g., an AQR document citing an AQR white paper,
    a PIMCO document citing a PIMCO report). false for any other institution or academic work.

# NOTE: venue_tier is computed in post-processing from recovered_journal.

CLASSIFICATION:
- academic_subfield: PRIMARY subfield — ONE of:
    "asset_pricing"            (risk premia, factors, return predictability, derivatives)
    "corporate_finance"        (capital structure, M&A, governance, payout)
    "financial_intermediation" (banks, funds, credit, liquidity, systemic risk)
    "behavioral_finance"       (psychology, biases, anomalies, sentiment)
    "market_microstructure"    (trading, liquidity, bid-ask, HFT, price discovery)
    "macro_finance"            (macro-financial linkages, term structure, consumption)
    "other_academic"           (international finance, household finance, fintech, econometrics)
    "not_academic"             (practitioner pubs, white papers, books, data sources)

- secondary_academic_subfield: SECONDARY subfield for papers spanning two areas.
    null if cleanly in one subfield.

ADDITIONAL GUIDANCE:
- Data sources (CRSP, Compustat, Bloomberg, FactSet, MSCI indexes) → citation_object: "data_source",
  is_academic: false, academic_subfield: "not_academic"
- ML return prediction → "asset_pricing"
- NBER/SSRN working papers by finance faculty → is_academic: true
- AQR white paper cited in AQR document → is_academic: false, is_self_citation: true
- AQR white paper cited in non-AQR document → is_academic: false, is_self_citation: false
- FAJ/JPM → is_academic: false
- recovered_first_version_year: ONLY extract if explicitly stated in the text.
  Never infer from author or title knowledge.
Output ONLY valid JSON. No preamble, no markdown fences.

{
  "source": {
    "title": "...",
    "year": 2024,
    "source_topic": "...",
    "source_academic_subfield": "...",
    "doc_has_bibliography": true
  },
  "citations": [
    {
      "raw_mention": "...",
      "raw_authors": "...",
      "recovered_authors": ["..."],
      "recovered_title": "...",
      "recovered_year": 2020,
      "recovered_first_version_year": 2018,
      "recovered_journal": "...",
      "recovered_doi": null,
      "citation_object": "empirical_result",
      "citation_context": "...",
      "citation_function": "empirical_evidence",
      "citation_polarity": "positive_building",
      "is_canonical": false,
      "confidence": "high",
      "resolution_type": "references_section",
      "citation_location": "methodology",
      "location_subtype": "main_text",
      "is_academic": true,
      "is_self_citation": false,
      "academic_subfield": "asset_pricing",
      "secondary_academic_subfield": null
    }
  ]
}

Document text:
{text}"""


# Phase 0 provenance: hash the extraction contract as a string.
# This is the constant that defines what the LLM is being asked to do; a
# single-character change here produces a new prompt_hash.
PROMPT_HASH = hashlib.sha256(EXTRACTION_PROMPT.encode()).hexdigest()[:8]


# %% Single file processing (runs in thread)
# POST-PROCESSING NOTE: Within-document deduplication (collapsing multiple mentions
# of the same paper in one doc) is intentionally NOT done here. The LLM extracts
# all mentions; a separate script (src/post_processing/dedup.py) collapses them
# using fuzzy title+author matching keyed on doc_id. See gap_structure.md §Pipeline
# Notes for the dedup algorithm.
def process_file(file_path: Path) -> tuple[str, dict, list[dict], str]:
    path_str    = str(file_path)
    source_type = infer_source_type(file_path)

    if file_path.suffix.lower() == ".pdf":
        text, strategy, page_count, word_count, extraction_method, has_bibliography, ref_start_char, text_truncated, total_chars = extract_pdf_text(file_path)
    else:
        text, strategy, page_count, word_count, extraction_method, has_bibliography, ref_start_char, text_truncated, total_chars = extract_txt_text(file_path)
    institution = infer_institution(source_type)
    extraction_char_ratio = round(min(len(text), total_chars) / max(total_chars, 1), 4)

    # Code-derived publication year from filename/path — free, fast, 90%+ accurate
    year_match = re.search(r"(19[89]\d|20[012]\d)", file_path.stem + "/" + file_path.parent.name)
    source_year_path = int(year_match.group(1)) if year_match else None

    # Extract DOI candidates via regex before LLM call — eliminates LLM DOI hallucination
    doi_pattern = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")
    doi_candidates = list(set(doi_pattern.findall(text)))

    # Skip documents with no extractable text — scanned images with no text layer.
    # word_count==0 means PyMuPDF returned an empty string; sending to LLM is pure waste.
    if word_count == 0:
        source = {
            "title": file_path.stem, "year": None,
            "source_type": source_type, "source_institution": institution,
            "doc_id": make_doc_id(file_path),
            "source_topic": None, "source_academic_subfield": None,
            "doc_has_bibliography": False, "doc_page_count": page_count,
            "doc_word_count": 0, "doc_total_chars": total_chars,
            "doc_text_truncated": False, "doc_extraction_char_ratio": 0.0,
            "doc_text_strategy": strategy, "doc_ref_section_char": ref_start_char,
            "source_year_path": source_year_path, "doi_candidates": json.dumps([]),
            "pdf_extraction_method": extraction_method, "schema_version": SCHEMA_VERSION,
            "prompt_hash": PROMPT_HASH, "patterns_hash": PATTERNS_HASH, "code_version": CODE_VERSION,
            "llm_status": "skipped_no_text", "json_valid": None, "retry_count": 0,
            "extraction_note": None,
        }
        return path_str, source, [], "SKIPPED: no text extracted (likely scanned image)"

    # Patch 12 — skip PDFs flagged likely_scanned (wpp < 50 in extract_pdf_text).
    # Post-OCR corpus scan (2026-04-20) left 48 such docs across JFI/JOR/MSCI/RA.
    # Running the LLM on them wastes tokens and produces near-empty citation sets.
    # Row still lands in documents_deep.csv with llm_status="skipped_likely_scanned"
    # and extraction_note carrying the wpp value for audit. No citations emitted.
    # TXT files return extraction_method="html_derived" and are unaffected.
    if extraction_method == "likely_scanned":
        wpp = word_count / max(page_count, 1)
        source = {
            "title": file_path.stem, "year": None,
            "source_type": source_type, "source_institution": institution,
            "doc_id": make_doc_id(file_path),
            "source_topic": None, "source_academic_subfield": None,
            "doc_has_bibliography": has_bibliography, "doc_page_count": page_count,
            "doc_word_count": word_count, "doc_total_chars": total_chars,
            "doc_text_truncated": text_truncated, "doc_extraction_char_ratio": extraction_char_ratio,
            "doc_text_strategy": strategy, "doc_ref_section_char": ref_start_char,
            "source_year_path": source_year_path, "doi_candidates": json.dumps(doi_candidates),
            "pdf_extraction_method": extraction_method, "schema_version": SCHEMA_VERSION,
            "prompt_hash": PROMPT_HASH, "patterns_hash": PATTERNS_HASH, "code_version": CODE_VERSION,
            "llm_status": "skipped_likely_scanned", "json_valid": None, "retry_count": 0,
            "extraction_note": f"skipped_likely_scanned_wpp_{wpp:.1f}",
        }
        return path_str, source, [], f"SKIPPED: likely scanned (wpp={wpp:.1f})"

    prompt = EXTRACTION_PROMPT.replace("{text}", text)

    max_retries = 5
    retry_count = 0
    for attempt in range(max_retries):
        retry_count = attempt
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
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
                source["title"] = file_path.stem
            source["source_type"]             = source_type
            source["source_institution"]      = institution
            source["doc_id"]                  = make_doc_id(file_path)
            source["doc_page_count"]          = page_count
            source["doc_word_count"]          = word_count
            source["doc_has_bibliography"]    = has_bibliography
            source["doc_total_chars"]         = total_chars
            source["doc_text_truncated"]      = text_truncated
            source["doc_extraction_char_ratio"] = extraction_char_ratio
            source["doc_text_strategy"]       = strategy
            source["doc_ref_section_char"]    = ref_start_char
            source["source_year_path"]        = source_year_path
            source["doi_candidates"]          = json.dumps(doi_candidates)
            source["pdf_extraction_method"]   = extraction_method
            source["schema_version"]          = SCHEMA_VERSION
            source["prompt_hash"]             = PROMPT_HASH
            source["patterns_hash"]           = PATTERNS_HASH
            source["code_version"]            = CODE_VERSION
            source["llm_status"]              = "ok"
            source["json_valid"]              = True
            source["retry_count"]             = retry_count
            source["extraction_note"]         = None

            status = (f"OK | {source.get('title', '')[:45]} ({source.get('year')}) "
                      f"| {len(citations)} citations")
            return path_str, source, citations, status

        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"  Rate limit — waiting {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue

        except json.JSONDecodeError as e:
            source = {
                "title": file_path.stem, "year": None,
                "source_type": source_type, "source_institution": institution,
                "doc_id": make_doc_id(file_path),
                "source_topic": None, "source_academic_subfield": None,
                "doc_has_bibliography": has_bibliography, "doc_page_count": page_count,
                "doc_word_count": word_count, "doc_total_chars": total_chars,
                "doc_text_truncated": text_truncated, "doc_extraction_char_ratio": extraction_char_ratio,
                "doc_text_strategy": strategy, "doc_ref_section_char": ref_start_char,
                "source_year_path": source_year_path, "doi_candidates": json.dumps(doi_candidates),
                "pdf_extraction_method": extraction_method, "schema_version": SCHEMA_VERSION,
                "prompt_hash": PROMPT_HASH, "patterns_hash": PATTERNS_HASH, "code_version": CODE_VERSION,
                "llm_status": "json_error", "json_valid": False, "retry_count": retry_count,
                "extraction_note": None,
            }
            return path_str, source, [], f"JSON_ERROR: {e}"

        except Exception as e:
            source = {
                "title": file_path.stem, "year": None,
                "source_type": source_type, "source_institution": institution,
                "doc_id": make_doc_id(file_path),
                "source_topic": None, "source_academic_subfield": None,
                "doc_has_bibliography": has_bibliography, "doc_page_count": page_count,
                "doc_word_count": word_count, "doc_total_chars": total_chars,
                "doc_text_truncated": text_truncated, "doc_extraction_char_ratio": extraction_char_ratio,
                "doc_text_strategy": strategy, "doc_ref_section_char": ref_start_char,
                "source_year_path": source_year_path, "doi_candidates": json.dumps(doi_candidates),
                "pdf_extraction_method": extraction_method, "schema_version": SCHEMA_VERSION,
                "prompt_hash": PROMPT_HASH, "patterns_hash": PATTERNS_HASH, "code_version": CODE_VERSION,
                "llm_status": "api_error", "json_valid": False, "retry_count": retry_count,
                "extraction_note": None,
            }
            return path_str, source, [], f"API_ERROR: {e}"

    source = {
        "title": file_path.stem, "year": None,
        "source_type": source_type, "source_institution": institution,
        "doc_id": make_doc_id(file_path),
        "source_topic": None, "source_academic_subfield": None,
        "doc_has_bibliography": has_bibliography, "doc_page_count": page_count,
        "doc_word_count": word_count, "doc_total_chars": total_chars,
        "doc_text_truncated": text_truncated, "doc_extraction_char_ratio": extraction_char_ratio,
        "doc_text_strategy": strategy, "doc_ref_section_char": ref_start_char,
        "source_year_path": source_year_path, "doi_candidates": json.dumps(doi_candidates),
        "pdf_extraction_method": extraction_method, "schema_version": SCHEMA_VERSION,
        "prompt_hash": PROMPT_HASH, "patterns_hash": PATTERNS_HASH, "code_version": CODE_VERSION,
        "llm_status": "max_retries", "json_valid": False, "retry_count": max_retries,
        "extraction_note": None,
    }
    return path_str, source, [], "FAILED: max retries exceeded"


# %% Main pipeline
def run_pipeline(pdfs_only: bool = False, n: int | None = None, workers: int = MAX_WORKERS, source_start: str | None = None, randomize: bool = False) -> pd.DataFrame:
    """
    Collect files, process with thread pool, save incrementally.
    By default processes both PDFs and Other_Corpus .txt files.
    --pdf: PDFs only  |  --n N: stop after N files  |  --workers W: parallel workers
    """
    files: list[Path] = sorted(PDF_DIR.glob("**/*.pdf"))

    if not pdfs_only:
        txt_files = sorted(OTHER_DIR.glob("**/*.txt"))
        txt_files = [f for f in txt_files if not f.name.startswith("_")]
        files += txt_files

    if not files:
        print(f"No files found under {PDF_DIR}" +
              ("" if pdfs_only else f" or {OTHER_DIR}"))
        return pd.DataFrame()

    processed_paths:  set[str] = set()    # fast path-based resume filter
    processed_doc_ids: set[str] = set()    # doc_id set for dedup (from existing CSV)
    all_citations:     list[dict] = []
    all_documents:     list[dict] = []
    save_lock = threading.Lock()

    if OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH)
        if "source_file" in existing.columns:
            processed_paths = set(existing["source_file"].dropna().unique())
        if "doc_id" in existing.columns:
            processed_doc_ids = set(existing["doc_id"].dropna().unique())
        # Normalize schema_version to string — prevents float/string mismatch warning
        if "schema_version" in existing.columns:
            existing["schema_version"] = existing["schema_version"].astype(str)
        all_citations = existing.to_dict("records")
        print(f"Resuming — {len(processed_paths)} files already processed")

    if DOC_OUT_PATH.exists():
        all_documents = pd.read_csv(DOC_OUT_PATH).to_dict("records")

    # --source filter: skip all subfolders alphabetically before source_start
    if source_start:
        prefix = source_start.lower()
        folders = sorted(set(f.parent.name.lower() for f in files))
        start_folder = next((fo for fo in folders if fo.startswith(prefix)), None)
        if start_folder is None:
            print(f"[warn] --source '{source_start}' matched no subfolder. Available: {folders}")
        else:
            files = [f for f in files if f.parent.name.lower() >= start_folder]
            print(f"  --source filter: starting from '{start_folder}' ({len(files)} files remain)")

    # Path-based resume filter — O(1) string lookup, no disk I/O for all files.
    # doc_id is still computed per-file during processing for dedup and integrity.
    pending = [f for f in files if str(f) not in processed_paths]
    if randomize:
        import random as _random
        _random.shuffle(pending)
    if n is not None:
        pending = pending[:n]

    subfolders = sorted(set(f.parent.name for f in files))
    n_pdf = sum(1 for f in files if f.suffix.lower() == ".pdf")
    n_txt = sum(1 for f in files if f.suffix.lower() == ".txt")
    print(f"Corpus         : {n_pdf} PDFs  |  {n_txt} TXTs  |  subfolders: {subfolders}")
    print(f"Pending        : {len(pending)}  |  Workers: {workers}")
    print(f"Schema version : {SCHEMA_VERSION}")
    print(f"Provenance     : prompt={PROMPT_HASH} patterns={PATTERNS_HASH} code={CODE_VERSION}\n")

    if not pending:
        print("All files already processed.")
        return pd.DataFrame(all_citations)

    completed = 0
    pipeline_start = time.time()

    def save_result(path_str, source, citations):
        # Document-level row — always written, even for zero-citation documents
        doc_row = {
            "doc_id":                     source.get("doc_id"),
            "source_file":                path_str,
            "source_year":                source.get("year"),
            "source_year_path":           source.get("source_year_path"),
            "source_title":               source.get("title"),
            "source_type":                source.get("source_type"),
            "source_institution":         source.get("source_institution"),
            "source_topic":               source.get("source_topic"),
            "source_academic_subfield":   source.get("source_academic_subfield"),
            "doc_has_bibliography":       source.get("doc_has_bibliography"),
            "doc_page_count":             source.get("doc_page_count"),
            "doc_word_count":             source.get("doc_word_count"),
            "doc_total_chars":            source.get("doc_total_chars"),
            "doc_text_truncated":         source.get("doc_text_truncated"),
            "doc_extraction_char_ratio":  source.get("doc_extraction_char_ratio"),
            "doc_text_strategy":          source.get("doc_text_strategy"),
            "doc_ref_section_char":       source.get("doc_ref_section_char"),
            "doc_citation_count":         len(citations),
            "pdf_extraction_method":      source.get("pdf_extraction_method"),
            "llm_status":                 source.get("llm_status"),
            "json_valid":                 source.get("json_valid"),
            "retry_count":                source.get("retry_count"),
            "extraction_note":            source.get("extraction_note"),
            "schema_version":             source.get("schema_version"),
            "prompt_hash":                source.get("prompt_hash"),
            "patterns_hash":              source.get("patterns_hash"),
            "code_version":               source.get("code_version"),
        }

        # Citation-level rows — only written when citations exist
        citation_rows = []
        for c in citations:
            row = {
                "doc_id":                   source.get("doc_id"),
                "source_file":              path_str,
                "source_year":              source.get("year"),
                "source_year_path":         source.get("source_year_path"),
                "source_title":             source.get("title"),
                "source_type":              source.get("source_type"),
                "source_institution":       source.get("source_institution"),
                "source_topic":             source.get("source_topic"),
                "source_academic_subfield": source.get("source_academic_subfield"),
                "doc_has_bibliography":     source.get("doc_has_bibliography"),
                "doc_page_count":           source.get("doc_page_count"),
                "doc_word_count":           source.get("doc_word_count"),
                "doc_total_chars":          source.get("doc_total_chars"),
                "doc_text_truncated":       source.get("doc_text_truncated"),
                "doc_extraction_char_ratio":source.get("doc_extraction_char_ratio"),
                "doc_text_strategy":        source.get("doc_text_strategy"),
                "doc_ref_section_char":     source.get("doc_ref_section_char"),
                "doc_citation_count":       len(citations),
                "doi_candidates":           source.get("doi_candidates"),
                "pdf_extraction_method":    source.get("pdf_extraction_method"),
                "schema_version":           source.get("schema_version"),
                "prompt_hash":              source.get("prompt_hash"),
                "patterns_hash":            source.get("patterns_hash"),
                "code_version":             source.get("code_version"),
            }
            row.update(c)
            citation_rows.append(row)

        with save_lock:
            all_documents.append(doc_row)
            all_citations.extend(citation_rows)
            pd.DataFrame(all_documents).to_csv(DOC_OUT_PATH, index=False)
            if citation_rows:
                pd.DataFrame(all_citations).to_csv(OUT_PATH, index=False)

    # Track per-file timing and per-thread cumulative totals
    file_timing:       dict = {}
    thread_cumulative: dict = {}
    thread_worker_id:  dict = {}
    thread_lock = threading.Lock()

    def timed_process(f: Path):
        tid     = threading.get_ident()
        t0      = time.time()
        result  = process_file(f)
        elapsed = time.time() - t0
        with thread_lock:
            if tid not in thread_worker_id:
                thread_worker_id[tid] = len(thread_worker_id) + 1
            thread_cumulative[tid] = thread_cumulative.get(tid, 0.0) + elapsed
            file_timing[str(f)]    = (tid, elapsed)
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(timed_process, f): f for f in pending}
        for future in as_completed(futures):
            completed += 1
            file_path = futures[future]
            try:
                path_str, source, citations, status = future.result()
            except Exception as e:
                print(f"[{completed}/{len(pending)}] THREAD ERROR {file_path.name}: {e}")
                continue
            tid, file_secs = file_timing.get(str(file_path), (None, 0.0))
            cumul_secs     = thread_cumulative.get(tid, 0.0)
            worker_id      = thread_worker_id.get(tid, "?")
            print(f"[{completed}/{len(pending)}] worker {worker_id} | {file_path.parent.name}/{file_path.name}")
            print(f"  {status}")
            print(f"  time: {file_secs:.1f}s | {cumul_secs/60:.1f}m cumulative")
            save_result(path_str, source, citations)

    return pd.DataFrame(all_citations)


# %% Summary stats
def print_summary(df: pd.DataFrame):
    if df.empty:
        print("No data.")
        return

    # Drop low-confidence rows from counts (they're still in the CSV)
    df_main = df[df["confidence"] != "low"].copy() if "confidence" in df.columns else df

    print("\n=== SUMMARY STATISTICS ===")
    # Summary uses documents_deep.csv for doc-level stats if available
    if DOC_OUT_PATH.exists():
        df_docs = pd.read_csv(DOC_OUT_PATH)
        print(f"Documents processed          : {len(df_docs)}")
        print(f"  ok extractions             : {(df_docs['llm_status'] == 'ok').sum()}")
        print(f"  json errors                : {(df_docs['llm_status'] == 'json_error').sum()}")
        print(f"  api errors                 : {(df_docs['llm_status'] == 'api_error').sum()}")
        print(f"  zero citations (ok)        : {((df_docs['llm_status']=='ok') & (df_docs['doc_citation_count']==0)).sum()}")
        print(f"  truncated docs             : {df_docs['doc_text_truncated'].sum()}")
        print(f"  avg extraction_char_ratio  : {df_docs['doc_extraction_char_ratio'].mean():.2f}")
        print()

    print(f"Total citations (all)        : {len(df)}")
    print(f"High/medium confidence       : {len(df_main)}")
    print(f"Low confidence (excluded)    : {len(df) - len(df_main)}")
    print(f"Academic                     : {df_main['is_academic'].sum()}")
    print(f"Non-academic                 : {(~df_main['is_academic']).sum()}")
    print(f"Self-citations               : {df_main['is_self_citation'].sum() if 'is_self_citation' in df_main.columns else 0}")
    print(f"Canonical citations          : {df_main['is_canonical'].sum()}")
    print(f"Non-canonical academic       : {(df_main['is_academic'] & ~df_main['is_canonical']).sum()}")
    print(f"First version year captured  : {df_main['recovered_first_version_year'].notna().sum()}")
    print(f"DOI recovered                : {df_main['recovered_doi'].notna().sum()}")

    if "pdf_extraction_method" in df.columns:
        print("\nExtraction method (unique docs):")
        doc_methods = df.drop_duplicates("doc_id")["pdf_extraction_method"].value_counts()
        print(doc_methods.to_string())

    if "schema_version" in df.columns:
        sv = df["schema_version"].value_counts()
        if len(sv) > 1:
            print(f"\n[WARN] Multiple schema versions in output: {sv.to_dict()}")
        else:
            print(f"\nSchema version: {sv.index[0]}")

    print("\nVenue tiers (post-processing field — empty until dedup.py runs):")
    if "venue_tier" in df_main.columns:
        print(df_main["venue_tier"].value_counts().to_string())
    else:
        print("  (not yet computed — run post_processing/dedup.py)")

    print("\nCitation objects:")
    if "citation_object" in df_main.columns:
        print(df_main["citation_object"].value_counts().to_string())

    print("\nCitation functions:")
    print(df_main["citation_function"].value_counts().to_string())

    print("\nCitation polarity:")
    if "citation_polarity" in df_main.columns:
        print(df_main["citation_polarity"].value_counts().to_string())

    print("\nSource types:")
    print(df_main.groupby("source_type")["source_title"].nunique().to_string())

    print("\nTop 10 cited journals/venues:")
    print(df_main["recovered_journal"].value_counts().head(10).to_string())

    print("\nAcademic subfields (academic, excl. AQR, excl. canonical):")
    mask = df_main["is_academic"] & ~df_main["is_self_citation"] & ~df_main["is_canonical"]
    print(df_main[mask]["academic_subfield"].value_counts().to_string())

    print("\nCitations per source year:")
    print(df_main.groupby("source_year").size().sort_index().to_string())

    print("\nTop 15 cited authors:")
    authors = []
    for val in df_main["recovered_authors"].dropna():
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            if isinstance(parsed, list):
                authors.extend(parsed)
        except (json.JSONDecodeError, TypeError):
            continue
    for author, count in Counter(authors).most_common(15):
        print(f"  {author}: {count}")


# %% Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GAP deep citation extraction pipeline")
    parser.add_argument(
        "--pdf",
        action="store_true",
        help="Process only PDFs under data/pdfs/; skip Other_Corpus .txt files",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=None,
        metavar="N",
        help="Stop after processing N files (useful for testing)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        metavar="W",
        help="Number of parallel workers (default: 2)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        metavar="SRC",
        help="Start from the first subfolder whose name starts with SRC (case-insensitive). "
             "Files from earlier subfolders are skipped. E.g. --source aqr",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Shuffle pending files randomly before processing. "
             "If combined with --source, shuffles only files from that source onward.",
    )
    args = parser.parse_args()

    if args.pdf:
        print("Mode: PDFs only (data/pdfs/**/*.pdf)")
    else:
        print("Mode: PDFs + Other_Corpus (data/pdfs/**/*.pdf + data/Other_Corpus/**/*.txt)")
    if args.source:
        print(f"Source filter : starting from subfolders matching '{args.source}*'")
    if args.random:
        print(f"Order         : random")

    df = run_pipeline(pdfs_only=args.pdf, n=args.n, workers=args.workers, source_start=args.source, randomize=args.random)
    if not df.empty:
        print_summary(df)