"""
01c_resolve.py — Phase 1 shadow-column resolver + venue_scope.

Runs downstream of 01b_merge.py (LLM + canonical structural merge) and
dedup.py (within-doc dedup, if run). Adds shadow columns for the three
Phase 1 fields moving from LLM to code:

  - is_academic : venue-table cascade on recovered_journal
  - is_canonical: propagated from canonical_pattern_id column that
                  01b_merge.py transfers onto matched LLM rows (and
                  carries on unmatched canonical rows). Respects
                  likely_metric_only flag.
  - source_year : path -> frontmatter -> LLM -> pdf_metadata -> null
                  (requires documents CSV; skipped if absent)

source_year cascade detail (2026-04-21 second revision):
  Tier 1: path regex on filename/folder                      -> "path"
  Tier 2: frontmatter regex on first 3000 chars              -> "frontmatter"
  Tier 3: LLM-extracted source_year (from 01_extract_deep)   -> "llm"
  Tier 4: PyMuPDF creationDate metadata                      -> "pdf_metadata"
  Tier 5: none                                               -> "none"

pdf_metadata is intentionally the LAST-RESORT tier, not a primary code
tier. The 2026-04-21 audit showed 12/12 pdf_metadata disagreements were
LLM-correct — the failure mode is publisher bulk-digitization stamping
the scan year as creationDate rather than the paper's actual publication
year (e.g., JPM vol. 14, 1988 with creationDate 2004 because Portfolio
Management Research bulk-digitized their back catalog in 2004). The
shadow column source_year_pdf_metadata is still emitted for audit.

Frontmatter patterns were also tightened in this revision: the original
bare "Month YYYY" pattern matched body-text years (e.g., "February 1992"
in a 2023 JACF article on monetary history → 31-year miss). Bare
Month+Year removed; replaced with publication-context-gated variants and
journal-standard Vol/No/Issue and Season patterns. Restricted bare
Month+Year retained inside the first 400 chars (title-block window).

Also emits `venue_scope`, a code-only additive classification orthogonal
to is_academic. Values:
  finance_core / finance_adjacent / finance_practitioner /
  non_finance_academic / non_finance_other / None

Each shadow field gets split into {field}_llm (input value) and
{field}_code (newly derived), then coalesced back into {field} with
{field}_source recording which tier produced the final value.

Citations input auto-detection (first match wins):
  output/citations_merged_deduped.csv   (01b + dedup)
  output/citations_merged.csv           (01b only; canonical_scraper.py live)
  output/citations_deep_deduped.csv     (dedup only; 01b not yet runnable)
  output/citations_deep.csv             (raw LLM extraction only)

Documents input auto-detection:
  --docs flag (override)
  <citations_dir>/documents_deep.csv    (co-located with citations)
  output/documents_deep.csv             (default)
  none                                  (skip source_year cascade, warn)

Outputs:
  output/citations_resolved.csv
  output/documents_resolved.csv         (only if documents input was found)

Phase 1 blocker: canonical_scraper.py not yet implemented. Until it lands,
01b_merge.py cannot run (it requires canonical_citations.csv). In that state
01c_resolve.py falls back to reading citations_deep_deduped.csv or
citations_deep.csv directly; is_canonical_code is all-null and coalesce
falls through to LLM.

Runs AFTER 01b_merge.py + dedup.py (when both exist) and BEFORE 03_join.py.
Downstream scripts should read citations_resolved.csv.
"""

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

# --------------------------------------------------------------------------- #
# Paths — anchored to gap/src/01c_resolve.py
# --------------------------------------------------------------------------- #
REPO_ROOT = Path(__file__).resolve().parent.parent  # gap/
OUT_DIR   = REPO_ROOT / "output"
SRC_DIR   = REPO_ROOT / "src"

DOCUMENTS_IN_DEFAULT = OUT_DIR / "documents_deep.csv"

CITATIONS_INPUT_CANDIDATES = [
    OUT_DIR / "citations_merged_deduped.csv",  # 01b + dedup (preferred)
    OUT_DIR / "citations_merged.csv",          # 01b only
    OUT_DIR / "citations_deep_deduped.csv",    # dedup only
    OUT_DIR / "citations_deep.csv",            # raw
]

DOCUMENTS_OUT = OUT_DIR / "documents_resolved.csv"
CITATIONS_OUT = OUT_DIR / "citations_resolved.csv"

VENUES_YAML = SRC_DIR / "post_processing" / "venues.yaml"

SCHEMA_VERSION = "2.2"


# --------------------------------------------------------------------------- #
# Venue classification (is_academic cascade + venue_scope)
# --------------------------------------------------------------------------- #
def _norm(s) -> str | None:
    """Normalize a venue string for matching: lowercase, replace & with and,
    strip parens/punct/volume, collapse whitespace, strip leading 'the'."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).lower()
    s = s.replace("&", "and")
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r",?\s*(vol\.?|volume|issue|no\.?|number)\s.*$", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = " ".join(s.split())
    s = re.sub(r"^the\s+", "", s)
    return s or None


def load_venues(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"venues.yaml not found at {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {
        "academic":               {_norm(v) for v in data.get("academic", [])},
        "practitioner":           {_norm(v) for v in data.get("practitioner", [])},
        "aliases":                {_norm(k): _norm(v)
                                   for k, v in (data.get("aliases") or {}).items()},
        "ambiguous":              {_norm(v) for v in data.get("ambiguous", [])},
        "working_paper_patterns": [_norm(v) for v in data.get("working_paper_patterns", [])],
        "scopes":                 {scope_name: {_norm(v) for v in venue_list}
                                   for scope_name, venue_list in (data.get("scopes") or {}).items()},
    }


def classify_is_academic(venue, venues_cfg: dict):
    """Returns True | False | None. None means unknown — defer to LLM."""
    n = _norm(venue)
    if n is None:
        return None
    n = venues_cfg["aliases"].get(n, n)
    if n in venues_cfg["ambiguous"]:
        return None
    if n in venues_cfg["academic"]:
        return True
    if n in venues_cfg["practitioner"]:
        return False
    for wp in venues_cfg["working_paper_patterns"]:
        if wp and wp in n:
            return None
    return None


def classify_venue_scope(venue, venues_cfg: dict) -> str | None:
    """Finer-grained scope classification; orthogonal to is_academic.

    Priority: explicit scope override > default by academic/practitioner > None.
    Returns one of:
      finance_core, finance_adjacent, finance_practitioner,
      non_finance_academic, non_finance_other, None
    """
    n = _norm(venue)
    if n is None:
        return None
    n = venues_cfg["aliases"].get(n, n)
    for scope_name, venue_set in venues_cfg["scopes"].items():
        if n in venue_set:
            return scope_name
    if n in venues_cfg["academic"]:
        return "finance_adjacent"
    if n in venues_cfg["practitioner"]:
        return "finance_practitioner"
    return None


# --------------------------------------------------------------------------- #
# source_year cascade — frontmatter regex + PyMuPDF metadata
# --------------------------------------------------------------------------- #
_YEAR_RE = r"(19[89]\d|20[012]\d)"  # 1980–2029

# Publication-context tokens that, when adjacent, authorize a Month-Year or
# bare-year match. The goal is to match journal frontmatter ("Published
# March 2023") without matching body text ("the February 1992 crisis").
_PUB_CONTEXT = (
    r"(?:published|issued|released|appeared|publication\s+date|"
    r"print\s+edition|online\s+edition|first\s+online|"
    r"received|accepted|revised|submitted)"
)

_MONTHS = (
    r"(?:january|february|march|april|may|june|july|august|"
    r"september|october|november|december)"
)

FRONTMATTER_PATTERNS = [
    # 1. Copyright © YYYY / © YYYY
    re.compile(rf"\u00a9\s*(?:copyright\s+)?{_YEAR_RE}", re.IGNORECASE),

    # 2. Copyright YYYY (with or without symbol)
    re.compile(rf"copyright\s*(?:\u00a9\s*)?{_YEAR_RE}", re.IGNORECASE),

    # 3. First published YYYY
    re.compile(rf"first\s+published[^\n]{{0,40}}{_YEAR_RE}", re.IGNORECASE),

    # 4. Publication-context + optional Month + YYYY
    #    e.g. "Published March 2023", "Issued 2021", "Received January 2019".
    #    Replaces the prior bare Month+YYYY pattern that caused the 31-year miss.
    re.compile(
        rf"{_PUB_CONTEXT}[^\n]{{0,30}}?(?:{_MONTHS}\s+)?{_YEAR_RE}",
        re.IGNORECASE,
    ),

    # 5. Journal-standard Vol/No/Issue + YYYY
    #    e.g. "Vol. 74, No. 3 (2018)", "Volume 12 Issue 4 2021"
    re.compile(
        rf"vol(?:\.|ume)?\s*\d+[^\n]{{0,5}}?(?:no\.?|number|issue|iss\.?)\s*\d+"
        rf"[^\n]{{0,40}}?{_YEAR_RE}",
        re.IGNORECASE,
    ),

    # 6. Season + YYYY (quarterlies: Winter/Spring/Summer/Fall/Autumn)
    re.compile(
        rf"\b(?:winter|spring|summer|fall|autumn)\s+{_YEAR_RE}",
        re.IGNORECASE,
    ),

    # 7. Q1..Q4 + YYYY
    re.compile(rf"\bq[1-4]\s+{_YEAR_RE}", re.IGNORECASE),

    # 8. Restricted bare Month+YYYY — only if in first 400 chars (title block).
    #    Applied by extract_frontmatter_year separately; see below.
]


def extract_frontmatter_year(text: str):
    """Returns a publication year from frontmatter, or None.

    Tier A: scan first 3000 chars with publication-context-gated patterns.
    Tier B: scan first 400 chars with permissive bare-Month+Year pattern
            (title-block window where body-text collisions are rare).
    """
    if not text:
        return None
    head = text[:3000]

    # Tier A — context-gated patterns over the full head
    for pat in FRONTMATTER_PATTERNS:
        m = pat.search(head)
        if m:
            year = int(m.group(1))
            if 1980 <= year <= 2029:
                return year

    # Tier B — restricted bare Month+Year inside the first 400 chars.
    # Inside the title block the risk of hitting body-text years is low.
    title_block = text[:400]
    m = re.search(rf"\b{_MONTHS}\s+{_YEAR_RE}", title_block, re.IGNORECASE)
    if m:
        year = int(m.group(1))
        if 1980 <= year <= 2029:
            return year

    return None


def extract_pdf_metadata_year(source_file, current_year: int | None = None):
    """Extract publication year from PDF creationDate metadata.

    PDFs store dates as 'D:YYYYMMDDHHmmSS...'. The creation date is often
    but not always the publication date — PDFs can be re-exported or
    re-stamped. We accept only years in [1990, current_year + 1] to filter:
      - very old defaults (some tools stamp 1904/1970 when unset)
      - forward-looking stamps from buggy exporters
    Returns int or None.
    """
    if current_year is None:
        current_year = datetime.now().year
    try:
        if source_file is None or (isinstance(source_file, float) and pd.isna(source_file)):
            return None
        path = Path(source_file)
        if not path.exists() or path.suffix.lower() != ".pdf":
            return None
        import fitz  # PyMuPDF
        doc = fitz.open(path)
        meta = doc.metadata or {}
        doc.close()
        cdate = str(meta.get("creationDate", ""))
        if cdate.startswith("D:") and len(cdate) >= 6:
            try:
                y = int(cdate[2:6])
                if 1990 <= y <= current_year + 1:
                    return y
            except ValueError:
                pass
    except Exception:
        pass
    return None


def load_head(source_file, n_chars: int = 3000) -> str:
    try:
        if source_file is None or (isinstance(source_file, float) and pd.isna(source_file)):
            return ""
        path = Path(source_file)
        if not path.exists():
            return ""
        if path.suffix.lower() == ".pdf":
            import fitz  # PyMuPDF
            doc = fitz.open(path)
            parts, acc = [], 0
            for i in range(min(3, len(doc))):
                t = doc[i].get_text()
                parts.append(t)
                acc += len(t)
                if acc >= n_chars:
                    break
            doc.close()
            return "".join(parts)[:n_chars]
        else:
            return path.read_text(encoding="utf-8", errors="ignore")[:n_chars]
    except Exception:
        return ""


def compute_source_year_code(source_file, source_year_path):
    """Code cascade: path -> frontmatter. pdf_metadata is NOT a code tier
    after the 2026-04-21 audit (see module docstring). It's still computed
    here and returned for the shadow column, but not chosen as the code
    answer; coalesce_source_year uses it only as a last-resort tier after
    the LLM value.

    Returns (year, source_label, fm_year, md_year). fm_year and md_year
    are preserved as shadow columns regardless of which tier won.
    """
    # Compute both auxiliary signals once per doc for the shadow columns.
    md = extract_pdf_metadata_year(source_file)
    fm = extract_frontmatter_year(load_head(source_file))

    # Tier 1: path regex (already computed upstream)
    if pd.notna(source_year_path):
        try:
            return int(source_year_path), "path", fm, md
        except (ValueError, TypeError):
            pass

    # Tier 2: frontmatter (tightened patterns — ~87% precision in audit)
    if fm is not None:
        return fm, "frontmatter", fm, md

    # pdf_metadata NOT returned here — demoted to after-LLM tier in coalesce
    return None, None, fm, md


# --------------------------------------------------------------------------- #
# Coalesce helpers
# --------------------------------------------------------------------------- #
def _to_bool_or_none(v):
    """Defensive bool parsing — CSV round-trips turn True/False into 'True'/'False'
    or numpy.bool_ or object. Normalize to Python bool or None."""
    if v is None:
        return None
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "t"):
            return True
        if s in ("false", "0", "no", "f"):
            return False
    return None


def coalesce_is_academic(code, llm):
    """Code precedence; LLM fallback when code is unknown."""
    c = _to_bool_or_none(code)
    if c is not None:
        return c
    return _to_bool_or_none(llm)


def resolve_is_canonical(code, llm, likely_metric_only):
    """
    Phase 1 priority order:
      1. code == True                   -> True, "code"
      2. likely_metric_only == True     -> True, "structural_uncertain"
      3. llm == True                    -> True, "llm"
      4. llm == False                   -> False, "llm"
      5. else                           -> None, "none"

    (2) preserves the row-level assertion from 01b_merge.py for unmatched
    canonical rows that hit a METRIC_AMBIGUOUS pattern — the structural
    is_canonical=True was set deliberately so the row survives; the
    likely_metric_only flag is the downstream filter signal.
    """
    c    = _to_bool_or_none(code)
    l    = _to_bool_or_none(llm)
    mo   = _to_bool_or_none(likely_metric_only)
    if c is True:
        return True, "code"
    if mo is True:
        return True, "structural_uncertain"
    if l is True:
        return True, "llm"
    if l is False:
        return False, "llm"
    return None, "none"


def coalesce_source_year(code, llm, md):
    """Cascade: code (path/frontmatter) -> LLM -> pdf_metadata -> None.

    pdf_metadata demoted to last-resort tier after the 2026-04-21 audit
    found 12/12 pdf_metadata disagreements were LLM-correct, driven by
    publisher bulk-digitization stamping the scan year as creationDate
    (JPM/JFI back catalog → 2004 and 2009 on papers from the 80s-90s).
    """
    if pd.notna(code):
        try:
            return int(code)
        except (ValueError, TypeError):
            pass
    if pd.notna(llm):
        try:
            return int(llm)
        except (ValueError, TypeError):
            pass
    if pd.notna(md):
        try:
            return int(md)
        except (ValueError, TypeError):
            pass
    return None


# --------------------------------------------------------------------------- #
# Input auto-detection
# --------------------------------------------------------------------------- #
def pick_citations_input(override: Path | None) -> Path:
    if override is not None:
        if not override.exists():
            raise FileNotFoundError(f"--input path does not exist: {override}")
        return override
    for p in CITATIONS_INPUT_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError(
        "No citations input found. Tried:\n  " +
        "\n  ".join(str(p) for p in CITATIONS_INPUT_CANDIDATES)
    )


def pick_documents_input(override: Path | None, citations_path: Path) -> Path | None:
    """Resolve documents path: CLI flag > co-located with citations > default > None."""
    if override is not None:
        if not override.exists():
            raise FileNotFoundError(f"--docs path does not exist: {override}")
        return override
    colocated = citations_path.parent / "documents_deep.csv"
    if colocated.exists():
        return colocated
    if DOCUMENTS_IN_DEFAULT.exists():
        return DOCUMENTS_IN_DEFAULT
    return None


def describe_input_pipeline_state(path: Path) -> str:
    """Human-readable description of what pipeline stages produced this input."""
    name = path.name
    return {
        "citations_merged_deduped.csv": "01b_merge + dedup (full Phase 1 path)",
        "citations_merged.csv":         "01b_merge only (no dedup)",
        "citations_deep_deduped.csv":   "dedup only (01b not yet runnable — "
                                        "canonical_scraper.py pending)",
        "citations_deep.csv":           "raw LLM extraction only "
                                        "(neither 01b nor dedup run)",
    }.get(name, f"unrecognized ({name})")


# --------------------------------------------------------------------------- #
# Main resolver
# --------------------------------------------------------------------------- #
def main(citations_in: Path | None = None, docs_in: Path | None = None):
    t0 = datetime.now()
    print(f"=== 01c_resolve.py  |  schema v{SCHEMA_VERSION}  |  "
          f"{t0.isoformat(timespec='seconds')} ===\n")

    citations_in = pick_citations_input(citations_in)
    print(f"Citations input          : {citations_in}")
    print(f"  pipeline state          : {describe_input_pipeline_state(citations_in)}")

    docs_in = pick_documents_input(docs_in, citations_in)

    # Load configs --------------------------------------------------------- #
    print(f"\nLoading venues config    : {VENUES_YAML}")
    venues_cfg = load_venues(VENUES_YAML)
    venues_hash = hashlib.sha256(VENUES_YAML.read_bytes()).hexdigest()[:8]
    print(f"  venues_hash            : {venues_hash}")
    print(f"  academic venues        : {len(venues_cfg['academic'])}")
    print(f"  practitioner venues    : {len(venues_cfg['practitioner'])}")
    print(f"  aliases                : {len(venues_cfg['aliases'])}")
    print(f"  ambiguous              : {len(venues_cfg['ambiguous'])}")
    scope_totals = {k: len(v) for k, v in venues_cfg["scopes"].items()}
    print(f"  scopes                 : {scope_totals}")

    # Load frames ---------------------------------------------------------- #
    if docs_in is not None:
        print(f"\nLoading documents        : {docs_in}")
        docs = pd.read_csv(docs_in, low_memory=False)
        print(f"  rows                   : {len(docs)}")
    else:
        print(f"\n[warn] No documents CSV found. Skipping source_year cascade.")
        print(f"       Looked for:")
        print(f"         {citations_in.parent / 'documents_deep.csv'}")
        print(f"         {DOCUMENTS_IN_DEFAULT}")
        print(f"       is_academic and is_canonical still run on citations.")
        docs = None

    print(f"Loading citations        : {citations_in.name}")
    cites = pd.read_csv(citations_in, low_memory=False)
    print(f"  rows                   : {len(cites)}")

    # Ensure merge-layer columns exist even when 01b wasn't run ------------ #
    for col, default in [
        ("citation_source",          "llm"),
        ("within_doc_mention_count", pd.NA),
        ("canonical_pattern_id",     pd.NA),
        ("likely_metric_only",       False),
    ]:
        if col not in cites.columns:
            cites[col] = default

    # Preserve original schema label under _src; stamp output at SCHEMA_VERSION
    frames_with_schema = [cites] + ([docs] if docs is not None else [])
    for df in frames_with_schema:
        if "schema_version" in df.columns:
            df["schema_version"] = df["schema_version"].astype(str)
            df["schema_version_src"] = df["schema_version"]

    sv_cites = sorted(
        cites.get("schema_version", pd.Series(dtype=str)).dropna().unique().tolist()
    )
    if len(sv_cites) > 1:
        print(f"\n[warn] multiple schema versions in citations: {sv_cites}")
    elif sv_cites and sv_cites[0] != SCHEMA_VERSION:
        print(f"\n[info] citations stamped v{sv_cites[0]}; relabeling to v{SCHEMA_VERSION}")
        print("       Phase 0 columns may or may not be present in v<2.1 rows;")
        print("       audit downstream if schema_version_src is mixed.")

    # --------------------------------------------------------------------- #
    # DOCUMENT-LEVEL: source_year cascade (skipped if no documents input)
    # --------------------------------------------------------------------- #
    if docs is not None:
        print("\n--- Document-level cascade: source_year ---")

        docs["source_year_path"] = pd.to_numeric(
            docs.get("source_year_path"), errors="coerce"
        ).astype("Int64")

        if "source_year" in docs.columns:
            docs["source_year_llm"] = pd.to_numeric(
                docs["source_year"], errors="coerce"
            ).astype("Int64")
        else:
            docs["source_year_llm"] = pd.array([pd.NA] * len(docs), dtype="Int64")
            print("  [warn] no source_year column in documents; _llm column all null")

        n_need_fm = docs["source_year_path"].isna().sum()
        print(f"  docs needing frontmatter/metadata read : {n_need_fm} / {len(docs)}")

        if "source_file" in docs.columns:
            source_file_col = docs["source_file"]
        else:
            source_file_col = pd.Series([None] * len(docs))
            print("  [warn] no source_file column; frontmatter cascade disabled")

        code_years, code_sources, fm_years, md_years = [], [], [], []
        for i, (sf, syp) in enumerate(
            zip(source_file_col, docs["source_year_path"]), start=1
        ):
            cy, cs, fm, md = compute_source_year_code(sf, syp)
            code_years.append(cy)
            code_sources.append(cs)
            fm_years.append(fm)
            md_years.append(md)
            if i % 250 == 0:
                print(f"    processed {i}/{len(docs)}")

        docs["source_year_code"]          = pd.array(code_years, dtype="Int64")
        docs["source_year_code_source"]   = code_sources
        docs["source_year_frontmatter"]   = pd.array(fm_years, dtype="Int64")
        docs["source_year_pdf_metadata"]  = pd.array(md_years, dtype="Int64")

        docs["source_year"] = [
            coalesce_source_year(c, l, m)
            for c, l, m in zip(
                docs["source_year_code"],
                docs["source_year_llm"],
                docs["source_year_pdf_metadata"],
            )
        ]
        docs["source_year"] = pd.array(docs["source_year"].tolist(), dtype="Int64")
        docs["source_year_source"] = [
            cs if cs is not None
            else ("llm" if pd.notna(l)
                  else ("pdf_metadata" if pd.notna(m) else "none"))
            for cs, l, m in zip(
                docs["source_year_code_source"],
                docs["source_year_llm"],
                docs["source_year_pdf_metadata"],
            )
        ]

        src_counts = docs["source_year_source"].value_counts().to_dict()
        print(f"  source_year_source      : {src_counts}")
        both = docs[docs["source_year_code"].notna() & docs["source_year_llm"].notna()]
        if len(both):
            agree = (both["source_year_code"] == both["source_year_llm"]).sum()
            print(f"  code vs llm agreement   : {agree}/{len(both)} = {agree/len(both):.1%}")

        # Diagnostic: how often do the three signals disagree? Useful for
        # auditing the demoted pdf_metadata tier specifically.
        has_all = docs[docs["source_year_frontmatter"].notna()
                       & docs["source_year_llm"].notna()
                       & docs["source_year_pdf_metadata"].notna()]
        if len(has_all):
            md_fm_close = (abs(has_all["source_year_frontmatter"]
                               - has_all["source_year_pdf_metadata"]) <= 3).sum()
            md_llm_close = (abs(has_all["source_year_llm"]
                                - has_all["source_year_pdf_metadata"]) <= 3).sum()
            print(f"  3-signal overlap        : {len(has_all)} docs")
            print(f"    fm within 3y of md    : {md_fm_close}/{len(has_all)}")
            print(f"    llm within 3y of md   : {md_llm_close}/{len(has_all)}  "
                  f"(low number = md/llm diverge; expected for bulk-digitized corpora)")

    # --------------------------------------------------------------------- #
    # CITATION-LEVEL: is_academic
    # --------------------------------------------------------------------- #
    print("\n--- Citation-level cascade: is_academic ---")

    if "is_academic" not in cites.columns:
        cites["is_academic"] = pd.NA
    if "is_canonical" not in cites.columns:
        cites["is_canonical"] = pd.NA

    cites["is_academic_llm"]  = cites["is_academic"].map(_to_bool_or_none)
    cites["is_canonical_llm"] = cites["is_canonical"].map(_to_bool_or_none)

    if "recovered_journal" not in cites.columns:
        print("  [warn] no recovered_journal column; is_academic_code all null")
        cites["is_academic_code"] = pd.NA
    else:
        cites["is_academic_code"] = cites["recovered_journal"].apply(
            lambda v: classify_is_academic(v, venues_cfg)
        )

    n_code_t    = (cites["is_academic_code"] == True).sum()
    n_code_f    = (cites["is_academic_code"] == False).sum()
    n_code_none = cites["is_academic_code"].isna().sum()
    print(f"  is_academic_code        : True={n_code_t}  False={n_code_f}  None={n_code_none}")

    cites["is_academic"] = [
        coalesce_is_academic(c, l)
        for c, l in zip(cites["is_academic_code"], cites["is_academic_llm"])
    ]
    cites["is_academic_source"] = [
        "code" if c is not None and not pd.isna(c)
        else ("llm" if l is not None and not pd.isna(l) else "none")
        for c, l in zip(cites["is_academic_code"], cites["is_academic_llm"])
    ]

    both = cites[cites["is_academic_code"].notna() & cites["is_academic_llm"].notna()]
    if len(both):
        agree = (both["is_academic_code"].astype(bool)
                 == both["is_academic_llm"].astype(bool)).sum()
        print(f"  code vs llm agreement   : {agree}/{len(both)} = {agree/len(both):.1%}")
        disagree = both[both["is_academic_code"].astype(bool)
                        != both["is_academic_llm"].astype(bool)]
        if len(disagree) and "recovered_journal" in disagree.columns:
            top = disagree["recovered_journal"].value_counts().head(10)
            print(f"  top disagreeing venues (first 10):")
            for venue, n in top.items():
                print(f"    {n:4d}  {venue}")

    # --------------------------------------------------------------------- #
    # CITATION-LEVEL: venue_scope (code-only, additive; orthogonal to is_academic)
    # --------------------------------------------------------------------- #
    print("\n--- Citation-level classification: venue_scope ---")
    if "recovered_journal" in cites.columns:
        cites["venue_scope"] = cites["recovered_journal"].apply(
            lambda v: classify_venue_scope(v, venues_cfg)
        )
    else:
        cites["venue_scope"] = pd.NA
    scope_counts = cites["venue_scope"].value_counts(dropna=False).to_dict()
    # Sort by count desc; NaN key shows up as "nan" — relabel for clarity
    items = sorted(
        ((("<null>" if pd.isna(k) else k), v) for k, v in scope_counts.items()),
        key=lambda kv: -kv[1],
    )
    for scope, n in items:
        print(f"  {scope:<30} : {n}")

    # --------------------------------------------------------------------- #
    # CITATION-LEVEL: is_canonical
    # --------------------------------------------------------------------- #
    print("\n--- Citation-level cascade: is_canonical ---")

    has_pattern  = cites["canonical_pattern_id"].notna()
    metric_hedge = cites["likely_metric_only"].map(_to_bool_or_none).fillna(False)
    # True only when confident (pattern hit AND not metric-hedged); None otherwise.
    # No "False" case — absence of a canonical pattern is not proof of non-canonical.
    cites["is_canonical_code"] = pd.array(
        [True if (hp and not mh) else pd.NA
         for hp, mh in zip(has_pattern, metric_hedge)],
        dtype="object",
    )

    n_code_t    = (cites["is_canonical_code"] == True).sum()
    n_hedged    = (has_pattern & metric_hedge).sum()
    n_code_none = cites["is_canonical_code"].isna().sum()
    print(f"  canonical_pattern_id set: {has_pattern.sum()}")
    print(f"  confident code hits     : {n_code_t}")
    print(f"  metric-ambiguous hedged : {n_hedged}  (structural_uncertain)")
    print(f"  no canonical signal     : {n_code_none}")

    canon_results = [
        resolve_is_canonical(c, l, m)
        for c, l, m in zip(
            cites["is_canonical_code"],
            cites["is_canonical_llm"],
            cites["likely_metric_only"],
        )
    ]
    cites["is_canonical"]        = [r[0] for r in canon_results]
    cites["is_canonical_source"] = [r[1] for r in canon_results]

    n_canon_true = sum(1 for v in cites["is_canonical"] if v is True)
    src_counts = pd.Series(cites["is_canonical_source"]).value_counts().to_dict()
    print(f"  is_canonical (final)    : True={n_canon_true}")
    print(f"  is_canonical_source     : {src_counts}")

    # --------------------------------------------------------------------- #
    # Stamp version + write
    # --------------------------------------------------------------------- #
    cites["schema_version"] = SCHEMA_VERSION
    cites["venues_hash"]    = venues_hash
    if docs is not None:
        docs["schema_version"] = SCHEMA_VERSION
        docs["venues_hash"]    = venues_hash

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if docs is not None:
        print(f"\nWriting documents resolved : {DOCUMENTS_OUT}")
        docs.to_csv(DOCUMENTS_OUT, index=False)
    else:
        print(f"\n[info] documents_resolved.csv not written (no input documents).")
    print(f"Writing citations resolved : {CITATIONS_OUT}")
    cites.to_csv(CITATIONS_OUT, index=False)

    dt = (datetime.now() - t0).total_seconds()
    print(f"\n=== done in {dt/60:.1f} min ===")


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Phase 1 shadow-column resolver "
                    "(is_academic, is_canonical, source_year) + venue_scope"
    )
    parser.add_argument(
        "--input", type=Path, default=None,
        help="Override citations input path. Default: auto-detect, preferring "
             "citations_merged_deduped.csv > citations_merged.csv > "
             "citations_deep_deduped.csv > citations_deep.csv.",
    )
    parser.add_argument(
        "--docs", type=Path, default=None,
        help="Override documents input path. Default: same dir as citations, "
             "then output/documents_deep.csv. source_year cascade is skipped "
             "if no documents CSV is found.",
    )
    args = parser.parse_args()
    main(citations_in=args.input, docs_in=args.docs)