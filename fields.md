# GAP Project — Fields Registry
Last updated: 2026-04-20 | Schema: v2.1 | Planned: v2.2

## Purpose
Authoritative registry of every field in `citations_deep.csv` and
`documents_deep.csv`. Tracks current provenance (code vs LLM), migration
candidacy, and implementation plan. This file is the coordination point for
decisions about what moves from LLM extraction to code derivation.

Accuracy is valued highly; cost/speed are secondary. A field only moves to
code when the code path matches or exceeds LLM accuracy for the relevant
downstream use, OR when the field is so deterministic (lookup table, file hash)
that LLM is strictly worse.

---

## Legend

**Current provenance**
- `CODE` — computed deterministically in `01_extract_deep.py` today
- `LLM` — extracted by Claude Sonnet via the extraction prompt today
- `LLM+CODE` — LLM primary with code fallback or code primary with LLM fallback

**Code-feasibility score (1-10)**
Weighted by accuracy, not just "is it technically possible":
- **10** — purely deterministic, zero judgment, lookup table or file operation
- **8-9** — deterministic with well-defined edge cases that code handles cleanly
- **6-7** — code handles majority of cases accurately; LLM fallback needed for residual
- **4-5** — code feasible but meaningful accuracy loss vs LLM on real corpus edge cases
- **2-3** — requires semantic judgment the LLM is genuinely better at
- **1** — core LLM recognition task; no deterministic substitute

**Migration status**
- `locked_code` — already code-derived, no change
- `migrate_phase_1` — planned for v2.2 (is_academic, is_canonical, source_year cascade)
- `migrate_phase_2` — planned for v2.3 (location, resolution_type, is_self_citation firm)
- `migrate_phase_3` — planned for v2.4 (confidence post-join, academic_subfield fallback)
- `keep_llm` — reviewed, keeping LLM; reversals from prior plan noted in rationale
- `under_review` — pending decision

---

## DOCUMENT-LEVEL FIELDS (`documents_deep.csv`)

### Identifiers

| Field | Current | Score | Status | Description & Implementation |
|---|---|---|---|---|
| `doc_id` | CODE | 10 | locked_code | SHA-256 of file bytes, first 12 hex chars. Content-stable across moves. |
| `source_file` | CODE | 10 | locked_code | Absolute path string. Used for resume logic. |

### Source metadata (code-derived today)

| Field | Current | Score | Status | Description & Implementation |
|---|---|---|---|---|
| `source_type` | CODE | 10 | locked_code | Lookup in `SOURCE_TYPE_MAP` keyed on folder name. |
| `source_institution` | CODE | 10 | locked_code | Lookup in `INSTITUTION_MAP` keyed on `source_type`. |
| `source_year_path` | CODE | 9 | locked_code | Regex `(19[89]\d\|20[012]\d)` on filename + parent folder. ~70% hit rate; null otherwise. |

### Source metadata (LLM-extracted today)

| Field | Current | Score | Status | Description & Migration plan |
|---|---|---|---|---|
| `source_year` | LLM | 8 | migrate_phase_1 | **Move to code cascade.** (1) `source_year_path` from filename; (2) regex on first 3,000 chars of text for © year, copyright lines, "Q2 2017", month+year, "First published" patterns; (3) LLM fallback; (4) null. Emit `source_year_source` column for provenance. Failure mode is benign: a wrong source_year shifts a single data point, doesn't break joins like `recovered_year` would. |
| `source_title` | LLM | 3 | keep_llm | PDF titles are layout-dependent; code fallback to `file_path.stem` already exists. LLM does the semantic work of ignoring running headers and page numbers. Not worth engineering first-page title recognition. |
| `source_topic` | LLM | 2 | keep_llm | Semantic classification across 10-value enum. No deterministic proxy. |
| `source_academic_subfield` | LLM | 2 | keep_llm | Semantic classification. Document-level, no `recovered_journal` proxy available. |

### Document properties (code-derived today)

| Field | Current | Score | Status | Description |
|---|---|---|---|---|
| `doc_has_bibliography` | CODE | 9 | locked_code | Regex for "references\n" / "bibliography\n" markers in last 40% of text. |
| `doc_page_count` | CODE | 10 | locked_code | PyMuPDF `len(doc)`. |
| `doc_word_count` | CODE | 10 | locked_code | `text.split()` length. |
| `doc_total_chars` | CODE | 10 | locked_code | `len(full_text)`. |
| `doc_text_truncated` | CODE | 10 | locked_code | Boolean: `total_chars > FULL_TEXT_CHAR_LIMIT`. |
| `doc_extraction_char_ratio` | CODE | 10 | locked_code | Chars sent to LLM / total chars. |
| `doc_text_strategy` | CODE | 10 | locked_code | "full" / "refs@N" / "first+last". |
| `doc_ref_section_char` | CODE | 9 | locked_code | Char index where references begin; -1 if not found. |
| `doc_citation_count` | CODE | 10 | locked_code | `len(citations)` from LLM return. Derived. |
| `pdf_extraction_method` | CODE | 9 | locked_code | "native_text" vs "likely_scanned"; threshold words/page < 50. |

### Pipeline metadata (code-derived today)

| Field | Current | Score | Status | Description |
|---|---|---|---|---|
| `llm_status` | CODE | 10 | locked_code | "ok" / "json_error" / "api_error" / "max_retries" / "skipped_no_text". |
| `json_valid` | CODE | 10 | locked_code | Parse success flag. |
| `retry_count` | CODE | 10 | locked_code | Attempt counter. |
| `schema_version` | CODE | 10 | locked_code | Constant bumped on schema changes. |
| `prompt_hash` | *not present* | 10 | **add phase 0** | SHA-256 of extraction prompt string, first 8 hex. Critical for splitting LLM vs code provenance. Project structure doc says rows should carry this; currently they don't. |
| `patterns_hash` | *not present* | 10 | **add phase 0** | SHA-256 of `patterns.yaml`, first 8 hex. Ties output rows to the specific canonical pattern set used. |
| `code_version` | *not present* | 10 | **add phase 0** | Git commit hash at time of extraction. Fails gracefully to "dirty" if uncommitted. |

---

## CITATION-LEVEL FIELDS (`citations_deep.csv`)

### Raw extraction (LLM — keep)

| Field | Current | Score | Status | Description & Rationale |
|---|---|---|---|---|
| `raw_mention` | LLM | 1 | keep_llm | Verbatim snippet as the citation appears. Core LLM recognition task. |
| `raw_authors` | LLM | 2 | keep_llm | Full author names as written. Libraries like `anystyle`/GROBID fail on informal practitioner citations. |
| `recovered_authors` | LLM | 2 | keep_llm | List of surnames. Rule-based parsers break on "Fama and French" vs "Fama, E.F. & French, K.R." vs "Eugene F. Fama and Kenneth R. French". |
| `recovered_title` | LLM | 2 | keep_llm | Paper/book title. Semantic extraction from messy practitioner refs. |
| `recovered_journal` | LLM | 2 | keep_llm | Journal/publisher/venue. Messy formatting; LLM handles abbreviations and variants. |
| `citation_context` | LLM | 3 | keep_llm | 1-2 sentence verbatim snippet. LLM selects the relevant sentence; code-locating would require the Phase 2 citation-key matcher. Potential Phase 3 supplement. |

### Recovered bibliographic data (mixed)

| Field | Current | Score | Status | Description & Migration plan |
|---|---|---|---|---|
| `recovered_year` | LLM | 4 | keep_llm | **Reversed from prior plan.** Regex on `raw_mention` fails on "forthcoming 2026", "1993a/b" suffixes, year ranges, multi-year citations ("Fama 1970, 1991"), and sample years mixed with publication years. Critical: `03_join.py` uses `\|recovered_year - bench_year\| <= 2` constraint — a wrong deterministic year blocks correct matches more aggressively than a null LLM year. Keep LLM primary. Planned: add `recovered_year_code` shadow column post-join for audit; do NOT overwrite. |
| `recovered_first_version_year` | LLM | 5 | keep_llm | **Reversed from prior plan.** Sparse field (~5-10% of citations have explicit WP year in text). Regex `(NBER WP\|SSRN\|working paper)[^\n]{0,60}(19\|20)\d\d` catches ~80% of explicit cases but adds false positives on contextual mentions like "NBER WP series from 2018-2020". Cost savings minimal ($20-40 of $800 run). OpenAlex post-join is the real source of truth; keep LLM as the text-level extractor. |
| `recovered_doi` | LLM=null | 10 | locked_code | Forced null by design. Separate `doi_candidates` field (code-extracted via regex on full text) attached to every citation row from the same doc. |
| `doi_candidates` | CODE | 10 | locked_code | Regex `10\.\d{4,9}/[-._;()/:A-Za-z0-9]+` on full text. List attached to every row from that doc. Per-citation DOI assignment is a `03_join.py` concern. |

### Policy classification (LLM today, move to code)

| Field | Current | Score | Status | Description & Implementation |
|---|---|---|---|---|
| `is_academic` | LLM | 9 | migrate_phase_1 | **Move to code cascade.** (1) Exact venue lookup from `recovered_journal` against a journal classification table with aliases; (2) if OpenAlex match available post-join, use that; (3) for NBER/SSRN working papers, check against finance-author network from `02_openalex.py`; (4) fallback to "unknown" not "false". Runs in `01b_merge.py` post-extraction. Key nuance: not all NBER/SSRN are academic (practitioner SSRNs exist); don't classify solely by venue prefix. LLM will run in parallel as `is_academic_llm` during Phase 1 transition; drop from prompt only after audit agreement >95%. |
| `is_canonical` | LLM | 9 | migrate_phase_1 | **Move to code via pattern match.** Propagate `canonical_pattern_id` from `canonical_scraper.py` via `doc_id + recovered_authors` join. Do NOT fuzzy-match `(recovered_authors, recovered_year)` — that re-implements pattern matching with extra failure modes (year mismatch when multiple papers by same authors appear in one doc, e.g. Fama-French 1992/1993/2015). LLM row is kept for other fields; `is_canonical` gets overwritten to True iff a canonical_scraper hit exists for same doc with matching author set. |
| `is_self_citation` | LLM | 6 | migrate_phase_2 | **Partial move to code.** Firm-level: (1) `recovered_journal` matches a firm's own outlet in `INSTITUTION_MAP` reverse lookup (AQR White Paper, AQR Alternative Thinking, PIMCO Quantitative Research, etc.); (2) `source_institution.lower() in raw_mention.lower()` AND mention is in authorship context not incidental. Author-level self-citation (Asness citing Asness before 1998 at Goldman) NOT attempted — requires author-to-firm mapping with dates that's too much maintenance. Residual ambiguous cases left as null for LLM to fill during transition, then dropped. |

### Classification (LLM, mixed migration)

| Field | Current | Score | Status | Description & Migration plan |
|---|---|---|---|---|
| `academic_subfield` | LLM | 5 | migrate_phase_3 | **LLM primary, code override for joined benchmark papers.** Once `03_join.py` matches to OpenAlex, use OpenAlex subject/concept data to override `academic_subfield` for high-confidence joins. Code fallback from `recovered_journal` mapping (JF/JFE/RFS → asset_pricing, JCF → corporate_finance, JFI-academic → financial_intermediation) fills null LLM values. Cross-field papers in top journals remain LLM-classified. |
| `secondary_academic_subfield` | LLM | 2 | keep_llm | Semantic judgment on cross-field papers. No deterministic derivation. |
| `citation_object` | LLM | 2 | keep_llm | 6-value enum classifying what's being cited (theory vs method vs data source etc.). Semantic. |
| `citation_function` | LLM | 2 | keep_llm | 7-value enum classifying how citation is used in argument. Core semantic work. |
| `citation_polarity` | LLM | 2 | keep_llm | 5-value enum for engagement stance. Semantic. |

### Location & form (LLM today, move to code in Phase 2)

| Field | Current | Score | Status | Description & Implementation |
|---|---|---|---|---|
| `citation_location` | LLM | 7 | migrate_phase_2 | **Move to code with citation-key matcher.** Requires building infrastructure that maps `raw_mention` + `(recovered_authors, recovered_year)` back to character positions in `full_text`. Naive `full_text.find(raw_mention)` fails ~60% of the time (whitespace, hyphenation, LLM paraphrasing, multiple occurrences, truncated extraction). Once positions are found: (1) `> doc_ref_section_char` → references_section; (2) zone assignment by percentage (abstract: first 5%, intro: 5-20%, etc.) with section-header regex override when present. Report "unknown" explicitly; don't guess. |
| `location_subtype` | LLM | 6 | migrate_phase_2 | Derivable once `citation_location` works. main_text vs footnote (superscript markers) vs figure_caption (regex "Figure N:") vs table_note vs bibliography. |
| `resolution_type` | LLM | 7 | migrate_phase_2 | Pure derivation from location + form. Rules: `has_reference_entry AND NOT has_body_mention` → references_section; `has_body_mention AND is_bib_format(raw_mention)` → formal_citation; `has_body_mention AND has_year_in_mention` → informal_named; else → implicit. Depends on Phase 2.1 citation-key matcher. |

### Quality flags (LLM today)

| Field | Current | Score | Status | Description & Migration plan |
|---|---|---|---|---|
| `confidence` | LLM | 6 | migrate_phase_3 | **Move to provenance-based code, post-join.** LLM self-confidence is uncalibrated across model versions. Better rule: `high` if DOI exact match OR OpenAlex strong match OR canonical pattern hit; `medium` if title+author+year present but unjoined; `low` if authors-only or implicit. Implement in `01b_merge.py` AFTER `03_join.py` so join status informs confidence. During transition, keep `confidence_llm` column; drop from prompt after audit. Since confidence determines which rows enter main analysis (low-confidence dropped), this must be auditable and deterministic. |

---

## PHASE SUMMARY

### Phase 0 — Provenance infrastructure (no schema bump)
Add `prompt_hash`, `patterns_hash`, `code_version` to every row in both CSVs.
Required before any LLM→code migration so that transition audits can identify
which extraction version produced which row.

### Phase 1 — v2.2 (safe, high-value wins)
- `is_academic` → code cascade (venue table + OpenAlex + author network)
- `is_canonical` → code via canonical_scraper pattern propagation
- `source_year` → code cascade (path + frontmatter + LLM fallback)

Strategy: all three keep LLM version in prompt during Phase 1 as
`is_academic_llm`, `is_canonical_llm`, `source_year_llm`. Compare on 200-300
doc audit. Drop from prompt only after Cohen's κ > 0.85 or disagreement < 5%.

### Phase 2 — v2.3 (depends on citation-key matcher)
- Build citation-key matcher (foundational infrastructure)
- `citation_location` → code
- `location_subtype` → code
- `resolution_type` → code
- `is_self_citation` → firm-level code, ambiguous LLM residual

### Phase 3 — v2.4 (post-join derivations)
- `confidence` → provenance-based code, runs after `03_join.py`
- `academic_subfield` → OpenAlex override for joined rows, journal fallback for null LLM

### Permanently LLM
- `raw_mention`, `raw_authors`, `recovered_authors`, `recovered_title`, `recovered_journal`
- `recovered_year`, `recovered_first_version_year` (reversed from prior plan)
- `citation_context`
- `citation_object`, `citation_function`, `citation_polarity`
- `secondary_academic_subfield`
- `source_title`, `source_topic`, `source_academic_subfield`

---

## REVERSALS FROM PRIOR PLAN (for audit trail)

1. **`recovered_year`**: initially proposed for code migration; reversed after
   adversarial review flagged downstream risk. Wrong code year in `03_join.py`
   year constraint (`\|recovered_year - bench_year\| <= 2`) actively blocks
   correct matches; null LLM year allows title/author fuzzy match to proceed.
   Edge cases LLM handles: "forthcoming 2026", "1993a/b", year ranges,
   disambiguating sample years vs publication years.

2. **`recovered_first_version_year`**: initially proposed for regex migration;
   reversed. Prompt already says "only if literally in text" which is what
   regex does, and the LLM does it better in messy cases. OpenAlex post-join
   is the real source of truth for this field anyway.

---

## OPEN QUESTIONS

- JFI dual-venue issue (Journal of Financial Intermediation = academic; Journal
  of Fixed Income = practitioner). Venue table needs precise journal name
  matching, not abbreviations. Unresolved; blocks Phase 1 `is_academic` move.
- Validation holdout: need ~50 docs with full LLM extraction (all fields)
  preserved as ongoing regression ground truth even after migrations.
  Coordinate with the 50-doc validation audit that's already planned.
