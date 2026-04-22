# GAP Project — Script Registry
Last updated: 2026-04-20

This registry focuses on **actively developed code** — probes, pipeline,
and post-processing scripts. Scrapers are condensed into a status table
below since they're mostly complete and stable; full detail on scraper
methodology lives in `definer.md`.

---

## Scrapers (condensed)

All at `src/scrapers/`. Counts verified 2026-04-20 where possible.

| Script | Source | Status | PDFs |
|---|---|---|---|
| `arch_scraper.py` | Alpha Architect (v7) | ✅ | 75 (+113 junk, 451 .txt) |
| `bii_scraper.py` | BlackRock BII | ✅ | 49 |
| `cfa_scraper.py` | CFA Research Foundation | ✅ | 265 |
| `dfa_scraper.py` | Dimensional Fund Advisors | 🟡 | partial |
| `edhec_scraper.py` | EDHEC / Scientific Beta | 🟡 | 1 (live), 32 .txt |
| `faj_scraper.py` | FAJ (vols 67–82) | ✅ | 497 |
| `gmo_scraper.py` | GMO (5 types) | ✅ | ✓ |
| `jacf_scraper.py` | J. Applied Corporate Finance | ✅ | 1161 |
| `jai_scraper.py` | J. Alternative Investments | 🔴 | no corpus yet |
| `jbis_scraper.py` | J. Beta Investment Strategies | 🟡 | 4 |
| `jfi_scraper.py` | J. Fixed Income (practitioner) | ✅ | ~1038 |
| `jis_scraper.py` | J. Investment Strategies | ✅ | 184 |
| `jor_scraper.py` | J. Risk | ✅ | 555 |
| `jpm_scraper.py` | JPM | ✅ | 3436 (OCR running on vols 1–24) |
| `man_scraper.py` | Man Group / Man AHL | ✅ | 121 |
| `msci_scraper_v26.py` | MSCI | 🟡 | ~54 / 743 (locked-form bug) |
| `pgim_scraper.py` | PGIM Fixed Income / Quant Solutions | ✅ | 13 (+7 CMA, 13 .txt) |
| `pimco_scraperv2.py` | PIMCO | ✅ | 140 |
| `ra_scraper.py` | Research Affiliates | ✅ | 299 |
| `robeco_scraper.py` | Robeco | 🟡 | 8 (in-progress build) |
| `ssga_scraper.py` | SSGA | ✅ | 25 |
| `twosigma_scraper.py` | Two Sigma Venn | ✅ | — (.txt only) |
| `vanguard_scraper.py` | Vanguard Research | ✅ | 70 |

### Unscraped / pre-convention
- **AQR Alternative Thinking** — 41 PDFs at `data/pdfs/AQR/`; no
  `aqr_scraper.py` on disk (predates the scrapers/ convention)

### Pending / blocked
- Scientific Beta — registration required; access method TBD
- EDHEC legacy Wayback (1028 archived PDFs) — firewall block on current network
- FAJ vols 49–66 via JSTOR
- MSCI locked articles — Selenium `el.click()` fallback fix needed
- `jai_scraper.py` — script exists but no corpus folder; either never run,
  produced elsewhere, or folder uses different casing. Run
  `Get-ChildItem data\pdfs\ -Directory` to check.

---

## Probes (full detail)

One-off DOM/API/PDF-quality reconnaissance at `src/scrapers_probes/`.
Reusable probes should be committed; one-shot diagnostics with outcomes
captured in `current_snapshot.md` or `locked_decisions.md` stay uncommitted.

| Script | Purpose | Type | Last Modified |
|---|---|---|---|
| `dfa_probe.py` | DOM inspection for DFA scraper build | Reusable | — |
| `extraction_quality_scan.py` | Per-PDF wpp scan across full corpus (all sources); outputs `output/extraction_quality_scan.csv` with `source_file, source, format, page_count, word_count, total_chars, words_per_page, extraction_method, error` columns. Primary tool for identifying scan-related issues. Re-run after any corpus change or OCR batch. | **Reusable (keep committed)** | 2026-04-20 |
| `jpm_investigate.py` | Per-volume scan breakdown of JPM that identified vols 1–24 as OCR targets (10–55% scan rates vs 0% for vols 29–52). Outcome folded into OCR pipeline. | One-shot (outcome captured in snapshot) | 2026-04-20 |
| `pdf_diagnose.py` | Three-extractor (PyMuPDF, pdfplumber, pdfminer.six) + raw-stream diagnosis of a single PDF. Confirms true image scans vs extractor failures. Use when a single file's extraction behavior is suspicious. | **Reusable (keep committed)** | 2026-04-20 |
| `aa_scanned_probe.py` | One-shot diagnostic for 9 AA PDFs flagged likely_scanned. Identified 3 academic reprints + 6 Democratize Quant conference decks. Superseded by `aa_v7_audit.py`. | One-shot (outcome captured) | 2026-04-20 |
| `aa_v7_audit.py` | Read-only audit of current AA `data/pdfs/AlphaArchitect/` against v7 filter rules (filename + zero-text + low-finance-vocab checks). Run after filter changes or periodically to detect corpus drift. | **Reusable (keep committed)** | 2026-04-20 |
| `jpm_ocr_sample.py` | Stratified sample of JPM low-word PDFs to decide OCR scope. Surfaced the wpp=42 pm-research.com watermark finding. Decision (OCR all ~443) locked; outcome captured. | One-shot (outcome captured) | 2026-04-20 |

**Recommended for commit:** `dfa_probe.py`, `extraction_quality_scan.py`,
`pdf_diagnose.py`, `aa_v7_audit.py`.

**Leave uncommitted:** `jpm_investigate.py`, `aa_scanned_probe.py`,
`jpm_ocr_sample.py` — their findings are in `current_snapshot.md`.

---

## Pipeline Scripts (full detail)

All at `src/`. These are the backbone of the extraction → benchmark → join flow.

| Script | Function | Status | Last Modified |
|---|---|---|---|
| `01_extract_deep.py` | LLM citation extraction with deep schema. Emits `citation_function`, `venue_tier`, `is_canonical`, `citation_location`, `recovered_first_version_year`, `doc_id`, `schema_version`, `prompt_hash`, `patterns_hash`, `code_version` among many others. Parallel via ThreadPoolExecutor (MAX_WORKERS=2), resume-aware, max_tokens=16000. **Phase 0 provenance columns live** (as of 2026-04-20). Known issue: `SCHEMA_VERSION` still reads `"2.0"` but should be `"2.1"` per `fields.md` — one-line fix deferred to Phase 1 (which bumps to `2.2`). | 🟡 Pending full-corpus run (~34h @ 2 workers) | 2026-04-20 |
| `02_openalex.py` | Build academic benchmark from OpenAlex API. Benchmark journals: JF, JFE, RFS, JFQA, RF, RAPS, RCFS, JFI, JCF. Seed-only (MS filter authors): JBF, JFM. Management Science filtered via finance-author network (~25-26% retention). Includes `referenced_works` for Divergence framing. | ✅ Usable; rerun when benchmark list changes | — |
| `03_join.py` | Match extracted citations to benchmark. Two-pass: DOI exact → composite fuzzy (title + first-author lastname, threshold 85) → title-only fallback (threshold 93). Year constraint `\|recovered_year − bench_year\| ≤ 2`. Assigns `paper_cluster_id` for cross-document paper-level aggregation. Target ≥86% match rate on benchmark-eligible citations. | 🟡 Rerun pending after extract rerun | — |
| `01b_merge.py` | **Phase 1 deliverable (not yet written).** Post-extraction merge that resolves LLM vs code-derived columns: `is_academic` (venue lookup + OpenAlex cascade), `is_canonical` (pattern propagation from `canonical_sample.py`/`full_corpus_scan.py` via `patterns.yaml`), `source_year` (path + frontmatter + LLM fallback). Phase 2 extends with `citation_location`, `resolution_type`, `is_self_citation`. Phase 3 extends with post-join `confidence` and `academic_subfield`. Schema bumps to `2.2` when this lands. | 🔴 Not yet written | — |

---

## Post-Processing Scripts (full detail)

All at `src/post_processing/`.

| Script | Function | Status | Last Modified |
|---|---|---|---|
| `backfill_provenance.py` | One-shot stamper: adds `prompt_hash`, `patterns_hash`, `code_version` with `pre_phase_0` sentinel to pre-existing pilot rows in `output/citations_deep.csv` and `output/documents_deep.csv`. Ran once post-Phase-0 deployment; stamped 110 rows. Not in pipeline. Keep for audit trail. | ✅ Done (one-shot) | 2026-04-20 |
| `canonical_sample.py` | Builds validation sample of canonical-pattern hits from the extraction output for manual audit. Complement to `full_corpus_scan.py` — together they form the canonical-pattern validation loop against `patterns.yaml`. | 🟡 Status unverified; exists on disk | — |
| `full_corpus_scan.py` | Corpus-wide regex scan against `patterns.yaml` for canonical-pattern candidates. Used to surface additional patterns (Grinold 1989, Merton 1974, Pastor-Stambaugh 2003, Frazzini-Pedersen BAB, Roll 1977, Treynor 1965, etc.) deferred from the v1.1 pattern set. Output feeds pattern additions. | ✅ Exists on disk | — |
| `ocr_jpm.py` | OCR pipeline for low-wpp JPM PDFs (wpp < 50). Safe flow: copy to `data/ocr_backups/JPM/`, run `ocrmypdf --force-ocr --deskew -l eng --jobs N`, atomic-replace only on word-count improvement. Resume-safe via candidate rediscovery. Logs to `data/logs/JPM/_ocr_log.tsv`. Watermark noise from pm-research.com text layer is acceptable — downstream LLM ignores copyright boilerplate. | 🟡 Full run in progress (launched 2026-04-20, ~2.3h ETA) | 2026-04-20 |
| `patterns.yaml` | YAML source of truth for canonical citation patterns. Schema `canonical_v2`, yaml `v1.1`. 21+ patterns bucketed `canonical` vs `canonical_metric_ambiguous`. Adversarial-review-synthesized (v1.0 → v1.1 captured review fixes). Read by `full_corpus_scan.py`, `canonical_sample.py`, and (pending) `01b_merge.py`. Deferred patterns list tracks candidates pending full-corpus scan. | ✅ Active | 2026-04-19 |

### Missing but historically referenced
- **`dedup.py`** — gap_structure.md documents within-document fuzzy
  dedup (title+author ~88 threshold, collapses to `within_doc_mention_count`)
  as a post-extraction step. Script is **not on disk** at
  `src/post_processing/` as of 2026-04-20. Either inlined into `01b_merge.py`
  planning, or deferred. Reconcile before pipeline assumes it exists.

---

## Analysis Scripts

Phase 4 scripts. None written yet. Expected at `src/analysis/` with one
script per framing, output to `output/{framing}/`.

| Script | Framing | Status |
|---|---|---|
| `share.py` | Share | 🔴 Not started |
| `selectivity.py` | Selectivity | 🔴 Not started |
| `lag.py` | Lag | 🔴 Not started |
| `divergence.py` | Divergence | 🔴 Not started |
| `absorption.py` | Absorption by Channel | 🔴 Not started |
| `age_profile.py` | Citation Age Profile | 🔴 Not started |

See `gap_structure.md` §"Six Analytical Framings" for methodology per script.

---

## Deprecated

| Script | Reason | Replaced By |
|---|---|---|
| ~~`src/01_extract.py`~~ | Shallow schema; `is_academic` fix not applied; no provenance columns. Still producing `output/citations.csv` as of 2026-04-20 — safe to remove. | `src/01_extract_deep.py` |
