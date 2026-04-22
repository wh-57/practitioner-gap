# GAP Project — Current Snapshot
Last updated: 2026-04-21 (Phase 1 audited + signed off; ready to commit)

This file reflects the current project state — what is done, what is next,
what is blocked. It is overwritten every turn; it does not accumulate
history. Session-level continuity comes from this file plus
`locked_decisions.md`.

## Where things stand right now

**Phase 0 (provenance infrastructure):** COMPLETE, pushed (commit
`253485f`). `prompt_hash`, `patterns_hash`, `code_version` on every row.

**Group A commits (AA v7 + JPM OCR + Patch 12):** COMPLETE, PUSHED.
Confirmed via `git log --oneline`: `4a8dbca` (Patch 12 + schema v2.2),
`9d16588` (ocr_jpm.py), `7e86085` (arch_scraper v7 + log-path migration).
The previous snapshot incorrectly listed these as queued.

**JPM OCR:** COMPLETE. 424/424 OK, zero failures, ~2h 24m wall-clock.
JPM `likely_scanned` count dropped 424 → 0.

**Phase 1 integration (v2.2):** CODE COMPLETE, AUDITED, NOT YET COMMITTED.
All four pipeline scripts live + audited via `audit_phase1.py`:

  Audit results (2026-04-21, 70-doc sample, 9200 citations):
  - `is_academic`   : 98.4% agreement; 25/25 labeled disagreements
                      code-correct (137 FAJ + 4 JoR + 1 JoD). Ships.
  - `is_canonical`  : 100% agreement; zero disagreements. Ships.
  - `source_year`   : 88.0% agreement; 6/6 labeled disagreements
                      LLM-correct (all frontmatter false positives).
                      Ships with LLM fallback retained in prompt
                      (never planned for prompt removal).

**source_year cascade — revised this session (two iterations):**
  Initial patch tightened FRONTMATTER_PATTERNS (removed bare
  `Month+YYYY` after it caused a 31-year miss on a JACF monetary-history
  article — "February 1992" in body text of a 2023 paper) and added
  `extract_pdf_metadata_year` via PyMuPDF `creationDate`. First version
  arbitrated md/fm and promoted md when they disagreed by >3y.
  Audit showed 12/12 pdf_metadata disagreements were LLM-correct,
  driven by PM-Research bulk-digitization stamping 2004/2009 on
  JPM/JFI back-catalog papers from the 80s-90s. pdf_metadata demoted
  to last-resort fallback (after LLM); agreement jumped 72.3% → 88.0%.

**venues.yaml — expanded this session:**
  Added to academic set: Journal of Econometrics, Review of Economics
  and Statistics, Journal of Economic Theory, Contemporary Accounting
  Research, Journal of Labor Economics, Journal of Risk and
  Uncertainty, European Journal of Operational Research, Journal of
  Futures Markets. Corrected JFM practitioner → academic after initial
  misclassification caught by audit. `is_academic_code=null` dropped
  from 14.2% to 4.0% of citation rows.

**Edit tooling — added this session:**
  `src/post_processing/edit_venues.py` and `edit_patterns.py`. CLI
  editors with ruamel.yaml (preserves comments). Idempotent.
  `classify` subcommand on edit_venues.py mirrors the `01c_resolve.py`
  cascade for preview. Requires `pip install ruamel.yaml`.

**Parallel deliverables from earlier sessions (ready to commit):**
  - `03_join.py` updated to consume `citations_resolved.csv` with all
    Phase 1 shadow columns
  - `ocr_pdfs.py` generalized from `ocr_jpm.py` with `--source` flag

## Immediate next actions (ordered)

1. **Save updated docs to repo:**
   ```
   fields.md            → gap/
   locked_decisions.md  → gap/
   scripts.md           → gap/
   current_snapshot.md  → gap/   (this file)
   01c_resolve.py       → gap/src/
   edit_venues.py       → gap/src/post_processing/
   edit_patterns.py     → gap/src/post_processing/
   audit_phase1.py      → gap/src/validation/   (if not already saved)
   ```

2. **Verify working-tree state:**
   ```
   git status
   ```
   Confirm which files are modified vs untracked. Paste output before
   staging anything — catches leftovers from other sessions.

3. **Single bundled commit** (Group A already pushed, Patch 12 already
   pushed as `4a8dbca`, so only the Phase 1 bundle remains):
   ```
   git add src/01b_merge.py src/01c_resolve.py src/03_join.py \
           src/validation/audit_phase1.py \
           src/post_processing/venues.yaml \
           src/post_processing/canonical_scraper.py \
           src/post_processing/ocr_pdfs.py \
           src/post_processing/edit_venues.py \
           src/post_processing/edit_patterns.py \
           fields.md scripts.md gap_structure.md \
           locked_decisions.md current_snapshot.md
   git commit -m "Phase 1 v2.2: shadow-column resolver + venue scope + canonical merge"
   git log --oneline -5
   git push
   ```

4. **After push:** kick off full-corpus extraction (~34h, ~$800).

## Known state / gotchas

- `fields.md` `canonical_metric_ambiguous` description parenthetical
  still says "(sharpe/jensen-alpha/fama-macbeth)" — outdated. Actual
  bucket has 8 patterns (added: shiller_cape, amihud_illiquidity,
  bollerslev_garch, brinson_attribution, black_litterman). Low priority.
- `SCHEMA_VERSION` normalized to `"2.2"` across `01_extract_deep.py`
  and `01c_resolve.py`. Prior 2.0 / 2.1 mismatches resolved.
- Canonical scraper resume log is path-based — must use `--fresh`
  after any in-place OCR batch. Docstring now warns; hash-based log
  is backlog.
- `jai_scraper.py` exists but no corpus folder. Unresolved.
- `dedup.py` referenced in `gap_structure.md` but not on disk.
  `01c_resolve.py` input auto-detection handles its absence
  gracefully. Reconcile before Phase 2.
- `[warn] multiple schema versions in citations: ['2.2', 'canonical_v2']`
  warning in `01c_resolve.py` output is benign — `canonical_scraper.py`
  writes `schema_version = "canonical_v2"` (pattern-system version)
  into the same column used for pipeline-schema version. Naming
  clash, not a bug. Low-priority cleanup: rename to
  `pattern_schema_version` in canonical scraper, or stop writing from
  it entirely.
- `source_file` path format is inconsistent across scripts:
  `01_extract_deep.py` writes absolute Windows paths with backslashes;
  `canonical_scraper.py` writes repo-relative paths with forward
  slashes. Merge uses `doc_id` not path, so doesn't break anything.
  Cosmetic; tracked in `locked_decisions.md` §Explicitly Deferred.
- `MuPDF error: format error: cmsOpenProfileFromMem failed` in
  `01c_resolve.py` output is benign — malformed ICC color profile on
  one PDF. MuPDF skips color management; text extraction unaffected.

## Not blocking but on the horizon

**Corpus-side:**
- Scientific Beta registration
- EDHEC Track 2 Wayback downloads (firewall block)
- FAJ vols 49–66 via JSTOR
- MSCI locked-article download bug (Selenium `el.click()` fallback)
- OCR remaining 48 `likely_scanned` PDFs via `ocr_pdfs.py`
  (JFI=42, JOR=3, MSCI=2, RA=1)

**Pipeline-side:**
- Full-corpus extraction (~34h, $800) after commits pushed
- Frontmatter `source_year` precision tuning — 5 false positives in
  70-doc audit; tighten patterns post-Phase-1 from larger sample
- LLM source-year-only backfill for Patch-12-skipped docs — proposed
  script at `src/post_processing/source_year_llm_backfill.py`;
  ≈ $0.06 for full residual. Deferred until remaining OCR is done.
- Phase 2 work (`citation_location`, `resolution_type`,
  `is_self_citation` firm-level) — depends on citation-key matcher.
  Design doc could be drafted in parallel while full extraction runs.
- Phase 3 work (post-join `confidence`, `academic_subfield` override)

**Analysis-side:**
- Source purity weighting as Phase 4 appendix robustness check
  (deferred per `locked_decisions.md` §Analysis). Only if primary
  results surface source-type heterogeneity concerns. Pre-committed
  author-network-based score, not hand-assigned weights.

**Cleanup-side:**
- Remove deprecated `src/01_extract.py` (still producing
  `output/citations.csv`)
- Directory casing normalization across `data/pdfs/`
- Hash-based resume log for `canonical_scraper.py`
- Update `fields.md` `canonical_metric_ambiguous` description (now 8 patterns)
- Normalize `source_file` path format across scripts (repo-relative, forward slashes)
