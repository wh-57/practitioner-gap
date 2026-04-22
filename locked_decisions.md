# GAP Project — Locked Decisions
Last updated: 2026-04-20

Settled methodology questions. Never remove entries. Only Hill can reopen
a locked decision. When a decision is settled in chat, append here with
rationale + date.

This is the authoritative registry; `gap_structure.md` may reference these
entries but should not duplicate them.

---

## Corpus Scope

| Decision | Rationale | Date Locked |
|----------|-----------|-------------|
| Sell-side research is a structural exclusion, not a limitation | Measuring direct buy-side engagement, not full academic → practitioner transmission chain | (pre-registry) |
| Corpus skew toward asset pricing is a finding to document, not a bias to correct | Skew reflects the empirical shape of practitioner research; "correcting" it would hide the answer | (pre-registry) |
| Denominator cleanliness over coverage | Only unambiguously original practitioner research counts; uncertain-provenance sources are retained for analysis-time filtering, not denominator inflation | (pre-registry) |

## Methodology

| Decision | Rationale | Date Locked |
|----------|-----------|-------------|
| `is_academic` = community membership, not peer review | FAJ/JPM are practitioner venues even if peer-reviewed; author/audience community is the operative criterion | (pre-registry) |
| Join constraint: `\|recovered_year - bench_year\| <= 2` | Tolerant enough for forthcoming/WP-vs-published year slippage, tight enough to avoid false positives on common title + common author collisions | (pre-registry) |
| ~86.7% fuzzy-match rate acceptable for `03_join.py` | Remaining unmatched rows can still be analyzed as "academic but unjoined"; chasing higher rates risks false positives | (pre-registry) |
| `recovered_year` stays LLM, not code | Reversed from earlier plan after adversarial review. Regex on `raw_mention` fails on "forthcoming 2026", "1993a/b" suffixes, year ranges, and sample-vs-publication year confusion. Null LLM year is safer for `03_join.py`'s year constraint than wrong deterministic year. See `fields.md` §Reversals. | 2026-04-20 |
| `recovered_first_version_year` stays LLM, not code | Reversed from earlier plan. Prompt already says "only if literally in text" which matches what regex does; LLM handles messy cases better. OpenAlex post-join is the real source of truth for this field. | 2026-04-20 |
| Phase 1 split: structural merge (`01b_merge.py`) separate from field-level resolution (`01c_resolve.py`) | Two different concerns. `01b_merge.py` combines LLM + canonical rows structurally; `01c_resolve.py` resolves LLM-vs-code disagreements per-field. Neither supersedes the other. | 2026-04-20 |
| `likely_metric_only` = `structural_uncertain` | Sharpe ratio, Jensen's alpha, Fama-MacBeth, GARCH, etc. are often cited as methods/metrics rather than as papers. When canonical_scraper hits and LLM doesn't, hedge with "structural_uncertain" instead of forcing True or False. | 2026-04-20 |
| `venues.yaml` `ambiguous:` set returns None (defer to LLM) | Better a null LLM fallback than a wrong deterministic classification. Includes genuinely ambiguous abbreviations (JFI), publishers-not-journals (Oxford UP, Wiley), and contexts where disambiguation requires post-join OpenAlex lookup. | 2026-04-20 |
| Patch 12 threshold: `extraction_method == "likely_scanned"` ⇔ wpp < 50 | Post-OCR corpus scan showed bimodal wpp distribution; threshold cleanly separates scanned from native-text PDFs without false positives on text-heavy docs | 2026-04-20 |

## Analysis

| Decision | Rationale | Date Locked |
|----------|-----------|-------------|
| Jaccard as baseline only for Divergence framing | Mechanically biased by set-size asymmetry when practitioner and academic sets differ in size. Use overlap coefficient and cosine similarity on citation-frequency vectors as primaries; Jaccard reported only as baseline for transparency. | (pre-registry) |
| Share normalized by annual academic output volume | Raw share is confounded by exponential growth in academic publications. Normalization makes trends interpretable across time. | (pre-registry) |
| Lag framed as citation age distribution, not causal diffusion | Cannot identify causal diffusion from citation data. Separate canonical (ceremonial) from recent (fresh adoption) citations; report hazard of first citation plus age distribution conditional on citation. | (pre-registry) |
| `is_canonical` flag separates ceremonial from substantive citations | Needed for unbiased Citation Age Profile and Lag estimates. Practitioners citing Markowitz 1952 is categorically different from citing a 2023 JF paper. | (pre-registry) |
| Six analytical framings: Share, Selectivity, Lag, Divergence, Absorption by Channel, Citation Age Profile | Four original + two added after adversarial review to address corpus-composition and canonical-vs-substantive critiques. See `gap_structure.md` §"Six Analytical Framings". | (pre-registry) |
| Source purity weighting deferred to Phase 4 robustness check, not primary analysis | Framing 5 (Absorption by Channel) already stratifies by source_type, giving visibility into practitioner-corpus heterogeneity without committing to a single weighted aggregate. Purity weighting would duplicate this for the main analysis and invite "why those weights?" critique. Deferred as an appendix robustness check — to be built only if primary results surface source-type heterogeneity concerns. If built: use pre-committed author-network-based score (e.g., `1 − share of source's authors with ≥1 publication in benchmark journals`), not hand-assigned weights. | 2026-04-20 |

## Schema / Provenance

| Decision | Rationale | Date Locked |
|----------|-----------|-------------|
| `prompt_hash` hashes `EXTRACTION_PROMPT` as a string, not full API payload | The prompt string defines the extraction contract; payload includes model ID and other metadata that shouldn't invalidate the hash | 2026-04-20 (Phase 0) |
| `patterns_hash` hashes `patterns.yaml` bytes directly | Comment-only edits change the hash; that's desired (audit-visible) | 2026-04-20 (Phase 0) |
| `code_version` uses git hash; falls through to `no_git` / `dirty` on failure | No silent lies about version; explicit "can't tell" states | 2026-04-20 (Phase 0) |
| Schema bumps on any field semantics change, additive or otherwise | Phase 0 added three columns without bump (treated as infrastructure); Phase 1 bumped to 2.2 (added fields + coalesce logic). Future: any rename, added-enum-value, or changed-meaning bumps schema. | 2026-04-20 |

---

## Explicitly Deferred (not yet decided)

- **`dedup.py` location** — `gap_structure.md` references it as a post-extraction step but it's not on disk. Either inlined into `01b_merge.py` planning, or legitimately deferred to Phase 2. Reconcile before pipeline assumes it exists.
- **Hash-based resume log for `canonical_scraper.py`** — current resume log is path-based, requires `--fresh` after any OCR batch. Switch to hash-based is cheap; deferred for now because `--fresh` runs take minutes.
- **`03_join.py` handling of resolved None `is_academic`** — rows where code returned None and LLM also null: currently fall to `cit_other` (non-academic) in `03_join.py`. Probably fine; worth confirming against real data once full extraction runs.
- **Directory casing normalization across `data/pdfs/`** — mixed CamelCase / UPPER / lower / lower_underscore. Not urgent; `extraction_quality_scan.py` treats folder name as literal `source` value.
