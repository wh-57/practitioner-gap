"""
phase1_audit.py — Phase 1 code-vs-LLM accuracy audit.

Reads citations_resolved.csv + documents_resolved.csv and produces:

  output/validation/phase1_audit.csv
      One row per (doc_id, field, code_value, llm_value, agree, manual_label)
      Spans all three Phase 1 fields: is_academic, is_canonical, source_year.
      Includes rows where BOTH code and LLM produced a non-null value
      (needed for agreement computation). Rows where one side is null are
      tracked in the report but not in the pairwise agreement table.

  output/validation/phase1_audit_sample.csv
      Stratified random sample of DISAGREEMENT rows for manual review.
      Sampled up to --n-per-field per field (default 20), stratified by
      source_type when possible. Pre-fills recovered_journal / source_file
      / citation_context to speed up manual labeling.

  output/validation/phase1_audit_report.md
      Markdown summary: agreement rates, Cohen's kappa, disagreement
      distribution by source, top disagreeing venues, top source_year
      disagreement patterns, venues.yaml null rate with top-20 offenders.

Usage:
  python src/validation/phase1_audit.py
  python src/validation/phase1_audit.py --n-per-field 30 --seed 42

The manual workflow after this runs:
  1. Open phase1_audit_sample.csv in Excel
  2. Fill in the `manual_label` column ("code", "llm", "both_wrong",
     or "ambiguous") for each row
  3. Save back to phase1_audit_sample_labeled.csv
  4. Rerun with --labeled phase1_audit_sample_labeled.csv to compute
     code precision / LLM precision from the manual labels
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np


# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent.parent   # gap/
OUT_DIR   = REPO_ROOT / "output"
VAL_DIR   = OUT_DIR / "validation"

CIT_IN   = OUT_DIR / "citations_resolved.csv"
DOC_IN   = OUT_DIR / "documents_resolved.csv"

AUDIT_OUT      = VAL_DIR / "phase1_audit.csv"
SAMPLE_OUT     = VAL_DIR / "phase1_audit_sample.csv"
REPORT_OUT     = VAL_DIR / "phase1_audit_report.md"


# ── Helpers ───────────────────────────────────────────────────────────────────
def _to_bool_or_none(v):
    """CSV round-trip-safe bool parsing."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
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
        if s in ("", "nan", "none", "null"):
            return None
    return None


def _to_int_or_none(v):
    if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v):
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def cohen_kappa(agree_count: int, disagree_count: int,
                p_pos_code: float, p_pos_llm: float) -> float | None:
    """Cohen's kappa for binary agreement.

    Uses observed agreement p_o and expected chance agreement p_e
    based on the marginals. Returns None when undefined.
    """
    total = agree_count + disagree_count
    if total == 0:
        return None
    p_o = agree_count / total
    # Expected chance agreement assuming independence on each side's marginals
    p_neg_code = 1 - p_pos_code
    p_neg_llm  = 1 - p_pos_llm
    p_e = p_pos_code * p_pos_llm + p_neg_code * p_neg_llm
    if p_e == 1:
        return None
    return (p_o - p_e) / (1 - p_e)


# ── Field extractors (citation-level) ────────────────────────────────────────
def extract_is_academic_pairs(cites: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "doc_id":            cites.get("doc_id"),
        "source_type":       cites.get("source_type"),
        "source_file":       cites.get("source_file"),
        "recovered_title":   cites.get("recovered_title"),
        "recovered_journal": cites.get("recovered_journal"),
        "citation_context":  cites.get("citation_context"),
        "field":             "is_academic",
        "code_value":        cites.get("is_academic_code").map(_to_bool_or_none)
                             if "is_academic_code" in cites.columns else None,
        "llm_value":         cites.get("is_academic_llm").map(_to_bool_or_none)
                             if "is_academic_llm" in cites.columns else None,
    })
    return out


def extract_is_canonical_pairs(cites: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "doc_id":            cites.get("doc_id"),
        "source_type":       cites.get("source_type"),
        "source_file":       cites.get("source_file"),
        "recovered_title":   cites.get("recovered_title"),
        "recovered_journal": cites.get("recovered_journal"),
        "citation_context":  cites.get("citation_context"),
        "field":             "is_canonical",
        "code_value":        cites.get("is_canonical_code").map(_to_bool_or_none)
                             if "is_canonical_code" in cites.columns else None,
        "llm_value":         cites.get("is_canonical_llm").map(_to_bool_or_none)
                             if "is_canonical_llm" in cites.columns else None,
    })
    # Attach canonical_pattern_id and likely_metric_only for context
    if "canonical_pattern_id" in cites.columns:
        out["canonical_pattern_id"] = cites["canonical_pattern_id"]
    if "likely_metric_only" in cites.columns:
        out["likely_metric_only"] = cites["likely_metric_only"]
    return out


# ── Field extractor (document-level) ──────────────────────────────────────────
def extract_source_year_pairs(docs: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "doc_id":            docs.get("doc_id"),
        "source_type":       docs.get("source_type"),
        "source_file":       docs.get("source_file"),
        "recovered_title":   docs.get("source_title"),
        "recovered_journal": None,
        "citation_context":  None,
        "field":             "source_year",
        "code_value":        docs.get("source_year_code").map(_to_int_or_none)
                             if "source_year_code" in docs.columns else None,
        "llm_value":         docs.get("source_year_llm").map(_to_int_or_none)
                             if "source_year_llm" in docs.columns else None,
    })
    # Attach the cascade source for diagnostic
    if "source_year_code_source" in docs.columns:
        out["source_year_code_source"] = docs["source_year_code_source"]
    return out


# ── Agreement metrics ─────────────────────────────────────────────────────────
def compute_agreement(pairs: pd.DataFrame, field: str) -> dict:
    """Compute agreement stats for one field's pairs DataFrame."""
    sub = pairs[pairs["field"] == field]
    total_rows = len(sub)

    # Null-state breakdown
    both_null    = sub[sub["code_value"].isna() & sub["llm_value"].isna()]
    code_only    = sub[sub["code_value"].notna() & sub["llm_value"].isna()]
    llm_only     = sub[sub["code_value"].isna() & sub["llm_value"].notna()]
    both_present = sub[sub["code_value"].notna() & sub["llm_value"].notna()]

    # Agreement on the both-present subset
    if len(both_present):
        # Normalize: booleans or ints, both sides the same type by construction
        agree_mask = both_present["code_value"] == both_present["llm_value"]
        agree = int(agree_mask.sum())
        disagree = int((~agree_mask).sum())
        agreement_rate = agree / len(both_present)
    else:
        agree = disagree = 0
        agreement_rate = None

    # Cohen's kappa only makes sense for the binary fields
    kappa = None
    if field in ("is_academic", "is_canonical") and len(both_present):
        p_pos_code = (both_present["code_value"] == True).sum() / len(both_present)
        p_pos_llm  = (both_present["llm_value"]  == True).sum() / len(both_present)
        kappa = cohen_kappa(agree, disagree, p_pos_code, p_pos_llm)

    return {
        "field": field,
        "total_rows": total_rows,
        "both_null": len(both_null),
        "code_only": len(code_only),
        "llm_only": len(llm_only),
        "both_present": len(both_present),
        "agree": agree,
        "disagree": disagree,
        "agreement_rate": agreement_rate,
        "cohen_kappa": kappa,
    }


def stratified_disagreement_sample(pairs: pd.DataFrame, field: str,
                                    n_per_field: int, seed: int) -> pd.DataFrame:
    """Sample up to n_per_field disagreement rows, stratified by source_type."""
    sub = pairs[pairs["field"] == field]
    both_present = sub[sub["code_value"].notna() & sub["llm_value"].notna()]
    disagree = both_present[both_present["code_value"] != both_present["llm_value"]]

    if len(disagree) == 0:
        return pd.DataFrame()

    # Stratify by source_type; proportional allocation with floor of 1 per stratum
    rng = np.random.default_rng(seed)
    strata = disagree["source_type"].fillna("__unknown__").unique()
    samples = []

    if len(disagree) <= n_per_field:
        # Take everything
        sample = disagree.copy()
    else:
        # Proportional allocation
        per_stratum = {}
        total_d = len(disagree)
        for s in strata:
            n_s = (disagree["source_type"].fillna("__unknown__") == s).sum()
            alloc = max(1, int(round(n_per_field * n_s / total_d)))
            per_stratum[s] = alloc
        # Adjust so total = n_per_field
        over = sum(per_stratum.values()) - n_per_field
        while over > 0:
            # Trim from largest strata first (but never below 1)
            candidates = [s for s, a in per_stratum.items() if a > 1]
            if not candidates:
                break
            largest = max(candidates, key=lambda s: per_stratum[s])
            per_stratum[largest] -= 1
            over -= 1

        for s, alloc in per_stratum.items():
            pool = disagree[disagree["source_type"].fillna("__unknown__") == s]
            take = min(alloc, len(pool))
            idx = rng.choice(pool.index, size=take, replace=False)
            samples.append(pool.loc[idx])

        sample = pd.concat(samples, ignore_index=False) if samples else pd.DataFrame()

    sample = sample.copy()
    sample["manual_label"] = ""  # user fills: "code" / "llm" / "both_wrong" / "ambiguous"
    return sample


# ── Report writer ─────────────────────────────────────────────────────────────
def write_report(stats: list[dict], pairs: pd.DataFrame,
                 cites: pd.DataFrame, report_path: Path):
    lines: list[str] = []
    lines.append(f"# Phase 1 Code-vs-LLM Audit Report")
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append("")
    lines.append("Three code paths under audit:")
    lines.append("- `is_academic` — venues.yaml cascade in `01c_resolve.py`")
    lines.append("- `is_canonical` — canonical_pattern_id propagation via `01b_merge.py`")
    lines.append("- `source_year` — path regex → frontmatter regex → LLM in `01c_resolve.py`")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Agreement Summary")
    lines.append("")
    lines.append("| field | total rows | both present | agree | disagree | agreement rate | Cohen's κ |")
    lines.append("|---|---|---|---|---|---|---|")
    for s in stats:
        ag = f"{s['agreement_rate']:.1%}" if s['agreement_rate'] is not None else "—"
        kp = f"{s['cohen_kappa']:.3f}" if s['cohen_kappa'] is not None else "—"
        lines.append(
            f"| `{s['field']}` | {s['total_rows']} | {s['both_present']} | "
            f"{s['agree']} | {s['disagree']} | {ag} | {kp} |"
        )
    lines.append("")
    lines.append("**Interpretation threshold** (per `fields.md` §Phase 1): "
                 "κ > 0.85 or disagreement < 5% means code can replace LLM in the prompt. "
                 "Below that, the LLM fallback should remain active.")
    lines.append("")
    lines.append("## Null-State Breakdown")
    lines.append("")
    lines.append("| field | total | both null | code only | llm only | both present |")
    lines.append("|---|---|---|---|---|---|")
    for s in stats:
        lines.append(
            f"| `{s['field']}` | {s['total_rows']} | {s['both_null']} | "
            f"{s['code_only']} | {s['llm_only']} | {s['both_present']} |"
        )
    lines.append("")
    lines.append("- **`code_only`**: rows where venues.yaml/pattern-match produced "
                 "an answer but LLM said null. Benign if code is right.")
    lines.append("- **`llm_only`**: rows where code returned None (unknown) and LLM "
                 "classified. These are the fall-through cases — worth reviewing "
                 "the `recovered_journal` values to see if venues.yaml needs extensions.")
    lines.append("")

    # Venues.yaml null rate — top 20 offending journals
    is_ac = pairs[pairs["field"] == "is_academic"]
    nulls = is_ac[is_ac["code_value"].isna() & is_ac["recovered_journal"].notna()]
    if len(nulls):
        lines.append("## `venues.yaml` Coverage Gaps")
        lines.append("")
        lines.append(f"Rows where `is_academic_code` is null (venue not classifiable by "
                     f"venues.yaml): {len(nulls)} / {len(is_ac)} = {len(nulls)/len(is_ac):.1%}")
        lines.append("")
        lines.append("**Top 20 uncovered venues** (add to `venues.yaml` if finance-relevant):")
        lines.append("")
        lines.append("| count | recovered_journal |")
        lines.append("|---|---|")
        top = nulls["recovered_journal"].value_counts().head(20)
        for venue, n in top.items():
            lines.append(f"| {n} | {venue} |")
        lines.append("")

    # Top disagreeing venues for is_academic
    both_ac = pairs[(pairs["field"] == "is_academic") &
                    pairs["code_value"].notna() & pairs["llm_value"].notna()]
    disagree_ac = both_ac[both_ac["code_value"] != both_ac["llm_value"]]
    if len(disagree_ac):
        lines.append("## `is_academic` — Top Disagreement Venues")
        lines.append("")
        lines.append("| count | recovered_journal | code says | llm says |")
        lines.append("|---|---|---|---|")
        grouped = (disagree_ac.groupby(["recovered_journal", "code_value", "llm_value"])
                   .size().reset_index(name="n")
                   .sort_values("n", ascending=False).head(15))
        for _, row in grouped.iterrows():
            venue = str(row["recovered_journal"])[:60]
            lines.append(f"| {row['n']} | {venue} | {row['code_value']} | {row['llm_value']} |")
        lines.append("")

    # source_year disagreement magnitude
    sy = pairs[pairs["field"] == "source_year"]
    sy_both = sy[sy["code_value"].notna() & sy["llm_value"].notna()]
    sy_disagree = sy_both[sy_both["code_value"] != sy_both["llm_value"]]
    if len(sy_disagree):
        lines.append("## `source_year` — Disagreement Magnitudes")
        lines.append("")
        diffs = (sy_disagree["code_value"] - sy_disagree["llm_value"]).dropna()
        lines.append(f"- N disagreements: {len(sy_disagree)}")
        lines.append(f"- Code - LLM year diff (mean): {diffs.mean():+.1f}")
        lines.append(f"- Code - LLM year diff (median): {diffs.median():+.0f}")
        lines.append(f"- |diff| <= 1 year: {(diffs.abs() <= 1).sum()} / {len(diffs)}")
        lines.append(f"- |diff| > 3 years: {(diffs.abs() > 3).sum()} / {len(diffs)}")
        lines.append("")
        lines.append("A positive mean means code tends to pick a later year than LLM "
                     "(likely copyright / reprint year on older papers). "
                     "A large |diff| > 3 count suggests frontmatter regex is hitting "
                     "republication years instead of original publication.")
        lines.append("")

    # is_canonical — false positive check via likely_metric_only
    ic = pairs[pairs["field"] == "is_canonical"]
    ic_both = ic[ic["code_value"].notna() & ic["llm_value"].notna()]
    if "canonical_pattern_id" in ic.columns and len(ic_both):
        code_true_llm_false = ic_both[
            (ic_both["code_value"] == True) & (ic_both["llm_value"] == False)
        ]
        if len(code_true_llm_false):
            lines.append("## `is_canonical` — Code=True but LLM=False")
            lines.append("")
            lines.append(f"These are the likeliest false-positive candidates: "
                         f"canonical_scraper regex fired but LLM did not mark canonical.")
            lines.append(f"N = {len(code_true_llm_false)}")
            lines.append("")
            lines.append("**Top patterns in this bucket:**")
            lines.append("")
            lines.append("| count | canonical_pattern_id |")
            lines.append("|---|---|")
            top = code_true_llm_false["canonical_pattern_id"].value_counts().head(10)
            for pid, n in top.items():
                lines.append(f"| {n} | {pid} |")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## Next Steps")
    lines.append("")
    lines.append(f"1. Open `{SAMPLE_OUT.name}` in Excel.")
    lines.append(f"2. Fill in the `manual_label` column (one of: "
                 f"`code`, `llm`, `both_wrong`, `ambiguous`).")
    lines.append(f"3. Save as `phase1_audit_sample_labeled.csv` next to the sample.")
    lines.append(f"4. Rerun: "
                 f"`python src/validation/phase1_audit.py --labeled phase1_audit_sample_labeled.csv`.")
    lines.append(f"   That will produce a code-precision / LLM-precision comparison.")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def compute_labeled_stats(labeled_df: pd.DataFrame) -> str:
    """When a labeled file is provided, compute code-vs-LLM precision per field."""
    out = ["", "## Labeled Manual Review Results", ""]
    for field in ["is_academic", "is_canonical", "source_year"]:
        sub = labeled_df[labeled_df["field"] == field]
        sub = sub[sub["manual_label"].isin(["code", "llm", "both_wrong", "ambiguous"])]
        if len(sub) == 0:
            continue
        n = len(sub)
        code_right = (sub["manual_label"] == "code").sum()
        llm_right  = (sub["manual_label"] == "llm").sum()
        both_wrong = (sub["manual_label"] == "both_wrong").sum()
        ambig      = (sub["manual_label"] == "ambiguous").sum()
        out.append(f"### `{field}` ({n} manually labeled disagreements)")
        out.append("")
        out.append(f"- Code correct: {code_right} ({code_right/n:.0%})")
        out.append(f"- LLM correct:  {llm_right} ({llm_right/n:.0%})")
        out.append(f"- Both wrong:   {both_wrong} ({both_wrong/n:.0%})")
        out.append(f"- Ambiguous:    {ambig} ({ambig/n:.0%})")
        out.append("")
    return "\n".join(out)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Phase 1 code-vs-LLM audit")
    parser.add_argument("--n-per-field", type=int, default=20,
                        help="Disagreement sample size per field (default 20)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for stratified sampling")
    parser.add_argument("--labeled", type=Path, default=None,
                        help="Optional: path to a labeled sample CSV. "
                             "If provided, appends labeled-review stats to the report.")
    args = parser.parse_args()

    if not CIT_IN.exists():
        raise FileNotFoundError(
            f"{CIT_IN} not found. Run 01_extract_deep → canonical_scraper → "
            "01b_merge → 01c_resolve first."
        )
    if not DOC_IN.exists():
        raise FileNotFoundError(
            f"{DOC_IN} not found. Run 01c_resolve first (it writes this file)."
        )

    print(f"Loading {CIT_IN}")
    cites = pd.read_csv(CIT_IN, low_memory=False)
    print(f"  {len(cites)} citation rows across {cites['doc_id'].nunique()} docs")

    print(f"Loading {DOC_IN}")
    docs = pd.read_csv(DOC_IN, low_memory=False)
    print(f"  {len(docs)} document rows")

    # Extract the three field pairs
    pairs_ac  = extract_is_academic_pairs(cites)
    pairs_ic  = extract_is_canonical_pairs(cites)
    pairs_sy  = extract_source_year_pairs(docs)

    # Align columns and concat
    common_cols = ["doc_id", "source_type", "source_file", "recovered_title",
                   "recovered_journal", "citation_context", "field",
                   "code_value", "llm_value"]
    for df in (pairs_ac, pairs_ic, pairs_sy):
        for col in common_cols:
            if col not in df.columns:
                df[col] = None

    pairs = pd.concat([
        pairs_ac[common_cols + [c for c in pairs_ac.columns if c not in common_cols]],
        pairs_ic[common_cols + [c for c in pairs_ic.columns if c not in common_cols]],
        pairs_sy[common_cols + [c for c in pairs_sy.columns if c not in common_cols]],
    ], ignore_index=True)

    # Agreement flag (True/False/None for rows where both sides null)
    def _agree(r):
        if pd.isna(r["code_value"]) or pd.isna(r["llm_value"]):
            return None
        return r["code_value"] == r["llm_value"]
    pairs["agree"] = pairs.apply(_agree, axis=1)
    pairs["manual_label"] = ""

    VAL_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nWriting {AUDIT_OUT}")
    pairs.to_csv(AUDIT_OUT, index=False)

    # Per-field stats
    stats = [
        compute_agreement(pairs, "is_academic"),
        compute_agreement(pairs, "is_canonical"),
        compute_agreement(pairs, "source_year"),
    ]

    print("\nAgreement summary:")
    for s in stats:
        ag = f"{s['agreement_rate']:.1%}" if s['agreement_rate'] is not None else "—"
        kp = f"{s['cohen_kappa']:.3f}" if s['cohen_kappa'] is not None else "—"
        print(f"  {s['field']:14s}  both_present={s['both_present']:5d}  "
              f"agree={s['agree']:5d}  disagree={s['disagree']:5d}  "
              f"rate={ag}  κ={kp}")

    # Stratified disagreement samples per field
    print("\nSampling disagreements for manual review...")
    samples = []
    for field in ["is_academic", "is_canonical", "source_year"]:
        s = stratified_disagreement_sample(pairs, field,
                                           args.n_per_field, args.seed)
        if len(s):
            samples.append(s)
            print(f"  {field:14s}  sampled {len(s)} disagreements")
        else:
            print(f"  {field:14s}  no disagreements found")

    if samples:
        sample_df = pd.concat(samples, ignore_index=True)
        print(f"\nWriting {SAMPLE_OUT}")
        sample_df.to_csv(SAMPLE_OUT, index=False)
    else:
        print("\nNo disagreements to sample. Sample file not written.")

    # Report
    print(f"\nWriting {REPORT_OUT}")
    write_report(stats, pairs, cites, REPORT_OUT)

    # Labeled review stats, if provided
    if args.labeled is not None and args.labeled.exists():
        print(f"\nReading labeled sample: {args.labeled}")
        labeled_df = pd.read_csv(args.labeled)
        stats_md = compute_labeled_stats(labeled_df)
        with open(REPORT_OUT, "a", encoding="utf-8") as f:
            f.write(stats_md)
        print("  Appended labeled-review stats to report.")

    print("\nDone.")
    print(f"\nOpen {REPORT_OUT} first for the summary.")
    print(f"Then open {SAMPLE_OUT} to do manual labeling.")


if __name__ == "__main__":
    main()