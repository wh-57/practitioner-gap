"""
Analyze the completed validation audit workbook.

Reads  output/validation/audit_template.xlsx  (with the 'audit' sheet filled in)
Writes output/validation/validation_results.csv
       output/validation/validation_results.md    (methods-appendix-ready)

Metrics:
  Detection precision = (correct + wrong_id) / (correct + wrong_id + hallucinated)
  Detection recall    = (correct + wrong_id) / (correct + wrong_id + fn_found)
  Identification acc  = correct / (correct + wrong_id)
  Field accuracy      = agreement rate on is_academic, academic_subfield,
                        is_canonical, citation_function

  Field accuracy treats a blank in the human column as "human agrees with LLM".
  The auditor fills the human column only when they disagree.

Stratification: by source_type, pdf_extraction_method, and doc_text_truncated
(joined in from output/validation/sample.csv).
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

REPO_ROOT   = Path(__file__).resolve().parent.parent.parent
IN_PATH     = REPO_ROOT / "output" / "validation" / "audit_template.xlsx"
SAMPLE_PATH = REPO_ROOT / "output" / "validation" / "sample.csv"
OUT_CSV     = REPO_ROOT / "output" / "validation" / "validation_results.csv"
OUT_MD      = REPO_ROOT / "output" / "validation" / "validation_results.md"


def cohens_kappa(a, b):
    """Cohen's kappa for two parallel label lists."""
    a, b = list(a), list(b)
    if not a:
        return np.nan
    n = len(a)
    po = sum(1 for i in range(n) if a[i] == b[i]) / n
    categories = sorted(set(a) | set(b))
    pe = sum((a.count(c) / n) * (b.count(c) / n) for c in categories)
    if abs(1 - pe) < 1e-9:
        return 1.0 if po == 1.0 else np.nan
    return (po - pe) / (1 - pe)


def _is_blank(val):
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    return str(val).strip() == ""


def _metrics_for_group(subset: pd.DataFrame) -> dict:
    """Compute metrics for one group (all rows, or one stratum)."""
    # Only rows where the auditor gave a verdict
    audited = subset[~subset["human_verdict"].apply(_is_blank)].copy()

    llm_rows = audited[audited["row_type"] == "llm"]
    fn_rows  = audited[audited["row_type"] == "fn_blank"]

    correct  = int((llm_rows["human_verdict"] == "correct").sum())
    wrong_id = int((llm_rows["human_verdict"] == "wrong_identification").sum())
    halluc   = int((llm_rows["human_verdict"] == "hallucinated").sum())
    fn_found = int((fn_rows["human_verdict"] == "human_found").sum())

    det_tp = correct + wrong_id
    det_fp = halluc
    det_fn = fn_found

    det_prec = det_tp / (det_tp + det_fp) if (det_tp + det_fp) > 0 else np.nan
    det_rec  = det_tp / (det_tp + det_fn) if (det_tp + det_fn) > 0 else np.nan
    id_acc   = correct / det_tp if det_tp > 0 else np.nan

    # Field-level agreement (on correct + wrong_id rows only)
    judgeable = llm_rows[llm_rows["human_verdict"].isin(["correct", "wrong_identification"])].copy()

    def field_agreement(human_col):
        if len(judgeable) == 0 or human_col not in judgeable.columns:
            return np.nan, 0
        # Blank in human col => agree with LLM
        disagree = judgeable[human_col].apply(lambda v: not _is_blank(v)).sum()
        agree = len(judgeable) - disagree
        return agree / len(judgeable), int(disagree)

    is_acad_acc, _   = field_agreement("human_is_academic")
    subfield_acc, _  = field_agreement("human_academic_subfield")
    canonical_acc, _ = field_agreement("human_is_canonical")
    function_acc, _  = field_agreement("human_citation_function")

    # Kappa only where human explicitly filled (so we have a true label per row).
    # This is conservative but correct — kappa undefined on "assumed agree" rows.
    def kappa_on_filled(llm_col, human_col):
        if llm_col not in judgeable.columns or human_col not in judgeable.columns:
            return np.nan
        mask = judgeable[human_col].apply(lambda v: not _is_blank(v))
        if mask.sum() < 5:
            return np.nan
        return cohens_kappa(
            judgeable.loc[mask, llm_col].astype(str).tolist(),
            judgeable.loc[mask, human_col].astype(str).tolist(),
        )

    canonical_kappa = kappa_on_filled("is_canonical", "human_is_canonical")
    function_kappa  = kappa_on_filled("citation_function", "human_citation_function")

    return {
        "n_docs":                     int(subset["doc_id"].nunique()),
        "n_llm_audited":              int(len(llm_rows)),
        "correct":                    correct,
        "wrong_id":                   wrong_id,
        "hallucinated":               halluc,
        "fn_found":                   fn_found,
        "detection_precision":        det_prec,
        "detection_recall":           det_rec,
        "identification_accuracy":    id_acc,
        "is_academic_accuracy":       is_acad_acc,
        "subfield_accuracy":          subfield_acc,
        "is_canonical_accuracy":      canonical_acc,
        "is_canonical_kappa":         canonical_kappa,
        "citation_function_accuracy": function_acc,
        "citation_function_kappa":    function_kappa,
    }


def compute_all(audit: pd.DataFrame) -> pd.DataFrame:
    rows = []
    overall = _metrics_for_group(audit)
    rows.append({"stratum": "OVERALL", **overall})

    for col in ("source_type", "pdf_extraction_method", "doc_text_truncated"):
        if col in audit.columns:
            for val, grp in audit.groupby(col, dropna=False):
                label = f"{col}={val}"
                rows.append({"stratum": label, **_metrics_for_group(grp)})

    return pd.DataFrame(rows)


def _fmt(v):
    if pd.isna(v):
        return "n/a"
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def write_markdown(df: pd.DataFrame, path: Path):
    lines = [
        "# Validation Results",
        "",
        "Manual audit of LLM-extracted citations against human coding of PDFs.",
        "",
        "**Detection metrics** — whether the LLM identified a citation that actually",
        "exists in the document. TP = LLM extracted a real citation (correct or",
        "misidentified); FP = LLM hallucinated a citation; FN = human found a citation",
        "LLM missed.",
        "",
        "**Identification accuracy** — conditional on detection, whether the LLM got",
        "the citation's identity (title, authors, year) correct.",
        "",
        "**Field accuracy** — agreement rate on `is_academic`, `academic_subfield`,",
        "`is_canonical`, and `citation_function`. A blank entry in the human column is",
        "treated as human agreement with the LLM. Cohen's κ is computed only on rows",
        "where the auditor explicitly entered a label (disagreement signal only).",
        "",
        "## Results by stratum",
        "",
    ]
    cols = [
        "stratum", "n_docs", "n_llm_audited", "correct", "wrong_id",
        "hallucinated", "fn_found",
        "detection_precision", "detection_recall", "identification_accuracy",
        "is_academic_accuracy", "subfield_accuracy",
        "is_canonical_accuracy", "is_canonical_kappa",
        "citation_function_accuracy", "citation_function_kappa",
    ]
    present = [c for c in cols if c in df.columns]
    lines.append("| " + " | ".join(present) + " |")
    lines.append("| " + " | ".join(["---"] * len(present)) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(_fmt(row.get(c, "")) for c in present) + " |")

    path.write_text("\n".join(lines), encoding="utf-8")


def print_results(df: pd.DataFrame):
    print("\n" + "=" * 78)
    print("VALIDATION RESULTS")
    print("=" * 78)
    for _, row in df.iterrows():
        print(f"\n[{row['stratum']}]")
        print(f"  n docs audited      : {int(row['n_docs'])}")
        print(f"  LLM citations       : {int(row['n_llm_audited'])}")
        print(f"  correct/wrong/halluc: {int(row['correct'])} / {int(row['wrong_id'])} / {int(row['hallucinated'])}")
        print(f"  FN found            : {int(row['fn_found'])}")
        print(f"  detection precision : {_fmt(row['detection_precision'])}")
        print(f"  detection recall    : {_fmt(row['detection_recall'])}")
        print(f"  identification acc  : {_fmt(row['identification_accuracy'])}")
        print(f"  is_academic acc     : {_fmt(row['is_academic_accuracy'])}")
        print(f"  subfield acc        : {_fmt(row['subfield_accuracy'])}")
        print(f"  canonical acc / κ   : {_fmt(row['is_canonical_accuracy'])} / κ={_fmt(row['is_canonical_kappa'])}")
        print(f"  function acc / κ    : {_fmt(row['citation_function_accuracy'])} / κ={_fmt(row['citation_function_kappa'])}")


def main():
    parser = argparse.ArgumentParser(description="Analyze completed audit workbook")
    parser.add_argument("--input", type=str, default=str(IN_PATH),
                        help="Completed audit workbook path (default: output/validation/audit_template.xlsx)")
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"{in_path} not found.")

    audit = pd.read_excel(in_path, sheet_name="audit")
    print(f"Loaded {len(audit)} audit rows from {in_path.name}")

    # Join in stratification columns from sample.csv
    if SAMPLE_PATH.exists():
        sample = pd.read_csv(SAMPLE_PATH, low_memory=False)
        join_cols = [c for c in ("source_type", "pdf_extraction_method", "doc_text_truncated")
                     if c in sample.columns]
        meta = sample[["doc_id"] + join_cols].drop_duplicates("doc_id")
        audit = audit.merge(meta, on="doc_id", how="left")

    df = compute_all(audit)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    write_markdown(df, OUT_MD)
    print_results(df)
    print(f"\nWrote: {OUT_CSV}")
    print(f"       {OUT_MD}")


if __name__ == "__main__":
    main()
