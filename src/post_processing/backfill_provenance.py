# %% Backfill provenance columns on pre-Phase-0 pilot output
# One-shot. Not part of the pipeline.
#
# Context: The 110-doc pilot ran before Phase 0 added prompt_hash / patterns_hash /
# code_version to every output row. This script stamps those three columns onto
# the existing rows in output/citations_deep.csv and output/documents_deep.csv
# with the literal value "pre_phase_0", so the column-exists-in-every-row
# invariant holds for the full corpus.
#
# Safe to rerun: if the columns already contain non-null, non-"pre_phase_0"
# values, those are preserved (idempotent).
#
# Run from repo root:
#     python src/post_processing/backfill_provenance.py
# Or from this script's directory — paths are anchored via Path(__file__).
#
# If a CSV is locked (Excel, file preview), the write fails with a clear
# message and the script continues to the next file. Rerun after closing.

import sys
from pathlib import Path
import pandas as pd

# Anchor: gap/src/post_processing/backfill_provenance.py → REPO_ROOT = gap/
REPO_ROOT     = Path(__file__).resolve().parent.parent.parent
OUT_DIR       = REPO_ROOT / "output"
CITATIONS_CSV = OUT_DIR / "citations_deep.csv"
DOCUMENTS_CSV = OUT_DIR / "documents_deep.csv"

PROVENANCE_COLS = ["prompt_hash", "patterns_hash", "code_version"]
SENTINEL        = "pre_phase_0"


def backfill(csv_path: Path) -> bool:
    """
    Returns True on success (including skip when file is missing or all
    columns already populated). Returns False if a write was attempted but
    failed (e.g., file locked). Never raises PermissionError — surfaces as
    a clean message and a False return.
    """
    if not csv_path.exists():
        print(f"[skip] {csv_path.name} does not exist")
        return True

    df = pd.read_csv(csv_path)
    n_rows = len(df)
    print(f"[{csv_path.name}] {n_rows} rows")

    # Plan the changes in memory. Do NOT print "added" yet — we haven't
    # written. The previous version of this script lied about this.
    plan: list[tuple[str, str, int]] = []  # (col, action, count)
    for col in PROVENANCE_COLS:
        if col not in df.columns:
            df[col] = SENTINEL
            plan.append((col, "add", n_rows))
            continue
        is_null = df[col].isna() | (df[col].astype(str).str.strip() == "")
        n_null  = int(is_null.sum())
        if n_null == 0:
            plan.append((col, "keep", 0))
        else:
            df.loc[is_null, col] = SENTINEL
            plan.append((col, "fill", n_null))

    # If nothing needs changing, skip the write entirely.
    if all(action == "keep" for _, action, _ in plan):
        print(f"  . all provenance columns already populated - no write needed")
        return True

    # Attempt the write. Only log results after it returns cleanly.
    try:
        df.to_csv(csv_path, index=False)
    except PermissionError:
        print(f"  [ERROR] cannot write {csv_path.name} - file is locked.")
        print(f"          Close it in Excel / file explorer preview and rerun.")
        print(f"          No changes were written to disk for this file.")
        return False

    for col, action, count in plan:
        if action == "add":
            print(f"  + added '{col}' on {count} rows")
        elif action == "fill":
            print(f"  + filled {count} null cells in '{col}'")
        else:
            print(f"  . '{col}' already populated - no change")
    print(f"  wrote {csv_path}")
    return True


def main() -> int:
    if not OUT_DIR.exists():
        print(f"[error] output directory missing: {OUT_DIR}", file=sys.stderr)
        return 1

    print(f"Backfill sentinel : '{SENTINEL}'")
    print(f"Target directory  : {OUT_DIR}\n")

    ok_docs = backfill(DOCUMENTS_CSV)
    print()
    ok_cits = backfill(CITATIONS_CSV)

    if ok_docs and ok_cits:
        print("\nDone. Pre-Phase-0 rows now carry provenance sentinel.")
        print("New rows written by 01_extract_deep.py will carry real hashes.")
        return 0

    print("\n[WARN] One or more files could not be written. Fix the cause and rerun.")
    print("       Script is idempotent - rerunning is safe.")
    return 2


if __name__ == "__main__":
    sys.exit(main())