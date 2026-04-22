"""
Copy the 50 audit PDFs out of data/pdfs/{source}/ into one flat folder at
src/validation/audit_pdfs/ so you can walk through them easily during the
validation audit.

Filenames are prefixed with their source folder (e.g. FAJ__some_paper.pdf) to
prevent collisions when two sources happen to use the same filename.

Uses shutil.copy2 — COPIES the files, does not move them. The originals in
data/pdfs/ are untouched.

Idempotent: re-running skips files that already exist in the target folder.
"""

import argparse
import shutil
from pathlib import Path
import openpyxl

# Anchor paths to script location: gap/src/validation/copy_pdfs.py
HERE       = Path(__file__).resolve().parent                    # src/validation/
REPO_ROOT  = HERE.parent.parent                                 # gap/
DEFAULT_XLSX = REPO_ROOT / "output" / "validation" / "audit_template.xlsx"
TARGET_DIR = HERE / "audit_pdfs"

SOURCE_FILE_COL = 6   # column F in the audit sheet


def read_source_paths(xlsx_path: Path) -> list[Path]:
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb["audit"]
    seen = set()
    paths = []
    for row in ws.iter_rows(min_row=2, min_col=SOURCE_FILE_COL, max_col=SOURCE_FILE_COL, values_only=True):
        val = row[0]
        if val and val not in seen:
            seen.add(val)
            paths.append(Path(val))
    wb.close()
    return paths


def target_filename(src: Path) -> str:
    """Flatten {source_folder}/{filename} → {source_folder}__{filename}."""
    source_folder = src.parent.name
    return f"{source_folder}__{src.name}"


def main():
    parser = argparse.ArgumentParser(description="Copy audit PDFs into src/validation/audit_pdfs/")
    parser.add_argument("--xlsx", type=str, default=str(DEFAULT_XLSX),
                        help=f"Path to audit_template.xlsx (default: {DEFAULT_XLSX})")
    parser.add_argument("--target", type=str, default=str(TARGET_DIR),
                        help=f"Target folder (default: {TARGET_DIR})")
    args = parser.parse_args()

    xlsx_path = Path(args.xlsx)
    target_dir = Path(args.target)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Audit workbook not found: {xlsx_path}")

    paths = read_source_paths(xlsx_path)
    print(f"Found {len(paths)} unique source files in {xlsx_path.name}")

    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"Target folder: {target_dir}\n")

    copied = 0
    skipped_exists = 0
    missing = []

    for src in paths:
        dst = target_dir / target_filename(src)
        if not src.exists():
            missing.append(src)
            print(f"  [MISSING] {src}")
            continue
        if dst.exists():
            skipped_exists += 1
            continue
        shutil.copy2(src, dst)
        copied += 1
        print(f"  [copied]  {src.parent.name}/{src.name}")

    print(f"\nSummary:")
    print(f"  copied         : {copied}")
    print(f"  already present: {skipped_exists}")
    print(f"  missing on disk: {len(missing)}")

    if missing:
        print("\nMissing files (check that the scraper actually saved these):")
        for p in missing:
            print(f"  {p}")


if __name__ == "__main__":
    main()