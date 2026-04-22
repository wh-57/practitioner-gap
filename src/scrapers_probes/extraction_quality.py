# %% Extraction-quality scan — one-shot diagnostic for Patch 12
# Not part of the pipeline. Reads every PDF and TXT in data/pdfs/ and
# data/Other_Corpus/ and reports page_count, word_count, words_per_page,
# and the extraction_method that 01_extract_deep.py would assign.
#
# Purpose: decide where to set the skip threshold for likely-scanned
# documents in 01_extract_deep.py. Current code skips only when
# word_count == 0; Patch 12 adds a density/absolute-count threshold.
# That threshold should come from the actual distribution of the corpus,
# not guessed.
#
# Run from repo root:
#     python src/scrapers_probes/extraction_quality_scan.py
#
# Outputs:
#     output/extraction_quality_scan.csv  — one row per file
#     Console: summary stats + histogram + per-source scan counts +
#              list of 20 worst offenders (lowest word_count)

import sys
from pathlib import Path

import fitz
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_DIR   = REPO_ROOT / "data" / "pdfs"
OTHER_DIR = REPO_ROOT / "data" / "Other_Corpus"
OUT_PATH  = REPO_ROOT / "output" / "extraction_quality_scan.csv"

# Mirrors the threshold used in 01_extract_deep.py::extract_pdf_text
WPP_SCANNED_CUTOFF = 50


def scan_pdf(path: Path) -> dict:
    try:
        doc = fitz.open(path)
        page_count = len(doc)
        text = "\n".join(page.get_text() for page in doc)
        doc.close()
    except Exception as e:
        return {
            "source_file": str(path),
            "source": path.parent.name,
            "format": "pdf",
            "page_count": -1,
            "word_count": -1,
            "total_chars": -1,
            "words_per_page": -1.0,
            "extraction_method": "error",
            "error": str(e)[:200],
        }
    word_count  = len(text.split())
    total_chars = len(text)
    wpp = word_count / max(page_count, 1)
    return {
        "source_file": str(path),
        "source": path.parent.name,
        "format": "pdf",
        "page_count": page_count,
        "word_count": word_count,
        "total_chars": total_chars,
        "words_per_page": round(wpp, 2),
        "extraction_method": "likely_scanned" if wpp < WPP_SCANNED_CUTOFF else "native_text",
        "error": "",
    }


def scan_txt(path: Path) -> dict:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        return {
            "source_file": str(path),
            "source": path.parent.name,
            "format": "txt",
            "page_count": 0,
            "word_count": -1,
            "total_chars": -1,
            "words_per_page": -1.0,
            "extraction_method": "error",
            "error": str(e)[:200],
        }
    return {
        "source_file": str(path),
        "source": path.parent.name,
        "format": "txt",
        "page_count": 0,
        "word_count": len(text.split()),
        "total_chars": len(text),
        "words_per_page": -1.0,   # undefined for txt
        "extraction_method": "html_derived",
        "error": "",
    }


def main() -> int:
    pdfs = sorted(PDF_DIR.glob("**/*.pdf"))
    txts = sorted(f for f in OTHER_DIR.glob("**/*.txt") if not f.name.startswith("_"))
    print(f"Scanning {len(pdfs)} PDFs + {len(txts)} TXTs ...\n")

    rows = []
    for i, p in enumerate(pdfs, start=1):
        rows.append(scan_pdf(p))
        if i % 500 == 0 or i == len(pdfs):
            print(f"  [{i:>5}/{len(pdfs)}] scanned")
    for p in txts:
        rows.append(scan_txt(p))

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"\nWrote {len(df)} rows -> {OUT_PATH}")

    # ── Summary ──────────────────────────────────────────────────────────────
    pdf_df = df[df["format"] == "pdf"].copy()
    n_pdf  = len(pdf_df)
    print("\n=== EXTRACTION QUALITY SUMMARY (PDFs only) ===")
    print(f"Total PDFs                    : {n_pdf}")
    print(f"  extraction errors           : {(pdf_df['extraction_method'] == 'error').sum()}")
    print(f"  native_text  (wpp >= 50)    : {(pdf_df['extraction_method'] == 'native_text').sum()}")
    print(f"  likely_scanned (wpp < 50)   : {(pdf_df['extraction_method'] == 'likely_scanned').sum()}")
    print(f"  word_count == 0             : {(pdf_df['word_count'] == 0).sum()}")

    wpp = pdf_df.loc[pdf_df["words_per_page"] >= 0, "words_per_page"]
    print(f"\nwords_per_page distribution (PDFs, extraction succeeded):")
    print(f"  min     : {wpp.min():>8.1f}")
    print(f"  5 pct   : {wpp.quantile(0.05):>8.1f}")
    print(f"  25 pct  : {wpp.quantile(0.25):>8.1f}")
    print(f"  median  : {wpp.median():>8.1f}")
    print(f"  75 pct  : {wpp.quantile(0.75):>8.1f}")
    print(f"  95 pct  : {wpp.quantile(0.95):>8.1f}")
    print(f"  max     : {wpp.max():>8.1f}")

    print("\nHistogram of words_per_page (PDFs):")
    bins   = [0, 10, 25, 50, 75, 100, 150, 200, 300, 500, 1000, 100000]
    labels = [f"{bins[i]:>5}-{bins[i+1]:<5}" for i in range(len(bins) - 1)]
    cut    = pd.cut(wpp, bins=bins, labels=labels, right=False, include_lowest=True)
    counts = cut.value_counts().sort_index()
    peak   = counts.max()
    for label, count in counts.items():
        bar_len = int(60 * count / peak) if peak else 0
        bar = "#" * bar_len
        print(f"  wpp {label} : {count:>5}  {bar}")

    print("\nLowest word_count PDFs (top 20 — scanning junk candidates):")
    low = pdf_df.sort_values("word_count").head(20)
    print(f"  {'wc':>6} {'pages':>5} {'wpp':>6}  file")
    for _, r in low.iterrows():
        name = Path(r["source_file"]).name
        if len(name) > 70:
            name = name[:67] + "..."
        print(f"  {r['word_count']:>6} {r['page_count']:>5} {r['words_per_page']:>6.1f}  {name}")

    print("\nlikely_scanned PDFs by source:")
    scanned = pdf_df[pdf_df["extraction_method"] == "likely_scanned"]
    if len(scanned) == 0:
        print("  (none)")
    else:
        by_source = scanned.groupby("source").size().sort_values(ascending=False)
        for src, n in by_source.items():
            total = (pdf_df["source"] == src).sum()
            pct   = 100 * n / total if total else 0
            print(f"  {src:<25} : {n:>4} / {total:>4}  ({pct:>5.1f}%)")

    print(f"\nFull per-file data at: {OUT_PATH}")
    print("Paste the summary above back and we can set the Patch 12 threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())