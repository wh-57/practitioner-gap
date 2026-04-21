"""
ocr_jpm.py
==========
OCR pipeline for old JPM PDFs. Targets all JPM files currently below
CANDIDATE_WPP_THRESHOLD words-per-page — covers both zero-word true-image
scans (~173) and watermarked image scans (~270).

Why force-OCR: the watermarked files have a thin pm-research.com copyright
text layer (~42 wpp) overlaid on image-based body content. Normal ocrmypdf
would see the text layer and skip the page. --force-ocr re-OCRs anyway,
producing a file with both the watermark string AND the body text. The
watermark is noise the downstream LLM extractor already ignores.

Flow per file:
  1. Scan pdfs/JPM/ for files with wpp < 50 (natural resume — already-OCR'd
     files get wpp >> 50 and won't be rediscovered)
  2. Copy original pdfs/JPM/file → ocr_backups/JPM/file (preserve pristine)
  3. ocrmypdf backup/file → pdfs/JPM/file.ocr_tmp (write to temp)
  4. Verify temp has more words than original, then atomic-move into place
  5. On any failure: delete temp, original in pdfs/JPM/ is untouched

Output:
  data/pdfs/JPM/            — OCR'd PDFs (original filenames)
  data/ocr_backups/JPM/     — pre-OCR originals (for revert/audit)
  data/logs/JPM/_ocr_log.tsv — one row per file: ts, name, status, wc_before,
                                wc_after, seconds, error (truncated)

Language: English only (--lang eng). JPM is English text; math equations
will OCR imperfectly but citation extraction needs plain text, not LaTeX.
If post-audit shows problems, rerun with OCR_LANG = "eng+equ" (requires
installing the equ trained-data pack).

Usage:
  python src/post_processing/ocr_jpm.py --limit 5     # dry run on 5 files
  python src/post_processing/ocr_jpm.py               # full batch
  python src/post_processing/ocr_jpm.py --workers 2   # 2 files in parallel

Notes on parallelism:
  Default: 1 Python worker, ocrmypdf uses --jobs 4 internally (full
  per-file parallelism). Most JPM papers are 10-15 pages so internal
  parallelism is efficient. If you pass --workers N, OCR_JOBS auto-reduces
  to keep total Tesseract processes ≈ 4.

Dependencies (all must be on PATH):
  - ocrmypdf (pip install ocrmypdf)
  - tesseract (https://github.com/UB-Mannheim/tesseract/wiki)
  - ghostscript (https://www.ghostscript.com/download/gsdnld.html)
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    import fitz
except ImportError:
    print("ERROR: PyMuPDF not installed. Run: pip install pymupdf")
    sys.exit(1)


# ── Paths ─────────────────────────────────────────────────────────────────────
# Script at gap/src/post_processing/ocr_jpm.py
# parent.parent.parent → gap/
REPO_ROOT  = Path(__file__).resolve().parent.parent.parent
PDF_DIR    = REPO_ROOT / "data" / "pdfs"         / "JPM"
BACKUP_DIR = REPO_ROOT / "data" / "ocr_backups"  / "JPM"
LOG_DIR    = REPO_ROOT / "data" / "logs"         / "JPM"
LOG_FILE   = LOG_DIR / "_ocr_log.tsv"

# ── Config ────────────────────────────────────────────────────────────────────
CANDIDATE_WPP_THRESHOLD = 50      # wpp >= this means file is natively readable
PER_FILE_TIMEOUT_SEC    = 900     # hard cap per file (15 min; avg expected ~60s)
DEFAULT_OCR_JOBS        = 4       # --jobs passed to ocrmypdf
OCR_LANG                = "eng"   # upgrade path: "eng+equ" for math


def build_ocr_cmd(input_path: Path, output_path: Path, jobs: int) -> list[str]:
    return [
        "ocrmypdf",
        "--force-ocr",          # re-OCR over watermark text layer
        "--deskew",             # straighten tilted scans
        "--output-type", "pdf", # not PDF/A — faster, still valid
        "--jobs", str(jobs),
        "--language", OCR_LANG,
        "--quiet",
        str(input_path),
        str(output_path),
    ]


# ── Dependency checks ─────────────────────────────────────────────────────────
def check_dependencies() -> bool:
    missing = []
    if not shutil.which("ocrmypdf"):
        missing.append("ocrmypdf — install with `pip install ocrmypdf`")
    if not shutil.which("tesseract"):
        missing.append("tesseract — https://github.com/UB-Mannheim/tesseract/wiki")
    if not (shutil.which("gs") or shutil.which("gswin64c") or shutil.which("gswin32c")):
        missing.append("ghostscript — https://www.ghostscript.com/download/gsdnld.html")
    if missing:
        print("MISSING DEPENDENCIES:")
        for m in missing:
            print(f"  - {m}")
        print("\nInstall all three, ensure they're on PATH, then re-run.")
        return False
    return True


# ── File scanning ─────────────────────────────────────────────────────────────
def count_words_pages(pdf_path: Path) -> tuple[int, int]:
    """Return (word_count, page_count). (0, 0) on any error."""
    try:
        with fitz.open(pdf_path) as doc:
            pc = len(doc)
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text() or "")
            wc = sum(len(t.split()) for t in text_parts)
            return (wc, pc)
    except Exception:
        return (0, 0)


def discover_candidates(threshold: int = CANDIDATE_WPP_THRESHOLD) -> list[Path]:
    """Files in PDF_DIR with wpp < threshold (and page_count > 0)."""
    out = []
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    for pdf in pdfs:
        wc, pc = count_words_pages(pdf)
        if pc == 0:
            continue
        wpp = wc / pc
        if wpp < threshold:
            out.append(pdf)
    return out


# ── Per-file OCR ──────────────────────────────────────────────────────────────
def ocr_one(pdf_path: Path, jobs: int) -> dict:
    """Run OCR on one file. Safe: original untouched on any failure."""
    name = pdf_path.name
    backup = BACKUP_DIR / name
    tmp_out = pdf_path.with_name(pdf_path.name + ".ocr_tmp")

    start = time.time()
    wc_before, pc_before = count_words_pages(pdf_path)

    def _result(status: str, wc_after: int = 0, error: str = "") -> dict:
        return {
            "file": name,
            "status": status,
            "words_before": wc_before,
            "words_after": wc_after,
            "pages": pc_before,
            "seconds": time.time() - start,
            "error": error[:500],
        }

    # Step 1: ensure backup exists (copy if not)
    if not backup.exists():
        try:
            shutil.copy2(pdf_path, backup)
        except Exception as e:
            return _result("backup_failed", error=f"{type(e).__name__}: {e}")

    # Step 2: clean up any stale tmp from a prior crashed run
    if tmp_out.exists():
        try:
            tmp_out.unlink()
        except Exception:
            pass

    # Step 3: run ocrmypdf backup → tmp
    cmd = build_ocr_cmd(backup, tmp_out, jobs)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=PER_FILE_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        if tmp_out.exists():
            tmp_out.unlink()
        return _result("timeout", error="ocrmypdf timeout")
    except FileNotFoundError as e:
        return _result("failed", error=f"ocrmypdf not found: {e}")

    if r.returncode != 0:
        if tmp_out.exists():
            tmp_out.unlink()
        # ocrmypdf stderr can be verbose — last 500 chars usually has the cause
        return _result("failed", error=r.stderr[-500:] if r.stderr else f"rc={r.returncode}")

    # Step 4: verify tmp was actually produced and improved
    if not tmp_out.exists():
        return _result("no_output", error="ocrmypdf reported success but no output file")

    wc_after, _ = count_words_pages(tmp_out)
    if wc_after <= wc_before:
        tmp_out.unlink()
        return _result("no_improvement", wc_after=wc_after,
                       error=f"wc_after ({wc_after}) <= wc_before ({wc_before})")

    # Step 5: atomic replace
    try:
        # On Windows, Path.replace atomically replaces the destination
        tmp_out.replace(pdf_path)
    except Exception as e:
        if tmp_out.exists():
            tmp_out.unlink()
        return _result("replace_failed", wc_after=wc_after, error=f"{type(e).__name__}: {e}")

    return _result("ok", wc_after=wc_after)


# ── Logging ───────────────────────────────────────────────────────────────────
def ensure_log_header() -> None:
    if not LOG_FILE.exists():
        with LOG_FILE.open("w", encoding="utf-8") as f:
            f.write("timestamp\tfile\tstatus\twords_before\twords_after\tpages\tseconds\terror\n")


def append_log(r: dict) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    err = (r.get("error", "") or "").replace("\t", " ").replace("\n", " ")
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{ts}\t{r['file']}\t{r['status']}\t{r['words_before']}\t"
                f"{r['words_after']}\t{r['pages']}\t{r['seconds']:.1f}\t{err}\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description="OCR low-wpp JPM PDFs.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N candidates (for dry runs)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Files to OCR in parallel (default 1)")
    parser.add_argument("--threshold", type=int, default=CANDIDATE_WPP_THRESHOLD,
                        help=f"wpp cutoff for candidates (default {CANDIDATE_WPP_THRESHOLD})")
    args = parser.parse_args()

    # Auto-scale internal Tesseract jobs so total processes ≈ DEFAULT_OCR_JOBS
    per_file_jobs = max(1, DEFAULT_OCR_JOBS // max(1, args.workers))

    # Ensure dirs
    for d in [BACKUP_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("JPM OCR Pipeline")
    print(f"  pdfs      : {PDF_DIR}")
    print(f"  backups   : {BACKUP_DIR}")
    print(f"  log       : {LOG_FILE}")
    print(f"  threshold : wpp < {args.threshold}")
    print(f"  workers   : {args.workers} python × {per_file_jobs} ocrmypdf jobs")
    print("=" * 80)

    if not check_dependencies():
        return 2

    ensure_log_header()

    print("\nDiscovering candidates...")
    candidates = discover_candidates(threshold=args.threshold)
    print(f"  {len(candidates)} files below threshold")

    if args.limit:
        candidates = candidates[:args.limit]
        print(f"  (limited to first {len(candidates)} for this run)")

    if not candidates:
        print("\nNothing to do. Exiting.")
        return 0

    total_pages = sum(count_words_pages(p)[1] for p in candidates)
    est_sec = total_pages * 5 / max(1, args.workers)
    print(f"\n{total_pages} pages queued. Rough estimate: {est_sec/60:.0f} min at ~5s/page.\n")

    results: dict[str, int] = {}
    t0 = time.time()

    def _handle(res: dict, idx: int) -> None:
        results[res["status"]] = results.get(res["status"], 0) + 1
        append_log(res)
        print(f"[{idx:4d}/{len(candidates)}] {res['status']:15s} "
              f"{res['file'][:60]:60s} "
              f"{res['words_before']:5d}→{res['words_after']:6d} words "
              f"{res['seconds']:6.1f}s")

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            future_idx = {ex.submit(ocr_one, p, per_file_jobs): (i, p)
                          for i, p in enumerate(candidates, 1)}
            done_count = 0
            for fut in as_completed(future_idx):
                done_count += 1
                try:
                    res = fut.result()
                except Exception as e:
                    path = future_idx[fut][1]
                    res = {"file": path.name, "status": "exception",
                           "words_before": 0, "words_after": 0, "pages": 0,
                           "seconds": 0.0, "error": f"{type(e).__name__}: {e}"}
                _handle(res, done_count)
    else:
        for i, p in enumerate(candidates, 1):
            res = ocr_one(p, per_file_jobs)
            _handle(res, i)

    total_min = (time.time() - t0) / 60
    print("\n" + "=" * 80)
    print(f"Done in {total_min:.1f} min")
    print("Status counts:")
    for k in sorted(results):
        print(f"  {k:20s} {results[k]:4d}")
    print(f"\nFull log: {LOG_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())