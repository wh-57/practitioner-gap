# %% Diagnose a single "zero-word" PDF three ways
# One-shot. Takes a JPM PDF path as argument and reports:
#   1. PyMuPDF extraction (what the main pipeline uses today)
#   2. pdfplumber extraction  (alternative text extractor)
#   3. pdfminer.six extraction (another alternative)
#   4. Raw PDF stream inspection — is there /Font? /Image? text objects?
#
# If all three extractors return 0 words, the PDF is a true image scan.
# If any extractor returns text, we should change extractors, not add OCR.
#
# Run:
#     python src/scrapers_probes/pdf_diagnose.py <path-to-pdf>
#
# pdfplumber / pdfminer may not be installed. Script handles missing imports
# gracefully and reports "(not installed)" rather than crashing.

import sys
from pathlib import Path


def inspect_pymupdf(path: Path) -> dict:
    try:
        import fitz
    except ImportError:
        return {"tool": "pymupdf", "status": "not installed"}
    try:
        doc = fitz.open(path)
        pages = len(doc)
        text = "\n".join(p.get_text() for p in doc)
        doc.close()
        return {
            "tool": "pymupdf",
            "status": "ok",
            "pages": pages,
            "word_count": len(text.split()),
            "char_count": len(text),
            "first_chars": text[:200].replace("\n", " | "),
        }
    except Exception as e:
        return {"tool": "pymupdf", "status": f"error: {e}"}


def inspect_pdfplumber(path: Path) -> dict:
    try:
        import pdfplumber
    except ImportError:
        return {"tool": "pdfplumber", "status": "not installed"}
    try:
        text_parts = []
        with pdfplumber.open(path) as pdf:
            pages = len(pdf.pages)
            for p in pdf.pages:
                t = p.extract_text()
                if t:
                    text_parts.append(t)
        text = "\n".join(text_parts)
        return {
            "tool": "pdfplumber",
            "status": "ok",
            "pages": pages,
            "word_count": len(text.split()),
            "char_count": len(text),
            "first_chars": text[:200].replace("\n", " | "),
        }
    except Exception as e:
        return {"tool": "pdfplumber", "status": f"error: {e}"}


def inspect_pdfminer(path: Path) -> dict:
    try:
        from pdfminer.high_level import extract_text
    except ImportError:
        return {"tool": "pdfminer.six", "status": "not installed"}
    try:
        text = extract_text(str(path))
        return {
            "tool": "pdfminer.six",
            "status": "ok",
            "word_count": len(text.split()),
            "char_count": len(text),
            "first_chars": text[:200].replace("\n", " | "),
        }
    except Exception as e:
        return {"tool": "pdfminer.six", "status": f"error: {e}"}


def inspect_raw_stream(path: Path) -> dict:
    """Look at the raw PDF bytes for markers that indicate image vs text content."""
    try:
        content = path.read_bytes()
    except Exception as e:
        return {"tool": "raw", "status": f"error: {e}"}

    # Common PDF operators and object types
    markers = {
        "/Font":          content.count(b"/Font"),
        "/Image":         content.count(b"/Image"),
        "/XObject":       content.count(b"/XObject"),
        "BT...ET (text)": content.count(b"BT\n") + content.count(b"BT "),
        "Tj (show text)": content.count(b"Tj"),
        "Do (draw obj)":  content.count(b"Do\n") + content.count(b"Do "),
        "/Subtype /Image":content.count(b"/Subtype /Image") + content.count(b"/Subtype/Image"),
        "FlateDecode":    content.count(b"FlateDecode"),
        "DCTDecode (JPEG)": content.count(b"DCTDecode"),
    }
    return {"tool": "raw", "status": "ok", "file_size": len(content), "markers": markers}


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python pdf_diagnose.py <path-to-pdf>", file=sys.stderr)
        return 1
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"[error] not found: {path}", file=sys.stderr)
        return 1

    print(f"=== PDF DIAGNOSIS: {path.name} ===")
    print(f"    size: {path.stat().st_size} bytes\n")

    # Three extractors
    for inspect in (inspect_pymupdf, inspect_pdfplumber, inspect_pdfminer):
        r = inspect(path)
        tool = r["tool"]
        status = r["status"]
        if status == "not installed":
            print(f"  [{tool:<14}] (not installed)")
            continue
        if status != "ok":
            print(f"  [{tool:<14}] {status}")
            continue
        wc = r.get("word_count", "?")
        cc = r.get("char_count", "?")
        first = r.get("first_chars", "")
        print(f"  [{tool:<14}] words={wc:<6} chars={cc:<6}  first 200 chars:")
        print(f"                   '{first}'")

    # Raw stream markers
    raw = inspect_raw_stream(path)
    if raw["status"] == "ok":
        print(f"\n  [raw stream]     size={raw['file_size']} bytes")
        for marker, count in raw["markers"].items():
            print(f"                   {marker:<20} count={count}")
    else:
        print(f"\n  [raw stream]     {raw['status']}")

    print("\n  Interpretation hints:")
    print("    - If /Font > 0 and Tj > 0 and BT...ET > 0: PDF contains text objects.")
    print("      If PyMuPDF returns 0 words but these markers are present, a different")
    print("      extractor may work.")
    print("    - If /Image or DCTDecode dominate and text markers are absent: true image scan,")
    print("      OCR is the only path.")
    print("    - If all three extractors return ~same nonzero word count: PyMuPDF is fine,")
    print("      the scan result was wrong somehow.")
    return 0


if __name__ == "__main__":
    sys.exit(main())