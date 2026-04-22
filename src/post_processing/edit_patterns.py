#!/usr/bin/env python
"""edit_patterns.py — programmatic editor for patterns.yaml.

Preserves comments and formatting via ruamel.yaml.

Requires:
    pip install ruamel.yaml

Commands:
    list [--bucket BUCKET]
    show PATTERN_ID
    remove PATTERN_ID
    set-bucket PATTERN_ID BUCKET
    set-regex PATTERN_ID 'REGEX'
    set-field PATTERN_ID KEY VALUE
    add [--file FILE]
    bump-version VERSION

Buckets: canonical, canonical_metric_ambiguous

For `add`, paste a YAML pattern dict on stdin (ctrl-Z+Enter on Windows to
end), or use --file to read from a file. Required keys:

    pattern_id, bucket, regex, academic_subfield, paper

`paper` must contain: authors (list), year (int), title, journal.
Optional: doi, notes.

Example stdin input:

    pattern_id: my_new_pattern
    bucket: canonical
    regex: '\\bfoo\\b'
    academic_subfield: asset_pricing
    paper:
      authors: [Foo, B.]
      year: 2020
      title: The Foo Effect
      journal: Journal of Finance
      doi: 10.xxxx/xxxx

Note: regex strings with backslashes should be single-quoted in YAML.
Use set-regex for mid-edit regex updates — safer than set-field.
"""

import argparse
import sys
from pathlib import Path
from io import StringIO

try:
    from ruamel.yaml import YAML
except ImportError:
    sys.stderr.write(
        "Missing dependency: ruamel.yaml\n"
        "  Install: pip install ruamel.yaml\n"
    )
    sys.exit(1)

PATTERNS_PATH = Path(__file__).resolve().parent / "patterns.yaml"

VALID_BUCKETS = {"canonical", "canonical_metric_ambiguous"}
REQUIRED_PATTERN_KEYS = {"pattern_id", "bucket", "regex", "academic_subfield", "paper"}
REQUIRED_PAPER_KEYS = {"authors", "year", "title", "journal"}


def _yaml():
    y = YAML()
    y.preserve_quotes = True
    y.width = 4096
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def load():
    y = _yaml()
    with PATTERNS_PATH.open("r", encoding="utf-8") as f:
        return y, y.load(f)


def save(y, data):
    with PATTERNS_PATH.open("w", encoding="utf-8") as f:
        y.dump(data, f)


def find_pattern(data, pattern_id):
    patterns = data.get("patterns") or []
    for i, p in enumerate(patterns):
        if p.get("pattern_id") == pattern_id:
            return i, p
    return -1, None


# ------------------------------------------------------------------ #
# Commands
# ------------------------------------------------------------------ #
def cmd_list(args):
    y, data = load()
    patterns = data.get("patterns") or []
    rows = []
    for p in patterns:
        bucket = p.get("bucket", "")
        if args.bucket and bucket != args.bucket:
            continue
        pid = p.get("pattern_id", "<no-id>")
        sub = p.get("academic_subfield", "")
        rows.append((pid, bucket, sub))
    header = f"# patterns ({len(rows)} shown / {len(patterns)} total)"
    if args.bucket:
        header += f"  bucket={args.bucket}"
    print(header)
    if not rows:
        return
    w1 = max(len(r[0]) for r in rows)
    w2 = max(len(r[1]) for r in rows)
    for pid, bucket, sub in rows:
        print(f"  {pid:<{w1}}  {bucket:<{w2}}  {sub}")


def cmd_show(args):
    y, data = load()
    i, p = find_pattern(data, args.pattern_id)
    if p is None:
        sys.stderr.write(f"not found: {args.pattern_id}\n")
        sys.exit(1)
    buf = StringIO()
    _yaml().dump({"pattern": p}, buf)
    print(buf.getvalue())


def cmd_remove(args):
    y, data = load()
    i, p = find_pattern(data, args.pattern_id)
    if p is None:
        sys.stderr.write(f"not found: {args.pattern_id}\n")
        sys.exit(1)
    data["patterns"].pop(i)
    save(y, data)
    print(f"removed: {args.pattern_id}")


def cmd_set_bucket(args):
    if args.bucket not in VALID_BUCKETS:
        sys.stderr.write(f"invalid bucket: {args.bucket}\n")
        sys.stderr.write(f"valid: {sorted(VALID_BUCKETS)}\n")
        sys.exit(2)
    y, data = load()
    i, p = find_pattern(data, args.pattern_id)
    if p is None:
        sys.stderr.write(f"not found: {args.pattern_id}\n")
        sys.exit(1)
    old = p.get("bucket")
    if old == args.bucket:
        print(f"no change: {args.pattern_id} already in {args.bucket}")
        return
    p["bucket"] = args.bucket
    save(y, data)
    print(f"bucket changed: {args.pattern_id}  {old} -> {args.bucket}")


def cmd_set_regex(args):
    y, data = load()
    i, p = find_pattern(data, args.pattern_id)
    if p is None:
        sys.stderr.write(f"not found: {args.pattern_id}\n")
        sys.exit(1)
    old = p.get("regex")
    p["regex"] = args.regex
    save(y, data)
    print(f"regex updated: {args.pattern_id}")
    print(f"  was: {old!r}")
    print(f"  now: {args.regex!r}")


def cmd_set_field(args):
    y, data = load()
    i, p = find_pattern(data, args.pattern_id)
    if p is None:
        sys.stderr.write(f"not found: {args.pattern_id}\n")
        sys.exit(1)
    key = args.key
    if key == "pattern_id":
        sys.stderr.write("refusing to change pattern_id — remove + add instead\n")
        sys.exit(2)
    # Simple coercion on scalar values
    val = args.value
    lowered = val.lower()
    if val.lstrip("-").isdigit():
        val = int(val)
    elif lowered in ("null", "none", "~"):
        val = None
    elif lowered == "true":
        val = True
    elif lowered == "false":
        val = False
    old = p.get(key)
    p[key] = val
    save(y, data)
    print(f"field updated: {args.pattern_id}.{key}")
    print(f"  was: {old!r}")
    print(f"  now: {val!r}")


def cmd_add(args):
    y_in = _yaml()
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            new_pat = y_in.load(f)
    else:
        text = sys.stdin.read()
        if not text.strip():
            sys.stderr.write("no input on stdin (use --file or paste YAML + EOF)\n")
            sys.exit(2)
        new_pat = y_in.load(text)

    if not hasattr(new_pat, "keys"):
        sys.stderr.write("input is not a YAML mapping\n")
        sys.exit(2)

    missing = REQUIRED_PATTERN_KEYS - set(new_pat.keys())
    if missing:
        sys.stderr.write(f"missing required keys: {sorted(missing)}\n")
        sys.exit(2)
    bucket = new_pat.get("bucket")
    if bucket not in VALID_BUCKETS:
        sys.stderr.write(f"invalid bucket: {bucket!r}\n")
        sys.stderr.write(f"valid: {sorted(VALID_BUCKETS)}\n")
        sys.exit(2)
    paper = new_pat.get("paper")
    if not hasattr(paper, "keys"):
        sys.stderr.write("paper must be a mapping\n")
        sys.exit(2)
    paper_missing = REQUIRED_PAPER_KEYS - set(paper.keys())
    if paper_missing:
        sys.stderr.write(f"paper missing keys: {sorted(paper_missing)}\n")
        sys.exit(2)

    y, data = load()
    existing_idx, existing = find_pattern(data, new_pat["pattern_id"])
    if existing is not None:
        sys.stderr.write(f"pattern_id already exists: {new_pat['pattern_id']}\n")
        sys.stderr.write("use set-regex / set-bucket / set-field / remove to modify\n")
        sys.exit(1)

    if data.get("patterns") is None:
        data["patterns"] = []
    data["patterns"].append(new_pat)
    save(y, data)
    print(f"added: {new_pat['pattern_id']}  bucket={bucket}")
    print(f"  regex: {new_pat['regex']!r}")


def cmd_bump_version(args):
    y, data = load()
    old = data.get("yaml_version")
    data["yaml_version"] = args.version
    save(y, data)
    print(f"yaml_version: {old} -> {args.version}")


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #
def main():
    p = argparse.ArgumentParser(
        description="Edit patterns.yaml programmatically (preserves comments)."
    )
    sp = p.add_subparsers(dest="cmd", required=True)

    sp_list = sp.add_parser("list", help="list patterns")
    sp_list.add_argument("--bucket", choices=sorted(VALID_BUCKETS))
    sp_list.set_defaults(func=cmd_list)

    sp_show = sp.add_parser("show", help="show one pattern's full body")
    sp_show.add_argument("pattern_id")
    sp_show.set_defaults(func=cmd_show)

    sp_rm = sp.add_parser("remove", help="remove a pattern")
    sp_rm.add_argument("pattern_id")
    sp_rm.set_defaults(func=cmd_remove)

    sp_sb = sp.add_parser("set-bucket", help="move a pattern between buckets")
    sp_sb.add_argument("pattern_id")
    sp_sb.add_argument("bucket", choices=sorted(VALID_BUCKETS))
    sp_sb.set_defaults(func=cmd_set_bucket)

    sp_sr = sp.add_parser("set-regex", help="update a pattern's regex")
    sp_sr.add_argument("pattern_id")
    sp_sr.add_argument("regex")
    sp_sr.set_defaults(func=cmd_set_regex)

    sp_sf = sp.add_parser("set-field",
        help="update a top-level pattern field (e.g. academic_subfield)")
    sp_sf.add_argument("pattern_id")
    sp_sf.add_argument("key")
    sp_sf.add_argument("value")
    sp_sf.set_defaults(func=cmd_set_field)

    sp_add = sp.add_parser("add",
        help="add a new pattern from YAML on stdin or --file")
    sp_add.add_argument("--file", help="read YAML pattern dict from file")
    sp_add.set_defaults(func=cmd_add)

    sp_bv = sp.add_parser("bump-version",
        help="set yaml_version (e.g., 1.2)")
    sp_bv.add_argument("version")
    sp_bv.set_defaults(func=cmd_bump_version)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
