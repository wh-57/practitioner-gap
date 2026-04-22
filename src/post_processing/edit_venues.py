#!/usr/bin/env python
"""edit_venues.py — programmatic editor for venues.yaml.

Preserves comments and formatting via ruamel.yaml.

Requires:
    pip install ruamel.yaml

Commands:
    list [--section SECTION]
    add SECTION VENUE [VENUE...]
    remove VENUE [VENUE...]
    alias SHORT FULL
    unalias SHORT
    scope SCOPE VENUE [VENUE...]
    unscope VENUE [VENUE...]
    classify VENUE

Sections: academic, practitioner, ambiguous, wpp (working_paper_patterns)
Scopes:   finance_core, non_finance_academic, non_finance_other

All venue inputs are normalized (lowercase, & -> and, strip parens, etc.)
before lookup and storage, matching the 01c_resolve.py cascade.

Examples:
    python edit_venues.py add academic "Journal of Econometrics" "Review of Economics and Statistics"
    python edit_venues.py add practitioner "Journal of Futures Markets"
    python edit_venues.py classify "FAJ"
    python edit_venues.py scope finance_core "Journal of Finance"
    python edit_venues.py alias rfs "review of financial studies"
"""

import argparse
import re
import sys
from pathlib import Path

try:
    from ruamel.yaml import YAML
except ImportError:
    sys.stderr.write(
        "Missing dependency: ruamel.yaml\n"
        "  Install: pip install ruamel.yaml\n"
    )
    sys.exit(1)

VENUES_PATH = Path(__file__).resolve().parent / "venues.yaml"

SECTION_ALIASES = {
    "academic": "academic",
    "practitioner": "practitioner",
    "ambiguous": "ambiguous",
    "working-paper-patterns": "working_paper_patterns",
    "working_paper_patterns": "working_paper_patterns",
    "wpp": "working_paper_patterns",
}

SCOPE_NAMES = {"finance_core", "non_finance_academic", "non_finance_other"}


# ------------------------------------------------------------------ #
# Normalization — mirrors the matching policy in venues.yaml header
# ------------------------------------------------------------------ #
def normalize(s: str) -> str:
    s = str(s).lower().strip()
    s = s.replace("&", "and")
    s = re.sub(r"\([^)]*\)", "", s)          # drop parentheticals
    s = re.sub(r"[^a-z0-9 ]+", " ", s)       # keep alnum + space
    s = re.sub(r"\s+", " ", s).strip()
    if s.startswith("the "):
        s = s[4:]
    return s


# ------------------------------------------------------------------ #
# YAML I/O (ruamel round-trip to preserve comments)
# ------------------------------------------------------------------ #
def _yaml():
    y = YAML()
    y.preserve_quotes = True
    y.width = 4096
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def load():
    y = _yaml()
    with VENUES_PATH.open("r", encoding="utf-8") as f:
        return y, y.load(f)


def save(y, data):
    with VENUES_PATH.open("w", encoding="utf-8") as f:
        y.dump(data, f)


# ------------------------------------------------------------------ #
# Lookups
# ------------------------------------------------------------------ #
def _find_venue(data, norm_venue):
    """Return list of (section, canonical_form) hits across all sections."""
    hits = []
    for sec in ("academic", "practitioner", "ambiguous", "working_paper_patterns"):
        for item in (data.get(sec) or []):
            if normalize(str(item)) == norm_venue:
                hits.append((sec, item))
    return hits


def classify_local(data, norm_venue):
    """Emulate classify_is_academic cascade from 01c_resolve.py for preview."""
    aliases = data.get("aliases") or {}
    if norm_venue in aliases:
        canonical = normalize(str(aliases[norm_venue]))
    else:
        canonical = norm_venue

    for item in (data.get("ambiguous") or []):
        if normalize(str(item)) == canonical:
            return "None (ambiguous — defer to LLM)", canonical
    for item in (data.get("academic") or []):
        if normalize(str(item)) == canonical:
            return "True (academic)", canonical
    for item in (data.get("practitioner") or []):
        if normalize(str(item)) == canonical:
            return "False (practitioner)", canonical
    for pat in (data.get("working_paper_patterns") or []):
        pat_norm = str(pat).lower()
        if pat_norm and pat_norm in canonical:
            return f"None (matches working-paper substring '{pat}')", canonical
    return "None (unknown)", canonical


def scope_of(data, norm_venue):
    scopes = data.get("scopes") or {}
    for scope_name, members in scopes.items():
        for item in (members or []):
            if normalize(str(item)) == norm_venue:
                return scope_name
    # defaults from is_academic membership
    for item in (data.get("academic") or []):
        if normalize(str(item)) == norm_venue:
            return "finance_adjacent (default)"
    for item in (data.get("practitioner") or []):
        if normalize(str(item)) == norm_venue:
            return "finance_practitioner (default)"
    return None


# ------------------------------------------------------------------ #
# Commands
# ------------------------------------------------------------------ #
def cmd_list(args):
    y, data = load()
    if args.section:
        section = SECTION_ALIASES.get(args.section, args.section)
        if section == "scopes":
            scopes = data.get("scopes") or {}
            for s, members in scopes.items():
                members = members or []
                print(f"# scopes.{s} ({len(members)} entries)")
                for m in members:
                    print(f"  - {m}")
                print()
        elif section == "aliases":
            aliases = data.get("aliases") or {}
            print(f"# aliases ({len(aliases)} entries)")
            for k, v in aliases.items():
                print(f"  {k}: {v}")
        else:
            entries = data.get(section) or []
            print(f"# {section} ({len(entries)} entries)")
            for e in entries:
                print(f"  - {e}")
        return
    # summary
    for name in ("academic", "practitioner", "ambiguous", "working_paper_patterns"):
        entries = data.get(name) or []
        print(f"  {name:<30} {len(entries)}")
    aliases = data.get("aliases") or {}
    print(f"  {'aliases':<30} {len(aliases)}")
    scopes = data.get("scopes") or {}
    for s, members in scopes.items():
        print(f"  {('scopes.' + s):<30} {len(members or [])}")


def cmd_add(args):
    y, data = load()
    section = SECTION_ALIASES.get(args.section)
    if section is None or section == "scopes":
        sys.stderr.write(f"unknown section: {args.section}\n")
        sys.stderr.write(f"valid: academic, practitioner, ambiguous, wpp\n")
        sys.exit(2)
    if section not in data or data[section] is None:
        data[section] = []
    added, dup, conflict = [], [], []
    for raw in args.venues:
        norm = normalize(raw)
        if not norm:
            continue
        hits = _find_venue(data, norm)
        if any(sec == section for sec, _ in hits):
            dup.append(raw)
            continue
        other = [sec for sec, _ in hits if sec != section]
        if other:
            conflict.append((raw, other))
            continue
        data[section].append(norm)
        added.append(norm)
    if added:
        save(y, data)
    print(f"section={section}")
    if added:
        print(f"  added ({len(added)}):")
        for v in added:
            print(f"    + {v}")
    for v in dup:
        print(f"  skip (already present): {v}")
    for raw, where in conflict:
        print(f"  CONFLICT: {raw!r} already in {where} — remove first")


def cmd_remove(args):
    y, data = load()
    removed, missing = [], []
    for raw in args.venues:
        norm = normalize(raw)
        hits = _find_venue(data, norm)
        if not hits:
            missing.append(raw)
            continue
        for sec, item in hits:
            data[sec].remove(item)
            removed.append((sec, item))
    if removed:
        save(y, data)
    for sec, item in removed:
        print(f"  - removed from {sec}: {item}")
    for raw in missing:
        print(f"  NOT FOUND: {raw}")


def cmd_alias(args):
    y, data = load()
    if data.get("aliases") is None:
        data["aliases"] = {}
    short = args.short.lower().strip()
    full = normalize(args.full)
    existing = data["aliases"].get(short)
    data["aliases"][short] = full
    save(y, data)
    if existing is None:
        print(f"alias added: {short} -> {full}")
    else:
        print(f"alias updated: {short}  {existing} -> {full}")


def cmd_unalias(args):
    y, data = load()
    short = args.short.lower().strip()
    aliases = data.get("aliases") or {}
    if short in aliases:
        old = aliases.pop(short)
        save(y, data)
        print(f"alias removed: {short} -> {old}")
    else:
        print(f"NOT FOUND: alias {short}")


def cmd_scope(args):
    y, data = load()
    scope = args.scope
    if scope not in SCOPE_NAMES:
        sys.stderr.write(f"unknown scope: {scope}\n")
        sys.stderr.write(f"valid: {sorted(SCOPE_NAMES)}\n")
        sys.exit(2)
    if data.get("scopes") is None:
        data["scopes"] = {}
    if data["scopes"].get(scope) is None:
        data["scopes"][scope] = []

    added, moved, dup = [], [], []
    for raw in args.venues:
        norm = normalize(raw)
        # Remove from other scopes first (a venue has at most one scope)
        prev = None
        for other in list(data["scopes"].keys()):
            members = data["scopes"][other] or []
            for item in list(members):
                if normalize(str(item)) == norm and other != scope:
                    members.remove(item)
                    prev = other
        current = data["scopes"][scope] or []
        if any(normalize(str(x)) == norm for x in current):
            dup.append(raw)
            continue
        current.append(norm)
        data["scopes"][scope] = current
        if prev:
            moved.append((raw, prev))
        else:
            added.append(raw)
    save(y, data)
    for v in added:
        print(f"  scope set: {v} -> {scope}")
    for v, prev in moved:
        print(f"  scope moved: {v}  {prev} -> {scope}")
    for v in dup:
        print(f"  skip (already in {scope}): {v}")


def cmd_unscope(args):
    y, data = load()
    scopes = data.get("scopes") or {}
    removed, missing = [], []
    for raw in args.venues:
        norm = normalize(raw)
        found = False
        for scope_name, members in scopes.items():
            if members is None:
                continue
            for item in list(members):
                if normalize(str(item)) == norm:
                    members.remove(item)
                    removed.append((scope_name, item))
                    found = True
        if not found:
            missing.append(raw)
    if removed:
        save(y, data)
    for scope_name, item in removed:
        print(f"  - removed from scopes.{scope_name}: {item}")
    for raw in missing:
        print(f"  NOT FOUND in any scope: {raw}")


def cmd_classify(args):
    y, data = load()
    norm = normalize(args.venue)
    result, canonical = classify_local(data, norm)
    print(f"  input       : {args.venue!r}")
    print(f"  normalized  : {norm!r}")
    if canonical != norm:
        print(f"  alias to    : {canonical!r}")
    print(f"  is_academic : {result}")
    scope_key = canonical if canonical != norm else norm
    print(f"  venue_scope : {scope_of(data, scope_key)}")


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #
def main():
    p = argparse.ArgumentParser(
        description="Edit venues.yaml programmatically (preserves comments)."
    )
    sp = p.add_subparsers(dest="cmd", required=True)

    sp_list = sp.add_parser("list", help="list entries in a section, or summary")
    sp_list.add_argument("--section",
        help="academic, practitioner, ambiguous, wpp, aliases, scopes")
    sp_list.set_defaults(func=cmd_list)

    sp_add = sp.add_parser("add", help="add venue(s) to a section")
    sp_add.add_argument("section",
        help="academic, practitioner, ambiguous, wpp")
    sp_add.add_argument("venues", nargs="+")
    sp_add.set_defaults(func=cmd_add)

    sp_rm = sp.add_parser("remove",
        help="remove venue(s) from whichever section contains them")
    sp_rm.add_argument("venues", nargs="+")
    sp_rm.set_defaults(func=cmd_remove)

    sp_al = sp.add_parser("alias", help="add or update an alias")
    sp_al.add_argument("short")
    sp_al.add_argument("full")
    sp_al.set_defaults(func=cmd_alias)

    sp_ual = sp.add_parser("unalias", help="remove an alias")
    sp_ual.add_argument("short")
    sp_ual.set_defaults(func=cmd_unalias)

    sp_sc = sp.add_parser("scope", help="set venue(s) to a specific scope")
    sp_sc.add_argument("scope", choices=sorted(SCOPE_NAMES))
    sp_sc.add_argument("venues", nargs="+")
    sp_sc.set_defaults(func=cmd_scope)

    sp_usc = sp.add_parser("unscope", help="remove venue(s) from any scope")
    sp_usc.add_argument("venues", nargs="+")
    sp_usc.set_defaults(func=cmd_unscope)

    sp_cl = sp.add_parser("classify",
        help="preview how a venue would be classified (does not modify the file)")
    sp_cl.add_argument("venue")
    sp_cl.set_defaults(func=cmd_classify)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
