#!/usr/bin/env python3 -I
"""Print a continuation sequence, query by query, in full.

Small on purpose: a selective pattern and tiny slices, so the whole exchange
fits on a screen and every query can be read exactly as it was sent — PREFIX
declarations included, nothing elided.

    python3 -I trace.py http://localhost:7731 [slice-size]
"""

import os
import sys

# `python3 -I` drops the script's own directory from the import path, which is
# what we want for everything except this one sibling module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filter_continuation import ask, row, term

PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "wsdbm": "http://db.uwaterloo.ca/~galuc/wsdbm/",
    "og": "http://ogp.me/ns#",
}

# Genre8 is the least populated WatDiv class: two subjects, six tags between
# them, which is small enough to print in full.
PATTERNS = ["?s rdf:type wsdbm:Genre8", "?s og:tag ?tag"]
VARIABLES = ["s", "tag"]
ORDER = ["s", "tag"]          # the permutation's column order


def shorten(t):
    """A term written with the prefixes above, when one applies."""
    if t.startswith("<"):
        iri = t[1:-1]
        for short, long in PREFIXES.items():
            if iri.startswith(long):
                return f"{short}:{iri[len(long):]}"
    return t


def filter_lines(last, order, indent):
    """The lexicographic FILTER, laid out so the nesting is visible.

    Only the last component takes a bare `>`. Every earlier one needs
    "greater, OR equal and keep looking" — which is why this is a nest of
    disjunctions and not a conjunction of `>`.
    """
    pad = " " * indent
    lines = []
    for depth, name in enumerate(order):
        var, value = "?" + name, shorten(last[name])
        step = " " * (depth * 2)
        if depth == len(order) - 1:
            lines.append(f"{pad}{step}{var} > {value}")
        else:
            lines.append(f"{pad}{step}{var} > {value} ||")
            lines.append(f"{pad}{step}({var} = {value} &&")
    lines[-1] += ")" * (len(order) - 1)
    return lines


def build(last, order, cap):
    """The continuation query, laid out for reading and sent as written."""
    lines = [f"PREFIX {p}: <{u}>" for p, u in PREFIXES.items()]
    lines.append("SELECT * WHERE {")
    lines.extend(f"  {p} ." for p in PATTERNS)
    if last:
        lines.append("  FILTER (")
        lines.extend(filter_lines(last, order, 4))
        lines.append("  )")
    lines.append(f"}} LIMIT {cap}")
    return "\n".join(lines)


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7731"
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 2

    print(f"slices of {cap}, resumed in ({', '.join(ORDER)}) order")
    last, step, seen = None, 0, 0
    while True:
        step += 1
        query = build(last, ORDER, cap)
        batch, ms = ask(endpoint, query)
        print(f"\n{'─' * 60}\n")
        print(query)
        print()
        if not batch:
            print("  → no solutions: the answer is complete.")
            return 0
        for b in batch:
            seen += 1
            print(f"  → {seen:>2}.  " + "   ".join(
                shorten(t) for t in row(b, VARIABLES)))
        print(f"      ({ms} ms)")
        if len(batch) < cap:
            print(f"\n  {seen} solutions. The slice came back short, so the"
                  " answer is complete\n  and no continuation follows.")
            return 0
        last = {v: term(b[v]) for v in VARIABLES}


if __name__ == "__main__":
    sys.exit(main())
