#!/usr/bin/env python3 -I
"""Print a continuation sequence, query by query.

Small on purpose: a selective pattern and tiny slices, so the whole exchange
fits on a screen and every FILTER can be read.

    python3 -I trace.py http://localhost:7731 [slice-size]
"""

import os
import sys

# `python3 -I` drops the script's own directory from the import path, which is
# what we want for everything except this one sibling module.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filter_continuation import ask, choose_order, filter_after, row, term

WSDBM = "http://db.uwaterloo.ca/~galuc/wsdbm/"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

# Genre8 is the least populated WatDiv class: two subjects, six tags between
# them, which is small enough to print in full.
TAG = "http://ogp.me/ns#tag"
BODY = f"?s <{TYPE}> <{WSDBM}Genre8> . ?s <{TAG}> ?tag"
VARIABLES = ["s", "tag"]


def short(t):
    """A term, shortened for the trace but still unambiguous."""
    if not t.startswith("<"):
        return t
    return t.replace("<" + WSDBM, "wsdbm:").replace("<" + TAG, "og:tag:") \
            .replace(">", "")


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7731"
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    order, _ = choose_order(endpoint, BODY, VARIABLES, cap)
    print(f"pattern   {BODY}")
    print(f"slice     {cap} solutions, resumed in ({', '.join(order)}) order\n")

    where, step, seen = BODY, 0, 0
    while True:
        step += 1
        query = f"SELECT * WHERE {{ {where} }} LIMIT {cap}"
        batch, ms = ask(endpoint, query)
        label = "Q " if step == 1 else f"Q{chr(0x2032) * (step - 1)}"
        print(f"── {label}  ({ms} ms)")
        for line in query.split(" . "):
            print(f"   {line.strip()}")
        if not batch:
            print("   → no solutions: the answer is complete\n")
            return 0
        for b in batch:
            seen += 1
            values = "  ".join(short(t) for t in row(b, VARIABLES))
            print(f"   → {seen:>2}. {values}")
        if len(batch) < cap:
            print(f"\n   {seen} solutions, complete: the last slice was short,"
                  " so no continuation follows.")
            return 0
        last = {v: term(b[v]) for v in VARIABLES}
        where = BODY + " . " + filter_after(last, order)
        print()


if __name__ == "__main__":
    sys.exit(main())
