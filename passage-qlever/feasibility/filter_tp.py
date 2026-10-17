#!/usr/bin/env python3 -I
"""Can a single triple pattern be continued with FILTER instead of OFFSET?

The triple pattern is the primitive. Passage composes BGPs, OPTIONAL and the
rest out of paused scans, and a BGP continuation is a union over the pause
points — so a keyset applied to a whole BGP's result is NOT a continuation.
What has to work is the scan itself: given a triple pattern, the solutions it
already emitted, and the last one, produce a query for exactly the rest.

    SELECT * WHERE { <pattern> . FILTER ( <key> > <last> ) } LIMIT n

This probe runs every shape of triple pattern — each combination of bound and
free positions — and for each one checks three things:

  1. the FILTER enumeration is the OFFSET enumeration, solution for solution;
  2. the cost per slice does not grow with depth;
  3. a key one column short loses solutions, so we know the full key is needed.

    python3 -I filter_tp.py http://localhost:7731 [slice-size]
"""

import os
import sys
import time
from itertools import permutations

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filter_continuation import Fold, ask, filter_after, row, term

W = "http://db.uwaterloo.ca/~galuc/wsdbm/"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def shapes(endpoint):
    """The eight shapes of a triple pattern, with constants from the data."""
    # A subject with many triples, and an object with many, so the bound
    # shapes are not trivially short.
    busiest_s, _ = ask(endpoint, "SELECT ?s (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"
                                 " GROUP BY ?s ORDER BY DESC(?n) LIMIT 1")
    busiest_o, _ = ask(endpoint, "SELECT ?o (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"
                                 " GROUP BY ?o ORDER BY DESC(?n) LIMIT 1")
    s = term(busiest_s[0]["s"])
    o = term(busiest_o[0]["o"])
    p = f"<{W}friendOf>"
    return [
        ("? ? ?", "?s ?p ?o", ["s", "p", "o"]),
        ("? p ?", f"?s {p} ?o", ["s", "o"]),
        ("s ? ?", f"{s} ?p ?o", ["p", "o"]),
        ("? ? o", f"?s ?p {o}", ["s", "p"]),
        ("s p ?", f"{s} {p} ?o", ["o"]),
        ("? p o", f"?s {p} {o}", ["s"]),
        ("s ? o", f"{s} ?p {o}", ["p"]),
        ("s p o", f"{s} {p} {o}", []),
    ]


def choose_order(endpoint, body, variables, cap):
    """The column order the scan enumerates in, chosen against OFFSET.

    It cannot be inferred by comparing terms: QLever sorts its vocabulary by
    ICU collation, so the Id order is not the byte order of the strings. A
    real implementation does not need this — the paused scan knows its own
    permutation."""
    if len(variables) < 2:
        return list(variables)
    counted, _ = ask(endpoint, f"SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}")
    total = int(counted[0]["n"]["value"])
    if total <= 1:
        return list(variables)

    # Several probe points, not one. Two different orders can agree at a
    # single boundary and part company everywhere else — which is how an
    # earlier version of this probe chose (o, s, p) over (o, p, s) and lost
    # 28,927 solutions out of ten million, silently.
    depths = sorted({d for d in (1, total // 3, total // 2,
                                 2 * total // 3, total - 2) if 0 < d < total - 1})
    probes = []
    for depth in depths:
        pair, _ = ask(endpoint,
                      f"SELECT * WHERE {{ {body} }} LIMIT 2 OFFSET {depth}")
        if len(pair) == 2:
            probes.append(({v: term(pair[0][v]) for v in variables},
                           row(pair[1], variables)))
    if not probes:
        return list(variables)

    for candidate in permutations(variables):
        order = list(candidate)
        if all(resumes_at(endpoint, body, variables, order, last, target)
               for last, target in probes):
            return order
    return None


def resumes_at(endpoint, body, variables, order, last, target):
    where = body + " . " + filter_after(last, order)
    got, _ = ask(endpoint, f"SELECT * WHERE {{ {where} }} LIMIT 1")
    return bool(got) and row(got[0], variables) == target


def walk(endpoint, body, variables, cap, order=None):
    """Enumerate to exhaustion, by FILTER when `order` is given, else OFFSET."""
    fold, steps, total, slowest, deepest = Fold(), 0, 0.0, 0.0, 0.0
    where = body
    while True:
        tail = "" if order is None else ""
        query = (f"SELECT * WHERE {{ {where} }} LIMIT {cap}" if order is not None
                 else f"SELECT * WHERE {{ {body} }} LIMIT {cap} OFFSET {fold.count}")
        batch, ms = ask(endpoint, query)
        steps += 1
        total += ms
        slowest = max(slowest, ms)
        if steps > 1:
            deepest = max(deepest, ms)        # ignore the first, cold slice
        for b in batch:
            fold.add(row(b, variables))
        if len(batch) < cap:
            return fold, steps, total, slowest, deepest
        if order is not None:
            last = {v: term(batch[-1][v]) for v in variables}
            where = body + " . " + filter_after(last, order)


def short_key_loss(endpoint, body, variables, order, depth):
    """What a key one column short loses, resuming at `depth`."""
    boundary, _ = ask(endpoint,
                      f"SELECT * WHERE {{ {body} }} LIMIT 1 OFFSET {depth}")
    last = {v: term(boundary[0][v]) for v in variables}
    out = []
    for width in range(len(order), 0, -1):
        where = body + " . " + filter_after(last, order[:width])
        counted, _ = ask(endpoint,
                         f"SELECT (COUNT(*) AS ?n) WHERE {{ {where} }}")
        out.append((order[:width], int(counted[0]["n"]["value"])))
    return out


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7731"
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 20000

    print(f"endpoint {endpoint} · slices of {cap:,}\n")
    print(f"{'shape':8s} {'solutions':>11s} {'slices':>7s}"
          f" {'FILTER':>9s} {'OFFSET':>9s}  {'order':14s} verdict")
    failures = 0
    for label, body, variables in shapes(endpoint):
        try:
            if not variables:
                found, _ = ask(endpoint, f"SELECT * WHERE {{ {body} }}")
                print(f"{label:8s} {len(found):>11,} {'—':>7s}"
                      f" {'—':>9s} {'—':>9s}  {'—':14s}"
                      " no variable: nothing to continue")
                continue
            order = choose_order(endpoint, body, variables, cap)
            if order is None:
                print(f"{label:8s} {'?':>11s} {'—':>7s} {'—':>9s} {'—':>9s}"
                      f"  {'—':14s} NO ORDER RESUMES CORRECTLY")
                failures += 1
                continue
            mine, steps, total, slowest, deepest = walk(
                endpoint, body, variables, cap, order)
            theirs, _, off_total, _, _ = walk(endpoint, body, variables, cap)
            same = mine.hex() == theirs.hex()
            failures += 0 if same else 1
            print(f"{label:8s} {mine.count:>11,} {steps:>7,}"
                  f" {total:>8,.0f}ms {off_total:>8,.0f}ms"
                  f"  ({', '.join(order)}){' ' * max(0, 12 - len(', '.join(order)))}"
                  f" {'identical' if same else 'DIFFERENT'}")
            if not same:
                print(f"{'':8s} {'':>11s} {'':>7s} {'':>9s} {'':>9s}"
                      f"  by OFFSET {theirs.count:,}"
                      f" — {mine.count - theirs.count:+,}")
            if same and len(order) > 1 and mine.count > 4:
                depth = mine.count // 2
                expected = mine.count - depth - 1
                losses = short_key_loss(endpoint, body, variables, order, depth)
                parts = []
                for columns, got in losses:
                    lost = expected - got
                    parts.append(f"({','.join(columns)})"
                                 + ("=exact" if not lost else f" -{lost}"))
                print(f"{'':8s} {'':>11s} {'':>7s} {'':>9s} {'':>9s}"
                      f"  short keys at depth {depth:,}: " + "  ".join(parts))
        except Exception as error:                        # noqa: BLE001
            print(f"{label:8s} FAILED: {str(error)[:90]}")
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
