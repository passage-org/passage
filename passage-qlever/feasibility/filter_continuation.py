#!/usr/bin/env python3 -I
"""Can a triple pattern be continued with FILTER instead of OFFSET?

A feasibility probe, not an implementation. It drives a QLever endpoint over
HTTP and asks three questions of each pattern:

  1. Is the continued answer EXACTLY the OFFSET answer — same sequence, same
     length, no gap, no duplicate?
  2. Does every slice stay fast, or does one fall off the prefilter cliff and
     scan from the beginning?
  3. How many continuations, and what do they cost against OFFSET?

The idea is keyset pagination: instead of `OFFSET k`, the continuation carries
the last solution emitted and asks for everything after it, in the order the
scan was enumerating:

    FILTER (?a > A || (?a = A && (?b > B || (?b = B && ?c > C))))

That order is the permutation's column order, not the order the variables
appear in the query. A real implementation reads it off the paused scan, which
knows it. Here we CHOOSE it empirically: each candidate order is tried against
the second OFFSET slice, and the one that resumes in the right place wins.
Inferring it instead by comparing strings does not work — QLever sorts its
vocabulary by ICU collation, not by bytes, so a client cannot reconstruct the
order from the terms alone. That is itself a result.

Nothing is materialised: both enumerations are folded into a rolling hash, so
a ten-million-solution pattern costs no memory.

    python3 -I filter_continuation.py http://localhost:7731 50000
"""

import hashlib
import http.client
import json
import sys
import time
import urllib.parse
import urllib.request
from itertools import permutations

WSDBM = "http://db.uwaterloo.ca/~galuc/wsdbm/"
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

PATTERNS = [
    ("?s a ?c", f"?s <{TYPE}> ?c", ["s", "c"]),
    ("?s friendOf ?o", f"?s <{WSDBM}friendOf> ?o", ["s", "o"]),
    ("?s ?p ?o", "?s ?p ?o", ["s", "p", "o"]),
]


_CONNECTIONS = {}


def ask(endpoint, query, timeout=900):
    """Run a query; return (bindings, server-reported milliseconds).

    The connection is kept and reused. Opening one per request exhausts the
    ephemeral port range after a few thousand slices — 16,384 ports against
    23,265 sockets sitting in TIME_WAIT, on the run that taught us this."""
    host = urllib.parse.urlsplit(endpoint).netloc
    path = "/?" + urllib.parse.urlencode({"query": query})
    for attempt in (1, 2):
        connection = _CONNECTIONS.get(host)
        if connection is None:
            connection = http.client.HTTPConnection(host, timeout=timeout)
            _CONNECTIONS[host] = connection
        try:
            connection.request("GET", path, headers={"Connection": "keep-alive"})
            payload = json.load(connection.getresponse())
            break
        except Exception:                                 # noqa: BLE001
            connection.close()
            _CONNECTIONS.pop(host, None)
            if attempt == 2:
                raise
    if "exception" in payload:
        raise RuntimeError(str(payload["exception"])[:200])
    return payload["results"]["bindings"], payload["meta"]["query-time-ms"]


def term(binding):
    """A binding as it must be written back into a SPARQL query."""
    kind = binding["type"]
    if kind == "uri":
        return "<" + binding["value"] + ">"
    if kind == "bnode":
        return "_:" + binding["value"]
    text = '"' + binding["value"].replace("\\", "\\\\").replace('"', '\\"') + '"'
    if "datatype" in binding:
        return text + "^^<" + binding["datatype"] + ">"
    if "xml:lang" in binding:
        return text + "@" + binding["xml:lang"]
    return text


def row(binding, variables):
    return tuple(term(binding[v]) for v in variables)


def filter_after(last, order):
    """FILTER selecting everything strictly after `last`, in `order`."""
    def nest(i):
        var, value = "?" + order[i], last[order[i]]
        if i == len(order) - 1:
            return f"{var} > {value}"
        return f"{var} > {value} || ({var} = {value} && ({nest(i + 1)}))"
    return "FILTER ( " + nest(0) + " )"


class Fold:
    """A rolling hash of a sequence of rows, plus its length."""

    def __init__(self):
        self.digest = hashlib.blake2b(digest_size=16)
        self.count = 0
        self.first = None
        self.last = None

    def add(self, r):
        self.digest.update(("\x1f".join(r) + "\x1e").encode())
        self.count += 1
        if self.first is None:
            self.first = r
        self.last = r

    def hex(self):
        return self.digest.hexdigest()


def choose_order(endpoint, body, variables, cap):
    """Try each column order; keep the one that resumes where OFFSET does."""
    first, _ = ask(endpoint, f"SELECT * WHERE {{ {body} }} LIMIT {cap}")
    if len(first) < cap:
        return list(variables), None          # one slice: any order will do
    expected, _ = ask(
        endpoint, f"SELECT * WHERE {{ {body} }} LIMIT 1 OFFSET {cap}")
    target = row(expected[0], variables)
    last = {v: term(first[-1][v]) for v in variables}
    tried = []
    for candidate in permutations(variables):
        order = list(candidate)
        where = body + " . " + filter_after(last, order)
        got, _ = ask(endpoint, f"SELECT * WHERE {{ {where} }} LIMIT 1")
        resumed = row(got[0], variables) if got else None
        tried.append((order, resumed == target))
        if resumed == target:
            return order, tried
    return None, tried


def truncated_key_loss(endpoint, body, variables, order, depth):
    """What a shorter key loses.

    The key has to name every column the scan is ordered by. A key one column
    short silently drops the solutions that share the boundary row's prefix —
    few enough to pass a smoke test, which is exactly what makes it dangerous.
    Returns a list of (columns used, solutions returned)."""
    boundary, _ = ask(
        endpoint, f"SELECT * WHERE {{ {body} }} LIMIT 1 OFFSET {depth}")
    last = {v: term(boundary[0][v]) for v in variables}
    out = []
    for width in range(len(order), 0, -1):
        where = body + " . " + filter_after(last, order[:width])
        counted, _ = ask(
            endpoint, f"SELECT (COUNT(*) AS ?n) WHERE {{ {where} }}")
        out.append((order[:width], int(counted[0]["n"]["value"])))
    return out


def enumerate_by_filter(endpoint, body, variables, cap, order):
    fold, steps, total, slowest = Fold(), 0, 0.0, 0.0
    where = body
    while True:
        batch, ms = ask(endpoint, f"SELECT * WHERE {{ {where} }} LIMIT {cap}")
        steps += 1
        total += ms
        slowest = max(slowest, ms)
        for b in batch:
            fold.add(row(b, variables))
        if len(batch) < cap:
            return fold, steps, total, slowest
        last = {v: term(batch[-1][v]) for v in variables}
        where = body + " . " + filter_after(last, order)


def enumerate_by_offset(endpoint, body, variables, cap):
    fold, steps, total, slowest = Fold(), 0, 0.0, 0.0
    while True:
        batch, ms = ask(
            endpoint,
            f"SELECT * WHERE {{ {body} }} LIMIT {cap} OFFSET {fold.count}")
        steps += 1
        total += ms
        slowest = max(slowest, ms)
        for b in batch:
            fold.add(row(b, variables))
        if len(batch) < cap:
            return fold, steps, total, slowest


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7731"
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    print(f"endpoint {endpoint} · slices of {cap:,} solutions\n")
    failures = 0
    for label, body, variables in PATTERNS:
        print(f"── {label}")
        try:
            order, tried = choose_order(endpoint, body, variables, cap)
            if order is None:
                print("   NO column order resumes where OFFSET does:")
                for candidate, ok in tried:
                    print(f"     ({', '.join(candidate)}) {'yes' if ok else 'no'}")
                failures += 1
                print()
                continue
            if tried:
                rejected = [c for c, ok in tried if not ok]
                print(f"   order ({', '.join(order)})"
                      f" — {len(rejected)} other orders resumed elsewhere")

            start = time.time()
            mine, steps, total, slowest = enumerate_by_filter(
                endpoint, body, variables, cap, order)
            wall = (time.time() - start) * 1000
            theirs, off_steps, off_total, off_slowest = enumerate_by_offset(
                endpoint, body, variables, cap)

            print(f"   by FILTER  {mine.count:>10,} solutions"
                  f"  {total:>7,} ms server  {wall:>7,.0f} ms wall"
                  f"  {steps:>4} slices  slowest {slowest:>6,} ms")
            print(f"   by OFFSET  {theirs.count:>10,} solutions"
                  f"  {off_total:>7,} ms server"
                  f"  {' ':>16}{off_steps:>4} slices  slowest {off_slowest:>6,} ms")

            if mine.hex() == theirs.hex():
                print("   IDENTICAL sequences")
                if len(order) > 1:
                    depth = theirs.count // 2
                    expected = theirs.count - depth - 1
                    print(f"   a shorter key, resuming at depth {depth:,}"
                          f" where {expected:,} should remain:")
                    for columns, got in truncated_key_loss(
                            endpoint, body, variables, order, depth):
                        lost = expected - got
                        print(f"     ({', '.join(columns)})"
                              f"{'':<{18 - len(', '.join(columns))}}"
                              f" {got:>10,}"
                              f"{'   exact' if not lost else f'   {lost} LOST'}")
            else:
                print(f"   DIFFERENT — {mine.count - theirs.count:+,} solutions")
                print(f"     filter first {mine.first}  last {mine.last}")
                print(f"     offset first {theirs.first}  last {theirs.last}")
                failures += 1
        except Exception as error:                       # noqa: BLE001
            print(f"   FAILED: {error}")
            failures += 1
        print()

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
