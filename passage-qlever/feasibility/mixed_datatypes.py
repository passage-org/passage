#!/usr/bin/env python3 -I
"""Where the FILTER continuation breaks: a column that is not one datatype.

On WatDiv every object is an IRI or a plain string, so the storage order and
SPARQL's `>` agree and the approach works. They are not the same order in
general. QLever sorts by the bits of the Id, which group by datatype tag —

    Bool < Int < Double < …VocabIndex (strings, IRIs)… < Date

— and inside Int the negatives land *after* the positives, because the payload
is two's complement. SPARQL's `>` compares values. The two disagree, and a
FILTER cannot then express "everything after row k in the scan".

Build the index from `mixed.ttl`, serve it, and run this:

    qlever-index -i mixed -f mixed.ttl -F ttl
    qlever-server -i mixed -p 7733 -j 2 &
    python3 -I mixed_datatypes.py http://localhost:7733
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filter_continuation import ask

XSD = "http://www.w3.org/2001/XMLSchema#"


def describe(binding):
    kind = (binding.get("datatype", "").split("#")[-1]
            or binding.get("xml:lang", "") or binding["type"])
    return f"{kind:8s} {binding['value'][:28]}"


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7733"

    scan, _ = ask(endpoint, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    print("the scan order — by Id bits, which group by datatype tag:\n")
    for i, b in enumerate(scan, 1):
        print(f"  {i:>2}. {describe(b['o'])}")

    print("\ninterrupting after the 6th row, the integer 42:")
    remainder = len(scan) - 6
    kept, _ = ask(endpoint, "SELECT ?o WHERE { ?s ?p ?o FILTER(?o > 42) }")
    print(f"  the scan has {remainder} rows left")
    print(f"  FILTER(?o > 42) returns {len(kept)}:"
          f" {', '.join(b['o']['value'][:16] for b in kept)}")

    ints, _ = ask(endpoint, "SELECT ?o WHERE { ?s ?p ?o"
                            f" FILTER(DATATYPE(?o) = <{XSD}int>) }}")
    after, _ = ask(endpoint, "SELECT ?o WHERE { ?s ?p ?o"
                             f" FILTER(DATATYPE(?o) = <{XSD}int> && ?o > 42) }}")
    print("\nand within one datatype, negatives still break it:")
    print(f"  integers in scan order: "
          f"{', '.join(b['o']['value'] for b in ints)}")
    print(f"  FILTER(?o > 42) keeps:  "
          f"{', '.join(b['o']['value'] for b in after)}"
          f"   — the negative is lost")
    return 0


if __name__ == "__main__":
    sys.exit(main())
