#!/usr/bin/env python3 -I
"""A continuation key that follows the storage order, not the value order.

A plain `FILTER(?o > K)` fails on a column that holds more than one datatype,
because QLever enumerates by the bits of the Id — grouped by datatype tag —
while `>` compares values. `mixed_datatypes.py` shows the damage.

The repair is to say the same thing in the order the scan uses: everything
left in the boundary's own datatype class, then every class that comes after
it. In storage order the classes are

    boolean  <  int  <  double  <  string / langString / IRI  <  date

and inside the numeric classes the negatives sit *after* the positives,
because the payload is two's complement. So "after 42" inside the integers is
`?o > 42 || ?o < 0`, not `?o > 42`.

This builds that expression, and the probe below checks it enumerates exactly
what the scan does.

    python3 -I typed_key.py http://localhost:7733
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from filter_continuation import Fold, ask, row, term

XSD = "http://www.w3.org/2001/XMLSchema#"

# The datatype classes in the order QLever's Ids put them, each with the
# SPARQL test that recognises it. `vocab` is one class: strings, language
# strings and IRIs share the vocabulary, ordered by their written form.
CLASSES = [
    ("boolean", f"DATATYPE(?{{v}}) = <{XSD}boolean>"),
    ("int", f"DATATYPE(?{{v}}) = <{XSD}int>"),
    ("double", f"DATATYPE(?{{v}}) = <{XSD}double>"),
    ("vocab", "isIRI(?{v}) || DATATYPE(?{v}) = <" + XSD + "string>"
              ' || LANG(?{v}) != ""'),
    ("date", f"DATATYPE(?{{v}}) = <{XSD}date>"),
]
NUMERIC = {"int", "double"}


def classify(binding):
    """Which class a solution's term belongs to, as QLever reports it."""
    if binding["type"] == "uri" or binding["type"] == "bnode":
        return "vocab"
    datatype = binding.get("datatype", "")
    if datatype.startswith(XSD):
        name = datatype[len(XSD):]
        # The result JSON and the engine's own DATATYPE() disagree: 0.5 comes
        # back tagged xsd:decimal, while DATATYPE(?o) on the same term answers
        # xsd:double. A builder that reads the type off the solutions it just
        # sent and writes a DATATYPE() test into the next query gets the class
        # wrong, and the continuation loops forever on the same slice.
        if name == "decimal":
            name = "double"
        if name in ("boolean", "int", "double", "date"):
            return name
        if name == "string":
            return "vocab"
    return "vocab"                      # plain and language-tagged literals


def after_in_class(var, name, value, numeric_value):
    """Everything left in the boundary's own class, in storage order."""
    if name in NUMERIC:
        # Negatives sort after positives, so "after v" wraps around.
        if numeric_value is not None and numeric_value < 0:
            return f"(?{var} > {value} && ?{var} < 0)"
        return f"(?{var} > {value} || ?{var} < 0)"
    if name == "boolean":
        return "false" if value in ("true", '"true"^^<%sboolean>' % XSD) \
               else f"?{var} = true"
    return f"?{var} > {value}"


def typed_filter(var, binding):
    """The continuation condition for one column, in storage order."""
    name = classify(binding)
    value = term(binding)
    numeric = None
    if name in NUMERIC:
        try:
            numeric = float(binding["value"])
        except ValueError:
            numeric = None
    index = [n for n, _ in CLASSES].index(name)
    # The within-class condition has to be fenced by the class test. SPARQL
    # compares numbers across types, so a bare `?o > 0.5` also matches the
    # integers the scan has already emitted, and the continuation loops.
    own = after_in_class(var, name, value, numeric)
    test = CLASSES[index][1].format(v=var)
    parts = [f"(({test}) && {own})"] if own != "false" else []
    for later, test in CLASSES[index + 1:]:
        parts.append("(" + test.format(v=var) + ")")
    return " || ".join(parts)


def walk(endpoint, body, variables, cap, typed):
    fold, steps, total = Fold(), 0, 0.0
    where = body
    while True:
        query = (f"SELECT * WHERE {{ {where} }} LIMIT {cap}" if typed
                 else f"SELECT * WHERE {{ {body} }} LIMIT {cap}"
                      f" OFFSET {fold.count}")
        batch, ms = ask(endpoint, query)
        steps += 1
        total += ms
        for b in batch:
            fold.add(row(b, variables))
        if len(batch) < cap:
            return fold, steps, total
        if typed:
            # One column only: enough for a scan whose leading column is the
            # one that mixes datatypes, which is the case this repairs.
            where = (body + " . FILTER ( "
                     + typed_filter(variables[0], batch[-1][variables[0]])
                     + " )")


def main():
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:7733"
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    body, variables = "?s ?p ?o", ["o"]
    print(f"endpoint {endpoint} · slices of {cap}\n")
    typed, steps, total = walk(endpoint, body, variables, cap, True)
    plain, off_steps, off_total = walk(endpoint, body, variables, cap, False)
    print(f"  by typed FILTER  {typed.count:>6,} solutions"
          f"  {steps:>3} slices  {total:>6,.0f} ms")
    print(f"  by OFFSET        {plain.count:>6,} solutions"
          f"  {off_steps:>3} slices  {off_total:>6,.0f} ms")
    same = typed.hex() == plain.hex()
    print(f"\n  {'IDENTICAL sequences' if same else 'DIFFERENT'}")
    return 0 if same else 1


if __name__ == "__main__":
    sys.exit(main())
