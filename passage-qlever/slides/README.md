# Slides — QLever as a Passage backend

Sixteen slides documenting QLever's storage layout and what continuations cost
on it. Published at https://claude.ai/artifact/62ux9soizTS5FS2rCLxhdD, where
the page exports to PPTX and PDF.

The visual style follows `WWW2025-Passage`, the continuation-queries deck: a
full-bleed `#0084D1` banner with a white Roboto Slab title, Roboto body, green
`#38761D` for good and red `#FF0000` for bad, page number bottom right. These
slides are meant to slot into that deck, not to stand alone — they have no
cover and no introduction to continuations.

## The files

`project/deck.json` is the index: title, the slide ids **in deck order**, the
section outline and the typefaces. `project/slides/<id>.html` is one slide
each, and the file name is its id.

A slide file holds exactly one `<section>`, with every style inline and no
`<html>` or `<head>` — it is assembled at render time, so **opening one in a
browser shows unstyled text**. To see the deck, use the published page.

## Where the numbers come from

Nothing on these slides is quoted from the literature, because the storage
layout is not in the literature; slide 15 says so and shows what was checked.
Everything was read from the source of the pinned QLever fork and from a real
index, and `probes/` holds the programs that did the reading:

| probe | what it produced |
|---|---|
| `Rows.java` | the rows of a PSO block on slide 9 |
| `Order.java` | the vocabulary in Id order, slide 6 left |
| `Order2.java` | the Ids of other datatypes, slide 6 right |
| `Limits.java` | the cost table on slide 13 |
| `Unknown.java` | the absent-IRI bug, now a regression test |

They are not part of the build — Maven compiles `src/main/java` and
`src/test/java` only. Compile and run one against a classpath built from the
module, the way `../README.md` describes for `ColdStart`:

```sh
mvn -q -pl passage-volcano dependency:build-classpath \
    -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/cp.txt
javac -cp "$(cat /tmp/cp.txt):passage-qlever/target/classes" \
    -d /tmp/probes passage-qlever/slides/probes/Rows.java
java -cp "$(cat /tmp/cp.txt):passage-qlever/target/classes:/tmp/probes" \
    Rows $HOME/bench-watdiv/qlever/watdiv10m 10
```

The file sizes and block counts on slides 4, 7 and 9 come from `ls` and from
the index's own `server.log` and `meta-data.json`, on the WatDiv 10M index
built by `../build-index.sh`.
