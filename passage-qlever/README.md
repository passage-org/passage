# Passage ⨯ QLever

[QLever](https://github.com/ad-freiburg/qlever) keeps its triples in six
permutations, each a column store cut into compressed blocks, and holds one
metadata record per block in RAM: that block's first and last triple, and its
number of rows.

That metadata is what makes a continuation affordable. Where Blazegraph reaches
an offset through *augmented* balanced trees, QLever reaches it by adding up row
counts it already has in memory and decompressing only the block the offset
falls in. In other terms, it makes efficient:

- [X] `SELECT <projected> WHERE {<triple_pattern>} OFFSET X`, i.e., jumping at
  an integer offset when the query is a triple pattern. On a relation spanning
  640 blocks, a deep offset costs what a shallow one costs, against 32 ms per
  query when the blocks have to be read and discarded;

- [X] `SELECT COUNT(*) WHERE {<triple_pattern>}`, i.e., provide the number of
  elements corresponding to the triple pattern, from the metadata alone unless
  a block has to be read to settle duplicates that differ only in their graph;

- [X] `SELECT DISTINCT ?x WHERE {<triple_pattern>}` on a leading component,
  which a column store answers by walking one column and stepping over equal
  runs, with no sorting and no hash table.

The first of these needs a change that upstream QLever does not carry, so the
`qlever` submodule is pinned at a commit that has it. Its absence changes how
long a scan takes and never what it returns, which means no test here can
detect it; the submodule pointer is the only record of which QLever this was
built against.

Scans cross into the JVM through a JNI bridge, so this module needs a shared
library built from that submodule. [NATIVE.md](NATIVE.md) covers building it,
how `QLeverNative` locates it, and why the tests skip rather than fail when it
is absent.
