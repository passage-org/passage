# Passage ⨯ Blazegraph

[Blazegraph](https://blazegraph.com/)'s database provides additional properties that we
exploit for continuation queries, and approximate query processing.

Under the hood, Blazegraph uses [*augmented* balanced trees](https://github.com/blazegraph/database/wiki/BTreeGuide)
where each node carries and maintains an additional counter field
representing its number of children. In other terms, it makes efficient:

- [X] `SELECT <projected> WHERE {<triple_pattern>} OFFSET X`, i.e., jumping at an integer 
  offset when the query is a triple pattern;

- [X] `SELECT COUNT(*) WHERE {<triple_pattern>}`, i.e., provide the number of elements
  corresponding to the triple pattern.