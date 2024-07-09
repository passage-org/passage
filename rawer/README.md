# RAWer

RAndom walks sampling over SPARQL queries, but
[better](https://github.com/GDD-Nantes/raw-jena). This project is
designed to handle multiple backends with a well-defined scope in
terms of supported operators.

## Backends

An RDF datastore can use this RAW engine by implementing the
interfaces `Backend` and `BackendIterator`. Most importantly, the data
structure in charge of storing the triples/quads must enable (__i__)
range query for a triple pattern, (__ii__) an efficient way to get a
random element from this triple pattern, and (__iii__) the probability
of having picked this element at random.

- [X] Support [Blazegraph](https://blazegraph.com/). 
- [ ] Support [Apache Jena](https://jena.apache.org/). Although
      RAW-Jena exists, it requires further modifications to adapt to
      the changes brought by Blazegraph.


## Operators

Operators are divided between sampling operators that return mappings,
and aggregate operators that produce approximate results based on
former mappings.

### Sampling operators

- [ ] Triple patterns
- [ ] Quad patterns with `GRAPH` clause
- [ ] Basic Graph Patterns (BGPs)
- [ ] Join  
