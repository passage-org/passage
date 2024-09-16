# Sager

Goofy name for a better, well-defined, version of
`sage-jena-volcano`. Eventually, it should replace the latter. As a
small overview:

- [ ] All operators that are handled are **explicitly** implemented,
      the rest throws an error. _So what about unimplemented
      operators?  Are some queries simply not executable?_ Yes and no:
      another SPARQL query engine built on top of Sage should handle
      it, detecting the parts that are preemptive of those that
      aren't.

- [ ] What happens in SPARQL stays in SPARQL. Sager exports its
      preempted state as a SPARQL query. Upon receiving it again, the
      engine interprets it as normal SPARQL where preempted parts
      merely activate some optimizations. For instance, a subquery
      such as `SELECT * WHERE { ?s ?p ?o } OFFSET 1 ORDER BY ?s ?p ?o`
      actually states that the iterator should skip its first triple
      (logarithmic time complexity) and use the index `spo`.

- [ ] Not bound to align with Jena's TDB engine. We allow ourselves
      all optimizations such as cardinality-based join ordering,
      jump-skipping. Preempted SPARQL queries could generate a lot of
      `UNION`, which could/should be parallelized.
