# RAWer

RAndom walks sampling over SPARQL queries, but
[better](https://github.com/GDD-Nantes/raw-jena) [2]. Ultimately, it
advocates for a support of online sampling [1] within SPARQL to enable
getting random elements, processing aggregations, optimizing join
order, building summaries etc.

This project is designed to handle multiple backends with a
well-defined scope in terms of supported operators.

## Usage

```shell
Usage: <main class> [-hr] [-cl] [-d=<database>] [-f=<queryFile>] [-l=<limit>]
                    [-n=<numberOfExecutions>] [-q=<queryAsString>]
                    [-sl=<subqueryLimit>] [-st=<subquerytimeout>] [-t=<timeout>]
  -cl, --chao-lee       Use Chao-Lee as count-distinct estimator. Default is CRAWD.
  -d, --database=<database> The path to your blazegraph database.
  -f, --file=<queryFile>    The file containing the SPARQL query to execute.
  -h, --help                Display this help message.
  -l, --limit=<limit>       Number of scans before the query execution is stopped.
  -n, --executions=<numberOfExecutions> Number of times that it executes the query in sequence (for performance analysis).
  -q, --query=<queryAsString> The SPARQL query to execute.
  -r, --report              Provides a concise report on query execution.
  -sl, --sublimit=<subqueryLimit> Number of scans before the subquery execution is stopped (if exists).
  -st, --subtimeout=<subquerytimeout> Timeout before the subquery execution is stopped (if exists).
  -t, --timeout=<timeout>   Timeout before the query execution is stopped.
```

```shell
mvn clean install -Dmaven.test.skip=true; \
mvn exec:java -pl rawer -Dexec.args="\
--database=/path/to/dataset/for/example/watdiv10m-blaze/watdiv10M.jnl \
--query='SELECT (COUNT (DISTINCT(?s)) AS ?count) WHERE {?s ?p ?o}' \
--limit=20000 \
-sl=1 \
--report" ## We expect 521585 distinct subjects:

# Path to database: /path/to/dataset/for/example/watdiv10m-blaze/watdiv10M.jnl
# SPARQL query: SELECT (COUNT (DISTINCT(?s)) AS ?count) WHERE {?s ?p ?o}
# [fr.gdd.sage.rawer.cli.RawerCLI.main()] DEBUG ApproximateAggCountDistinct - BigN SampleSize: 10000.0
# [fr.gdd.sage.rawer.cli.RawerCLI.main()] DEBUG ApproximateAggCountDistinct - CRAWD SampleSize: 10000
# [fr.gdd.sage.rawer.cli.RawerCLI.main()] DEBUG ApproximateAggCountDistinct - Nb Total Scans: 20000
# {?count-> ""512947.4830016874"^^http://www.w3.org/2001/XMLSchema#double" ; }
# Execution time:  1958 ms
# Number of Results:  1
```

## Backends

An RDF datastore can use this RAW engine by implementing the
interfaces `Backend` and `BackendIterator`. Most importantly, the data
structure in charge of storing the triples/quads must enable (__i__)
range query for a triple pattern, (__ii__) an efficient way to get a
random element from this triple pattern, and (__iii__) the probability
of having picked this element at random.

- [X] Support [Blazegraph](https://blazegraph.com/). Blazegraph uses
      augmented btrees as indexes which store the number of leaves in
      each node. This proves ideal for retrieving the number of
      elements within a range, then getting the element at the
      designated (uniform at random) index.
      
> [!NOTE]
> To ingest your data into a blazegraph dataset, you can use the CLI
> of [`blazegraph.jar`](https://github.com/blazegraph/database/releases/tag/BLAZEGRAPH_2_1_6_RC):
> `java -cp blazegraph.jar com.bigdata.rdf.store.DataLoader -defaultGraph http://example.com/watdiv watdiv10M.properties watdiv.10M.nt`
      

- [ ] Support [Apache Jena](https://jena.apache.org/). Although
      RAW-Jena exists, it requires further modifications to adapt to
      the changes brought by Blazegraph.


## Operators

Operators are divided between sampling operators that return mappings,
and aggregate operators that produce approximate results based on
former mappings. Unlisted operators are probably not implemented (yet).

### Sampling operators

- [X] Triple pattern
- [ ] Quad pattern with `GRAPH` clause
- [X] Basic Graph Patterns (BGPs)
- [X] Join
- [ ] Union
- [ ] Left join with `OPTIONAL` clause
- [X] Bind with a simple expression e.g. `BIND <http://exist_in_dataset> AS ?variable`

### Aggregate operators

- [X] Approximate Count the number of results with `COUNT(*) AS ?count` based on WanderJoin [3]
- [ ] Approximate Count on specific variables with `COUNT(?v) AS ?count`
- [ ] Approximate Count Distinct on every variable with `COUNT(DISTINCT(*)) AS ?count`
- [X] Approximate Count Distinct on specific variables with `COUNT(DISTINCT(?v)) AS ?count`

## References


[1] S. Agarwal, H. Milner, A. Kleiner, A. Talwalkar, M. I. Jordan,
S. Madden, B. Mozafari, I. Stoica, <i>Knowing when you’re wrong:
Building fast and reliable approximate query processing systems.</i>

[2] J. Aimonier-Davat, M.-H. Dang, P. Molli, B. Nédelec, and
H. Skaf-Molli, <i>[RAW-JENA: Approximate Query Processing for SPARQL
Endpoints.](https://hal.science/hal-04250060v1/file/paper.pdf)</i>

[3] F. Li, B. Wu, K. Yi, Z. Zhao, <i>Wander Join and XDB: Online
Aggregation via Random Walks.</i>
