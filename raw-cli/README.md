# RAW CLI

## `raw.jar`

```
Usage: raw [-rh] -d=<path> [-q=<SPARQL>] [-f=<path>] [-t=<ms>] [-l=<scans>]
           [-st=<ms>] [-sl=<scans>] [-cl] [--force-order] [--seed=1]
           [--no-seed] [--threads=1] [-n=1]
RAndom-Walk-based SPARQL query processing.
  -d, --database=<path>   The path to your blazegraph database.
  -q, --query=<SPARQL>    The SPARQL query to execute.
  -f, --file=<path>       The file containing the SPARQL query to execute.
  -t, --timeout=<ms>      Timeout before the query execution is stopped.
  -l, --limit=<scans>     Number of scans before the query execution is stopped.
      -st, --subtimeout=<ms> Timeout before the subquery execution is stopped (if exists).
      -sl, --sublimit=<scans> Number of scans before the subquery execution is stopped (if exists).
      -cl, --chao-lee     Use Chao-Lee as count-distinct estimator. Default is CRAWD.
      --force-order       Force the order of triple patterns to the one provided by the query.
      --seed=1            Set the seed of random number generators.
      --no-seed           Default is seeded, this disable it.
      --threads=1         Number of threads to process aggregate queries.
  -r, --report            Provides a concise report on query execution.
  -n, --executions=1      Number of times that it executes the query in sequence (for performance analysis).
  -h, --help              Display this help message.
```


```shell
## For example: 
java -jar raw 
  --database=/path/to/watdiv10m-blaze/watdiv10M.jnl \
  --query='SELECT (COUNT (DISTINCT(?s)) AS ?count) WHERE {?s ?p ?o}' \
  --limit=20000 \
  -sl=1 \
  --report" ## We expect 521585 distinct subjects:

# Path to database: /path/to/dataset/for/example/watdiv10m-blaze/watdiv10M.jnl
# SPARQL query: SELECT (COUNT (DISTINCT(?s)) AS ?count) WHERE {?s ?p ?o}
# [fr.gdd.sage.rawer.cli.RawCLI.main()] DEBUG ApproximateAggCountDistinct - BigN SampleSize: 10000.0
# [fr.gdd.sage.rawer.cli.RawCLI.main()] DEBUG ApproximateAggCountDistinct - CRAWD SampleSize: 10000
# [fr.gdd.sage.rawer.cli.RawCLI.main()] DEBUG ApproximateAggCountDistinct - Nb Total Scans: 20000
# {?count-> ""512947.4830016874"^^http://www.w3.org/2001/XMLSchema#double" ; }
# Execution time:  1958 ms
# Number of Results:  1
```


## `raw-server.jar`

```shell
Usage: raw-server [-h] -d=<path> [-t=<ms>] [-l=<scans>] [-al=<randomwalkattempt>] [--ui=<ui>] [-p=<3330>]
A server for preemptive SPARQL query processing!
  -d, --database=<path>   The path to your blazegraph database.
  -t, --timeout=<ms>      Timeout before the query execution is stopped.
  -l, --limit=<scans>     Number of scans before the query execution is stopped.
      -al, --attempt-limit=<randomwalkattempt>
                          Number of random walk attempts before the query execution is stopped.
      --ui=<ui>           The path to your UI folder.
  -p, --port=<3330>       The port of the server.
  -h, --help              Display this help message.
```
