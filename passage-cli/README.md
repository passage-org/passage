# Passage CLI

## `passage.jar`

```
Usage: passage [-rh] -d=<path> [-q=<SPARQL>] [-f=<path>] [-t=<ms>] [-l=<scans>]
               [--loop] [--force-order] [-n=1]
SPARQL continuation query processing. Looping until done!
  -d, --database=<path>   The path to your blazegraph database.
  -q, --query=<SPARQL>    The SPARQL query to execute.
  -f, --file=<path>       The file containing the SPARQL query to execute.
  -t, --timeout=<ms>      Timeout before the query execution is stopped.
  -l, --limit=<scans>     Number of scans before the query execution is stopped.
      --loop              Continue executing the query until completion.
      --force-order       Force the order of triple patterns to the one
                            provided by the query.
  -r, --report            Provides a concise report on query execution.
  -n, --executions=1      Number of times that it executes the query in
                            sequence (for performance analysis).
  -h, --help              Display this help message.
```

## `passage-server.jar`
```
Usage: passage-server [-h] -d=<path> [-t=<ms>] [--ui=<ui>] [-p=<3330>]
A server for preemptive SPARQL query processing!
  -d, --database=<path>   The path to your blazegraph database.
  -t, --timeout=<ms>      Timeout before the query execution is stopped.
      --ui=<ui>           The path to your UI folder.
  -p, --port=<3330>       The port of the server.
  -h, --help              Display this help message.
```