# Passage

Passage provides correct and complete results for SPARQL queries, despite 
time quotas enforced by public SPARQL endpoints. Contrarily to state-of-the-art
approaches [1, 2], Passage works solely within SPARQL boundaries: a query execution
returns correct (possibly partial) results along with a SPARQL continuation query; 
executing the continuation query provides missing results, itself with another SPARQL 
continuation query; until termination.

## Installation

```shell
git clone https://github.com/Chat-Wane/passage.git
cd passage

mvn clean package
```

## Usage

- [X] `passage.jar`[`--help`](./passage-cli) provides a one time execution without server.
- [X] `passage-server.jar`[`--help`](./passage-cli) provides a server running passage as a SPARQL endpoint.
- [X] `passage-comunica`[`--help`](https://github.com/Chat-Wane/passage-comunica) provides a client to automate the continuation loop.


- [X] `raw.jar`[`--help`](./raw-cli) provides a one time execution of a sample-based query.


## References

[1] T. Minier, H. Skaf-Molli and P. Molli. __SaGe: Web
 Preemption for Public SPARQL Query services__. In Proceedings of the
 World Wide Web Conference (2019).

[2] R. Verborgh, M. Vander Sande, O. Hartig, J. Van Herwegen, L. De Vocht, B. De Meester,
 G. Haesendonck and P. Colpaert. __Triple Pattern Fragments: A Low-Cost Knowledge Graph
 Interface for the Web__. In Journal of Web Semantics (2016).