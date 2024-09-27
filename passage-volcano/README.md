# Passage's iterator model

Iterator model (also called Volcano) of Passage. It is in charge of
reading a logical plan to build a physical plan. Once the execution
timeout is reached, Passage is able to save the physical plan into
a new logical plan to continue the execution later on, if needed.

- [X] All operators that are handled are **explicitly** implemented,
      the rest throws an error.

- [X] What happens in SPARQL stays in SPARQL.

## Supported operators

- [X] Triple patterns
- [X] Basic graph patterns (BGPs)
- [X] Joins
- [X] Unions
- [X] Binds (simple one without expressions)
- [X] Filters
- [X] Optionals



- [X] Count (simple ones)