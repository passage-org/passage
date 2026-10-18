# The container

Ubuntu 24.04, x86_64, with the compiler, Boost, OpenJDK 21 and Maven that
QLever and Passage need. `install-deps.sh` runs when the container is created;
`build-and-test.sh` is the whole chain and is meant to be run by hand, since
it takes about an hour.

The instructions, what each step should print, and what to do when one fails
are in **[`../README-QLEVER.md`](../README-QLEVER.md)**, so that there is one
copy of them and not two.
