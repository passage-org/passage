# Passage on QLever: build it, run it

One path, in order, verified end to end in a GitHub Codespace. Copy each block
as it is. No block depends on a shell variable set by another one, so you can
stop, close the tab, come back, and carry on.

The reference environment is a Codespace. It is not a recommendation: it is
where this sequence is known to work, on Ubuntu 24.04 / x86_64 with gcc 13 and
OpenJDK 21. On your own machine the same steps apply, and
[`passage-qlever/README.md`](passage-qlever/README.md) covers macOS.

Both repositories are public: `momo54/passage` and `momo54/qlever`, the second
being a submodule of the first. Nothing here needs an access grant or a
credential — it used to, and the whole authorization dance is gone.

---

## 1. Open a Codespace

On the repository page: **Code → Codespaces → Create codespace on
filter-continuation**. Or, from a terminal:

```bash
gh codespace create -R momo54/passage -b filter-continuation -m standardLinux32gb
```

Only one machine is offered, 4 cores and 16 GB, because `devcontainer.json`
asks for at least that. The C++ build is why.

When the terminal appears, the container has already installed the compiler,
Boost, the JDK and Maven. Check it:

```bash
head -2 /etc/os-release; command -v mvn javac cmake mold
```

Expect `Ubuntu 24.04`, then four paths. If `apt-get` or `mvn` is missing, go to
[§5](#5-when-it-goes-wrong) before anything else.

## 2. Build everything, one command

```bash
cd /workspaces/passage
nohup .devcontainer/build-and-test.sh > /tmp/build.log 2>&1 &
tail -f /tmp/build.log
```

**Budget an hour.** Almost all of it is compiling QLever, which is C++ with
heavy templates; long silences on a single file are normal, and so are
warnings. `nohup` means the build survives a closed tab or a dropped
connection — `Ctrl-C` stops only the `tail`, not the build. To look again:
`tail -f /tmp/build.log`.

## 3. What you should see

The script prints `===== <step> (<seconds>s)` as it goes. In order:

| step | what it does | the line that says it worked |
| --- | --- | --- |
| the pinned QLever | fetches the submodule at the recorded commit | `pinned at 55e9c7a8` (or a later one) |
| the JNI bridge, and every module | compiles QLever, builds `libqleverjni.so`, installs the eight Maven modules | `BUILD SUCCESS` |
| the library that came out | shows where it was put | two paths ending in `libqleverjni.so` |
| an index | builds the six triples of Figure 2 of the Passage paper | a path ending in `/qlever-fig2/fig2` |
| the tests | the backend, then a continuation through the engine | `Tests run: 8` then `Tests run: 4` |
| a continuation from the command line | one pause, printed as a SPARQL query | a `SELECT` with an `OFFSET 1` in it |

That last step should exit 0, and it does on macOS too, so it tells you
nothing about the allocator crash described in
[`passage-qlever/README.md`](passage-qlever/README.md) — six triples are not
enough work for QLever to start a worker thread, and that crash needs one to
start and then exit. If you do get `exit 132`, say so: it would be new.

## 4. The same thing by hand

If you want to run the steps yourself, or one of them failed and you are
retrying just that one:

```bash
cd /workspaces/passage
git submodule update --init passage-qlever/qlever
```

```bash
cd /workspaces/passage && mvn -Pnative install -DskipTests
```

```bash
mkdir -p $HOME/qlever-fig2
cat > $HOME/qlever-fig2/fig2.ttl <<'TTL'
<http://ex.org/H1> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H2> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H3> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O1> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O2> .
<http://ex.org/H3> <http://ex.org/occupation> <http://ex.org/O3> .
TTL
cd /workspaces/passage && passage-qlever/build-index.sh $HOME/qlever-fig2/fig2 $HOME/qlever-fig2/fig2.ttl
```

```bash
cd /workspaces/passage && mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' -DfailIfNoTests=false \
    -Dqlever.test.index=$HOME/qlever-fig2/fig2
```

```bash
cd /workspaces/passage && java -jar passage-cli/target/passage.jar \
    -d $HOME/qlever-fig2/fig2 \
    -q 'SELECT * WHERE { ?human <http://ex.org/type> <http://ex.org/Human> .
                         ?human <http://ex.org/occupation> ?occupation }' \
    -l 1 -r
```

Adding `--loop` to that last one feeds each continuation back in until the
query is done: five pauses for three results, every continuation a `UNION`
between what is already bound and a `SELECT ... OFFSET n` over the rest. That
is Example 3.10 of the Passage paper, over a QLever index, and it is the point
of the whole exercise.

## 5. When it goes wrong

Every row here is something that actually happened, in this order, on the day
this was written.

| what you see | what it is | what to do |
| --- | --- | --- |
| `apt-get: command not found`, or no `mvn` | the container failed to build and you are in Codespaces' **recovery container**: your files are there but the image is not the one asked for | `tail -40 /workspaces/.codespaces/.persistedshare/creation.log` says why. Fix that, then **Codespaces: Rebuild Container** from the command palette |
| `Could not find artifact fr.gdd:passage-commons` | you built one module alone; nothing under `fr.gdd` is published to any Maven repository | use `mvn -Pnative install -DskipTests`, with no `-pl` |
| `No rule to make target 'qleverjni'` | CMake found no `jni.h`, so it never created the target | `export JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")` and run the step again |
| `relocation R_X86_64_TPOFF32 ... recompile with -fPIC` | the static archives are not position-independent, and the bridge is a shared library | `build-native.sh` passes `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` on Linux; if you ran CMake by hand without it, add it. The flag change forces a full recompile |
| `c++: fatal error: Killed signal terminated program cc1plus` | not a compiler bug: the kernel killed it, out of memory | `JOBS=2 mvn -Pnative install -DskipTests`. What is already compiled is kept |
| `Tests run: 0` | the tests aborted rather than ran | the `SKIPPING <class>` line just above says why, usually an index that was not named. An empty `-Dqlever.test.index=` counts as naming none |
| `exit 132` at the very end | a SIGSEGV in the allocator's per-thread cleanup, after the results are printed | nothing is wrong with the run itself. On this six-triple index it is not expected at all, so report it |

## 6. Where to read on

- [`passage-qlever/README.md`](passage-qlever/README.md) — how the backend
  works, how QLever stores its triples, and every measurement, with the
  protocol that produced it.
- [`passage-qlever/journal.md`](passage-qlever/journal.md) — what was decided
  and why, newest first, including the conclusions that later measurements
  reversed. Read this before proposing a change.
- [`passage-qlever/feasibility/`](passage-qlever/feasibility/) — continuing a
  triple pattern with `FILTER` instead of `OFFSET`: it works, and it was
  dropped. The reason is worth knowing before you reinvent it.
- The QLever fork's `PASSAGE-README.md` — the `OFFSET` patch itself, which is
  the one change QLever carries for us.
