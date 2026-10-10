package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.core.Var;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Localise a difference between the two backends on a query, by counting the
 * solutions per value of one variable and diffing the two counts. The whole
 * join differs by 94 solutions out of 10,332,446 while `?s ?p ?o` and
 * `?s a ?c` agree exactly, so the difference is in the scans with a bound
 * subject, and this says which subjects.
 *
 *   java -cp … WhichSolutionsDiffer <qlever index> <blazegraph journal> [var]
 */
public class WhichSolutionsDiffer {

    static String QUERY = "SELECT * WHERE { ?s a ?c . ?s ?p ?o }";

    public static void main(String[] args) throws Exception {
        String variable = args.length >= 3 ? args[2] : "s";
        if (args.length >= 4) { QUERY = args[3]; }
        Map<String, Long> fromQLever = countsPer(new QLeverBackend(args[0]), variable);
        Map<String, Long> fromBlazegraph =
                countsPer(new BlazegraphBackend(args[1]), variable);

        System.out.printf("%n== %,d distinct ?%s from QLever, %,d from Blazegraph%n",
                fromQLever.size(), variable, fromBlazegraph.size());

        Map<String, String> differences = new TreeMap<>();
        for (var entry : fromQLever.entrySet()) {
            long other = fromBlazegraph.getOrDefault(entry.getKey(), 0L);
            if (other != entry.getValue()) {
                differences.put(entry.getKey(), entry.getValue() + " vs " + other);
            }
        }
        for (var entry : fromBlazegraph.entrySet()) {
            if (!fromQLever.containsKey(entry.getKey())) {
                differences.put(entry.getKey(), "0 vs " + entry.getValue());
            }
        }
        System.out.printf("== %d value(s) of ?%s differ%n", differences.size(), variable);
        differences.entrySet().stream().limit(20).forEach(e ->
                System.out.printf("   %-60s %s%n", e.getKey(), e.getValue()));
        Runtime.getRuntime().halt(0);
    }

    static <ID, VALUE> Map<String, Long> countsPer(Backend<ID, VALUE> backend,
                                                   String variable) {
        Map<String, Long> counts = new HashMap<>();
        Var var = Var.alloc(variable);
        PassageExecutionContext<ID, VALUE> context =
                new PassageExecutionContextBuilder<ID, VALUE>()
                        .setName("PUSH")
                        .setMaxParallel(1)
                        .setSplitScans(Long.getLong("bench.split", 2L))
                        .setBackend(backend)
                        .setExecutorFactory(ec ->
                                new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec))
                        .build();
        context.executor.execute(Algebra.compile(QueryFactory.create(QUERY)),
                (BackendBindings<ID, VALUE> solution) ->
                        counts.merge(solution.getBinding(var).getString(), 1L, Long::sum));
        return counts;
    }
}
