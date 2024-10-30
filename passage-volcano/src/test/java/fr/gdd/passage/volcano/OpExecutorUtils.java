package fr.gdd.passage.volcano;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class OpExecutorUtils {

    private static final Logger log = LoggerFactory.getLogger(OpExecutorUtils.class);

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, Backend<?,?,?> backend) {
        return executeWithPassage(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, PassageExecutionContext ec) {
        PassageOpExecutor<?,?> executor = new PassageOpExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.execute(query);

        int sum = 0;
        Multiset<BackendBindings<?,?>> bindings = HashMultiset.create();
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            bindings.add(binding);
            log.debug("{}: {}", sum, binding.toString());
            sum += 1;
        }
        return bindings;
    }

    /**
     * @param results The results of the query execution.
     * @param vars The variables to check associated to:
     * @param values The values corresponding to the variables aforementioned.
     * @return True if the set of results contains the values associated to the variables, false otherwise
     */
    public static boolean containsResult(Multiset<BackendBindings<?,?>> results, List<String> vars, List<String> values) {
        return results.stream().anyMatch(
                result -> matchVarsValues(result, vars, values));
    }

    /**
     * @param results The results of the query execution.
     * @param vars The variables to check associated to:
     * @param values The values corresponding to the variables aforementioned.
     * @param times The number of times the value must appear.
     * @return True if the set of results contains the values associated to the variables a certain number of times,
     *         false otherwise
     */
    public static boolean containsResultTimes(Multiset<BackendBindings<?,?>> results, List<String> vars, List<String> values, Integer times) {
        return results.stream().filter(result -> matchVarsValues(result, vars, values)).toList().size() == times;
    }

    /**
     * @param result A solutions mapping of the query execution.
     * @param vars The variables to check associated to:
     * @param values The values corresponding to the variables aforementioned.
     * @return True if the result contains the variables with associated values, false otherwise.
     */
    public static boolean matchVarsValues (BackendBindings<?,?> result, List<String> vars, List<String> values) {
        for (int i = 0; i < vars.size(); ++i) {
            if (Objects.isNull(values.get(i))) {
                // check if a variable does not have a value
                if (Objects.nonNull(result.getBinding(Var.alloc(vars.get(i))))) {
                    return false;
                }
            }

            if (Objects.nonNull(result.getBinding(Var.alloc(vars.get(i)))) &&
                    !result.getBinding(Var.alloc(vars.get(i))).getString().contains(values.get(i))) {
                return false;
            }
        }
        return true;
    };

    /**
     * @param results The results of the query execution.
     * @param vars The variables to check associated to:
     * @param valuess The list of values corresponding to the variables aforementioned.
     * @return True if the set of results contains the values associated to the variables, false otherwise
     */
    @SafeVarargs
    public static boolean containsAllResults(Multiset<BackendBindings<?,?>> results, List<String> vars, List<String>... valuess) {
        for (List<String> strings : valuess) {
            if (!containsResult(results, vars, strings)) {
                return false;
            }
        }
        return true;
    }
}