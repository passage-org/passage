package fr.gdd.passage.commons.utils;

import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.core.Var;

import java.util.List;
import java.util.Objects;

public class MultisetResultChecking {
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

            if (Objects.nonNull(values.get(i)) && Objects.isNull(result.getBinding(Var.alloc(vars.get(i))))) {
                return false;
            }

            if (Objects.nonNull(result.getBinding(Var.alloc(vars.get(i)))) &&
                    !result.getBinding(Var.alloc(vars.get(i))).getString().contains(values.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param results The results of the query execution.
     * @param vars The variables to check associated to:
     * @param valuess The list of values corresponding to the variables aforementioned.
     * @return True if the set of results contains the values associated to the variables, false otherwise
     */
    @SafeVarargs
    public static boolean containsAllResults(Multiset<BackendBindings<?,?>> results, List<String> vars, List<String>... valuess) {
        for (List<String> values : valuess) {
            if (!containsResult(results, vars, values)) {
                return false;
            }
        }
        return true;
    }
}
