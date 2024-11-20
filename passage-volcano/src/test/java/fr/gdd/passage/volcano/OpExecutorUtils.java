package fr.gdd.passage.volcano;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

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

}