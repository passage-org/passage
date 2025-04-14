package fr.gdd.passage.volcano.federation;

import com.google.common.collect.Multiset;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.Map;

public interface ILocalService {

    /**
     * Execute the service using the:
     * @param query as SPARQL query, and:
     * @param args as additional arguments to be used by the service if need be.
     * @return The resulting bindings along with optional metadata.
     */
    Pair<Multiset<Binding>, Map<String, ?>> query(Op query, Map<String, ?> args) throws RuntimeException;

}
