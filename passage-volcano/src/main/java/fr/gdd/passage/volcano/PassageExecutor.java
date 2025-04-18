package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;

import java.util.function.Consumer;

/**
 * Generic interface of Passage engines. Most importantly, executing a SPARQL query
 * may return another SPARQL query to retrieve missing results. It returns null if
 * the query execution is fully over.
 */
public interface PassageExecutor<ID,VALUE>  {

    Op execute(Op query, Consumer<BackendBindings<ID,VALUE>> consumer);
    Op execute(String query, Consumer<BackendBindings<ID,VALUE>> consumer);

    default double estimateCost(Op query) {throw new UnsupportedOperationException("Cannot estimate query cost.");}
    default double estimateCardinality(Op query) {throw new UnsupportedOperationException("Cannot estimate query cardinality.");}

}
