package fr.gdd.passage.databases.persistent;

import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.sparql.algebra.Op;

import java.util.Map;

public class DatasetAndQueries<ID,VALUE> {

    public final Backend<ID,VALUE,?> backend;
    public final Map<String, Op> queries; // name -> content
    public final Map<String, Long> truth; // name -> expected number of results

    public DatasetAndQueries(Backend<ID,VALUE,?> backend, Map<String, Op> queries, Map<String, Long> truth) {
        this.backend = backend;
        this.queries = queries;
        this.truth = truth;
    }

}
