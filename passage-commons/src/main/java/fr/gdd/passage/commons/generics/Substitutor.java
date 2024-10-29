package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.SPOC;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;

import java.util.Objects;

/**
 * From Backend, Node, Triple, creates a Tuple of ID.
 */
public class Substitutor {

    public static <ID, VALUE> Tuple3<ID> substitute(Triple triple, BackendBindings<ID, VALUE> binding, BackendCache<ID,VALUE> cache) {
        return TupleFactory.create3(substitute(triple.getSubject(), binding, SPOC.SUBJECT, cache),
                substitute(triple.getPredicate(),binding, SPOC.PREDICATE, cache),
                substitute(triple.getObject(), binding, SPOC.OBJECT, cache));
    }

    public static <ID, VALUE> ID substitute(Node sOrPOrO, BackendBindings<ID, VALUE> binding, Integer spoc, BackendCache<ID,VALUE> cache) {
        if (sOrPOrO.isVariable()) {
            BackendBindings.IdValueBackend<ID, VALUE> b = binding.getBinding(Var.alloc(sOrPOrO));
            return Objects.isNull(b) ? null : b.getId();
        } else {
            return cache.getId(sOrPOrO, spoc);
        }
    }

}
