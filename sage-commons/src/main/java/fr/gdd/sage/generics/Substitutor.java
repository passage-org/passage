package fr.gdd.sage.generics;

import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.SPOC;
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

    public static <ID, VALUE> Tuple3<ID> substitute(Backend<ID, VALUE, ?> backend, Triple triple, BackendBindings<ID, VALUE> binding) {
        return TupleFactory.create3(substitute(backend, triple.getSubject(), binding, SPOC.SUBJECT),
                substitute(backend, triple.getPredicate(),binding, SPOC.PREDICATE),
                substitute(backend, triple.getObject(), binding, SPOC.OBJECT));
    }

    protected static <ID, VALUE> ID substitute(Backend<ID, VALUE, ?> backend, Node sOrPOrO, BackendBindings<ID, VALUE> binding, Integer spoc) {
        if (sOrPOrO.isVariable()) {
            BackendBindings.IdValueBackend<ID, VALUE> b = binding.get(Var.alloc(sOrPOrO));
            return Objects.isNull(b) ? null : b.getId();
        } else {
            if (sOrPOrO.isURI()) { // ugly… TODO maybe a getId of Node in Backend…
                return backend.getId("<" + sOrPOrO + ">", spoc);
            } else {
                return backend.getId(sOrPOrO.toString(), spoc);
            }
        }
    }

}
