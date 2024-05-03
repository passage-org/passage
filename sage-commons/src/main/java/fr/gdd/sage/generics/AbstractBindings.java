package fr.gdd.sage.generics;

import fr.gdd.sage.interfaces.Backend;

import java.util.Objects;

/**
 * Closely related to Jena's `Binding` implementations, or `BindingSet`, etc.
 * Most often, SPARQL engines work with identifiers. These identifiers enable
 * retrieving the associated value, ultimately providing them to end users.
 * <br/>
 * But these bindings are also used by the engine itself. Often, it only
 * needs the identifier which enables efficient computation (e.g. of joins)
 * since it uses its internal indexes. Nevertheless, the engine also needs
 * the actual values sometimes.
 * <br/>
 * To be efficient, abstract bindings should provide (i) a tree structure so
 * children refer to parents instead of copying the parent; (ii) caching so
 * ids or values are retrieved once.
 */
public class AbstractBindings<ID, VALUE> {

    /**
     * Contains everything to lazily get a value, or an id
     * @param <ID> The type of identifier.
     * @param <VALUE> The type of value.
     */
    public static class IdValueBackend<ID, VALUE> {
        ID id = null;
        VALUE value = null;
        Backend<ID, ?> backend = null;

        public VALUE getValue() {
            throw new UnsupportedOperationException("TODO");
        }

        public ID getId() {
            throw new UnsupportedOperationException("TODO");
            // return (Objects.isNull(id)) ? backend.getId()
        }
    }


}
