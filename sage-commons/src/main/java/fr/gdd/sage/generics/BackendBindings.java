package fr.gdd.sage.generics;

import fr.gdd.sage.interfaces.Backend;
import org.apache.jena.sparql.core.Var;

import java.util.HashMap;
import java.util.Map;
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
public class BackendBindings<ID, VALUE> {

    /**
     * Contains everything to lazily get a value, or an id
     * @param <ID> The type of identifier.
     * @param <VALUE> The type of value.
     */
    public static class IdValueBackend<ID, VALUE> {
        ID id = null;
        VALUE value = null;
        String asString = null;
        Integer code = null;
        Backend<ID, VALUE, ?> backend = null;

        public IdValueBackend<ID, VALUE> setString(String asString) {
            this.asString = asString;
            return this;
        }

        public IdValueBackend<ID, VALUE> setValue(VALUE value) {
            this.value = value;
            return this;
        }

        public IdValueBackend<ID, VALUE> setId(ID id) {
            this.id = id;
            return this;
        }

        public IdValueBackend<ID, VALUE> setCode(Integer code) {
            this.code = code;
            return this;
        }

        public IdValueBackend<ID, VALUE> setBackend(Backend<ID, VALUE, ?> backend) {
            this.backend = backend;
            return this;
        }

        public VALUE getValue() {
            if (Objects.isNull(value))
                value = backend.getValue(id, code);
            return value;
        }

        public ID getId() {
            if (Objects.isNull(id))
                if (Objects.nonNull(value)) {
                    id = backend.getId(value, code);
                } else {
                    id = backend.getId(asString, code);
                }
            return id;
        }

        public String getString() {
            if (Objects.isNull(value))
                asString = backend.getString(id, code);
            return asString;
        }
    }

    final Map<Var, IdValueBackend<ID, VALUE>> var2binding = new HashMap<>();
    BackendBindings<ID, VALUE> parent = null;

    public BackendBindings<ID, VALUE> setParent(BackendBindings<ID, VALUE> parent) {
        this.parent = parent;
        return this;
    }

    public BackendBindings<ID, VALUE> put(Var var, IdValueBackend<ID, VALUE> entry) {
        var2binding.put(var, entry);
        return this;
    }

    public IdValueBackend<ID, VALUE> get(Var var) {
        if (var2binding.containsKey(var)) {
            return var2binding.get(var);
        }
        if (Objects.isNull(parent)) {
            return null;
        }
        return parent.get(var);
    }

    public boolean contains(Var var) {
        return Objects.nonNull(this.get(var));
    }

}
