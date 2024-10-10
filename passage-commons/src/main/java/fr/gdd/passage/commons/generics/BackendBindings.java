package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.sparql.core.Var;
import org.eclipse.rdf4j.query.BindingSet;
import org.openrdf.model.Value;

import java.util.*;
import java.util.stream.Collectors;

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
public class BackendBindings<ID, VALUE> implements BindingSet {

    public static class BackendBinding<ID,VALUE> implements org.eclipse.rdf4j.query.Binding {

        final IdValueBackend<ID,VALUE> wrapped;
        final String name;

        public BackendBinding (String name, IdValueBackend<ID,VALUE> wrapped) {
            this.wrapped = wrapped;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public org.eclipse.rdf4j.model.Value getValue() { // in another class otherwise it clashes with IdValueBackend
            // TODO should be IdValueBackend instead of wrapped
            // TODO this should be a prerequisite of <VALUE>
            return new org.eclipse.rdf4j.model.Value() {

                @Override
                public boolean isIRI() {
                    return true;
                }

                @Override
                public String stringValue() {
                    return ((Value) wrapped.getValue()).stringValue();
                }

                @Override
                public String toString() {
                    return stringValue();
                }
            };
        }
    }

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
                value = Objects.isNull(code) ? backend.getValue(id) : backend.getValue(id, code);
            return value;
        }

        public ID getId() {
            if (Objects.isNull(id))
                if (Objects.nonNull(value)) {
                    id = Objects.isNull(code) ? backend.getId(value) : backend.getId(value, code);
                } else {
                    id = Objects.isNull(code) ? backend.getId(asString) : backend.getId(asString, code);
                }
            return id;
        }

        public String getString() {
            /* // TODO everything should go through VALUE or ID, but
            // for now it's too short in time to make such critical change
            if (Objects.isNull(asString)) {
                return getValue().toString();
            }
            return asString;
             */

            if (Objects.isNull(asString)) {
                if (Objects.isNull(value)) {
                    asString = Objects.isNull(code) ? backend.getString(id) : backend.getString(id, code);
                } else {
                    return value.toString();
                }
            }
            return asString;
        }
    }

    final Map<Var, IdValueBackend<ID, VALUE>> var2binding = new HashMap<>();
    BackendBindings<ID, VALUE> parent = null;

    public BackendBindings () {}

    /**
     * Creates a new BackendBinding that copies the one as argument but only keep
     * the designated variables. Useful for projections.
     * @param copy The backend binding to copy from.
     * @param varsToKeep The variables to keep from the backend binding.
     */
    public BackendBindings (BackendBindings<ID,VALUE> copy, List<Var> varsToKeep) {
        for (Var v: varsToKeep) {
            if (copy.contains(v)) {
                this.put(v, copy.get(v));
            }
        }
    }

    public BackendBindings<ID, VALUE> setParent(BackendBindings<ID, VALUE> parent) {
        this.parent = parent;
        return this;
    }

    public BackendBindings<ID, VALUE> put(Var var, IdValueBackend<ID, VALUE> entry) {
        var2binding.put(var, entry);
        return this;
    }

    public BackendBindings<ID, VALUE> setCode(Var var, Integer code) {
        var2binding.get(var).setCode(code);
        return this;
    }

    public BackendBindings<ID, VALUE> put(Var var, ID id, Backend<ID, VALUE, ?> backend) {
        var2binding.put(var, new IdValueBackend<ID, VALUE>().setBackend(backend).setId(id));
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

    /**
     * @return All variables.
     */
    public Set<Var> vars() {
        if (Objects.isNull(parent)) {
            return this.var2binding.keySet();
        }
        Set<Var> result = new HashSet<>(this.var2binding.keySet());
        result.addAll(parent.vars());
        return result;
    }

    @Override
    public String toString() {
        Set<Var> vars = vars();
        StringBuilder builder = new StringBuilder("{");
        for (Var v : vars) {
            builder.append(v.toString()).append("-> ").append(this.get(v).getString()).append(" ; ");
        }
        builder.append("}");
        return builder.toString();
    }

    /**************************  BindingSet interface *****************************/

    @Override
    public Iterator<org.eclipse.rdf4j.query.Binding> iterator() {
        return this.vars().stream().map(v -> this.getBinding(v.getVarName())).iterator();
    }

    @Override
    public Set<String> getBindingNames() {
        return this.vars().stream().map(Var::getVarName).collect(Collectors.toSet());
    }

    @Override
    public org.eclipse.rdf4j.query.Binding getBinding(String bindingName) {
        return new BackendBinding<>(bindingName, this.get(Var.alloc(bindingName)));
    }

    @Override
    public boolean hasBinding(String bindingName) {
        return this.contains(Var.alloc(bindingName));
    }

    @Override
    public org.eclipse.rdf4j.model.Value getValue(String bindingName) {
        return new BackendBinding<>(bindingName, this.get(Var.alloc(bindingName))).getValue();
    }

    @Override
    public int size() {
        return this.vars().size();
    }

    @Override
    public boolean equals(Object obj) {
        if (Objects.isNull(obj)) return false;

        if (obj instanceof BackendBindings<?, ?> other) {
            if (!other.vars().equals(this.vars())) return false;

            // TODO optimize this by comparing on ID, VALUE etc, hence reducing the number of transformation to string
            return obj.toString().equals(this.toString());
        } else {
            return false; // other is not a BackendBinding
        }
    }
}
