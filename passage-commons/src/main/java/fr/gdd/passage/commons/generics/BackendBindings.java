package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.*;
import java.util.function.BiConsumer;
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
public class BackendBindings<ID, VALUE> implements Binding {

    @Override
    public int hashCode() {
        return Objects.hash(variables().stream()
                .map(v-> v.getVarName() + " -> " + getBinding(v).getString())
                .collect(Collectors.joining()));
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
        Backend<ID, VALUE> backend = null;

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

        public IdValueBackend<ID, VALUE> setBackend(Backend<ID, VALUE> backend) {
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

    /* ****************************** BINDINGS ************************************ */

    final Map<Var, IdValueBackend<ID, VALUE>> var2binding = new HashMap<>();
    BackendBindings<ID, VALUE> parent = null;

    public BackendBindings () {}

    public BackendBindings (QuerySolution solution, Backend<ID,VALUE> backend) {
        solution.varNames().forEachRemaining(v -> put(Var.alloc(v),
                new IdValueBackend<ID,VALUE>()
                        .setBackend(backend)
                        .setString(NodeFmtLib.str(solution.get(v).asNode(), null)))); // TODO context
    }

    /**
     * Creates a new BackendBinding that copies the one as argument but only keep
     * the designated variables. Useful for projections.
     * @param toCopy The backend binding to copy from.
     * @param varsToKeep The variables to keep from the backend binding.
     */
    public BackendBindings (BackendBindings<ID,VALUE> toCopy, List<Var> varsToKeep) {
        for (Var v: varsToKeep) {
            if (toCopy.contains(v)) {
                this.put(v, toCopy.getBinding(v));
            }
        }
        this.parent = null; // self-contained
    }

    public BackendBindings (BackendBindings<ID,VALUE> toCopy) {
        for (Var v: toCopy.variables()) {
            this.put(v, toCopy.getBinding(v));
        }
        this.parent = null; // self-contained
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

    public BackendBindings<ID, VALUE> put(Var var, ID id, Backend<ID, VALUE> backend) {
        var2binding.put(var, new IdValueBackend<ID, VALUE>().setBackend(backend).setId(id));
        return this;
    }

    public IdValueBackend<ID, VALUE> getBinding(Var var) {
        if (var2binding.containsKey(var)) {
            return var2binding.get(var);
        }
        if (Objects.isNull(parent)) {
            return null;
        }
        return parent.getBinding(var);
    }

    public Set<Var> variables() {
        if (Objects.isNull(parent)) {
            return this.var2binding.keySet();
        }
        Set<Var> result = new HashSet<>(this.var2binding.keySet());
        result.addAll(parent.variables());
        return result;
    }

    /* **************************** JENA BINDING INTERFACE *************************** */

    @Override
    public int size() {
        return var2binding.size();
    }

    @Override
    public boolean isEmpty() {
        return var2binding.isEmpty();
    }

    public boolean contains(Var var) {
        return Objects.nonNull(this.getBinding(var));
    }

    @Override
    public boolean contains(String varName) {
        return this.contains(Var.alloc(varName));
    }

    @Override
    public Node get(String varName) { // TODO cache this probably, put it in the specific binding?
        return NodeValue.parse(getBinding(Var.alloc(varName)).getString()).asNode();
    }

    @Override
    public Node get(Var var) { // TODO cache this probably, put it in the specific binding?
        return NodeValue.parse(getBinding(var).getString()).asNode();
    }

    @Override
    public Iterator<Var> vars() {
        return this.variables().iterator();
    }

    @Override
    public Set<Var> varsMentioned() {
        return this.variables();
    }

    @Override
    public void forEach(BiConsumer<Var, Node> action) {
        this.variables().forEach(v -> action.accept(v, get(v)));
    }

    /* ******************************** UTILITY ************************************* */

    @Override
    public String toString() {
        Set<Var> vars = variables();
        StringBuilder builder = new StringBuilder("{");
        for (Var v : vars) {
            builder.append(v.toString()).append("-> ").append(this.getBinding(v).getString()).append(" ; ");
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (Objects.isNull(obj)) return false;

        if (obj instanceof BackendBindings<?, ?> other) {
            if (!other.variables().equals(this.variables())) return false;

            // TODO optimize this by comparing on ID, VALUE etc, hence reducing the number of transformation to string
            return obj.toString().equals(this.toString());
        } else {
            return false; // other is not a BackendBinding
        }
    }

    /**
     * Check if the two bindings are compatible, i.e., if for every variable,
     * they either do not have a value, or have the same.
     * @param other The other binding to check the compatibility with.
     * @return True if they are compatible, false otherwise.
     */
    public boolean isCompatible (BackendBindings<ID,VALUE> other) {
        for (Var v : variables()) {
            IdValueBackend<ID,VALUE> valThis = this.getBinding(v);
            IdValueBackend<ID,VALUE> valOther = other.getBinding(v);
            if (Objects.nonNull(valOther) && Objects.nonNull(valThis)) {
                if (!valOther.getString().equals(valThis.getString())) { // compared as string
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @return a SPARQL pattern that represents the state of this binding.
     */
    public Op toOp() {
        Set<Var> vars = this.variables();

        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(this.getBinding(v).getString())));
        }

        return switch (seq.size()) {
            case 0 -> {
                seq.add(OpTable.unit());
                yield seq;
            }
            case 1 -> seq.get(0);
            default -> seq;
        };
    }
}
