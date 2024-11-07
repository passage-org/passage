package fr.gdd.passage.volcano.iterators;

import com.google.common.collect.Lists;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendValuesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.*;

/**
 * VALUES are important to inject data in the query. They are
 * useful for cases such as binding-restricted triple pattern fragment (brtpf)
 * but also to iterator over services or graphs in a concise manner.
 */
public class PassageValues<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendValuesFactory<ID,VALUE> factory() {
        return (context, input, table) -> {
            if (table.isJoinIdentity())
                return input; // nothing to do


            Backend<ID,VALUE,?> backend = context.getContext().get(BackendConstants.BACKEND);
            BackendCache<ID,VALUE> cache = context.getContext().get(BackendConstants.CACHE);
            return new PassageValuesFactory<>(input, table, backend, cache, context);
        };
    }

    /* ************************ FACTORY OF ITERATOR PER INPUT **************************** */

    public static class PassageValuesFactory<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

        final Iterator<BackendBindings<ID,VALUE>> input;
        final ExecutionContext context;
        final Backend<ID,VALUE,?> backend;
        final BackendCache<ID,VALUE> cache;
        final OpTable values;
        Iterator<BackendBindings<ID,VALUE>> wrapped;

        public PassageValuesFactory (Iterator<BackendBindings<ID,VALUE>> input, OpTable op,
                                        Backend<ID,VALUE,?> backend, BackendCache<ID,VALUE> cache,
                                        ExecutionContext context) {
            this.input = input;
            this.cache = cache;
            this.backend = backend;
            this.context = context;
            this.values = op;
        }

        @Override
        public boolean hasNext() {
            if ((Objects.isNull(wrapped) || !wrapped.hasNext()) && !input.hasNext()) return false;

            if (Objects.nonNull(wrapped) && !wrapped.hasNext()) {
                wrapped = null;
            }

            while (Objects.isNull(wrapped) && input.hasNext()) {
                BackendBindings<ID,VALUE> bindings = input.next();
                wrapped = new PassageValues<>(bindings, values, backend, cache, context);
                if (!wrapped.hasNext()) {
                    wrapped = null;
                }
            }

            return !Objects.isNull(wrapped);
        }

        @Override
        public BackendBindings<ID, VALUE> next() {
            return wrapped.next();
        }
    }

    /* *********************** ACTUAL VALUES ITERATOR **************************** */

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final BackendCache<ID,VALUE> cache;
    final List<BackendBindings<ID,VALUE>> values;
    final BackendBindings<ID,VALUE> current;
    final List<Var> vars;
    final List<Binding> table;

    int index = 0; // current position in the table

    public PassageValues(BackendBindings<ID,VALUE> current, OpTable values,
                         Backend<ID,VALUE,?> backend, BackendCache<ID,VALUE> cache,
                         ExecutionContext context) {
        this.vars = values.getTable().getVars();
        this.table = Lists.newArrayList(values.getTable().rows());
        this.context = context;
        this.backend = backend;
        this.cache = cache;
        this.values = new ArrayList<>();
        this.current = current;
        Pause2Next<ID,VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        saver.register(values, this);

        Iterator<Binding> bindings = values.getTable().rows();
        bindings.forEachRemaining(b -> {
            BackendBindings<ID,VALUE> mappings = new BackendBindings<>();
            b.vars().forEachRemaining(v -> {
                BackendBindings.IdValueBackend<ID,VALUE> newBinding = new BackendBindings.IdValueBackend<ID,VALUE>()
                        .setBackend(backend);
                Node node = b.get(v);
                String val = NodeFmtLib.str(node, null);
                if (Objects.isNull(cache.getId(node))) {
                    // cache it
                    try {
                        ID id = backend.getId(node.toString());
                        cache.register(node, id);
                    } catch (NotFoundException e) {
                        // the ID was not found in the database, so nothing
                        // to cache for now.
                    }
                }
                newBinding.setId(cache.getId(node)); // null if not found
                newBinding.setString(val);
                mappings.put(v, newBinding);
            });
            this.values.add(mappings);
        });
    }

    @Override
    public boolean hasNext() {
        if (values.isEmpty()) { return false; }
        boolean compatible = false;
        while (!compatible && index < values.size()) {
            compatible = current.isCompatible(values.get(index));
            if (!compatible) {
                ++index;
            }
        }
        return compatible;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        // index positioned in hasNext

        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>(
                values.get(index), // copy
                values.get(index).variables().stream().toList())
                .setParent(current);

        index += 1;

        return newBinding;
    }

    public Op pause() {
        if (index >= values.size()) {
            return null; // done everything
        }

        // Encode the current binding in the new operator
        Set<Var> vars = current.variables();
        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(current.getBinding(v).getString())));
        }

        // get the rest of the table
        Table newTable = TableFactory.create(this.vars);
        for (int i = index; i < values.size(); ++i) {
            newTable.addBinding(this.table.get(i));
        }

        //return OpJoin.create(seq, OpTable.create(newTable));
        seq.add(OpTable.create(newTable));

        return seq.size() > 1 ? seq : seq.get(0);
    }

}
