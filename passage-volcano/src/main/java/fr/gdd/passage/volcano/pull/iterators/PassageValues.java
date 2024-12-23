package fr.gdd.passage.volcano.pull.iterators;

import com.google.common.collect.Lists;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendValuesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * VALUES are important to inject data in the query. They are
 * useful for cases such as binding-restricted triple pattern fragment (brtpf)
 * but also to iterator over services or graphs in a concise manner.
 */
public class PassageValues<ID,VALUE> extends PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendValuesFactory<ID,VALUE> factory() {
        return (context, input, table) -> {
            if (table.isJoinIdentity())
                return input; // nothing to do

            return new BackendIteratorOverInput<>(context, input, table, PassageValues::new);
        };
    }

    final PassageExecutionContext<ID,VALUE> context;
    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final List<BackendBindings<ID,VALUE>> values;
    final BackendBindings<ID,VALUE> current;
    final List<Var> vars;
    final List<Binding> table;

    int index = 0; // current position in the table
    boolean consumed = true;

    public PassageValues(ExecutionContext context, BackendBindings<ID,VALUE> input, OpTable values) {
        super((PassageExecutionContext<ID, VALUE>) context, values);
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.vars = values.getTable().getVars();
        this.table = Lists.newArrayList(values.getTable().rows());

        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.values = new ArrayList<>();
        this.current = input;

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
        if (!consumed) { return true; }
        if (values.isEmpty()) { return false; }
        boolean compatible = false;
        while (!compatible && index < values.size()) {
            compatible = current.isCompatible(values.get(index));
            if (!compatible) {
                ++index;
            }
        }
        consumed = !compatible;
        return compatible;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        // index positioned in hasNext
        consumed = true;

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

        // get the rest of the table
        Table newTable = TableFactory.create(this.vars);
        for (int i = index; i < values.size(); ++i) {
            newTable.addBinding(this.table.get(i));
        }

        return OpJoin.create(current.asBindAs(), OpTable.create(newTable));
    }

}
