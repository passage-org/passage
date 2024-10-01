package fr.gdd.passage.volcano.iterators;

import com.google.common.collect.Lists;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.NodeUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * VALUES are important to inject data in the query. They are
 * useful for cases such as binding-restricted triple pattern fragment (brtpf)
 * but also to iterator over services or graphs in a concise manner.
 */
public class PassageValues<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final CacheId<ID,VALUE> cache;
    final List<BackendBindings<ID,VALUE>> values;
    BackendBindings<ID,VALUE> current;
    final List<Var> vars;
    final List<Binding> table;

    int index = 0; // current position in the table

    public PassageValues(Iterator<BackendBindings<ID,VALUE>> input, OpTable op,
                         Backend<ID,VALUE,?> backend, CacheId<ID,VALUE> cache,
                         ExecutionContext context) {
        this.vars = op.getTable().getVars();
        this.table = Lists.newArrayList(op.getTable().rows());
        this.context = context;
        this.input = input;
        this.backend = backend;
        this.cache = cache;
        this.values = new ArrayList<>();

        Iterator<Binding> bindings = op.getTable().rows();
        bindings.forEachRemaining(b -> {
            BackendBindings<ID,VALUE> mappings = new BackendBindings<>();
            b.vars().forEachRemaining(v -> {
                BackendBindings.IdValueBackend<ID,VALUE> newBinding = new BackendBindings.IdValueBackend<ID,VALUE>()
                        .setBackend(backend);
                Node node = b.get(v);
                String val = NodeFmtLib.str(node, null);
                if (Objects.isNull(cache.getId(node))) {
                    // cache it
                    ID id = backend.getId(node.toString());
                    cache.register(node, id);
                }
                newBinding.setId(cache.getId(node));
                newBinding.setString(val);
                mappings.put(v, newBinding);
            });
            this.values.add(mappings);
        });
    }

    @Override
    public boolean hasNext() {
        return input.hasNext() || index < values.size();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        if (index >= values.size()) {
            index = 0;
        }
        if (index == 0) {
            current = input.next();
        }
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>(
                values.get(index), // copy
                values.get(index).vars().stream().toList())
                .setParent(current);

        index += 1;

        return newBinding;
    }

    public OpTable pause() {
        if (index >= values.size()) {
            return null; // done everything
        }
        Table newTable = TableFactory.create(this.vars);
        for (int i = index; i < values.size(); ++i) {
            newTable.addBinding(this.table.get(i));
        }
        return OpTable.create(newTable);
    }

}
