package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendValuesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.*;

public class RandomValues<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendValuesFactory<ID,VALUE> factory() {
        return (context, input, table) -> {
            if (table.isJoinIdentity())
                return input; // nothing to do

            return new RandomValues<>(context, input, table);
        };
    }

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final BackendCache<ID,VALUE> cache;
    final List<BackendBindings<ID,VALUE>> values;
    final List<BackendBindings<ID,VALUE>> compatibleValues;

    final List<Var> vars;

    BackendBindings<ID, VALUE> current;

    boolean hasProduced = false;

    public RandomValues(ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> input, OpTable values) {
        this.context = context;
        this.vars = values.getTable().getVars();

        this.backend = this.context.getContext().get(RawConstants.BACKEND);
        this.cache = this.context.getContext().get(RawConstants.CACHE);
        this.values = new ArrayList<>();
        this.compatibleValues = new ArrayList<>();

        if(input.hasNext()) {
            current = input.next();
        }


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
                        ID id = backend.getId(val);
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
            if(mappings.isCompatible(current)) this.compatibleValues.add(mappings); // Inefficient TODO : optimize
        });
    }

    @Override
    public boolean hasNext() {
        // TODO: determine when to stop producing values. In a context of single walk attempt configurations,
        //  producing only one values does just fine but otherwise, this could prove problematic. Maybe the solution is
        //  to use factories like randomScan? Both operators are similar (nullary)

        return !compatibleValues.isEmpty() && !hasProduced;// produces only one value
//        if (!consumed) { return true; }
//        if (values.isEmpty()) { return false; }
//        boolean compatible = false;
//        while (!compatible && index < values.size()) {
//            compatible = current.isCompatible(values.get(index));
//            if (!compatible) {
//                ++index;
//            }
//        }
//        consumed = !compatible;
//        return compatible;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        BackendBindings random = compatibleValues.get((new Random()).nextInt(compatibleValues.size()));

        BackendBindings<ID,VALUE> binding = new BackendBindings<>(
                random, // copy
                random.variables().stream().toList())
                .setParent(current);

//        Double probability = 1.0 / compatibleValues.size();
//
//        // Is it relevant to have a buildValue? or is a build scan enough?
//        JsonObject scanJson = buildScan(binding, probability);
//
//        binding.put(RawConstants.RANDOM_WALK_HOLDER, new BackendBindings.IdValueBackend<ID,VALUE>().setString(stringify(scanJson)));

        hasProduced = true;

        return binding;
    }
}
