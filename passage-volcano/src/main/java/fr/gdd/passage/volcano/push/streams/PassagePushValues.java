package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PassagePushValues<ID,VALUE> extends PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final BackendBindings<ID,VALUE> input;
    final OpTable values;

    final LongAdder produced = new LongAdder();

    public PassagePushValues(ExecutionContext context, BackendBindings<ID,VALUE> input, OpTable table) {
        super((PassageExecutionContext<ID, VALUE>) context, table);
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.values = table;
        this.input = input;

        this.wrapped = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                ResultSetFactory.create(table.getTable().iterator(context), table.getTable().getVarNames()) ,
                                Spliterator.SIZED | Spliterator.SUBSIZED |
                                        Spliterator.NONNULL | Spliterator.IMMUTABLE |
                                        Spliterator.CONCURRENT),
                        this.context.maxParallelism > 1)
                .map(r -> new BackendBindings<>(r, this.context.backend).setParent(input)) // TODO backend in bindingfactory
                .peek(ignored -> produced.increment());
    }

    public Stream<BackendBindings<ID,VALUE>> getStream() {
        return wrapped;
    }

    @Override
    public Op pause() {
        if (produced.longValue() >= values.getTable().size()) {
            return null; // done everything
        }

        List<Binding> bindings = new ArrayList<>();
        this.values.getTable().rows().forEachRemaining(bindings::addLast);
        bindings = bindings.subList(produced.intValue(), values.getTable().size());

        Table newTable = TableFactory.create(values.getTable().getVars());
        bindings.forEach(newTable::addBinding);

        return OpJoin.create(input.toOp(), OpTable.create(newTable));
    }
}
