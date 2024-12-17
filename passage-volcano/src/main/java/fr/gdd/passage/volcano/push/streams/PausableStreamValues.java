package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.Pause2Continuation;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PausableStreamValues<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final OpTable table;

    Stream<BackendBindings<ID,VALUE>> wrapped;

    final LongAdder produced = new LongAdder();

    public PausableStreamValues(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpTable table) {
        this.table = table;
        this.input = input;
        this.context = context;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        if (table.isJoinIdentity()) { // BIND AS
            this.wrapped = Stream.of(input).peek(ignored -> produced.increment());
        } else { // VALUES
            this.wrapped = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                                    ResultSetFactory.create(table.getTable().iterator(context), table.getTable().getVarNames()),
                                    Spliterator.SIZED | Spliterator.SUBSIZED |
                                            Spliterator.NONNULL | Spliterator.IMMUTABLE),
                             // Spliterator.CONCURRENT), // TODO concurrent spliterator
                            false)
                    .map(r -> new BackendBindings<>(r, context.backend).setParent(input)) // TODO backend in bindingfactory
                    .peek(ignored -> produced.increment());
        }
        return wrapped;
    }

    @Override
    public Op pause() {
        if (table.isJoinIdentity() && produced.longValue() > 0) {
            return Pause2Continuation.DONE;
        } else if (table.isJoinIdentity()) {
            return input.toOp();
        }

        if (produced.longValue() >= table.getTable().size()) {
            return Pause2Continuation.DONE;
        }

        List<Binding> bindings = new ArrayList<>();
        this.table.getTable().rows().forEachRemaining(bindings::addLast);
        bindings = bindings.subList(produced.intValue(), table.getTable().size());

        Table newTable = TableFactory.create(table.getTable().getVars());
        bindings.forEach(newTable::addBinding);

        return input.joinWith(OpTable.create(newTable));
    }
}
