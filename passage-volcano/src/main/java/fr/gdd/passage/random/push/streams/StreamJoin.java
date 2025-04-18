package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.core.Var;

import java.util.stream.Stream;

public class StreamJoin<ID,VALUE> implements PausableStream<ID,VALUE> {

    final BackendPushExecutor<ID,VALUE> executor;
    final PassageExecutionContext<ID,VALUE> context;
    final PausableStream<ID,VALUE> wrappedLeft;
    final BackendBindings<ID,VALUE> input;
    final OpJoin op;

    int nbTrials = 0;
    int nbPasses = 0;

    public StreamJoin(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpJoin op) {
        this.context = context;
        this.executor = context.executor;
        this.wrappedLeft = (PausableStream<ID, VALUE>) executor.visit(op.getLeft(), input); // check if could be a problem to inject the input in the subquery
        this.op = op;
        this.input = input;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrappedLeft.stream().map(l -> {
                    // TODO double check where the error is routed
                    //      probably not where it should…
                    nbTrials++;
                    PausableStream<ID, VALUE> wrappedRight = (PausableStream<ID, VALUE>) executor.visit(op.getRight(), l); // may throw BackJumpException
                    return new ImmutablePair<>(wrappedRight, l.get("_probability").getLiteralValue().toString());
                }).flatMap(p ->
                        p.getLeft().stream().map(b ->
                                b.put(Var.alloc("_probability"),
                                        new BackendBindings.IdValueBackend<ID,VALUE>().setString(
                                                String.valueOf(Double.parseDouble(p.getRight())*
                                                        Double.parseDouble(b.get("_probability").getLiteralValue().toString()))))))
                .peek(b -> ++nbPasses);
    }

    @Override
    public Op pause() {
        return null;
    }
}
