package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;

/**
 * Iterators that are designed to be put on hold, returning a SPARQL
 * query that will produce the missing results of the paused query.
 */
public abstract class PausableSpliterator<ID,VALUE> {

    private final PassageExecutionContext<ID,VALUE> context;
    private final Op op;

    public PausableSpliterator(PassageExecutionContext<ID,VALUE> context, Op op) {
        context.op2its.register(op, this);
        this.context = context;
        this.op = op;
    }

    public void unregister() {
        context.op2its.unregister(op, this);
    }

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    public Op pause() { throw new UnsupportedOperationException("pause"); }

}
