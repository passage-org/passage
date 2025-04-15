package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.BackjumpException;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Base iterator for back jumping. I.e. assuming a nested loop join of more-than-2 iterators
 * when the last iterator creates a value that conflicts with a value created on topper iterators,
 * then it throws instead of enumerating other wrong results.
 * *
 * The topper iterator that produced the conflicting value is in charge of catching the exception
 * and iterate.
 * *
 * This implementation aims at providing a generic base that other operator-specialized iterators
 * can use.
 */
public class SpliteratorProducerBackJump<ID,VALUE,OP extends Op> implements PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PausableSpliterator<ID,VALUE> wrapped;
    final BackendBindings<ID,VALUE> input;
    final Op op;

    public SpliteratorProducerBackJump(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OP op,
                                       TriFunction<PassageExecutionContext<ID,VALUE>, BackendBindings<ID,VALUE>, OP,
            PausableSpliterator<ID,VALUE>> supplier) {
        this.context = context;
        this.op = op;
        this.input = input;

        this.wrapped = supplier.apply(context, input, op); // we try to produce a value if it's a producer.

        // an estimated cardinality of 0 should be 0 for sure, otherwise it misses possible elements
        if (this.estimateCardinality() == 0) {
            throw new BackjumpException(op, input);
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        try {
            return wrapped.tryAdvance(action);
        } catch (BackjumpException bje) {
            Set<Var> producedVars = getProducedVars(op, input); // TODO lazy
            if (producedVars.stream().noneMatch(bje.problematicVariables::contains)) {
                throw bje;
            }
        }
        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        return wrapped.trySplit();
    }

    @Override
    public long estimateSize() {
        return wrapped.estimateSize();
    }

    @Override
    public int characteristics() {
        return wrapped.characteristics();
    }

    @Override
    public Op pause() {
        return wrapped.pause();
    }

    /* ****************************** UTILS ********************************* */

    public static <ID,VALUE> Set<Var> getProducedVars(Op op, BackendBindings<ID,VALUE> input) {
        return switch (op) {
            case OpTriple t -> {
                Set<Var> vars = OpVars.visibleVars(t);
                vars.removeAll(input.variables());
                yield vars;
            }
            default -> new HashSet<>();
        };
    }

    public static <ID,VALUE> Set<Var> getConsumedVars(Op op, BackendBindings<ID,VALUE> input) {
        return switch (op) {
            case OpTriple t -> {
                Set<Var> vars = OpVars.visibleVars(t);
                vars.retainAll(input.variables());
                yield vars;
            }
            default -> new HashSet<>();
        };
    }

}
