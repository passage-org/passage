package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.BackjumpException;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
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
public class SpliteratorBackJump<ID,VALUE> implements PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PausableSpliterator<ID,VALUE> wrapped;
    final BackendBindings<ID,VALUE> input;
    final Op op;


    /**
     * @return A root backjumper, It's important when the top most producer fails. TODO double check this
     */
    @Deprecated // TODO double check this
    public static<ID,VALUE> SpliteratorBackJump<ID,VALUE> createRoot(PassageExecutionContext<ID,VALUE> context,
                                                                     PausableSpliterator<ID,VALUE> wrapped) {
        return new SpliteratorBackJump<>(context, new BackendBindings<>(), null, wrapped);
    }

    public SpliteratorBackJump(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op op, PausableSpliterator<ID,VALUE> wrapped) {
        this.context = context;
        this.op = op;
        this.input = input;
        this.wrapped = wrapped; // TODO, probably add a check on creation would be easier, instead of in scan
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        AtomicReference<BackendBindings<ID, VALUE>> produced = new AtomicReference<>();
        boolean advanced = false;

        while (!advanced) {
            try {
                advanced = wrapped.tryAdvance(produced::set);
            } catch (BackjumpException bje) {
                Set<Var> producedVars = getProducedVars(op, input);
                if (producedVars.stream().noneMatch(bje.problematicVariables::contains)) {
                    throw bje;
                }
            }

            if (!advanced) {
                Set<Var> consumed = getConsumedVars(op, input);
                if (!consumed.isEmpty()) {
                    throw new BackjumpException(consumed);
                }
            }

            try {
                action.accept(produced.get());
            } catch (BackjumpException bje) {
                advanced = false;
                Set<Var> producedVars = getProducedVars(op, input);
                if (!bje.matches(producedVars)) {
                    throw bje;
                }
            }
        }
        return true;
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
