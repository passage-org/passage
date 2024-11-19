package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple4;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * A scan executes only once, in random settings.
 */
public class RandomScan<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    boolean consumed = false;
    Double currentProbability;

    final ExecutionContext context;
    final Op0 tripleOrQuad;
    final BackendIterator<ID, VALUE, ?> iterator;
    final Backend<ID, VALUE, ?> backend;
    final Tuple<Var> vars;

    public RandomScan(ExecutionContext context, Op0 opTripleOrQuad, Tuple<ID> spo) {
        this.backend = context.getContext().get(RawConstants.BACKEND);
        this.tripleOrQuad = opTripleOrQuad;
        this.context = context;

        switch (opTripleOrQuad) {
            case OpTriple opTriple -> {
                this.iterator = backend.search(spo.get(0), spo.get(1), spo.get(2));
                this.vars = TupleFactory.create3(
                        opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(0)) ? Var.alloc(opTriple.getTriple().getSubject()) : null,
                        opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(1)) ? Var.alloc(opTriple.getTriple().getPredicate()) : null,
                        opTriple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(2)) ? Var.alloc(opTriple.getTriple().getObject()) : null);
            }
            case OpQuad opQuad -> {
                this.iterator = backend.search(spo.get(0), spo.get(1), spo.get(2), spo.get(3));
                this.vars = TupleFactory.create4(
                        opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spo.get(0)) ? Var.alloc(opQuad.getQuad().getSubject()) : null,
                        opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spo.get(1)) ? Var.alloc(opQuad.getQuad().getPredicate()) : null,
                        opQuad.getQuad().getObject().isVariable() && Objects.isNull(spo.get(2)) ? Var.alloc(opQuad.getQuad().getObject()) : null,
                        opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spo.get(3)) ? Var.alloc(opQuad.getQuad().getGraph()) : null);
            }

            default -> throw new UnsupportedOperationException("Operator unknown: " + opTripleOrQuad);
        }



        BackendSaver<ID,VALUE,?> saver = this.context.getContext().get(RawConstants.SAVER);
        if (Objects.nonNull(saver)) {
            saver.register(opTripleOrQuad, this);
        }
    }

    @Override
    public boolean hasNext() {
        return !consumed && iterator.hasNext(); // at least 1 element, only called once anyway
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        this.currentProbability = iterator.random(); // position at random index
        iterator.next(); // read the value

        RawConstants.incrementScans(context);

        BackendBindings<ID, VALUE> newBinding = new BackendBindings<>();

        if (Objects.nonNull(vars.get(0))) { // ugly x3
            newBinding.put(vars.get(0), iterator.getId(SPOC.SUBJECT), backend).setCode(vars.get(0), SPOC.SUBJECT);
        }
        if (Objects.nonNull(vars.get(1))) {
            newBinding.put(vars.get(1), iterator.getId(SPOC.PREDICATE), backend).setCode(vars.get(1), SPOC.PREDICATE);
        }
        if (Objects.nonNull(vars.get(2))) {
            newBinding.put(vars.get(2), iterator.getId(SPOC.OBJECT), backend).setCode(vars.get(2), SPOC.OBJECT);
        }

        // unsure if this is the right way to do it, but without this check it throws an error for triples
        if(vars instanceof Tuple4<Var> && Objects.nonNull(vars.get(3))) {
            newBinding.put(vars.get(3), iterator.getId(SPOC.GRAPH), backend).setCode(vars.get(3), SPOC.GRAPH);
        }

        return newBinding;
    }

    public double getProbability() {
        return currentProbability;
    }

    public double cardinality() {return iterator.cardinality();}
}
