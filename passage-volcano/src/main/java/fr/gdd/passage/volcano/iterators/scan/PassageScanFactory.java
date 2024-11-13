package fr.gdd.passage.volcano.iterators.scan;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendQuadsFactory;
import fr.gdd.passage.commons.factories.IBackendTriplesFactory;
import fr.gdd.passage.commons.generics.*;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

public class PassageScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendTriplesFactory<ID,VALUE> tripleFactory() { return PassageScanFactory::new; }
    public static <ID,VALUE> IBackendQuadsFactory<ID,VALUE> quadFactory() { return PassageScanFactory::new; }

    final ExecutionContext context;
    final Iterator<BackendBindings<ID, VALUE>> input;
    final Op0 tripleOrQuad; // Op0 because can be either a triple pattern, or a quad pattern.
    final Backend<ID, VALUE, Long> backend;
    final Long offset; // optional
    final Long limit; // optional
    final BackendCache<ID,VALUE> cache;
    final BackendSaver<ID,VALUE,Long> saver;

    boolean consumed = true;
    Iterator<BackendBindings<ID, VALUE>> wrapped = Iter.empty();

    public PassageScanFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, Op0 tripleOrQuad) {
        this.context = context;
        this.input = input;
        this.tripleOrQuad = tripleOrQuad;
        this.saver = context.getContext().get(PassageConstants.SAVER);
        this.backend = context.getContext().get(BackendConstants.BACKEND);
        this.cache = context.getContext().get(BackendConstants.CACHE);
        this.offset = context.getContext().get(PassageConstants.OFFSET);
        this.limit = context.getContext().get(PassageConstants.LIMIT);
    }

    @Override
    public boolean hasNext() {
        if (!consumed) { return true; };
        if (!wrapped.hasNext() && !input.hasNext()) { return false; }

        while (!wrapped.hasNext() && input.hasNext()) {
            BackendBindings<ID,VALUE> inputBinding = input.next();

            try{
                wrapped = switch (tripleOrQuad) {
                    case OpTriple opTriple -> {
                        Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), inputBinding, cache);
                        yield new PassageScan<>(context, inputBinding, tripleOrQuad,
                                spo,
                                backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT)),
                                offset, limit);
                    }
                    case OpQuad opQuad -> {
                        Tuple<ID> spoc = Substitutor.substitute(opQuad.getQuad(), inputBinding, cache);
                        yield new PassageScan<>(context, inputBinding, tripleOrQuad,
                                spoc,
                                backend.search(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH)),
                                offset, limit);
                    }
                    default -> throw new UnsupportedOperationException("Operator not handle here: " + tripleOrQuad);
                };
            } catch (NotFoundException | IllegalArgumentException e){
                wrapped = Iter.empty(); // can be an issue if we cast later on
            }
        }

        saver.register(tripleOrQuad, wrapped);
        consumed = !wrapped.hasNext();

        return consumed;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        return wrapped.next();
    }

    public double cardinality() {
        if (this.hasNext()) {
            return ((PassageScan<ID, VALUE>) wrapped).wrapped.cardinality();
        }
        return 0.;
    }

}
