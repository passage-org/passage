package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class PassageScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long skip; // offset
    final Backend<ID, VALUE, Long> backend;
    final ExecutionContext context;
    final OpTriple triple; // TODO OpQuad
    final CacheId<ID,VALUE> cache;

    final Iterator<BackendBindings<ID, VALUE>> input;
    BackendBindings<ID, VALUE> inputBinding;

    Iterator<BackendBindings<ID, VALUE>> instantiated = Iter.empty();

    public PassageScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.triple = triple;
        backend = context.getContext().get(PassageConstants.BACKEND);
        this.context = context;
        this.skip = 0L;
        Pause2Next<ID, VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        saver.register(triple, this);
        this.cache = context.getContext().get(PassageConstants.CACHE);
    }

    public PassageScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple, Long skip) {
        this.input = input;
        this.triple = triple;
        backend = context.getContext().get(PassageConstants.BACKEND);
        this.context = context;
        this.skip = skip;
        Pause2Next<ID, VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        saver.register(triple, this);
        this.cache = context.getContext().get(PassageConstants.CACHE);
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            inputBinding = input.next();
            Tuple3<ID> spo = Substitutor.substitute(triple.getTriple(), inputBinding, cache);

            instantiated = new PassageScan<>(context, triple, spo, backend.search(spo.get(0), spo.get(1), spo.get(2)));
            if (Objects.nonNull(skip) && skip > 0L) {
                ((PassageScan<ID,VALUE>) instantiated).skip(skip);
            }
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return instantiated.next().setParent(inputBinding);
    }


    public double cardinality() {
        if (instantiated instanceof PassageScan<ID,VALUE> scan) {
            return scan.cardinality();
        }
        return 0.;
    }

    public long offset() {
        if (instantiated instanceof PassageScan<ID,VALUE> scan) {
            return scan.current();
        }
        return 0L;
    }

    /**
     * @return The Jena operator that summarizes the current state of this scan iterator.
     * It is made of `Bind … As …` to save the state that created this iterator, plus the triple pattern
     * itself unmoved, plus a slice operator that defines an offset.
     * It returns `null` when the wrapped scan iterator does not have a next binding.
     */
    public Op preempt() {
        if (!instantiated.hasNext()) {
            return null;
        }

        Set<Var> vars = inputBinding.vars();
        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(inputBinding.get(v).getString())));
        }
        seq.add(triple);

        Op seqOrSingle = seq.size() > 1 ? seq : seq.get(0);
        return new OpSlice(seqOrSingle, ((PassageScan<ID, VALUE>) instantiated).current(), Long.MIN_VALUE);
    }

}
