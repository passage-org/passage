package fr.gdd.raw.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

import java.util.Objects;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery. This is based on WanderJoin.
 */
public class CountWanderJoin<ID, VALUE> implements BackendAccumulator<ID,VALUE> {

    final ExecutionContext context;
    final Op op;

    double fail = 0.;
    double success = 0.;
    double sampleSize = 0.;
    double sumOfInversedProba = 0.;

    WanderJoin<ID,VALUE> wj;

    public CountWanderJoin(ExecutionContext context, Op subOp) {
        this.context = context;
        this.op = subOp;
        BackendSaver<ID,VALUE,?> saver = context.getContext().get(RawConstants.SAVER);
        this.wj = new WanderJoin<>(saver);
    }

    @Override
    public void merge(BackendAccumulator<ID, VALUE> other) {
        if (Objects.isNull(other)) {return;}
        if (other instanceof CountWanderJoin<ID, VALUE> otherWJ) {
            fail += otherWJ.fail;
            success += otherWJ.success;
            sampleSize += otherWJ.sampleSize;
            sumOfInversedProba += otherWJ.sumOfInversedProba;
        }
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        if (Objects.nonNull(binding)) {
            success += 1;
            sumOfInversedProba += 1. / ReturningOpVisitorRouter.visit(wj, op);
        } else {
            fail += 1;
        }
        sampleSize += 1;
    }

    public void accumulate(double probability) {
        success += 1;
        sumOfInversedProba += 1./ probability;
        sampleSize += 1;
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(RawConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sampleSize == 0. ? 0. : sumOfInversedProba / sampleSize;
    }

    @Override
    public ExecutionContext getContext() {
        return context;
    }

}
