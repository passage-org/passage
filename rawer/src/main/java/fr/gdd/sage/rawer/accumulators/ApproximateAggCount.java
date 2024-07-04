package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

import java.util.Objects;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery. This is based on WanderJoin.
 */
public class ApproximateAggCount<ID, VALUE> implements SagerAccumulator<ID,VALUE> {

    final ExecutionContext context;
    final Op op;

    double sampleSize = 0.;
    double sumOfInversedProba = 0.;

    WanderJoinVisitor<ID,VALUE> wj;

    public ApproximateAggCount(ExecutionContext context, Op subOp) {
        this.context = context;
        this.op = subOp;
        Save2SPARQL<ID,VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        this.wj = new WanderJoinVisitor<>(saver.op2it);
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        if (Objects.nonNull(binding)) {
            sumOfInversedProba += 1. / ReturningOpVisitorRouter.visit(wj, op);
        }
        sampleSize += 1;
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sampleSize == 0. ? 0. : sumOfInversedProba / sampleSize;
    }

}
