package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

import java.util.Objects;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery.
 */
public class ApproximateAggCount<ID, VALUE> implements SagerAccumulator<ID,VALUE> {

    final ExecutionContext context;
    final Op op;

    Double numberOfRWs = 0.;
    Double sumOfInversedProba = 0.;

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
        numberOfRWs += 1;
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^xsd:double", getValueAsDouble()));
    }

    public double getValueAsDouble () {
        return sumOfInversedProba == 0. ? 0. : sumOfInversedProba /numberOfRWs;
    }

}
