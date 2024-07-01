package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.*;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery.
 */
public class ApproximateAggCountDistinct<ID,VALUE> implements SagerAccumulator<ID, VALUE> {

    final ExecutionContext context;
    final OpGroup group;

    Double sumOfInversedProba = 0.;
    Double sumOfInversedProbaOverFmu = 0.;

    WanderJoinVisitor<ID,VALUE> wj;

    ApproximateAggCount<ID,VALUE> bigN;

    final Set<Var> vars;


    public ApproximateAggCountDistinct(RawerOpExecutor<ID,VALUE> executor, ExprList varsAsExpr, ExecutionContext context, OpGroup group) {
        this.context = context;
        this.group = group;
        this.bigN = new ApproximateAggCount<>(context, group.getSubOp());
        Save2SPARQL<ID,VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        this.wj = new WanderJoinVisitor<>(saver.op2it);
        this.vars = varsAsExpr.getVarsMentioned();

    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        // #1 processing of N
        this.bigN.accumulate(binding, functionEnv);

        if (Objects.isNull(binding)) {return;}

        // #2 processing of P_mu
        double inversedProba = 1./ReturningOpVisitorRouter.visit(wj, group.getSubOp());
        sumOfInversedProba += inversedProba;

        // #3 processing of F_mu
        // #A bind the variable with their respective value
        Op right = group.getSubOp();
        for (Var v : vars) {
            right = OpJoin.create(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(binding.get(v).getString())),
            right);
        }

        // #B wrap as a COUNT query
        OpGroup countQuery = new OpGroup(right, new VarExprList(),
                List.of(new ExprAggregator(Var.alloc("count"), new AggCount())));

        ExecutionContext newExecutionContext = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        newExecutionContext.getContext().set(RawerConstants.BACKEND, context.getContext().get(RawerConstants.BACKEND));
        RawerOpExecutor<ID,VALUE> fmuExecutor = new RawerOpExecutor<>(newExecutionContext);
        fmuExecutor.setLimit(1L);

        // #C TODO optimize the join order of countQuery
        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // no time to execute maybe ?
            throw new UnsupportedOperationException("TODO need to look at this exception");
        }
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        // #D TODO ugly but need to be parsed to Double again…
        String fmu = estimatedFmu.get(Var.alloc("count")).getString();

        // Remove the datatype part and quotes from the string to get the numeric value
        String valueString = fmu.substring(2, fmu.indexOf("\"^^")); // TODO unuglify all this...

        // Create a Literal with the given value and the full xsd:double URI
        Literal literal = ResourceFactory.createTypedLiteral(valueString, TypeMapper.getInstance().getSafeTypeByName("http://www.w3.org/2001/XMLSchema#double"));
        Double doubleValue = literal.getDouble();
        sumOfInversedProbaOverFmu += inversedProba / doubleValue;

    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sumOfInversedProba == 0. ? 0. : bigN.getValueAsDouble()/sumOfInversedProba * sumOfInversedProbaOverFmu;
    }

}
