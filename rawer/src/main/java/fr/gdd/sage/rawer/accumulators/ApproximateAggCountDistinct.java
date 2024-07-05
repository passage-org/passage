package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.subqueries.CountSubqueryBuilder;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.function.FunctionEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery.
 */
public class ApproximateAggCountDistinct<ID,VALUE> implements SagerAccumulator<ID, VALUE> {

    private final static Logger log = LoggerFactory.getLogger(ApproximateAggCountDistinct.class);

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final OpGroup group;

    final ApproximateAggCount<ID,VALUE> bigN;
    Double sumOfInversedProba = 0.;
    Double sumOfInversedProbaOverFmu = 0.;
    final WanderJoin<ID,VALUE> wj;

    final Set<Var> vars;
    long sampleSize = 0; // for debug purposes
    long nbZeroFmu = 0; // for debug purposes TODO eventually, this should not exist with bootstrapping

    // TODO /!\ This is ugly. There should be a better way to devise
    // TODO a budget defined by a configuration, or adaptive, or etc.
    // TODO should check that one at least is set.
    public static long SUBQUERY_LIMIT = Long.MAX_VALUE;
    public static long SUBQUERY_TIMEOUT = Long.MAX_VALUE;

    public ApproximateAggCountDistinct(ExprList varsAsExpr, ExecutionContext context, OpGroup group) {
        this.context = context;
        this.backend = context.getContext().get(RawerConstants.BACKEND);
        this.group = group;
        this.bigN = new ApproximateAggCount<>(context, group.getSubOp());
        Save2SPARQL<ID,VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        this.wj = new WanderJoin<>(saver.op2it);
        this.vars = varsAsExpr.getVarsMentioned();

    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        // #1 processing of N
        this.bigN.accumulate(binding, functionEnv);

        if (Objects.isNull(binding)) {return;}
        sampleSize += 1; // only account for those which succeed

        // #2 processing of P_mu
        double proba = ReturningOpVisitorRouter.visit(wj, group.getSubOp());
        double inversedProba = proba == 0. ? 0. : 1./proba;
        sumOfInversedProba += inversedProba;

        // #3 processing of F_mu
        // #A bind the variable with their respective value
        CountSubqueryBuilder<ID,VALUE> subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, vars);
        Op countQuery = subqueryBuilder.build(group.getSubOp());

        RawerOpExecutor<ID,VALUE> fmuExecutor = new RawerOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(SUBQUERY_LIMIT)
                .setTimeout(SUBQUERY_TIMEOUT)
                .setCache(subqueryBuilder.getCache());

        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // no time to execute maybe ?
            // TODO handle this case when the budget is not enough to even get a 0.
            throw new UnsupportedOperationException("TODO need to look at this exception");
        }
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        long nbScansSubQuery = fmuExecutor.getExecutionContext().getContext().get(RawerConstants.SCANS);
        context.getContext().set(RawerConstants.SCANS,
                context.getContext().getLong(RawerConstants.SCANS,0L)
                        + nbScansSubQuery);

        // #D TODO ugly but need to be parsed to Double again…
        String fmuAsString = estimatedFmu.get(subqueryBuilder.getResultVar()).getString();

        // Remove the datatype part and quotes from the string to get the numeric value
        String valueString = fmuAsString.substring(2, fmuAsString.indexOf("\"^^")); // TODO unuglify all this...
        // Create a Literal with the given value and the full xsd:double URI
        Literal literal = ResourceFactory.createTypedLiteral(valueString, TypeMapper.getInstance().getSafeTypeByName("http://www.w3.org/2001/XMLSchema#double"));
        double fmuAsDouble = literal.getDouble();

//        // (for debugging TODO remove this)
//        String countQueryAsString = OpAsQuery.asQuery(countQuery).toString();
//        long actualFMU = -1;
//        try {
//            actualFMU = ((BlazegraphBackend) fmuExecutor.getBackend()).countQuery(countQueryAsString);
//        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (actualFMU != fmuAsDouble) {
//            System.out.println(binding);
//            System.out.println(actualFMU + "     VS     " + fmuAsDouble);
//        }

        // Math.max because we know for sure that there is at least one result,
        // but we failed to find it in the subquery.
        // TODO bootstrap the count with binding that we found in the count distinct part.
        if (fmuAsDouble == 0.) {
            nbZeroFmu += 1;
            log.warn(binding.toString());
        } // for debug purpose
        sumOfInversedProbaOverFmu += inversedProba / Math.max(1., fmuAsDouble);

    }

    @Override
    public VALUE getValue() {
        log.debug("BigN SampleSize: " + bigN.sampleSize);
        log.debug("CRAWD SampleSize: " + sampleSize);
        log.debug("Nb Total Scans: " + context.getContext().get(RawerConstants.SCANS));
        log.debug("Nb zeros in Fmu: " + nbZeroFmu);
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sumOfInversedProba == 0. ? 0. : (bigN.getValueAsDouble()/sumOfInversedProba) * sumOfInversedProbaOverFmu;
    }

}
