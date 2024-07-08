package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.iterators.RawerAgg;
import fr.gdd.sage.rawer.subqueries.CountSubqueryBuilder;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import fr.gdd.sage.sager.pause.Triples2BGP;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.jena.datatypes.xsd.XSDDatatype;
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

    public static Logger log = LoggerFactory.getLogger(ApproximateAggCountDistinct.class);

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final OpGroup group;
    final CacheId<ID,VALUE> cache;

    final ApproximateAggCount<ID,VALUE> bigN;
    Double sumOfInversedProbabilities = 0.;
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
        this.cache = context.getContext().get(RawerConstants.CACHE);
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        if (Objects.isNull(binding)) {
            // #1 processing of N
            this.bigN.accumulate(null, functionEnv); // still register the failure for bigN
            return;
        }

        // #2 processing of Fµ
        CountSubqueryBuilder<ID,VALUE> subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, vars);
        Op countQuery = subqueryBuilder.build(group.getSubOp());
        // need same join order to bootstrap
        countQuery = ReturningOpVisitorRouter.visit(new Triples2BGP(), countQuery);
        countQuery = new CardinalityJoinOrdering<>(backend, cache).visit(countQuery); // need to have bgp to optimize, no tps
        countQuery = ReturningOpVisitorRouter.visit(new BGP2Triples(), countQuery);

        RawerOpExecutor<ID,VALUE> fmuExecutor = new RawerOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(SUBQUERY_LIMIT)
                .setTimeout(SUBQUERY_TIMEOUT)
                .setCache(subqueryBuilder.getCache());

        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // Might happen when stopping conditions trigger immediatly, e.g. not
            // enough execution time left, or not enough scan left.
            return ; // do nothing, we don't even want to account the newly found value as 0.
        }
        // therefore, only then, we modify the inner state of this ApproximateAggCountDistinct

        this.bigN.accumulate(binding, functionEnv); // register the success for bigN
        sampleSize += 1; // only account for those which succeed (debug purpose)

        // #2 processing of Pµ
        double probability = ReturningOpVisitorRouter.visit(wj, group.getSubOp());
        double inversedProbability = probability == 0. ? 0. : 1./probability;
        sumOfInversedProbabilities += inversedProbability;

        // don't do anything with the value, but still need to create it.
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        long nbScansSubQuery = fmuExecutor.getExecutionContext().getContext().get(RawerConstants.SCANS);
        context.getContext().set(RawerConstants.SCANS,
                context.getContext().getLong(RawerConstants.SCANS,0L)
                        + nbScansSubQuery);

        // #3 get the aggregate and boostrap it with the value found in distinct sample: µ
        FmuBootstrapper<ID,VALUE> bootsrapper = new FmuBootstrapper<>(backend, cache, binding);
        double bindingProbability = bootsrapper.visit(countQuery);
        // TODO get op2it for rawer dedicated
        Save2SPARQL<ID,VALUE> fmuSaver = fmuExecutor.getExecutionContext().getContext().get(SagerConstants.SAVER);
        OpGroup groupOperator = new GetRootAggregator().visit(fmuSaver.getRoot());
        RawerAgg<ID,VALUE> aggIterator = (RawerAgg<ID, VALUE>) fmuSaver.op2it.get(groupOperator);
        ApproximateAggCount<ID,VALUE> accumulator = (ApproximateAggCount<ID, VALUE>) aggIterator.getAccumulator();
        accumulator.accumulate(bindingProbability);

        double fmu = accumulator.getValueAsDouble();
        sumOfInversedProbaOverFmu += inversedProbability / fmu;
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
        return sumOfInversedProbabilities == 0. ? 0. : (bigN.getValueAsDouble()/ sumOfInversedProbabilities) * sumOfInversedProbaOverFmu;
    }

}
