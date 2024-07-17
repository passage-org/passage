package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.BackendSaver;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendAccumulator;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.iterators.RandomAggregator;
import fr.gdd.sage.rawer.subqueries.CountSubqueryBuilder;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
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
 * Perform an estimate of the COUNT DISTINCT based on random walks
 * performed on the subquery. It makes use of CRAWD as underlying
 * formula.
 */
public class CountDistinctCRAWD<ID,VALUE> implements BackendAccumulator<ID, VALUE> {

    private final static Logger log = LoggerFactory.getLogger(CountDistinctCRAWD.class);

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final OpGroup group;
    final CacheId<ID,VALUE> cache;

    final WanderJoin<ID,VALUE> wj; // visitor that processes probability and cardinality

    final Set<Var> vars;
    double sampleSizeOfWJ = 0;
    double sumOfInversedProbaOverFmu = 0.;
    long sampleSizeOfCRAWD = 0; // for debug purposes

    public CountDistinctCRAWD(ExprList varsAsExpr, ExecutionContext context, OpGroup group) {
        this.context = context;
        this.backend = context.getContext().get(RawerConstants.BACKEND);
        this.group = group;
        BackendSaver<ID,VALUE,?> saver = context.getContext().get(RawerConstants.SAVER);
        this.wj = new WanderJoin<>(saver);
        this.vars = varsAsExpr.getVarsMentioned();
        this.cache = context.getContext().get(RawerConstants.CACHE);
    }

    @Override
    public void merge(BackendAccumulator<ID, VALUE> other) {
        if (Objects.isNull(other)) { return; } // do nothing

        if (other instanceof CountDistinctCRAWD<ID,VALUE> otherCRAWD) {
            sampleSizeOfWJ += otherCRAWD.sampleSizeOfWJ;
            sumOfInversedProbaOverFmu += otherCRAWD.sumOfInversedProbaOverFmu;
            sampleSizeOfCRAWD += otherCRAWD.sampleSizeOfCRAWD;
        }
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        // #1 processing of N
        this.sampleSizeOfWJ += 1;
        if (Objects.isNull(binding)) {
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
                .setLimit(RandomAggregator.SUBQUERY_LIMIT)
                .setTimeout(RandomAggregator.SUBQUERY_TIMEOUT)
                .setCache(subqueryBuilder.getCache());

        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // Might happen when stopping conditions trigger immediatly, e.g. not
            // enough execution time left, or not enough scan left.
            return ; // do nothing, we don't even want to account the newly found value as 0.
        }
        // therefore, only then, we modify the inner state of this ApproximateAggCountDistinct

        sampleSizeOfCRAWD += 1; // only account for those which succeed (debug purpose)

        // #2 processing of Pµ
        double probability = ReturningOpVisitorRouter.visit(wj, group.getSubOp());
        double inversedProbability = probability == 0. ? 0. : 1./probability;

        // don't do anything with the value, but still need to create it.
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        RawerConstants.incrementScansBy(context, fmuExecutor.getExecutionContext());

        // #3 get the aggregate and boostrap it with the value found in distinct sample: µ
        FmuBootstrapper<ID,VALUE> bootsrapper = new FmuBootstrapper<>(backend, cache, binding);
        double bindingProbability = bootsrapper.visit(countQuery);
        BackendSaver<ID,VALUE,?> fmuSaver = fmuExecutor.getExecutionContext().getContext().get(RawerConstants.SAVER);
        OpGroup groupOperator = new GetRootAggregator().visit(fmuSaver.getRoot());
        RandomAggregator<ID,VALUE> aggIterator = (RandomAggregator<ID, VALUE>) fmuSaver.getIterator(groupOperator);
        CountWanderJoin<ID,VALUE> accumulator = (CountWanderJoin<ID, VALUE>) aggIterator.getAccumulator();
        accumulator.accumulate(bindingProbability);

        double fmu = accumulator.getValueAsDouble();
        sumOfInversedProbaOverFmu += inversedProbability / fmu;
    }

    @Override
    public VALUE getValue() {
        log.debug("WJ SampleSize: " + sampleSizeOfWJ);
        log.debug("CRAWD SampleSize: " + sampleSizeOfCRAWD);
        log.debug("Nb Total Scans: " + context.getContext().get(RawerConstants.SCANS));
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sampleSizeOfWJ == 0. ? 0. : sumOfInversedProbaOverFmu / sampleSizeOfWJ;
    }

    @Override
    public ExecutionContext getContext() {
        return context;
    }
}
