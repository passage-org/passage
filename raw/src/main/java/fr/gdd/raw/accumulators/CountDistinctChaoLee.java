package fr.gdd.raw.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.volcano.optimizers.CardinalityJoinOrdering;
import fr.gdd.passage.volcano.pause.Triples2BGP;
import fr.gdd.passage.volcano.resume.BGP2Triples;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import fr.gdd.raw.iterators.RandomAggregator;
import fr.gdd.raw.subqueries.CountSubqueryBuilder;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.function.FunctionEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Perform an estimate of the COUNT DISTINCT based on random walks performed on
 * the subquery. It makes use of Chao-Lee's original formula. It's expected to
 * be much slower, and more memory consuming.
 */
public class CountDistinctChaoLee<ID,VALUE> implements BackendAccumulator<ID, VALUE> {

    private final static Logger log = LoggerFactory.getLogger(CountDistinctChaoLee.class);

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final OpGroup group;
    final CacheId<ID,VALUE> cache;

    final CountWanderJoin<ID,VALUE> bigN;
    final WanderJoin<ID,VALUE> wj;

    // Must keep track of already seen distinct elements to count them once
    // TODO make it not string, but BackendBinding needs to implement hashcode…
    // TODO should have a concurrentMap to work well.
    Map<String, Double> distinct2Fmu = new HashMap<>(); // important to merge when multithreaded

    final Set<Var> vars;
    long sampleSize = 0; // for debug purposes

    public CountDistinctChaoLee(ExprList varsAsExpr, ExecutionContext context, OpGroup group) {
        this.context = context;
        this.backend = context.getContext().get(RawConstants.BACKEND);
        this.group = group;
        this.bigN = new CountWanderJoin<>(context, group.getSubOp());
        BackendSaver<ID,VALUE,?> saver = context.getContext().get(RawConstants.SAVER);
        this.wj = new WanderJoin<>(saver);
        this.vars = varsAsExpr.getVarsMentioned();
        this.cache = context.getContext().get(RawConstants.CACHE);
    }

    @Override
    public void merge(BackendAccumulator<ID, VALUE> other) {
//         throw new UnsupportedOperationException("Does not support multithread yet. Must create map for the sake of correctness…");
        if (Objects.isNull(other)) { return; }
        if (other instanceof CountDistinctChaoLee<ID,VALUE> chaoLee) {
            this.sampleSize += chaoLee.sampleSize;
            this.distinct2Fmu.putAll(chaoLee.distinct2Fmu);
            this.bigN.merge(chaoLee.bigN);
        }
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        this.bigN.accumulate(binding, functionEnv); // register the success for bigN
        if (Objects.isNull(binding)) {
            // #1 processing of N
            return;
        }

        // #2 check if the element was already processed, in such case, ignore it.
        BackendBindings<ID,VALUE> key = RandomAggregator.getKeyBinding(vars, binding);
        if (distinct2Fmu.containsKey(key.toString())) {
            return; // do nothing
        }

        // #3 processing of Fµ
        CountSubqueryBuilder<ID,VALUE> subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, vars);
        RawOpExecutor<ID,VALUE> fmuExecutor = new RawOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(RandomAggregator.SUBQUERY_LIMIT)
                .setTimeout(RandomAggregator.SUBQUERY_TIMEOUT)
                .setCache(subqueryBuilder.getCache());

        Op countQuery = subqueryBuilder.build(group.getSubOp());
        // need same join order to bootstrap
        if (context.getContext().isFalseOrUndef(RawConstants.FORCE_ORDER)) {
            countQuery = ReturningOpVisitorRouter.visit(new Triples2BGP(), countQuery);
            countQuery = new CardinalityJoinOrdering<>(backend, cache).visit(countQuery); // need to have bgp to optimize, no tps
        } else {
            fmuExecutor.forceOrder();
        }
        countQuery = ReturningOpVisitorRouter.visit(new BGP2Triples(), countQuery);

        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // Might happen when stopping conditions trigger immediately, e.g. not
            // enough execution time left, or not enough scan left.
            return ; // do nothing, we don't even want to account the newly found value as 0.
        }
        // therefore, only then, we modify the inner state of this ApproximateAggCountDistinct



        sampleSize += 1; // only account for those which succeed (debug purpose)

        // don't do anything with the value, but still need to create it.
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        RawConstants.incrementScansBy(context, fmuExecutor.getExecutionContext());

        // #4 get the aggregate and boostrap it with the value found in distinct sample: µ
        FmuBootstrapper<ID,VALUE> bootsrapper = new FmuBootstrapper<>(backend, cache, binding);
        double bindingProbability = bootsrapper.visit(countQuery);
        BackendSaver<ID,VALUE,?> fmuSaver = fmuExecutor.getExecutionContext().getContext().get(RawConstants.SAVER);
        OpGroup groupOperator = new GetRootAggregator().visit(fmuSaver.getRoot());
        RandomAggregator<ID,VALUE> aggIterator = (RandomAggregator<ID, VALUE>) fmuSaver.getIterator(groupOperator);
        CountWanderJoin<ID,VALUE> accumulator = (CountWanderJoin<ID, VALUE>) aggIterator.getAccumulator();
        accumulator.accumulate(bindingProbability);

        double fmu = accumulator.getValueAsDouble();

        this.distinct2Fmu.put(key.toString(), fmu);
    }

    @Override
    public VALUE getValue() {
        log.debug("BigN SampleSize: " + bigN.sampleSize);
        log.debug("ChaoLee SampleSize: " + sampleSize);
        log.debug("Nb Total Scans: " + context.getContext().get(RawConstants.SCANS));
        Backend<ID,VALUE,?> backend = context.getContext().get(RawConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        double estimatedN = bigN.getValueAsDouble();
        if (estimatedN == 0.) {
            return 0.;
        }
        double fmusOverN = distinct2Fmu.values().stream().mapToDouble(d->d).sum()/estimatedN;
        if (fmusOverN == 0.) {
            return 0.;
        }

        return distinct2Fmu.size() / fmusOverN;
    }

    @Override
    public ExecutionContext getContext() {
        return context;
    }
}
