package fr.gdd.passage.commons.generics;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.factories.*;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Iterator;
import java.util.Objects;

/**
 * Base executor of operators that compose the logical plan.
 * Mostly exist to ease the creation of executor configurations.
 */
public class BackendPullExecutor<ID,VALUE> implements ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    public final ExecutionContext context;
    private final IBackendProjectsFactory<ID,VALUE> projects;
    private final IBackendTriplesFactory<ID,VALUE> triples;
    private final IBackendQuadsFactory<ID,VALUE> quads;
    private final IBackendJoinsFactory<ID,VALUE> joins;
    private final IBackendUnionsFactory<ID,VALUE> unions;
    private final IBackendValuesFactory<ID,VALUE> values;
    private final IBackendBindsFactory<ID,VALUE> binds;
    private final IBackendFiltersFactory<ID,VALUE> filters;
    private final IBackendDistinctsFactory<ID,VALUE> distincts;
    private final IBackendLimitOffsetFactory<ID,VALUE> slices;
    private final IBackendOptionalsFactory<ID,VALUE> optionals;
    private final IBackendCountsFactory<ID,VALUE> counts;
    private final IBackendServicesFactory<ID,VALUE> services;


    public BackendPullExecutor(ExecutionContext context,
                               IBackendProjectsFactory<ID,VALUE> projects,
                               IBackendTriplesFactory<ID,VALUE> triples,
                               IBackendQuadsFactory<ID,VALUE> quads,
                               IBackendJoinsFactory<ID,VALUE> joins,
                               IBackendUnionsFactory<ID,VALUE> unions,
                               IBackendValuesFactory<ID,VALUE> values,
                               IBackendBindsFactory<ID,VALUE> binds,
                               IBackendFiltersFactory<ID,VALUE> filters,
                               IBackendDistinctsFactory<ID,VALUE> distincts,
                               IBackendLimitOffsetFactory<ID,VALUE> slices,
                               IBackendOptionalsFactory<ID,VALUE> optionals,
                               IBackendCountsFactory<ID, VALUE> counts,
                               IBackendServicesFactory<ID,VALUE> services) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this);
        this.triples = triples;
        this.quads = quads;
        this.projects = projects;
        this.joins = joins;
        this.unions = unions;
        this.values = values;
        this.binds = binds;
        this.filters = filters;
        this.distincts = distincts;
        this.slices = slices;
        this.optionals = optionals;
        this.counts = counts;
        this.services = services;
    }

    public Iterator<BackendBindings<ID,VALUE>> execute(String opAsString) {
        return this.execute(Algebra.compile(QueryFactory.create(opAsString)));
    }

    public Iterator<BackendBindings<ID,VALUE>> execute(Op op) {
        return this.visit(op, Iter.of(new BackendBindings<>())); // start with an empty binding
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple triple, Iterator<BackendBindings<ID, VALUE>> input) {
        return triples.get(context, input, triple);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpQuad quad, Iterator<BackendBindings<ID, VALUE>> input) {
        return quads.get(context, input, quad);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        return projects.get(context, input, project);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID, VALUE>> input) {
        return joins.get(context, input, join);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTable table, Iterator<BackendBindings<ID, VALUE>> input) {
        if (table.isJoinIdentity()) {
            return input;
        }
        return values.get(context, input, table);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpUnion union, Iterator<BackendBindings<ID, VALUE>> input) {
        return unions.get(context, input, union);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpFilter filter, Iterator<BackendBindings<ID, VALUE>> input) {
        return filters.get(context, input, filter);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpDistinct distinct, Iterator<BackendBindings<ID, VALUE>> input) {
        return distincts.get(context, input, distinct);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID, VALUE>> input) {
        return binds.get(context, input, extend);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpSlice slice, Iterator<BackendBindings<ID, VALUE>> input) {
        return slices.get(context, input, slice);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpConditional cond, Iterator<BackendBindings<ID, VALUE>> input) {
        return optionals.get(context, input, cond);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpLeftJoin lj, Iterator<BackendBindings<ID, VALUE>> input) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            throw new UnsupportedOperationException("Expressions are not supported in left joins.");
        }
        return optionals.get(context, input, lj);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, Iterator<BackendBindings<ID, VALUE>> input) {
        for (int i = 0; i < groupBy.getAggregators().size(); ++i) {
            switch (groupBy.getAggregators().get(i).getAggregator()) {
                case AggCount ignored -> {} // nothing, just checking it's handled (this is COUNT(*))
                case AggCountVar ignored -> {}  // nothing, just checking it's handled (this is COUNT(?variable))
                case AggCountVarDistinct ignored -> throw new UnsupportedOperationException("COUNT DISTINCT with variable(s) is not supported.");
                case AggCountDistinct ignored -> throw new UnsupportedOperationException("COUNT DISTINCT of star (*) is not supported."); // TODO
                default -> throw new UnsupportedOperationException("The aggregation function is not implemented: " +
                        groupBy.getAggregators().get(i).toString());
            }
        }
//        if (!groupBy.getGroupVars().isEmpty()) {
//            throw new UnsupportedOperationException("Group keys are not supported.");
//        }
        return counts.get(context, input, groupBy);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpService req, Iterator<BackendBindings<ID, VALUE>> input) {
        return services.get(context, input, req);
    }
}
