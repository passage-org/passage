package fr.gdd.passage.commons.engines;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.factories.IBackendOperatorFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.streams.IWrappedStream;
import fr.gdd.passage.commons.streams.WrappedStreamSingleton;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Objects;

/**
 * Base executor of operators that compose the logical plan.
 * Mostly exist to ease the creation of executor configurations.
 */
public class BackendPushExecutor<ID,VALUE> implements ReturningArgsOpVisitor<IWrappedStream<BackendBindings<ID,VALUE>>,BackendBindings<ID,VALUE>> {

    public final ExecutionContext context;
    private final IBackendOperatorFactory<ID,VALUE,OpProject> projects;
    private final IBackendOperatorFactory<ID,VALUE,OpTriple> triples;
    private final IBackendOperatorFactory<ID,VALUE,OpQuad> quads;
    private final IBackendOperatorFactory<ID,VALUE,OpJoin> joins;
    private final IBackendOperatorFactory<ID,VALUE,OpUnion> unions;
    private final IBackendOperatorFactory<ID,VALUE,OpTable> values;
    private final IBackendOperatorFactory<ID,VALUE,OpExtend> binds;
    private final IBackendOperatorFactory<ID,VALUE,OpFilter> filters;
    private final IBackendOperatorFactory<ID,VALUE,OpDistinct> distincts;
    private final IBackendOperatorFactory<ID,VALUE,OpSlice> limitoffsets;
    private final IBackendOperatorFactory<ID,VALUE,OpLeftJoin> optionals;
    private final IBackendOperatorFactory<ID,VALUE,OpGroup> counts;
    private final IBackendOperatorFactory<ID,VALUE,OpService> services;

    public BackendPushExecutor(ExecutionContext context,
                               IBackendOperatorFactory<ID,VALUE,OpProject> projects,
                               IBackendOperatorFactory<ID,VALUE,OpTriple> triples,
                               IBackendOperatorFactory<ID,VALUE,OpQuad> quads,
                               IBackendOperatorFactory<ID,VALUE,OpJoin> joins,
                               IBackendOperatorFactory<ID,VALUE,OpUnion> unions,
                               IBackendOperatorFactory<ID,VALUE,OpTable> values,
                               IBackendOperatorFactory<ID,VALUE,OpExtend> binds,
                               IBackendOperatorFactory<ID,VALUE,OpFilter> filters,
                               IBackendOperatorFactory<ID,VALUE,OpDistinct> distincts,
                               IBackendOperatorFactory<ID,VALUE,OpSlice> limitoffsets,
                               IBackendOperatorFactory<ID,VALUE,OpLeftJoin> optionals,
                               IBackendOperatorFactory<ID, VALUE,OpGroup> counts,
                               IBackendOperatorFactory<ID,VALUE,OpService> services) {
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
        this.limitoffsets = limitoffsets;
        this.optionals = optionals;
        this.counts = counts;
        this.services = services;
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpTriple triple, BackendBindings<ID, VALUE> input) {
        return triples.get(context, input, triple);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpQuad quad, BackendBindings<ID, VALUE> input) {
        return quads.get(context, input, quad);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpProject project, BackendBindings<ID, VALUE> input) {
        return projects.get(context, input, project);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpJoin join, BackendBindings<ID, VALUE> input) {
        return joins.get(context, input, join);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpTable table, BackendBindings<ID, VALUE> input) {
        if (table.isJoinIdentity()) {
            return new WrappedStreamSingleton<>(input);
        }
        return values.get(context, input, table);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpUnion union, BackendBindings<ID, VALUE> input) {
        return unions.get(context, input, union);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpFilter filter, BackendBindings<ID, VALUE> input) {
        return filters.get(context, input, filter);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpDistinct distinct, BackendBindings<ID, VALUE> input) {
        return distincts.get(context, input, distinct);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpExtend extend, BackendBindings<ID, VALUE> input) {
        return binds.get(context, input, extend);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpSlice slice, BackendBindings<ID, VALUE> input) {
        return limitoffsets.get(context, input, slice);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpConditional cond, BackendBindings<ID, VALUE> input) {
        throw new UnsupportedOperationException("OpConditional not supported, use OpLeftJoin instead.");
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpLeftJoin lj, BackendBindings<ID, VALUE> input) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            throw new UnsupportedOperationException("Expressions are not supported in left joins.");
        }
        return optionals.get(context, input, lj);
    }

    @Override
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, BackendBindings<ID, VALUE> input) {
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
    public IWrappedStream<BackendBindings<ID, VALUE>> visit(OpService req, BackendBindings<ID, VALUE> input) {
        return services.get(context, input, req);
    }
}
