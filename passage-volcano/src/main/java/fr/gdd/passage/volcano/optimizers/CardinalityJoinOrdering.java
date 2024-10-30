package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.iterators.PassageScanFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.VarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Reorder basic graph patterns based on triple patterns cardinality. The
 * lower the cardinality the earlier it should be executed. When no information
 * about cardinality is provided, it resorts to variable counting, i.e., it
 * favors the execution of triple patterns with bounded variables.
 */
public class CardinalityJoinOrdering<ID,VALUE> extends ReturningArgsOpVisitor<
        Op, // built operator.
        Set<Var>> { // the variables already set when the operator is visited.

    final PassageExecutionContext<ID,VALUE> fakeContext; // (to create the iterators)
    private static final Logger log = LoggerFactory.getLogger(CardinalityJoinOrdering.class);

    private boolean hasCartesianProduct = false;

    public CardinalityJoinOrdering(Backend<ID,VALUE,Long> backend) {
        this.fakeContext = new PassageExecutionContextBuilder<ID,VALUE>().setBackend(backend).build().setQuery(null);
    }

    public CardinalityJoinOrdering(Backend<ID,VALUE,?> backend, BackendCache<ID,VALUE> cache) {
        this.fakeContext = new PassageExecutionContextBuilder<ID,VALUE>().setBackend(backend).build().setCache(cache).setQuery(null);
    }

    public Op visit(Op op) {
        return ReturningArgsOpVisitorRouter.visit(this, op, new HashSet<>());
    }

    public boolean hasCartesianProduct() {
        return hasCartesianProduct;
    }

    /* ********************************************************************* */

    @Override
    public Op visit(OpSequence sequence, Set<Var> alreadySetVars) {
        List<Op> ops = new ArrayList<>();
        for (Op op : sequence.getElements()) {
            Op newOp = this.visit(op, new HashSet<>(alreadySetVars));
            alreadySetVars.addAll(OpVars.visibleVars(newOp));
            ops.add(newOp);
        }
        return OpSequence.create().copy(ops);
    }

    @Override
    public Op visit(OpFilter filter, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(filter, this.visit(filter.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpDistinct distinct, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(distinct, this.visit(distinct.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpProject project, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(project, ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpJoin join, Set<Var> alreadySetVars) {
        // TODO flatten, then order operators based on their estimated cardinality
        // (but for now, it visits each op independently. (might be a job for another visitor though)
        Op left = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), new HashSet<>(alreadySetVars));
        alreadySetVars.addAll(OpVars.visibleVars(join.getLeft()));
        Op right = ReturningArgsOpVisitorRouter.visit(this, join.getRight(), alreadySetVars);
        return OpCloningUtil.clone(join, left, right);
    }

    @Override
    public Op visit(OpExtend extend, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(extend, ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpGroup groupBy, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(groupBy, ReturningArgsOpVisitorRouter.visit(this, groupBy.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpLeftJoin lj, Set<Var> alreadySetVars) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, lj.getLeft(), new HashSet<>(alreadySetVars));
        alreadySetVars.addAll(OpVars.visibleVars(lj.getLeft()));
        Op right = ReturningArgsOpVisitorRouter.visit(this, lj.getRight(), alreadySetVars);
        return OpCloningUtil.clone(lj, left, right);
    }

    @Override
    public Op visit(OpUnion union, Set<Var> alreadySetVars) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, union.getLeft(), new HashSet<>(alreadySetVars));
        Op right = ReturningArgsOpVisitorRouter.visit(this, union.getRight(), new HashSet<>(alreadySetVars));
        return OpCloningUtil.clone(union, left, right);
    }

    @Override
    public Op visit(OpSlice slice, Set<Var> alreadySetVars) {
        return OpCloningUtil.clone(slice,
                ReturningArgsOpVisitorRouter.visit(this, slice.getSubOp(), alreadySetVars));
    }

    @Override
    public Op visit(OpTable table, Set<Var> alreadySetVars) {
        return table;
    }

    @Override
    public Op visit(OpTriple triple, Set<Var> alreadySetVars) {
        return triple; // nothing to optimize with a single triple
    }

    @Override
    public Op visit(OpBGP bgp, Set<Var> alreadySetVars) {
        // we build the knowledge about cardinalities
        List<Pair<OpTriple, Double>> triple2card = new ArrayList<>();
        for (Triple t : bgp.getPattern()) {
            OpTriple key = new OpTriple(t);
            PassageScanFactory<ID,VALUE> scan = new PassageScanFactory<>(fakeContext, Iter.of(new BackendBindings<>()), key);
            try {
                if (scan.hasNext()) {
                    log.debug("{} => {}", key.getTriple(), scan.cardinality());
                    triple2card.add(new ImmutablePair<>(key, scan.cardinality()));
                } else {
                    log.debug("{} => Not results so 0", key.getTriple());
                    triple2card.add(new ImmutablePair<>(key, 0.)); // no results
                }
            } catch (NotFoundException e) {
                log.debug("{} => Not found so 0", key.getTriple());
                triple2card.add(new ImmutablePair<>(key, 0.)); // not found, so 0 it is, the query should stop quickly then
            }
        }

        // sort by cardinality
        triple2card.sort(Comparator.comparing(Pair::getRight));

        List<Triple> triples = new ArrayList<>();
        Set<Var> patternVarsScope = new HashSet<>();
        while (!triple2card.isEmpty()) {
            // #A contains at least one variable
            var filtered = triple2card.stream().filter(p -> patternVarsScope.stream()
                            .anyMatch(v -> VarUtils.getVars(p.getLeft().getTriple()).contains(v)) ||
                            VarUtils.getVars(p.getLeft().getTriple()).stream().anyMatch(alreadySetVars::contains))
                    .toList();
            if (filtered.isEmpty()) {
                // #B contains none
                filtered = triple2card; // everyone is candidate
                hasCartesianProduct = hasCartesianProduct || // stays set once set
                        !triples.isEmpty() || // means that the issue arise within the bgp itself
                        !alreadySetVars.isEmpty(); // means that the issue arise from up top as well
            }
            Triple toAdd = filtered.get(0).getLeft().getTriple();
            triple2card = triple2card.stream().filter(p -> p.getLeft().getTriple() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromTriple(patternVarsScope, toAdd);
            triples.add(toAdd);
        }

        return new OpBGP(BasicPattern.wrap(triples));
    }


// TODO TODO TODO quads
//
//    @Override
//    public Op transform(OpJoin opJoin, Op left, Op right) {
//        List<OpQuad> quads = getAllQuads(opJoin);
//        if (Objects.nonNull(quads)) {
//            // same as OpBGP with triples , but with quads
//            List<Pair<OpQuad, Double>> quadsToIt = quads.stream().map(quad -> {
//                try {
//                    NodeId g = quad.getQuad().getGraph().isVariable() ? backend.any() : backend.getId(quad.getQuad().getGraph());
//                    NodeId s = quad.getQuad().getSubject().isVariable() ? backend.any() : backend.getId(quad.getQuad().getSubject());
//                    NodeId p = quad.getQuad().getPredicate().isVariable() ? backend.any() : backend.getId(quad.getQuad().getPredicate());
//                    NodeId o = quad.getQuad().getObject().isVariable() ? backend.any() : backend.getId(quad.getQuad().getObject());
//
//                    BackendIterator<?, ?> it = backend.search(s, p, o, g);
//                    ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator<?, ?>) it).iterator;
//                    log.debug("quad {} => {} elements", quad, casted.cardinality());
//                    return new ImmutablePair<OpQuad, Double>(quad, casted.cardinality());
//                } catch (NotFoundException e) {
//                    return new ImmutablePair<OpQuad, Double>(quad, 0.);
//                }
//            }).sorted((p1, p2) -> { // sort ASC by cardinality
//                double c1 = p1.getRight();
//                double c2 = p2.getRight();
//                return Double.compare(c1, c2);
//            }).collect(Collectors.toList());
//
//            List<OpQuad> optimizedQuads = new ArrayList<>();
//            Set<Var> patternVarsScope = new HashSet<>();
//            while (!quadsToIt.isEmpty()) {
//                // #A contains at least one variable
//                var filtered = quadsToIt.stream().filter(p -> patternVarsScope.stream()
//                                .anyMatch(v -> VarUtils.getVars(p.getLeft().getQuad().asTriple()).contains(v)) ||
//                                VarUtils.getVars(p.getLeft().getQuad().asTriple()) // no `getVarsFromQuad` for some reason
//                                        .stream().anyMatch(v2 -> alreadySetVars.getUsageCount(v2) > 0) ||
//                                alreadySetVars.getUsageCount(p.getLeft().getQuad().getGraph().toString()) > 0)
//                        .toList();
//                if (filtered.isEmpty()) {
//                    // #B contains none
//                    filtered = quadsToIt; // everyone is candidate
//                }
//                OpQuad toAdd = filtered.get(0).getLeft();
//                quadsToIt = quadsToIt.stream().filter(p -> p.getLeft() != toAdd).collect(Collectors.toList());
//                VarUtils.addVarsFromQuad(patternVarsScope, toAdd.getQuad());
//                optimizedQuads.add(toAdd);
//            }
//
//            Op joinedQuads = optimizedQuads.get(0); // at least one
//            for (int i = 1; i < quads.size() ; ++i) {
//                joinedQuads = OpJoin.create(joinedQuads, optimizedQuads.get(i));
//            }
//
//            return joinedQuads;
//        } else {
//            return super.transform(opJoin, left, right);
//        }
//    }
//
//    /**
//     * Get all quads directly linked together by JOIN operators.
//     */
//    private static List<OpQuad> getAllQuads(Op op) {
//        if (op instanceof OpQuad) {
//            List<OpQuad> quads = new ArrayList<>();
//            quads.add((OpQuad) op);
//            return quads;
//        } else if (op instanceof OpJoin) {
//            var quads = getAllQuads(((OpJoin) op).getLeft());
//            quads.addAll(getAllQuads(((OpJoin) op).getRight()));
//            return quads;
//        }
//        return null;
//    }


}
