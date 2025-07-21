package fr.gdd.passage.commons;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadPattern;
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
public class CommonsCardinalityJoinOrdering<ID,VALUE> implements ReturningArgsOpVisitor<
        Op, // built operator.
        Set<Var>> { // the variables already set when the operator is visited.

    final Backend<ID, VALUE> backend; // (to create the iterators)
    final BackendBindings<ID, VALUE> input;
    final BackendCache<ID, VALUE> cache;
    private static final Logger log = LoggerFactory.getLogger(CommonsCardinalityJoinOrdering.class);

    private boolean hasCartesianProduct = false;

    public CommonsCardinalityJoinOrdering(Backend<ID,VALUE> backend) {
        this.backend = backend;
        this.input = new BackendBindings<>();
        this.cache = new BackendCache<>(backend);
    }

    public Op visit(Op op) {
        return ReturningArgsOpVisitorRouter.visit(this, op, new HashSet<>());
    }

    public boolean hasCartesianProduct() {
        return hasCartesianProduct;
    }

    public BackendIterator<ID, VALUE> createBackendIterator(Op op){
        BackendIterator<ID, VALUE> iterator;

        try {
            switch (op) {
                case OpTriple opTriple -> {
                    Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), input, cache);
                    iterator = backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT));
                }
                case OpQuad opQuad -> {
                    Tuple<ID> spoc = Substitutor.substitute(opQuad.getQuad(), input, cache);
                    iterator = backend.search(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH));
                }
                default -> throw new UnsupportedOperationException("Operator not handle here: " + op);
            }
        } catch (NotFoundException | IllegalArgumentException e) {
            iterator = null;
        }

        return iterator;
    }

    /* ********************************************************************* */



    @Override
    public Op visit(OpTriple triple, Set<Var> alreadySetVars) {
        return triple; // nothing to optimize with a single triple
    }

    @Override
    public Op visit(OpQuad quad, Set<Var> alreadySetVars) {
        return quad; // nothing to optimize with a single quad
    }

    @Override
    public Op visit(OpBGP bgp, Set<Var> alreadySetVars) {
        // we build the knowledge about cardinalities
        List<Pair<OpTriple, Double>> triple2card = new ArrayList<>();
        for (Triple t : bgp.getPattern()) {
            OpTriple key = new OpTriple(t);

            BackendIterator<ID, VALUE> iterator = createBackendIterator(key);
            log.debug("{} => {}", key.getTriple(), iterator.cardinality());
            triple2card.add(new ImmutablePair<>(key, iterator.cardinality()));
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
            Triple toAdd = filtered.getFirst().getLeft().getTriple();
            triple2card = triple2card.stream().filter(p -> p.getLeft().getTriple() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromTriple(patternVarsScope, toAdd);
            triples.add(toAdd);
        }

        return new OpBGP(BasicPattern.wrap(triples));
    }


    @Override
    public Op visit(OpQuadBlock opQuadblock, Set<Var> alreadySetVars) {
        // we build the knowledge about cardinalities
        List<Pair<OpQuad, Double>> quad2card = new ArrayList<>();
        for (Quad q : opQuadblock.getPattern().getList()) {
            OpQuad key = new OpQuad(q);

            BackendIterator<ID, VALUE> iterator = createBackendIterator(key);
            log.debug("{} => {}", key.getQuad(), iterator.cardinality());
            quad2card.add(new ImmutablePair<>(key, iterator.cardinality()));
        }

        // sort by cardinality
        quad2card.sort(Comparator.comparing(Pair::getRight));

        List<Quad> triples = new ArrayList<>();
        Set<Var> patternVarsScope = new HashSet<>();
        while (!quad2card.isEmpty()) {
            // #A contains at least one variable
            var filtered = quad2card.stream().filter(p -> patternVarsScope.stream()
                            .anyMatch(v -> getVars(p.getLeft().getQuad()).contains(v)) ||
                            getVars(p.getLeft().getQuad()).stream().anyMatch(alreadySetVars::contains))
                    .toList();
            if (filtered.isEmpty()) {
                // #B contains none
                filtered = quad2card; // everyone is candidate
                hasCartesianProduct = hasCartesianProduct || // stays set once set
                        !triples.isEmpty() || // means that the issue arise within the bgp itself
                        !alreadySetVars.isEmpty(); // means that the issue arise from up top as well
            }
            Quad toAdd = filtered.getFirst().getLeft().getQuad();
            quad2card = quad2card.stream().filter(p -> p.getLeft().getQuad() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromQuad(patternVarsScope, toAdd);
            triples.add(toAdd);
        }

        QuadPattern bqp = new QuadPattern();
        triples.forEach(bqp::add);
        return new OpQuadBlock(bqp);
    }

    public static Set<Var> getVars(Quad quad) { // because for w/e reason it does not exist in utils
        Set<Var> x = new HashSet<>();
        VarUtils.addVarsFromQuad(x, quad);
        return x;
    }

    /* *********************** EXPLORE AND UPDATE SET VARIABLES ***************************** */

    @Override
    public Op visit(OpGraph graph, Set<Var> alreadySetVars) {
        throw new UnsupportedOperationException("OpGraph should not exist at this stage.");
    }

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
    public Op visit(OpService req, Set<Var> alreadySetVars) {
        return req; // we don't go inside OpService yet.
    }
}
