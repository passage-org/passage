package fr.gdd.passage.volcano.optimizers.rules;

import fr.gdd.passage.volcano.optimizers.Jena2HeFQUINLogicalPlans;
import fr.gdd.passage.volcano.optimizers.Op2AsBinary;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RuleApplication;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRewritingRuleImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRuleApplicationImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Merge two adjacent triple patterns and/or BGPs.
 */
public class RuleMergingTP2BGP extends AbstractRewritingRuleImpl {

    public RuleMergingTP2BGP(double priority) {
        super(priority);
    }

    @Override
    protected boolean canBeAppliedTo(PhysicalPlan plan) {
        return (plan.getRootOperator() instanceof Op2AsBinary op2 && op2.getOp() instanceof OpJoin join &&
                (join.getLeft() instanceof OpBGP || join.getLeft() instanceof OpTriple) &&
                (join.getRight() instanceof OpBGP || join.getRight() instanceof OpTriple));
    }

    @Override
    protected RuleApplication createRuleApplication(PhysicalPlan[] pathToTargetPlan) {
        return new AbstractRuleApplicationImpl(pathToTargetPlan, this) {
            @Override
            protected PhysicalPlan rewritePlan( final PhysicalPlan plan ) {
                OpJoin join = (OpJoin) ((Op2AsBinary) plan.getRootOperator()).getOp();
                List<Triple> result = new ArrayList<>();
                switch (join.getLeft()) {
                    case OpBGP lbgp -> result.addAll(lbgp.getPattern().getList());
                    case OpTriple lt -> result.add(lt.getTriple());
                    default -> throw new IllegalStateException("Unexpected value: " + join.getLeft());
                }
                switch (join.getRight()) {
                    case OpBGP rbgp -> result.addAll(rbgp.getPattern().getList());
                    case OpTriple rt -> result.add(rt.getTriple());
                    default -> throw new IllegalStateException("Unexpected value: " + join.getLeft());
                }

                OpBGP merged = new OpBGP(BasicPattern.wrap(result));
                return Jena2HeFQUINLogicalPlans.convert(Jena2HeFQUINLogicalPlans.convert(merged));
            }
        };
    }
}
