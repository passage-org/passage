package fr.gdd.passage.volcano.optimizers.rules;

import fr.gdd.passage.volcano.optimizers.Jena2HeFQUINLogicalPlans;
import fr.gdd.passage.volcano.optimizers.Op2AsBinary;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RuleApplication;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRewritingRuleImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRuleApplicationImpl;

// op1 Join op2 becomes op2 Join op1
public class RuleOrderBinaryJoin extends AbstractRewritingRuleImpl {

    public RuleOrderBinaryJoin(double priority) { super(priority); }

    @Override
    protected boolean canBeAppliedTo(PhysicalPlan plan) {
        return plan.getRootOperator() instanceof Op2AsBinary op2 && op2.getOp() instanceof OpJoin;
    }

    @Override
    protected RuleApplication createRuleApplication(PhysicalPlan[] pathToTargetPlan) {
        return new AbstractRuleApplicationImpl(pathToTargetPlan, this) {
            @Override
            protected PhysicalPlan rewritePlan( final PhysicalPlan plan ) {
                OpJoin join = (OpJoin) ((Op2AsBinary) plan.getRootOperator()).getOp();
                Op newJoin = OpJoin.create(join.getLeft(),join.getRight());

                return Jena2HeFQUINLogicalPlans.convert(Jena2HeFQUINLogicalPlans.convert(newJoin));
            }
        };
    }
}
