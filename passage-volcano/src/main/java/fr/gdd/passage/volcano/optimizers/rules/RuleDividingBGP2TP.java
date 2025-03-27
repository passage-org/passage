package fr.gdd.passage.volcano.optimizers.rules;

import fr.gdd.passage.volcano.optimizers.Jena2HeFQUINLogicalPlans;
import fr.gdd.passage.volcano.optimizers.Op0AsNullary;
import fr.gdd.passage.volcano.transforms.BGP2Triples;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RuleApplication;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRewritingRuleImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRuleApplicationImpl;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Split a BGP into a series of triple patterns: { tp1. tp2. tp3 } becomes {{tp1} {{tp2} {tp3}}
 * The joins are binary joins.
 */
public class RuleDividingBGP2TP extends AbstractRewritingRuleImpl {

    public RuleDividingBGP2TP(double priority) {
        super(priority);
    }

    @Override
    protected boolean canBeAppliedTo(PhysicalPlan plan) {
        return (plan.getRootOperator() instanceof Op0AsNullary op0 && op0.getOp() instanceof OpBGP);
    }

    @Override
    protected RuleApplication createRuleApplication(PhysicalPlan[] pathToTargetPlan) {
        return new AbstractRuleApplicationImpl(pathToTargetPlan, this) {
            @Override
            protected PhysicalPlan rewritePlan( final PhysicalPlan plan ) {
                OpBGP bgp = (OpBGP) ((Op0AsNullary) plan.getRootOperator()).getOp();

                return switch (bgp.getPattern().size()) {
                    case 0 -> throw new ArrayIndexOutOfBoundsException("Empty BGP should not get there");
                    case 1 -> {
                        Triple triple = bgp.getPattern().get(0);
                        yield Jena2HeFQUINLogicalPlans.convert(Jena2HeFQUINLogicalPlans.convert(new OpTriple(triple)));
                    }
                    default -> Jena2HeFQUINLogicalPlans.convert(Jena2HeFQUINLogicalPlans.convert(
                            Objects.requireNonNull(BGP2Triples.asJoins(bgp.getPattern().getList().stream()
                                    .map(OpTriple::new)
                                    .collect(Collectors.toList())))));
                };
            }
        };
    }
}
