//package fr.gdd.passage.volcano.optimizers.rules;
//
//import fr.gdd.passage.volcano.optimizers.PhysicalOpBGP;
//import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
//import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RuleApplication;
//import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRewritingRuleImpl;
//import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.rules.AbstractRuleApplicationImpl;
//
///**
// * Split a BGP into series of triple patterns
// */
//public class BGP2TP extends AbstractRewritingRuleImpl {
//
//    @Override
//    protected boolean canBeAppliedTo(PhysicalPlan plan) {
//        if (plan instanceof PhysicalOpBGP opBGP) {
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    @Override
//    protected RuleApplication createRuleApplication(PhysicalPlan[] pathToTargetPlan) {
//        return new AbstractRuleApplicationImpl() {
//            @Override
//            protected PhysicalPlan rewritePlan(PhysicalPlan plan) {
//                return null;
//            }
//        }
//    }
//}
