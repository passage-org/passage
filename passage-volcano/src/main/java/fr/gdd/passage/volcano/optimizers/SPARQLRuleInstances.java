package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.volcano.optimizers.rules.RuleDividingBGP2TP;
import fr.gdd.passage.volcano.optimizers.rules.RuleMergingTP2BGP;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RewritingRule;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.rewriting.RuleInstances;

import java.util.HashSet;
import java.util.Set;

/**
 * Meaning the rules are ok for SPARQL, and are not federations.
 */
public class SPARQLRuleInstances extends RuleInstances {

    @Override
    protected Set<RewritingRule> addRuleInstancesForOrdering() {
        return super.addRuleInstancesForOrdering();
    }

    @Override
    protected Set<RewritingRule> addRuleInstancesForMerging() {
        Set<RewritingRule> mergingRules = new HashSet<>();
        mergingRules.add(new RuleMergingTP2BGP(0.35)); // a bit higher than dividing
        return mergingRules;
    }

    @Override
    protected Set<RewritingRule> addRuleInstancesForDividing() {
        Set<RewritingRule> dividingRules = new HashSet<>();
        dividingRules.add(new RuleDividingBGP2TP(0.3));
        return dividingRules;
    }

    @Override
    protected Set<RewritingRule> addRuleInstancesForAlgorithm() {
        return new HashSet<>(); // TODO
    }

    @Override
    protected Set<RewritingRule> addRuleInstancesForUnion() {
        return new HashSet<>(); // TODO
    }
}
