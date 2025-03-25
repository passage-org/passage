package fr.gdd.passage.volcano.optimizers;

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
        return new HashSet<>(); // TODO
    }

    @Override
    protected Set<RewritingRule> addRuleInstancesForDividing() {
        return new HashSet<>(); // TODO
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
