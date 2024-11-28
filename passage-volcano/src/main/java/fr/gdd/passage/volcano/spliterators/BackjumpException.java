package fr.gdd.passage.volcano.spliterators;

import org.apache.jena.sparql.core.Var;

import java.util.Set;

/**
 * Exception thrown by the backjump iterators when they
 * fail to produce any result based on a variable.
 */
public class BackjumpException extends RuntimeException {

    public final Set<Var> problematicVariables;

    public BackjumpException(Set<Var> problematicVariables){
        super("Issue on variables: " + problematicVariables.toString());
        this.problematicVariables = problematicVariables;
    }

}