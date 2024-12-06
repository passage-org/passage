package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;

import java.util.function.Consumer;

public interface PassageExecutor<ID,VALUE> {

    Op execute(Op query, Consumer<BackendBindings<ID,VALUE>> consumer);
    Op execute(String query, Consumer<BackendBindings<ID,VALUE>> consumer);

}
