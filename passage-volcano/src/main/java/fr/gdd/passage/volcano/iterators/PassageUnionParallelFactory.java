//package fr.gdd.passage.volcano.iterators;
//
//import fr.gdd.passage.commons.generics.BackendBindings;
//import org.apache.jena.sparql.algebra.op.OpUnion;
//import org.apache.jena.sparql.engine.ExecutionContext;
//
//import java.io.Serializable;
//import java.util.Iterator;
//import java.util.Objects;
//
///**
// * Unions can be processed in parallel.
// */
//public class PassageUnionParallelFactory<ID,VALUE,SKIP extends Serializable> implements Iterator<BackendBindings<ID, VALUE>> {
//
//    final ExecutionContext context;
//    final Iterator<BackendBindings<ID,VALUE>> input;
//    final OpUnion union;
//    Iterator<BackendBindings<ID,VALUE>> current;
//
//    public PassageUnionParallelFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OpUnion union) {
//        this.context = context;
//        this.input = input;
//        this.union = union;
//    }
//
//    @Override
//    public boolean hasNext() {
//        if (Objects.isNull(current) && !input.hasNext()) return false;
//
//        if (Objects.nonNull(current) && current.hasNext()) return true;
//
//        while (Objects.isNull(current) && input.hasNext()) {
//            BackendBindings<ID, VALUE> inputBinding = input.next();
//            current = new PassageUnionParallel<ID, VALUE, SKIP>(context, inputBinding, union);
//            if (!current.hasNext()) {
//                current = null;
//            }
//        }
//
//        if (Objects.isNull(current)) return false;
//
//        return current.hasNext();
//    }
//
//    @Override
//    public BackendBindings<ID, VALUE> next() {
//        return current.next();
//    }
//}
