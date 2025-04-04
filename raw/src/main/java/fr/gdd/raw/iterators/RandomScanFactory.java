package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendQuadsFactory;
import fr.gdd.passage.commons.factories.IBackendTriplesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.openrdf.util.iterators.EmptyIterator;

import java.util.Iterator;

public class RandomScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Backend<ID, VALUE, ?> backend;
    final Iterator<BackendBindings<ID, VALUE>> input;
    final ExecutionContext context;
    final Op0 opTripleOrQuad;
    final BackendCache<ID,VALUE> cache;

    BackendBindings<ID, VALUE> inputBinding;
    Iterator<BackendBindings<ID, VALUE>> instantiated = Iter.empty();


    public RandomScanFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, Op0 opTripleOrQuad) {
        this.input = input;
        this.context = context;
        this.opTripleOrQuad = opTripleOrQuad;
        this.backend = context.getContext().get(RawConstants.BACKEND);
        this.cache = context.getContext().get(RawConstants.CACHE);
    }

    public static <ID,VALUE> IBackendTriplesFactory<ID,VALUE> tripleFactory() {
        return RandomScanFactory::new;
    }
    public static <ID,VALUE> IBackendQuadsFactory<ID,VALUE> quadFactory() { return RandomScanFactory::new; }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {

            inputBinding = input.next();

            instantiated = switch (opTripleOrQuad) {
                case OpTriple opTriple -> {
                    try{
                        Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), inputBinding, cache);
                        yield new RandomScan<>(context, opTriple, spo);
                    }catch(NotFoundException e){
                        yield new EmptyIterator<>();
                    }
                }
                case OpQuad opQuad -> {
                    try{
                        Tuple<ID> spo = Substitutor.substitute(opQuad.getQuad(), inputBinding, cache);
                        yield new RandomScan<>(context, opQuad, spo);
                    }catch (NotFoundException e){
                        yield new EmptyIterator<>();
                    }
                }
                default -> throw new UnsupportedOperationException("Operator not handled here:" + opTripleOrQuad);
            };
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        BackendBindings<ID, VALUE> binding = instantiated.next();

//        RandomScan instantiatedScan = (RandomScan) instantiated;
//
//        Double probability = instantiatedScan.getProbability();
//
//        JsonObject scanJson = buildScan(binding, probability);
//
//        Node inputRwhNode = inputBinding.get(RawConstants.RANDOM_WALK_HOLDER);
//
//        if(inputRwhNode != null){
//            // there is an input binding, so this random scan operator represents a bound join, and thus we create a join object
//            if(inputRwhNode.isLiteral()){
//                JsonObject inputRwhJson = Json.createReader(new StringReader(inputRwhNode.getLiteralValue().toString())).readObject();
//                binding.put(RawConstants.RANDOM_WALK_HOLDER, new BackendBindings.IdValueBackend<ID,VALUE>().setString(stringify(buildJoin(inputRwhJson, scanJson))));
//            }else {
//                throw new RuntimeException("Not a literal node: " + inputRwhNode + ", even though it's a meta binding " +
//                        "and shouldn't ever be associated with a non literal value");
//            }
//        }else {
//            // there is no input binding, so this random scan operator represents only a scan
//            binding.put(RawConstants.RANDOM_WALK_HOLDER, new BackendBindings.IdValueBackend<ID,VALUE>().setString(stringify(scanJson)));
//        }

        return binding.setParent(inputBinding);
    }

}
