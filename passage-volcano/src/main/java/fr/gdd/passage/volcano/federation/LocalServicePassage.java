package fr.gdd.passage.volcano.federation;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.HashMap;
import java.util.Map;

public class LocalServicePassage<ID,VALUE> implements ILocalService {

    final PassageExecutionContextBuilder<ID,VALUE> builder;

    LocalServicePassage(PassageExecutionContextBuilder<ID,VALUE> builder) {
        this.builder = builder;
    }

    public Pair<Multiset<Binding>, Map<String, ?>> query(String query, String... args) {
        // TODO parse args
        Multiset<Binding> results = HashMultiset.create();
        Op continuation = new PassagePushExecutor<>(builder.build()).execute(query, results::add);

        Map<String, Op> metadata = new HashMap<>();
        metadata.put("next", continuation);
        return Pair.of(results, metadata);
    }

}
