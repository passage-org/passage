package fr.gdd.passage.volcano.federation;

import com.google.common.collect.Multiset;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.Map;

public interface ILocalService {

    Pair<Multiset<Binding>, Map<String, ?>> query(String query, String... args);

}
