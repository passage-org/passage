package fr.gdd.passage.databases.inmemory;

import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsBase;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.List;

public class IM4HDT {

    public static HDT triples9 () { return getDataset(InMemoryStatements.triples9); }

    public static HDT getDataset(List<String> statements) {
        List<TripleString> supplier = statements.stream().map(statement -> {
            String[] spo = statement.split("\\s+");
            return new TripleString(spo[0], spo[1], spo[2]);
        }).toList();

        try {
            HDT hdt = HDTManager.generateHDT(supplier.iterator(), "http://base_uri/", HDTSpecification.EMPTY, null);
            hdt = HDTManager.indexedHDT(hdt, null);
            return hdt;
        } catch (Exception e) {
            throw new RuntimeException("Shouldn't happen");
        }

    }
}
