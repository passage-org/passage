package fr.gdd.passage.blazegraph.datasets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BlazegraphInMemoryDatasetsTest {

    @Test
    public void in_blazegraph_creation_of_small_sails_datasets_for_test () {
        Assertions.assertDoesNotThrow( () -> {
            var ignored = BlazegraphInMemoryDatasetsFactory.triples3();
            ignored = BlazegraphInMemoryDatasetsFactory.triples6();
            ignored = BlazegraphInMemoryDatasetsFactory.triples9();
            ignored = BlazegraphInMemoryDatasetsFactory.graph3();
            ignored = BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals();
            // ignored = IM4Blazegraph.stars(); // TODO stars should be handled eventually
        });
    }
}
