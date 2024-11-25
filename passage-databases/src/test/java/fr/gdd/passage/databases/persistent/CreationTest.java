package fr.gdd.passage.databases.persistent;

import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.databases.inmemory.IM4Jena;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CreationTest {

    @Test
    public void in_blazegraph_creation_of_small_sails_datasets_for_test () {
        Assertions.assertDoesNotThrow( () -> {
            var ignored = IM4Blazegraph.triples3();
            ignored = IM4Blazegraph.triples6();
            ignored = IM4Blazegraph.triples9();
            ignored = IM4Blazegraph.graph3();
            ignored = IM4Blazegraph.triples9PlusLiterals();
            // ignored = IM4Blazegraph.stars(); // TODO stars should be handled eventually
        });
    }

    @Disabled("java.lang.NoSuchMethodError: 'javax.xml.stream.XMLInputFactory org.apache.jena.util.JenaXMLInput.initXMLInputFactory(javax.xml.stream.XMLInputFactory)'")
    @Test
    public void in_jena_create_of_small_inmemory_tdb2_datasets_for_test () {
        // Assertions.assertDoesNotThrow( () -> {
            var ignored = IM4Jena.triple3();
            ignored = IM4Jena.triple6();
            ignored = IM4Jena.triple9();
            //ignored = IM4Jena.graph3();
            ignored = IM4Jena.triples9PlusLiterals();
            // ignored = IM4Jena.stars();
        //});
    }

}
