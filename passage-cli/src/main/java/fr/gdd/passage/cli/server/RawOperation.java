package fr.gdd.passage.cli.server;

import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.server.FusekiVocab;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.irix.IRIException;
import org.apache.jena.irix.IRIx;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class RawOperation {
    private static Model model = ModelFactory.createDefaultModel();
    public static final Resource opRaw = resource("raw");

    public static final Operation Raw = Operation.alloc(opRaw.asNode(), "raw", "Raw SPARQL Query");

    /* ********************************************************************************* */

    private static Resource resource(String localname) { return model.createResource(iri(localname)); }
    private static Property property(String localname) { return model.createProperty(iri(localname)); }

    private static String iri(String localname) {
        String uri = FusekiVocab.NS + localname;
        try {
            IRIx iri = IRIx.create(uri);
            if ( ! iri.isReference() )
                throw new FusekiException("Bad IRI (relative): "+uri);
            return uri;
        } catch (IRIException ex) {
            throw new FusekiException("Bad IRI: "+uri);
        }
    }
}
