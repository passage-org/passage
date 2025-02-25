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

/**
 * Creates our own vocabulary and operation. So the endpoint would not be a SPARQL endpoint,
 * but a Passage endpoint. A client can adapt its behavior based on this difference.
 */
@Deprecated // to remove with PassageServerCLI
public class PassageOperation {

    private static Model model = ModelFactory.createDefaultModel();
    public static final Resource opPassage = resource("passage");

    public static final Operation Passage = Operation.alloc(opPassage.asNode(), "passage", "Passage SPARQL Query");

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
