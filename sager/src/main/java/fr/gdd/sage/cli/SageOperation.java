package fr.gdd.sage.cli;

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
 * but a Sage endpoint. A client can adapt its behavior based on this difference.
 */
public class SageOperation {

    private static Model model = ModelFactory.createDefaultModel();
    public static final Resource opSage = resource("sage");

    public static final Operation Sage = Operation.alloc(opSage.asNode(),"sage","Sage SPARQL Query");

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
