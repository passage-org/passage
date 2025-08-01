package fr.gdd.passage.cli.vocabularies;
/* CVS $Id: $ */
 
import org.apache.jena.rdf.model.*;
 
/**
 * Vocabulary definitions from ./engine-vocabulary.ttl 
 * @author Auto-generated by schemagen on 11 juin 2025 16:59 
 */
public class PassageVocabulary {
    /** <p>The RDF model that holds the vocabulary terms</p> */
    private static final Model M_MODEL = ModelFactory.createDefaultModel();
    
    /** <p>The namespace of the vocabulary as a string</p> */
    public static final String NS = "http://fr.gdd.passage/engine-vocabulary#";
    
    /** <p>The namespace of the vocabulary as a string</p>
     * @return namespace as String
     * @see #NS */
    public static String getURI() {return NS;}
    
    /** <p>The namespace of the vocabulary as a resource</p> */
    public static final Resource NAMESPACE = M_MODEL.createResource( NS );
    
    /** <p>Describes the features of the service.</p> */
    public static final Property description = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#description" );
    
    /** <p>Specifies the SPARQL query engines to use with the service.</p> */
    public static final Property engine = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#engine" );
    
    /** <p>Defines if the engine should try to optimize the join order or not.</p> */
    public static final Property force_order = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#force_order" );
    
    /** <p>The location of the file to read to create the dataset.</p> */
    public static final Property location = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#location" );
    
    /** <p>Maximum number of results returned by a query execution.</p> */
    public static final Property max_results = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#max_results" );
    
    /** <p>Maximum number of threads allocated to each SPARQL query execution.</p> */
    public static final Property parallel = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#parallel" );
    
    public static final Property support = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#support" );
    
    /** <p>Timeout on SPARQL query execution. Depending on the query engine, a specific 
     *  behavior is associated.</p>
     */
    public static final Property timeout = M_MODEL.createProperty( "http://fr.gdd.passage/engine-vocabulary#timeout" );
    
    /** <p>Class of configuration to define its own custom SPARQL engine operator per 
     *  operator.</p>
     */
    public static final Resource CustomEngine = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#CustomEngine" );
    
    /** <p>An instance of this class defines a dataset with a Blazegraph backend.</p> */
    public static final Resource DatasetBlazegraph = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#DatasetBlazegraph" );
    
    public static final Resource DatasetHDT = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#DatasetHDT" );
    
    public static final Resource Operation = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#Operation" );
    
    /** <p>Class of configuration for the PASSAGE engine.</p> */
    public static final Resource PassageEngine = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#PassageEngine" );
    
    /** <p>Class of configuration for the RAW engine.</p> */
    public static final Resource RawEngine = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#RawEngine" );
    
    /** <p>Abstract class of SPARQL engine configuration.</p> */
    public static final Resource SPARQLEngine = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#SPARQLEngine" );
    
    /** <p>SPARQL query operation for Fuseki that allows reading body's and header's 
     *  arguments.</p>
     */
    public static final Resource query_w_args = M_MODEL.createResource( "http://fr.gdd.passage/engine-vocabulary#query_w_args" );
    
}
