package fr.gdd.passage.commons.utils;

import java.util.Arrays;
import java.util.List;

public class InMemoryStatements {

    public static final List<String> triples3 = Arrays.asList(
            "<http://person> <http://named> <http://Alice>.",
            "<http://person> <http://named> <http://Bob>.",
            "<http://Alice>  <http://owns>  <http://cat>."
    );

    public static final List<String> triples6 = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> .",
            "<http://Bob>   <http://address> <http://paris>  .",
            "<http://Carol> <http://address> <http://nantes> .",
            "<http://Alice> <http://own>     <http://cat> .",
            "<http://Alice> <http://own>     <http://dog> .",
            "<http://Alice> <http://own>     <http://snake> ."
    );

    public static final List<String> triples9 = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> .",
            "<http://Bob>   <http://address> <http://paris>  .",
            "<http://Carol> <http://address> <http://nantes> .",

            "<http://Alice> <http://own>     <http://cat> .",
            "<http://Alice> <http://own>     <http://dog> .",
            "<http://Alice> <http://own>     <http://snake> .",

            "<http://cat>   <http://species> <http://feline> .",
            "<http://dog>   <http://species> <http://canine> .",
            "<http://snake> <http://species> <http://reptile> ."
    );

    public static final List<String> triples9PlusLiterals = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> .",
            "<http://Bob>   <http://address> <http://paris>  .",
            "<http://Carol> <http://address> <http://nantes> .",

            "<http://Alice> <http://own>     <http://cat> .",
            "<http://Alice> <http://own>     <http://dog> .",
            "<http://Alice> <http://own>     <http://snake> .",

            "<http://cat>   <http://species> <http://feline> .",
            "<http://dog>   <http://species> <http://canine> .",
            "<http://snake> <http://species> <http://reptile> .",

            "<http://cat>   <http://labeled>    \"cat\"@en .",
            "<http://cat>   <http://endangered> \"false\"^^<http://www.w3.org/2001/XMLSchema#boolean> .",
            "<http://cat>   <http://letters>    \"3\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            "<http://snake> <http://letters>    \"5\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
    );

    public static final List<String> triples9PlusLiteralsPlusStars = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> .",
            "<http://Bob>   <http://address> <http://paris>  .",
            "<http://Carol> <http://address> <http://nantes> .",

            "<http://Alice> <http://own>     <http://cat> .",
            "<http://Alice> <http://own>     << <http://dog>  <http://species> <http://canine> >> .",
            "<http://Alice> <http://own>     <http://snake> .",

            "<http://cat>   <http://species> <http://feline> .",
            "<http://snake> <http://species> <http://reptile> .",

            "<< <http://cat> <http://endangered> \"false\"^^<http://www.w3.org/2001/XMLSchema#boolean> >> " +
                    "  <http://labeled> " +
                    "  << \"cat\"@en <http://letters> \"3\"^^<http://www.w3.org/2001/XMLSchema#integer> >> .",
            "<http://dog>        <http://letters>    \"3\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
    );



    /* ********************************** GRAPHS ************************************** */
    // With NQuads, the graph is the fourth parameter.

    // Alice knows she has animals
    public static final List<String> graphA = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> <http://Alice>.",

            "<http://Alice> <http://own>     <http://cat>    <http://Alice>.",
            "<http://Alice> <http://own>     <http://dog>    <http://Alice>.",
            "<http://Alice> <http://own>     <http://snake>  <http://Alice>."
    );

    // Bob maintains a dictionary of species
    public static final List<String> graphB = Arrays.asList(
            "<http://Bob>   <http://address> <http://paris>   <http://Bob> .",

            "<http://cat>   <http://species> <http://feline>  <http://Bob> .",
            "<http://dog>   <http://species> <http://canine>  <http://Bob> .",
            "<http://snake> <http://species> <http://reptile> <http://Bob> ."
    );

    // Carol knows the location of everyone.
    public static final List<String> graphC = Arrays.asList(
            "<http://Alice> <http://address> <http://nantes> <http://Carol> .",
            "<http://Bob>   <http://address> <http://paris>  <http://Carol> .",
            "<http://Carol> <http://address> <http://nantes> <http://Carol> ."
    );

    /* *************************** EXTRACTED FROM WATDIV ****************************** */

    // Very first dataset for testing, not very useful now since barely understandable.
    public static final List<String> cities10 = Arrays.asList(
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City103> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country3> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City104> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City105> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country0> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City106> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country10> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City107> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country23> .",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City108> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>."
    );

    public static final List<String> cities3 = Arrays.asList(
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> ."
    );

    public static final List<String> city1 = List.of(
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> ."
    );

}
