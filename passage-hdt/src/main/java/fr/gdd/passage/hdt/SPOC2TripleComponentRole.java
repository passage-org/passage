package fr.gdd.passage.hdt;

import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.rdfhdt.hdt.enums.TripleComponentRole;

public class SPOC2TripleComponentRole {

    public static TripleComponentRole toTripleComponentRole (int spoc) {
        return switch (spoc) {
            case SPOC.SUBJECT -> TripleComponentRole.SUBJECT;
            case SPOC.PREDICATE -> TripleComponentRole.PREDICATE;
            case SPOC.OBJECT -> TripleComponentRole.OBJECT;
            default -> throw new UndefinedCode(spoc);
        };
    }

}
