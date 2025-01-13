package fr.gdd.passage.blazegraph;

import com.bigdata.bop.IPredicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.IKeyOrder;
import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;

import java.util.Objects;
import java.util.Set;

/**
 * A factory that will choose the proper scan iterator for distinct values, depending
 * on available indexes.
 */
public class BlazegraphDistinctIteratorFactory {

    // useful to force indexes to prioritize distinct variables.
    public static final TermId<BigdataURI> FAKE_BIND = new TermId<>(VTE.URI, -1);

    public static BackendIterator<IV, BigdataValue> get (AbstractTripleStore store,
                                                         IV s, IV p, IV o, IV c,
                                                         Set<Integer> codes) {
        IV[] ivs = new IV[] {s, p, o, c};
        // #1 we look for the proper index to use first
        for (int i = 0; i < ivs.length; ++i) {
            if (codes.contains((Integer) i)) {
                if (Objects.nonNull(ivs[i])) {
                    throw new RuntimeException();
                }
                ivs[i] = FAKE_BIND;
            }
        }

        if (isFullySet(ivs)) {
            // when all the unbounded variables are actually the one
            // that need to be distinct, then the basic iterator works well.
            return new BlazegraphIterator(store, s, p, o, c);
        }

        IPredicate<ISPO> fakePredicate = store.getSPORelation().getPredicate(ivs[0], ivs[1], ivs[2], ivs[3]);
        IPredicate<ISPO> predicate = store.getSPORelation().getPredicate(s, p, o, c, null, null);
        IKeyOrder<ISPO> fakeKeyOrder = store.getSPORelation().getKeyOrder(fakePredicate);
        IKeyOrder<ISPO> keyOrder = store.getSPORelation().getKeyOrder(predicate);

        if (!fakeKeyOrder.getIndexName().equals(keyOrder.getIndexName())) { // fully unbounded
            // this iterator is less efficient, but it preserves the order and
            // enjoys a few optimizations + it allows skipping elements, useful
            // to pause/resume the query execution.
            // if (!isLeftMostVariable(codes, ivs, fakeKeyOrder.getIndexName())) throw new RuntimeException("Chosen keys do not allow distinct.");
            // TODO this because it uses a thing that return only one IV at a time.
            if (Objects.isNull(c) && codes.contains((Integer) SPOC.CONTEXT)) {
                codes.remove(SPOC.CONTEXT);
            }
            // if (codes.size() > 1) throw new RuntimeException("Too many codes for this kind of distinct iterator.");
            return new BlazegraphDistinctIteratorDXV(store, s, p, o, c, codes);
        }
        return new BlazegraphDistinctIteratorXDV(store, s, p, o, c, codes);
    }


    public static boolean isLeftMostVariable (Set<Integer> codes, IV[] ivs, String fakeKeyName) {
        for (Integer code : codes) {
            int indexOfCode = fakeKeyName.indexOf(code2Char(code));
            for (int i = 0; i < indexOfCode; ++i) {
                int codeOfChar = char2Code(fakeKeyName.charAt(i));
                if (Objects.isNull(ivs[codeOfChar])) {
                    return false;
                }
            }
        }
        return true;
    }

    public static char code2Char (Integer code) {
        return switch (code) {
            case SPOC.SUBJECT -> 'S';
            case SPOC.PREDICATE -> 'P';
            case SPOC.OBJECT -> 'O';
            case SPOC.CONTEXT -> 'C';
            default -> throw new UndefinedCode(code);
        };
    }

    public static Integer char2Code (char ch) {
        return switch (ch) {
            case 'S' -> SPOC.SUBJECT;
            case 'P' -> SPOC.PREDICATE;
            case 'O' -> SPOC.OBJECT;
            case 'C' -> SPOC.CONTEXT;
            default -> throw new UndefinedCode(-1);
        };
    }

    /**
     * @param ivs The pattern to check.
     * @return True if all variables are of interest for the distinct, or a constant. False
     *         otherwise.
     */
    private static boolean isFullySet (IV[] ivs) {
        for (IV iv : ivs) {
            if (Objects.isNull(iv)) {
                return false;
            }
        }
        return true;
    }


}
