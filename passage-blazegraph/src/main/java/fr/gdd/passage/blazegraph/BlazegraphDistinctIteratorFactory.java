package fr.gdd.passage.blazegraph;

import com.bigdata.bop.IPredicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.IKeyOrder;
import fr.gdd.passage.commons.interfaces.BackendIterator;

import java.util.Objects;
import java.util.Set;

/**
 * A factory that will choose the proper scan iterator for distinct values, depending
 * on available indexes.
 */
public class BlazegraphDistinctIteratorFactory {

    public static BackendIterator<IV, BigdataValue, Long> get (AbstractTripleStore store,
                                                               IV s, IV p, IV o, IV c,
                                                               Set<Integer> codes) {
        IV[] ivs = new IV[] {s, p, o, c};
        // #1 we look for the proper index to use first
        for (int i = 0; i < ivs.length; ++i) {
            if (codes.contains((Integer) i)) {
                if (Objects.nonNull(ivs[i])) {
                    throw new RuntimeException();
                }
                ivs[i] = new TermId<>(VTE.URI, -1); // fake IV to fake bind the variable
            }
        }

        IPredicate<ISPO> fakePredicate = store.getSPORelation().getPredicate(ivs[0], ivs[1], ivs[2], ivs[3]);
        IPredicate<ISPO> predicate = store.getSPORelation().getPredicate(s, p, o, c, null, null);
        IKeyOrder<ISPO> fakeKeyOrder = store.getSPORelation().getKeyOrder(fakePredicate);
        IKeyOrder<ISPO> keyOrder = store.getSPORelation().getKeyOrder(predicate);

        if (!fakeKeyOrder.getIndexName().equals(keyOrder.getIndexName())) { // fully unbounded
            // this iterator is less efficient, but it preserves the order and
            // enjoys a few optimizations + it allows skipping elements, useful
            // to pause/resume the query execution.
            if (codes.size() > 1) throw new RuntimeException("Too many codes for this kind of distinct iterator.");
            return new BlazegraphDistinctIteratorDXV(store, s, p, o, c, codes);
        }
        return new BlazegraphDistinctIteratorXDV(store, s, p, o, c, codes);
    }
}
