package fr.gdd.passage.blazegraph;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IKeyOrder;
import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Explore the space of distinct values.
 * Careful: this might be inefficient in cases such as:
 * `SELECT distinct(?s) WHERE {?s :bounded_predicate ?o}`
 * Indeed, to ensure order over subjects ?s, the chosen index will be SPO.
 * but the range of keys cannot be set properly since S is a variable, therefore
 * we must explore the whole SPO index to get the distinct S.
 * *
 * Default triple store allows 3 indexes: SPO, POS, OSP. Therefore, this should be
 * used when:
 * distinct ?s , P, ?o
 * distinct ?p , O, ?s
 * distinct ?o , S, ?p
 */
public class BlazegraphDistinctIteratorDXV extends BackendIterator<IV, BigdataValue> {

    final AbstractTripleStore store;
    // final IIndex iindex;
    final Set<Integer> codes;
    final IV[] pattern;

    IChunkedIterator<IV> distincts;
    Boolean consumed = true;
    IV current; // current distinct value
    IV intern; // current distinct value but not necessarily matching pattern

    final IKeyOrder<ISPO> fakeKeyOrder;

    /**
     *
     * @param store The dataset.
     * @param s The subject to look for, or null if a variable.
     * @param p The predicate to look for, or null if a variable.
     * @param o The object to look for, or null if a variable.
     * @param c The context (or graph) to look for, or null if a variable.
     * @param codes The SPOC codes of distinct variables. If none, it's like a normal scan iterator.
     */
    public BlazegraphDistinctIteratorDXV(AbstractTripleStore store, IV s, IV p, IV o, IV c, Set<Integer> codes) {
        // TODO when distinct = unbounded variables, then it should be a normal iterator, no artificial bound in
        //      the pattern to force the key order, otherwise, it might be inefficient for nothing.
        this.codes = codes;
        this.store = store;
        this.pattern = new IV[] {s, p, o, c};

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
        this.fakeKeyOrder = store.getSPORelation().getKeyOrder(fakePredicate);

        this.distincts = store.getSPORelation().distinctTermScan(fakeKeyOrder);
    }

    /* ********************************************************************* */

    @Override
    public IV getId(int code) {
        if (!codes.contains(code)) {
            throw new UndefinedCode(code);
        }
        return this.current;
    }

    @Override
    public BigdataValue getValue(int code) {
        return this.getId(code).getValue();
    }

    @Override
    public String getString(int code) {
        IV iv = this.getId(code);
        String lexiconized = store.getLexiconRelation().getTerm(iv).toString();
        if (iv.isURI()) {
            return "<"+lexiconized+">";
        }
        return lexiconized;
    }

    @Override
    public boolean hasNext() {
        if (!consumed && Objects.nonNull(current)) { return true; }
        if (!distincts.hasNext()) return false;

        boolean found = false;
        while (!found && distincts.hasNext()) {
            intern = distincts.next();
            IV[] newPattern = createPatternFromDistinct(intern);

            IPredicate<ISPO> predicate = store.getSPORelation().getPredicate(
                    newPattern[0], newPattern[1], newPattern[2], newPattern[3], null, null);

            byte[] fromKey = fakeKeyOrder.getFromKey(new KeyBuilder(), predicate);
            byte[] toKey = fakeKeyOrder.getToKey(new KeyBuilder(), predicate);
            // found = store.getSPORelation().getIndex(fakeKeyOrder).rangeCount(fromKey,toKey) > 0;
            found = store.getSPORelation().getIndex(fakeKeyOrder).rangeIterator(fromKey, toKey).hasNext();
            if (found) {
                consumed = false;
                current = intern;
            }
        }

        return found;
    }

    private IV[] createPatternFromDistinct(IV distinct) {
        IV[] newPattern = Arrays.copyOf(pattern, 4);
        for (int i = 0; i < newPattern.length; ++i) {
            if (codes.contains(i)) {
                newPattern[i] = distinct;
            }
        }
        return newPattern;
    }

    @Override
    public void next() {
        // TODO remove an inconsistency when hasNext has been called, then getId is called, then
        //      next is called: It should return the previous id when getId was called, while for
        //      now, it returns the current id.
        consumed = true;
        // the rest is already set in `hasNext()`
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param internalTo The cursor location to skip to. This is *not* like
     *                   the offset of scans since it does not skip results. Instead,
     *                   it is the offset of the internal scan iterator.
     */
    @Override
    public void skip(long internalTo) {
        if (internalTo <= 0) { return; } // do nothing

        try {
            byte[] keyAt = ((AbstractBTree) store.getSPORelation().getIndex(fakeKeyOrder))
                    .keyAt(internalTo);

            this.distincts = store.getSPORelation().distinctTermScan(fakeKeyOrder, keyAt, null, null);
        } catch (IndexOutOfBoundsException e) {
            this.distincts = new EmptyChunkedIterator<>(null);
        }
    }

    @Override
    public long current() {
        if (Objects.isNull(current)) return 0L;

        IPredicate<ISPO> predicate = store.getSPORelation().getPredicate(
                codes.contains(SPOC.SUBJECT) ? current : null,
                codes.contains(SPOC.PREDICATE) ? current : null,
                codes.contains(SPOC.OBJECT) ? current : null,
                codes.contains(SPOC.CONTEXT) ? current : null);

        byte[] toKey = fakeKeyOrder.getToKey(new KeyBuilder(), predicate);

        // prefixed by "-" because indexOf return negative number when not found, and since we
        // build fromKey, they are by design not found
        return -((AbstractBTree) store.getSPORelation().getIndex(fakeKeyOrder)).indexOf(toKey)-1;
    }

    @Override
    public long previous() {
        // could be supported by keeping it in memory,
        // but flemme for now…
        throw new UnsupportedOperationException();
    }

    @Override
    public Double random() {
        // random was meant to be with replacement, don't know how
        // costly it would be to do it without replacement.
        throw new UnsupportedOperationException();
    }
}
