package fr.gdd.passage.blazegraph;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.EmptyTupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.BytesUtil;
import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;

import java.util.Objects;
import java.util.Set;

/**
 * Explore the space of distinct values. It is designed to be efficient when the
 * indexes match the key.
 * *
 * The default available indexes of the triple store are SPO, POS, and OSP.
 * So this iterator works well for:
 * distinct ?p, S, ?o
 * distinct ?o, P, ?s
 * distinct ?s, O, ?p
 */
public class BlazegraphDistinctIteratorXDV extends BackendIterator<IV, BigdataValue> {

    final AbstractTripleStore store;
    final IAccessPath<ISPO> accessPath;
    final IIndex iindex;
    private final byte[] min;
    private final byte[] max;
    private final Set<Integer> codes;
    private final IV[] pattern;

    ITupleIterator<?> tupleIterator;
    ITuple<?> currentValue;
    ISPO currentISPO = null;
    Boolean consumed = true;

    /**
     *
     * @param store The dataset.
     * @param s The subject to look for, or null if a variable.
     * @param p The predicate to look for, or null if a variable.
     * @param o The object to look for, or null if a variable.
     * @param c The context (or graph) to look for, or null if a variable.
     * @param codes The SPOC codes of distinct variables. If none, it's like a normal scan iterator.
     */
    public BlazegraphDistinctIteratorXDV(AbstractTripleStore store, IV s, IV p, IV o, IV c, Set<Integer> codes) {
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
        IPredicate<ISPO> predicate = store.getSPORelation().getPredicate(s, p, o, c, null, null);
        IKeyOrder<ISPO> fakeKeyOrder = store.getSPORelation().getKeyOrder(fakePredicate);
        this.accessPath = store.getSPORelation().getAccessPath(fakeKeyOrder, predicate);
        this.iindex = accessPath.getIndex();

        AccessPath<ISPO> access = (AccessPath<ISPO>) accessPath;

        this.min = access.getFromKey();
        this.max = access.getToKey();
        this.tupleIterator = (ITupleIterator<?>) iindex.rangeIterator(min, max);
    }

    /* ********************************************************************* */

    @Override
    public IV getId(int code) {
        if (Objects.isNull(currentISPO)) {
            // TODO use UtilityIV.decode(key, 0, numTerms)
            this.currentISPO = (ISPO) this.currentValue.getTupleSerializer().deserialize(this.currentValue);
        }
        if (!codes.contains(code)) {
            throw new UndefinedCode(code);
        }
        return switch (code) {
            case SPOC.SUBJECT -> currentISPO.s();
            case SPOC.PREDICATE -> currentISPO.p();
            case SPOC.OBJECT -> currentISPO.o();
            case SPOC.CONTEXT -> currentISPO.c();
            default -> throw new UndefinedCode(code);
        };
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
        // TODO should be able to throw an exception when the deadline is over, because looking for a next
        //      might be long. Even using cardinality, it remains unbounded on some indexes.
        if (!consumed && Objects.nonNull(currentValue)) return true;

        try {
            while (this.tupleIterator.hasNext() && (consumed || Objects.isNull(this.currentValue))) {
                ITuple<?> toCheck = this.tupleIterator.next();
                ISPO toCheckSPO = (ISPO) toCheck.getTupleSerializer().deserialize(toCheck);

                // We check if the tuple iterator returns something that matches the triple/quad pattern
                boolean matchPattern = true;
                int i = 0;
                while (matchPattern && i < pattern.length) {
                    if (Objects.nonNull(pattern[i])) { // variable always match
                        matchPattern = switch (i) {
                            case SPOC.SUBJECT -> toCheckSPO.s().equals(pattern[i]);
                            case SPOC.PREDICATE -> toCheckSPO.p().equals(pattern[i]);
                            case SPOC.OBJECT -> toCheckSPO.o().equals(pattern[i]);
                            case SPOC.CONTEXT -> toCheckSPO.c().equals(pattern[i]);
                            default -> throw new UndefinedCode(i);
                        };
                    };
                    ++i;
                }
                if (!matchPattern) continue;

                if (Objects.isNull(this.currentValue)) { // no previous values so we accept it by default
                    this.currentValue = toCheck;
                    this.currentISPO = toCheckSPO;
                    consumed = false;
                } else {
                    if (Objects.isNull(currentISPO)) {
                        this.currentISPO = (ISPO) this.currentValue.getTupleSerializer().deserialize(this.currentValue);
                    }
                    // We check if the tuple is different from the one we already provided
                    boolean same = codes.stream().allMatch(code -> switch (code) {
                        case SPOC.SUBJECT -> toCheckSPO.s().equals(currentISPO.s());
                        case SPOC.PREDICATE -> toCheckSPO.p().equals(currentISPO.p());
                        case SPOC.OBJECT -> toCheckSPO.o().equals(currentISPO.o());
                        case SPOC.CONTEXT ->  toCheckSPO.c().equals(currentISPO.c());
                        default -> throw new UndefinedCode(code);
                    });
                    if (!same) {
                        this.currentISPO = toCheckSPO;
                        this.currentValue = toCheck;
                        consumed = false;
                    }
                }
            }
            return !consumed;
        } catch (Exception e ) {
            return false;
        }
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
        tupleIterator = iindex.rangeIterator(min, max);
        currentValue = null;
        consumed = true;
    }

    /**
     * @param internalTo The cursor location to skip to. This is *not* like
     *                   the offset of scans since it does not skip results. Instead,
     *                   it is the offset of the internal scan iterator.
     */
    @Override
    public void skip(long internalTo) {
        consumed = true;
        if (internalTo == 0) { // Nothing to do
            return;
        }
        long startFrom = Objects.isNull(min) ? 0L: iindex.rangeCount(null, min);
        if (internalTo > 0) {
            try {
                byte[] keyAt = ((AbstractBTree) iindex).keyAt(startFrom + internalTo);
                if (Objects.isNull(max) || BytesUtil.compareBytes(keyAt, max) < 0) {
                    this.tupleIterator = iindex.rangeIterator(keyAt, max);
                } else {
                    this.tupleIterator = EmptyTupleIterator.INSTANCE;
                }
            } catch (IndexOutOfBoundsException oob) {
                this.tupleIterator = EmptyTupleIterator.INSTANCE;
            }
        }
    }

    @Override
    public long current() {
        if (Objects.isNull(currentValue)) return 0L;
        long offset = iindex.rangeCount(min, currentValue.getKey());
        return consumed ? offset : offset - 1;
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
