package fr.gdd.sage.blazegraph;

import com.bigdata.btree.*;
import com.bigdata.btree.filter.EmptyTupleIterator;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.util.BytesUtil;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;

import java.util.Objects;
import java.util.Random;
import java.util.random.RandomGenerator;

public class BlazegraphIterator extends BackendIterator<IV, BigdataValue, Long> {
    public static RandomGenerator RNG = new Random(12);

    private ITupleIterator<?> tupleIterator;
    private final byte[] min;
    private final byte[] max;
    private ITuple<?> currentValue;
    AbstractTripleStore store;
    IAccessPath<ISPO> accessPath;
    IIndex iindex;
    ISPO currentISPO = null;
    Long offset = 0L;

    public BlazegraphIterator(AbstractTripleStore store, IV s, IV p, IV o, IV c) {
        this.store = store;
        this.accessPath =  store.getAccessPath(s, p, o, c);
        this.iindex = accessPath.getIndex();
        IRangeQuery rangeQuery = iindex;
        AccessPath<ISPO> access = (AccessPath<ISPO>) accessPath;
        this.min = access.getFromKey();
        this.max = access.getToKey();
        this.tupleIterator = (ITupleIterator<?>) rangeQuery.rangeIterator(min, max);
    }

    public Long current() {
        return this.offset;
    }

    public IV getId(int type) {
        if (Objects.isNull(currentISPO)) {
            this.currentISPO = (ISPO) this.currentValue.getTupleSerializer().deserialize(this.currentValue);
        }
        return switch (type) {
            case SPOC.SUBJECT -> currentISPO.s();
            case SPOC.PREDICATE -> currentISPO.p();
            case SPOC.OBJECT -> currentISPO.o();
            case SPOC.CONTEXT -> currentISPO.c();
            default -> throw new UnsupportedOperationException("Unexpected type in getId.");
        };
    }

    @Override
    public BigdataValue getValue(int code) {
        return getId(code).getValue();
    }

    public String getString(int type) {
        IV iv = this.getId(type);
        return store.getLexiconRelation().getTerm(iv).toString();
    }

    public boolean hasNext() {
        return this.tupleIterator.hasNext();
    }

    public void next(){
        this.currentValue = tupleIterator.next();
        this.currentISPO = null; // lazily processed
    }

    @Override
    public Long previous() {
        return this.offset-1;
    }

    public void reset() {
        this.offset = 0L;
        tupleIterator = iindex.rangeIterator(min, max);
        currentValue = null;
    }

    @Override
    public void skip(Long to) {
        this.offset = to;
        long startFrom = Objects.isNull(min) ? 0L: iindex.rangeCount(null, min);
        if (to > 0) {
            byte[] keyAt = ((AbstractBTree) iindex).keyAt(startFrom + to);
            if (BytesUtil.compareBytes(keyAt, max) < 0) {
                this.tupleIterator = iindex.rangeIterator(keyAt, max);
            } else {
                this.tupleIterator = EmptyTupleIterator.INSTANCE;
            }
        }
    }

    @Override
    public double cardinality() {
        return accessPath.rangeCount(false);
    }

    public ISPO getUniformRandomSPO() {
        long rn = RNG.nextLong((long) cardinality());
        long startFrom = Objects.isNull(min) ? 0L: iindex.rangeCount(null, min);
        byte[] keyAt = ((AbstractBTree) iindex).keyAt(startFrom + rn ); // TODO double check boundaries
        ITupleIterator<?> iterator = iindex.rangeIterator(keyAt, max);

        ITuple<?> val = iterator.next();
        return (ISPO) val.getTupleSerializer().deserialize(val);
    }

    @Override
    public Double random() {
        this.offset = 0L;
        currentValue = null;
        long rn = RNG.nextLong((long) cardinality());
        long startFrom = Objects.isNull(min) ? 0L: iindex.rangeCount(null, min);
        byte[] keyAt = ((AbstractBTree) iindex).keyAt(startFrom + rn ); // TODO double check boundaries
        tupleIterator = iindex.rangeIterator(keyAt, max);
        return 1./cardinality();
    }

}

