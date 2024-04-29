package fr.gdd.sage.blazegraph;

import com.bigdata.btree.*;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;

public class BlazegraphIterator implements BackendIterator<IV, byte[]>{
    private ITupleIterator<?> tupleIterator;
    private byte[] currentKey = null;
    private byte[] previousKey = null;
    private byte[] min;
    private byte[] max;
    private ITuple<?> currentValue;
    BlazegraphBackend blazegraphBackend;
    AbstractTripleStore store;
    IIndex iindex;

    public BlazegraphIterator(AbstractTripleStore store, BlazegraphBackend blazegraphBackend, IV s,IV p, IV o) {
        this.store = store;
        this.blazegraphBackend = blazegraphBackend;
        IAccessPath<ISPO> accessPath =  store.getAccessPath(s,p,o);
        this.iindex = accessPath.getIndex();
        IRangeQuery rangeQuery = iindex;
        AccessPath<ISPO> access = (AccessPath<ISPO>) accessPath;
        this.min = access.getFromKey();
        this.max = access.getToKey();
        this.tupleIterator = (ITupleIterator<?>) rangeQuery.rangeIterator(min, max);
    }

    public byte[] current() {
        return this.currentKey;
    }

    public IV getId(int type) {
        ISPO ispo = (ISPO) this.currentValue.getTupleSerializer().deserialize(this.currentValue);
        //TermId<?> p = (TermId<?>) ispo.p();
        switch (type) {
            case SPOC.SUBJECT:
                //return new TermId(VTE.URI,0);
                // return blazegraphBackend.getId(store.asStatement(ispo).getSubject().toString(), 1);
                return ispo.s();
            case SPOC.PREDICATE:
                //return new TermId(VTE.URI,0);
                //return blazegraphBackend.getId(store.asStatement(ispo).getPredicate().toString(), 2);
                return ispo.p();

            case SPOC.OBJECT:
                //return new TermId(VTE.LITERAL,0);
                //return blazegraphBackend.getId(store.asStatement(ispo).getObject().toString(), 3);
                return ispo.o();
            case SPOC.CONTEXT:
                //return new TermId(VTE.URI,0);
                //return blazegraphBackend.getId(store.asStatement(ispo).getContext().toString(), 0);
                return ispo.c();
        }
        return null;
    }
    public boolean hasNext() {
		/*if(this.tupleIterator.hasNext()) {
			return true;
		}*/
        //à tester
		/*if(currentValue == null) {
			return false;
		}*/
        return this.tupleIterator.hasNext();
    }

    public void next(){
        this.previousKey = this.currentKey;
        this.currentValue = tupleIterator.next();
        this.currentKey = currentValue.getKey();
    }

    public byte[] previous() {
        return this.previousKey;
    }

    //il faudra sauvegarder la première clé pour y retourner en cas de besoin
    public void reset() {
        previousKey = null;
        currentKey = null;
        tupleIterator = iindex.rangeIterator(min, null);
        currentValue = tupleIterator.next();

    }

    public void skip(byte[] to) {
        if (to == null) {
            return;
        }
        this.tupleIterator = iindex.rangeIterator(to, max);
        currentValue = this.tupleIterator.next();
    }

    public void skip(Integer to) { // TODO TEST IT
        this.tupleIterator = iindex.rangeIterator(min, max);
        if (to > 0) {
            byte[] keyAt = ((AbstractBTree) this.tupleIterator).keyAt(to-1);
            this.tupleIterator = iindex.rangeIterator(keyAt, max);
        }
        currentValue = this.tupleIterator.next();
    }

    public byte[] getCurrentKey() {
        return currentKey;
    }

    public ITuple<?> getCurrentValue() {
        return currentValue;
    }

    public ITupleIterator<?> getTupleIterator() {
        return tupleIterator;
    }

    public byte[] getMin() {
        return min;
    }

    public byte[] getMax() {
        return max;
    }

    public void setPreviousKey(byte[] previousKey) {
        this.previousKey = previousKey;
    }

}

