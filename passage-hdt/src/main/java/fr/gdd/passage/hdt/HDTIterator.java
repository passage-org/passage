package fr.gdd.passage.hdt;

import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Random;

public class HDTIterator  extends BackendIterator<Long, String, Long> {
    public static ThreadLocal<Random> RNG = ThreadLocal.withInitial(() -> {
        // Seed can be derived from a common seed or generated uniquely per thread
        long seed = System.nanoTime() + Thread.currentThread().threadId();
        return new Random(seed);
    });


    final HDTBackend backend;
    final TripleID start;

    IteratorTripleID iterator;
    TripleID current;
    long offset = 0L;

    public HDTIterator(HDTBackend backend, Long s, Long p, Long o) {
        this.backend = backend;
        this.start = new TripleID(s, p, o);
        this.iterator = this.backend.hdt.getTriples().search(start);
    }

    @Override
    public Long getId(int code) {
        return switch (code) {
            case SPOC.SUBJECT -> current.getSubject();
            case SPOC.PREDICATE -> current.getPredicate();
            case SPOC.OBJECT -> current.getObject();
            default -> throw new UndefinedCode(code);
        };
    }

    @Override
    public String getValue(int code) {
        return backend.getValue(this.getId(code), code);
    }

    @Override
    public String getString(int code) {
        return getValue(code);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void next() {
        current = iterator.next();
    }

    @Override
    public void reset() {
        this.iterator = this.backend.hdt.getTriples().search(this.start);
        this.offset = 0L;
        this.current = null;
    }

    @Override
    public void skip(Long to) {
        this.iterator.goTo(to);
        this.offset = to;
        this.current = null;
    }

    @Override
    public Long current() {
        return this.offset;
    }

    @Override
    public Long previous() {
        return this.offset - 1;
    }

    @Override
    public Double random() {
        this.current = null;
        long cardinality = this.iterator.estimatedNumResults();
        long rn = RNG.get().nextLong(cardinality);
        this.iterator.goTo(rn);
        return 1./cardinality;
    }

    @Override
    public double cardinality() throws UnsupportedOperationException {
        return this.iterator.estimatedNumResults();
    }

    @Override
    public double cardinality(long strength) throws UnsupportedOperationException {
        return this.iterator.estimatedNumResults();
    }
}
