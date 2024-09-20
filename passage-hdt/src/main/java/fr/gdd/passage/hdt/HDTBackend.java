package fr.gdd.passage.hdt;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import java.io.IOException;

public class HDTBackend implements Backend<Long, String, Long> {

    final HDT hdt;

    public HDTBackend(String path) throws IOException {
        this.hdt = HDTManager.loadHDT(path);
    }

    public HDTBackend(HDT hdt) {
        this.hdt = hdt;
    }

    @Override
    public BackendIterator<Long, String, Long> search(Long s, Long p, Long o) {
        return new HDTIterator(this, s, p, o);
    }

    @Override
    public BackendIterator<Long, String, Long> search(Long s, Long p, Long o, Long c) {
        throw new UnsupportedOperationException("HDT does not support quads.");
    }

    @Override
    public Long any() {
        return 0L;
    }

    @Override
    public String getValue(Long id, int... type) {
        return this.hdt.getDictionary().idToString(id, SPOC2TripleComponentRole.toTripleComponentRole(type[0])).toString();
    }

    @Override
    public String getString(Long id, int... type) {
        return this.getValue(id, type);
    }

    @Override
    public Long getId(String s, int... type) {
        long id = this.hdt.getDictionary().stringToId(s, SPOC2TripleComponentRole.toTripleComponentRole(type[0]));
        if (id <= 0) {
            throw new NotFoundException(s);
        } else {
            return id;
        }
    }

    @Override
    public String getValue(String value, int... type) {
        return value;
    }
}
