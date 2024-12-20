package fr.gdd.passage.hdt;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.expr.NodeValue;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import java.io.IOException;
import java.util.Objects;

public class HDTBackend implements Backend<Long, String> {

    final HDT hdt;

    public HDTBackend(String path) throws IOException {
        this.hdt = HDTManager.loadHDT(path);
        HDTManager.indexedHDT(this.hdt, null);
    }

    public HDTBackend(HDT hdt) {
        this.hdt = hdt;
    }

    @Override
    public BackendIterator<Long, String> search(Long s, Long p, Long o) {
        return new HDTIterator(this,
                Objects.isNull(s) ? any() : s,
                Objects.isNull(p) ? any() : p,
                Objects.isNull(o) ? any() : o);
    }

    @Override
    public BackendIterator<Long, String> search(Long s, Long p, Long o, Long c) {
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
            try {
                NodeValue nv = NodeValue.parse(s);
                id = this.hdt.getDictionary().stringToId(nv.asString(), SPOC2TripleComponentRole.toTripleComponentRole(type[0]));
                if (id <= 0) {
                    throw new NotFoundException(s);
                }
            } catch (RiotException re) {
                throw new NotFoundException(s);
            }
        }
        return id;
    }

    @Override
    public String getValue(String value, int... type) {
        return value;
    }

    @Override
    public void close() throws Exception {
        this.hdt.close();
    }
}
