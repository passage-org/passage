import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;

public class Unknown {
    public static void main(String[] args) throws Exception {
        QLeverBackend b = new QLeverBackend(args[0]);
        long any = QLeverNative.any();
        long absent = b.getId("<http://ex.org/no-such-thing>");
        System.out.printf("any()              = %d%n", any);
        System.out.printf("getId(absent IRI)  = %d%n", absent);
        System.out.printf("equal ?              %s%n%n", absent == any);
        BackendIterator<Long, String> it = b.search(any, absent, any);
        System.out.printf("scan ?s <absent> ?o  cardinality = %d%n", (long) it.cardinality());
        long n = 0;
        while (it.hasNext()) { it.next(); n++; }
        System.out.printf("                     solutions   = %d%n", n);
        System.out.println(n == 0 ? "correct: empty" : "WRONG: an unknown IRI matched everything");
        b.close();
        Runtime.getRuntime().halt(0);
    }
}
